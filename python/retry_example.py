#!/usr/bin/python3.5

"""Script to pull data from getstat."""
import json
import os
import sys
import time
import zlib
import http.client
from urllib.parse import urlsplit
from hdfs.ext.kerberos import KerberosClient

# apparently, the current directory is not part of the python path, so let's add it for vault
sys.path.append(".")
import data_vault

request_url = 'https://avvo.getstat.com/api/v2/%s/bulk/ranks?date=%s&crawled_keywords_only=true&format=json'
list_url = 'https://avvo.getstat.com/api/v2/%s/bulk/list?format=json'
request_wait_time = 600  # 300  # sec == 5 min
request_inc_wait_time = 300  # 30  # sec
request_inc_wait_count = 48  # 24


class GetstatApiException(Exception):
    pass

class Retry(object):
    def __init__(self, tries=4, delay=10):
        self.tries = tries
        self.delay = delay

    def __call__(self, func):
        def _retry(*args, **kwargs):
            output_error = Exception
            while self.tries > 1:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    output_error = e
                    msg = "Error: {}, Retrying in {} seconds...\n".format(str(e), self.delay)
                    sys.stderr.write(msg)
                    time.sleep(self.delay)
                    self.tries -= 1
            raise output_error
        return _retry

@Retry()
def make_request(url, decompress=False, log_output=False):
    """Make HTTP request to url and return the json response as an object."""
    print('Making request to: %s' % url)
    parsed = urlsplit(url)

    conn = http.client.HTTPSConnection(parsed.netloc)
    conn.request("GET", '%s?%s' % (parsed.path, parsed.query))
    response = conn.getresponse()
    print('Response: %d %s' % (response.status, response.reason))

    if response.status != 200:
        message = ("Request failed due to a responce status '{}' sent back from "
            "getstat.com, we expect a responce status of '200' for a successful call").format(responce.status)
        raise GetstatApiException(message)

    output = response.read()

    if log_output:
        print('Request output: ')
        try:
            print(json.dumps(json.loads(output.decode('ascii')), indent=4, separators=(',', ': ')))
        except:
            print(output)

    if decompress:
        output = zlib.decompress(output, 16+zlib.MAX_WBITS)
    return json.loads(output.decode('ascii'))


def stringify(value):
    """Make things into ascii string."""
    if value is None:
        return ''
    else:
        return value


def process_keyword(keyword, site):
    """Extract fields from JSON object tree, and return as tab-delimited string."""
    l = []
    l.append(keyword['Ranking']['date'])
    l.append(site)
    l.append(keyword['Keyword'])
    l.append(keyword['KeywordMarket'])
    l.append(keyword['KeywordLocation'])
    l.append(keyword['KeywordDevice'])
    l.append(keyword['KeywordTags'])
    l.append(keyword['KeywordStats']['CPC'])
    l.append(keyword['Ranking']['Bing']['Url'])
    l.append(keyword['Ranking']['Bing']['Rank'])
    l.append(keyword['Ranking']['Google']['Url'])
    l.append(keyword['Ranking']['Google']['BaseRank'])
    l.append(keyword['Ranking']['Google']['Rank'])
    l.append('') # used to be keyword['Ranking']['Yahoo']['Url'] which stat has deprecated
    l.append('') # used to be keyword['Ranking']['Yahoo']['Rank'] which stat has deprecated
    l.append(keyword['KeywordStats']['GlobalSearchVolume'])
    l.append(keyword['KeywordStats']['TargetedSearchVolume'])
    return '\t'.join(map(stringify, l))


def make_header():
    """Create tab-delimited header field names."""
    l = []
    l.append('date')
    l.append('site')
    l.append('Keyword')
    l.append('KeywordMarket')
    l.append('KeywordLocation')
    l.append('KeywordDevice')
    l.append('KeywordTags')
    l.append('CPC')
    l.append('Bing_Url')
    l.append('Bing_Rank')
    l.append('Google_Url')
    l.append('Google_BaseRank')
    l.append('Google_Rank')
    l.append('Yahoo_Url')
    l.append('Yahoo_Rank')
    l.append('GlobalSearchVolume')
    l.append('RegionalSearchVolume')
    return '\t'.join(l)


def process_rank_site(site):
    """Iterate through all Keyword."""
    l = []
    for k in site['Keyword']:
        l.append(process_keyword(k, site['Url']))
    return '\n'.join(l)


def process_rank_data(all_data):
    """Iterate through all Site."""
    if all_data['Response']['Project']['TotalSites'] == '1':
        return process_rank_site(all_data['Response']['Project']['Site'])
    else:
        sites = []
        for site in all_data['Response']['Project']['Site']:
            sites.append(process_rank_site(site))
        return '\n'.join(sites)

if __name__ == '__main__':
    process_date = os.environ.get('process_date', None)  # '2015-12-29'
    if not process_date:
        raise GetstatApiException('Error: process_date not defined')

    output_hdfs_path = os.environ.get('output_hdfs_path', None)
    if not output_hdfs_path:
        raise GetstatApiException('Error: output_hdfs_path not defined')
    if output_hdfs_path[-1] != '/':
        output_hdfs_path = output_hdfs_path + '/'

    vault_client = data_vault.get_client(vault_appid=os.environ.get("VAULT_KEY_JOBRUNNER"))
    api_key = data_vault.read_key(vault_client, 'getstat_api_key')

    request_rank = make_request(request_url % (api_key, process_date), log_output=True)

    request_id = request_rank['Response']['Result']['Id']
    print('getstat Rank request made [id:' + request_id + '], waiting...')

    # bulk rank download request takes about 5 min to process. Need to wait here..
    wait_time = request_wait_time
    wait_count = request_inc_wait_count
    while wait_count > 0:
        time.sleep(wait_time)
        request_list = make_request(list_url % (api_key))
        g = (r['Url'] for r in request_list['Response']['Result']
             if r['Id'] == request_id and r['Status'] == 'Completed')
        download_url = next(g, None)
        if download_url is not None:
            break
        print('still not completed, waiting some more...')
        wait_time = request_inc_wait_time
        wait_count = wait_count - 1

    if download_url is None:
        raise GetstatApiException('getstat Rank request timeout')

    print('getstat downloading result...')
    rank_data = make_request(download_url, decompress=True)
    print('getstat transforming and writing result...')
    output_file_name = 'getstat_output_' + process_date + '.csv'
    with open(output_file_name, 'w') as output_file:
        output_file.write(make_header())
        output_file.write(process_rank_data(rank_data))

    line_count = 0
    with open(output_file_name) as fh:
        for line in fh:
            line_count += 1

    print('line count of output: %d' % (line_count))
    if line_count <= 1:
        raise GetstatApiException('empty output file')

    print('uploading output to HDFS...')
    hdfs_url = os.environ['hdfs_url']
    client = KerberosClient(hdfs_url)
    client.upload(output_hdfs_path, output_file_name, overwrite=True)

    print('Done')

