import csv
from datetime import datetime
import json
import urllib
from urlparse import urlparse, parse_qs


def get_responses(url):
    """ http request to get typeform response data """
    request = urllib.urlopen(url).read()
    content = json.loads(request)
    responses = content['responses']
    data = []
    for response in responses:
        completed = response['completed']
        metadata = response['metadata']
        parsed_referer = urlparse(metadata['referer'])
        parms = parse_qs(parsed_referer.query)
        src_parm = parms.get('src', None)
        pid_parm = parms.get('pid', None)
        src = src_parm[0] if src_parm else src_parm
        pid = pid_parm[0] if pid_parm else pid_parm
        data.append((completed,
                     metadata['browser'],
                     metadata['platform'],
                     metadata['date_land'],
                     metadata['date_submit'],
                     metadata['user_agent'],
                     metadata['referer'],
                     metadata['network_id'],
                     src,
                     pid))

    return data


def write_csv(data):
    """ write csv file from typeform response data """
    header = ['completed',
              'browser',
              'platform',
              'date_land',
              'date_submit',
              'user_agent',
              'referer',
              'network_id',
              'os',
              'src',
              'pid']
    with open(outfile, 'w') as f:
        csv_writer = csv.DictWriter(f, fieldnames=header)
        csv_writer.writeheader()
        for row in data:
            iphone = row[5].lower().find('iphone')
            android = row[5].lower().find('android')
            mac = row[5].lower().find('mac os')
            windows = row[5].lower().find('windows nt')
            linux = row[5].lower().find('linux')
            chromebook = row[5].lower().find('cros')
            if row[2].lower() == 'robot':
                os = 'Robot'
            elif row[2].lower() == 'mobile' and iphone != -1:
                os = 'iOS'
            elif row[2].lower() == 'mobile' and android != -1:
                os = 'Android'
            elif row[2].lower() != 'mobile' and mac != -1:
                os = 'Mac OS'
            elif row[2].lower() != 'mobile' and windows != -1:
                os = 'Windows'
            elif row[2].lower() != 'mobile' and linux != -1:
                os = 'Linux'
            elif row[2].lower() != 'mobile' and chromebook != -1:
                os = 'Chromebook'
            else:
                os = 'Unknown'
            csv_writer.writerow({'completed': row[0],
                                 'browser': row[1],
                                 'platform': row[2],
                                 'date_land': row[3],
                                 'date_submit': row[4],
                                 'user_agent': row[5],
                                 'referer': row[6],
                                 'network_id': row[7],
                                 'os': os,
                                 'src': row[8],
                                 'pid': row[9]})



if __name__ == '__main__':
    current_date = datetime.now().strftime('%Y%m%d')
    outfile = 'typeform_as_of_' + current_date + '.csv'
    url = 'https://api.typeform.com/v1/form/bNKLG2?key=5e73c9ec7a381f4a2c4e362ff9f5ec8eacc46810&completed=false'
    data = get_responses(url)
    write_csv(data)
    print('**** Process complete, see {} for results. ****'.format(outfile))
