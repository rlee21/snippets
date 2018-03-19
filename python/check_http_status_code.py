#!/usr/bin/python

import csv
import requests
import time


def read_file():
    with open('profile_external_links.csv', 'r', encoding='utf-8', errors='ignore') as f:
        csv_reader = csv.reader(f)
        data = [row for row in csv_reader]
    return data


def get_http(data):
    results = []
    for row in data:
        professional_id = row[0]
        url = row[1]
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'}
        try:
            http_req = requests.get(url, headers=headers, verify=False, timeout=10)
        except Exception:
            status_code = 'Invalid'
        else:
            status_code = http_req.status_code
        finally:
            results.append((professional_id, status_code, url))

    return results


def write_results(results):
    with open('output.csv', 'w', encoding='utf-8', errors='ignore') as f:
        for result in results:
            professional_id = result[0]
            status_code = result[1]
            url = result[2]
            csv_writer = csv.writer(f)
            csv_writer.writerow((professional_id, status_code, url))


def main():
    start_time = time.time()
    data = read_file()
    print('Executing http requests...')
    results = get_http(data)
    print('Writing results...')
    write_results(results)
    print('Completed in: {0} seconds'.format(time.time()-start_time))


if __name__ == '__main__':
    main()





