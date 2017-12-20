import csv
import json


def read_json(infile):
    """ read nested json file and fetch metadata """
    with open(infile, 'r') as f:
        data = json.load(f)
    responses = data['responses']
    rows = []
    for response in responses:
        metadata = response['metadata']
        rows.append((metadata['browser'],
                     metadata['platform'],
                     metadata['date_land'],
                     metadata['date_submit'],
                     metadata['user_agent'],
                     metadata['referer'],
                     metadata['network_id']))
    return rows


def write_csv(outfile, rows):
    """ write csv with metadata rows from json file """
    header = ['browser', 'platform', 'date_land', 'date_submit', 'user_agent', 'referer', 'network_id']
    with open(outfile, 'w') as f:
        csv_writer = csv.DictWriter(f, fieldnames=header)
        csv_writer.writeheader()
        for row in rows:
            csv_writer.writerow({'browser': row[0],
                                 'platform': row[1],
                                 'date_land': row[2],
                                 'date_submit': row[3],
                                 'user_agent': row[4],
                                 'referer': row[5],
                                 'network_id': row[6]})


if __name__ == '__main__':
    infile = 'input.json'
    outfile = 'output.csv'
    rows = read_json(infile)
    write_csv(outfile, rows)
