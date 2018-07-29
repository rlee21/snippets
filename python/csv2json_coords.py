import csv
import json


def read_csv():
    markers = []
    with open('../data/coords.csv') as infile:
        reader = csv.reader(infile)
        for row in reader:
            markers.append({
                'coords': {
                     'lat': float(row[1]),
                     'lng': float(row[2])
                },
                'content': '<h3>{city}</h3>'.format(city=row[0]).replace(',', ', ')
            })

    return markers

def write_json(markers):
    with open('../data/coords.json', 'w') as outfile:
        json.dump(markers, outfile, indent=2)

if __name__ == '__main__':
    markers = read_csv()
    write_json(markers)
