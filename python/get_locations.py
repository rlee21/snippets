import csv


def parse_locations():
    """ Read file and return dict of parsed locations """
    locations = []
    with open('stations.txt') as lines:
        for line in lines:
            # line = line.partition('|')
            # states.append(line[0][-2:])
            state = line.split('|')[0][-2:]
            cities = line.split('|')[1].split(',')[0]
            # TODO: add station name
            locations.append((state, cities))
    return locations

def get_coords():
    """ Retrieve latitude and longitude for each city """
    pass


def write_locations(locations):
    """ Write parsed locations to CSV file """
    # TODO: add coords to locations
    with open('cities.csv', 'w') as outfile:
        writer = csv.writer(outfile)
        for location in locations:
            state = location[0]
            cities = location[1].split(',')[0].split('-')
            # stations = [city.strip() + ', ' + state for city in cities]
            stations = [(city.strip(), state) for city in cities]
            for station in stations:
                # print(station)
                writer.writerow((station[0], station[1]))


if __name__ == '__main__':
    locations = parse_locations()
    write_locations(locations)
