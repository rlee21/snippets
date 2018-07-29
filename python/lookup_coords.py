import csv
import os
import requests


def get_addresses(address_file):
    """ Read CSV file and return set of addresses """
    with open('../data/addresses.csv') as lines:
        addresses = {line.rstrip('\n') for line in lines}

    return addresses


def get_coords(addresses):
    """ Fetch coords from Google Maps Geocode API and return dict """
    coords = {}
    for address in addresses:
        url = 'https://maps.googleapis.com/maps/api/geocode/json?region=us&key={0}&address={1}'.format(API_KEY, address)
        req = requests.get(url)
        if req.status_code == 200:
            resp = req.json()
            lat_long = resp['results'][0]['geometry']['location']
            if address not in coords:
                coords[address] = lat_long
        else:
            if address not in coords:
                coords[address] = {'lat': 'unknown', 'lng': 'unknown'}

    return coords

def write_coords(coords):
    """ Write to CSV file """
    with open('coords.csv', 'w') as outfile:
        writer = csv.writer(outfile)
        for city, coord in coords.items():
            writer.writerow((city, coord['lat'], coord['lng']))


if __name__ == '__main__':
    API_KEY = os.environ['GOOGLE_MAPS_KEY']
    address_file = '../data/addresses.csv'
    addresses = get_addresses(address_file)
    coords = get_coords(addresses)
    write_coords(coords)
