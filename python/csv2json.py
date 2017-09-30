#!/usr/bin/python

import csv
import json


def read_csv(infile):
    """
    prepare content of csv file to be converted to json
    :param infile: filename path of csv file, must contain header row
    :return rows: list of ordered dictionaries
    """
    with open(infile, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)
    return rows


def write_json(rows, outfile):
    """
    convert rows from csv to json file
    :param rows: list of ordered dictionaries
    :param outfile: filename path of json file
    :return: write json file
    """
    with open(outfile, 'w') as json_file:
        return json.dump(rows, json_file)


if __name__ == '__main__':
    infile = 'TopLawSchoolsList.csv'
    outfile = 'TopLawSchoolsList.json'
    rows = read_csv(infile)
    write_json(rows, outfile)
