#!/usr/bin/python

import os
import fnmatch


def finder(pattern, path="/Users/relee/"):
    """
    finds local files based on name pattern
    :param pattern: file name pattern
    :return: flat file with results
    """
    results = []
    outfile = open("/Users/relee/Desktop/FileFinderResults.txt", "w+")
    for root, dirs, files in os.walk(path):
        for name in files:
            if fnmatch.fnmatch(name, pattern):
                results.append(os.path.join(root, name))
    for result in results:
        outfile.write(result + "\n")
    outfile.close()

if __name__ == "__main__":
    pattern = raw_input("Enter File Name Pattern: ")
    #pattern = input("Enter File Name Pattern: ")
    finder(pattern)
