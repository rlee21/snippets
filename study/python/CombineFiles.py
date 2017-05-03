#!/usr/bin/python

import os


def main(path):
    """Combines contents of multiple Python files into one file called CombinedFile.py"""
    os.chdir(path)
    if os.path.exists("CombinedFile.py"):
        os.remove("CombinedFile.py")
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".py"):
                with open(os.path.join(root, file), "r") as infile:
                    data = infile.read()
                    for item in data:
                        with open("CombinedFile.py", "a") as outfile:
                            outfile.write(item)

if __name__ == "__main__":
    path = "/Users/relee/github/aqac"
    #path = "/Users/relee/PythonProjects/Ex_Files_Python3_Library/ExerciseFiles"
    main(path)
