"""
TODO:
consider list comprehension(s)
prevent bad data from being written to csv file
column headers
spark to generate reviews csv file
keywords and topics csv file convert to lowercase and cleanup
    add space before and after iep, isf, ead, irs, nsf
    remove \"" \ " '
"""

import os
import csv


def get_skills():
    """
    load dictionary from csv file containg keywords and topics from greenhouse
    """
    skills = {}
    with open("keywords_topics.csv", "r") as infile:
        rows = csv.reader(infile)
        for row in rows:
            skills[row[0]] = row[1]

    return skills


def classify_write_reviews(skills):
    """
    read csv file containing reviews data
    search reviews for keywords and classify matches with topic 
    :parm dictionary containing keywords and topics 
    :return: write output to csv file
    """
    if os.path.exists("skills_reviews.csv"):
        os.remove("skills_reviews.csv")

    for keyword, topic in skills.items():
        with open("reviews.csv", "r", encoding='utf-8', errors='ignore') as infile:
            reviews = infile.read().splitlines()
            #text = infile.read().lower().splitlines()
        with open("skills_reviews.csv", "a") as outfile:
            for review in reviews:
                # print(keyword)
                if keyword in review:
                    data = review + "," + topic + "," + keyword
                    #data = keyword + "," + value
                    #print(data)
                    outfile.write(data + "\n")


if __name__ == "__main__":
    #print(get_skills())
    skills = get_skills() 
    classify_write_reviews(skills)
