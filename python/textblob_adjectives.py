import csv
from textblob import TextBlob
from textblob.utils import strip_punc

with open('reviews_soft_skills.csv', 'r') as infile:
    reviews = infile.read()

cleansed_reviews = strip_punc(reviews, all=all)
blob = TextBlob(cleansed_reviews)
adjectives = [word for word, pos in blob.tags if pos == 'JJ']
uniq_adj = set(adjectives)
output = {adj: adjectives.count(adj) for adj in uniq_adj}
# print(output)
with open('results.csv', 'w') as outfile:
    writer = csv.writer(outfile)
    for k, v in output.items():
        writer.writerow([k, v])
