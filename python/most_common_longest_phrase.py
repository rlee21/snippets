import sys
from textblob import TextBlob


document = 'document.txt'
with open(document, 'r') as doc:
    text = doc.read()

blob = TextBlob(text)
tags = blob.tags
np = blob.noun_phrases
n_grams = blob.ngrams(n=3)

print('blob:', blob)
print('\ntags:', tags)
print('\nnoun phrases:', np)
print('\nn-grams:', n_grams)








if __name__ == '__main__':
    document = 'document.txt'
