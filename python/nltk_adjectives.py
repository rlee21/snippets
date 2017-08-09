from collections import defaultdict
import csv
import nltk


def get_stopwords(stopwordFile):
    with open(stopwordFile, 'r') as f:
        words = f.readlines()
        stopwords = [word.replace('\n', '') for word in words]

    return stopwords


def read_reviews(reviewsFile):
    with open(reviewsFile, 'r') as f:
        reviews = f.read()

    return reviews


def get_adjectives(reviews, stopwords):
    #regex_tokenize = nltk.tokenize.RegexpTokenizer('(?u)\W+|\$[\d\.]+|\S+')
    #words = regex_tokenize.tokenize(reviews)
    words = nltk.word_tokenize(reviews)
    tags = nltk.pos_tag(words)
    #stopwords = set(nltk.corpus.stopwords.words("english"))
    #ps = nltk.stem.PorterStemmer()
    adjectives = defaultdict(int) 
    for tag in tags:
        if tag[1] == 'JJ' and tag[0] not in stopwords:
            #adjectives[ps.stem(tag[0])] += 1
            adjectives[tag[0]] += 1

    return adjectives


def write_adjectives(resultsFile, adjectives):
    with open(resultsFile, 'w') as f:
        writer = csv.writer(f)
        for word, cnt in adjectives.items():
            #writer.writerow([word, cnt])
            if cnt > 1:
                writer.writerow([word, cnt])
    


if __name__ == '__main__':
    stopwordFile = 'stopwords.txt'
    stopwords = get_stopwords(stopwordFile)
    reviewsFile = 'reviews_soft_skills_nltk.csv'
    reviews = read_reviews(reviewsFile)
    adjectives = get_adjectives(reviews, stopwords)
    resultsFile = 'results_nltk.csv'
    write_adjectives(resultsFile, adjectives)

