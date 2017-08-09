import csv
import nltk


def main():
    """
    read reviews from csv
    find words before and after certain soft-skills in reviews
    :return: csv file
    """
    soft_skills = [
                   'great',
                   'good',
                   'excellent',
                   'easy',
                   'real',
                   'clear',
                   'amazing',
                   'wonderful',
                   'big',
                   'exceptional',
                   'fantastic',
                   'huge',
                   'safe',
                   'remarkable',
                   'extraordinary',
                   'impeccable',
                   'superb',
                   'terrific',
                   'fabulous',
                   'interpersonal'
                   ]

    with open('reviews_soft_skills.csv', 'r', encoding='utf-8', errors='ignore') as f:
        reviews = f.read()

    words = nltk.word_tokenize(reviews)
    bigrams = nltk.bigrams(words)
    freq = nltk.FreqDist(bigrams)

    with open('bigrams_output.csv', 'w') as f:
        csv_writer = csv.writer(f)
        for soft_skill in soft_skills:
            for bigram, cnt, in freq.items():
                if soft_skill in bigram:
                    csv_writer.writerow([soft_skill, bigram, cnt])


if __name__ == '__main__':
    main()
