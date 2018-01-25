import csv
import sys
import time
from collections import Counter
from collections import defaultdict
from datetime import datetime


def get_questions(inputfile):
    """ Read questions and group by pa, city and topic (i.e. key) """
    with open(inputfile, 'r', encoding='utf-8', errors='ignore') as f:
        reader = csv.reader(f, delimiter=',')
        next(reader)
        questions = defaultdict(list)
        for row in reader:
            question_id, created_at, pa, city, root_topic, topic, question_text = row
            key = pa + '|' + city + '|' + topic
            questions[key].append(question_text)
    return questions


def get_words(questions, stopwords):
    """
    Combine questions per key, split into words and filter obsolete words.
    Note when combining questions 'xxx' is an alphanumeric delimiter later used
    for filtering to avoid counting ngrams between different quesitons
    """
    with open(stopwords, 'r') as f:
        lines = f.readlines()
        stop_words = [line.rstrip('\n') for line in lines]
    words = defaultdict(list)
    for key, text in questions.items():
        combine_ques = ' xxx '.join(text)
        split_words = combine_ques.lower().split()
        clean_words = [word for word in split_words
                       if word not in stop_words and
                       word.isalnum() and len(word) > 2]
        words[key] = clean_words
    return words


def group_words(words, ngram_size):
    """ Build unigram, bigram, trigram, etc groupings based on ngram size """
    return zip(*[words[i:] for i in range(ngram_size)])


def generate_ngrams(words, ngram_size):
    """ Build ngrams by key """
    ngrams = defaultdict(list)
    for key, word in words.items():
        data = group_words(word, ngram_size)
        ngrams[key] = data
    return ngrams


def get_counts(ngrams, top_n):
    """ Count ngrams by key """
    counts = defaultdict(list)
    for key, ngram in ngrams.items():
        # filter ngrams containing 'xxx'
        clean_ngram = [i for i in ngram if not 'xxx' in i]
        freq = Counter(clean_ngram).most_common(top_n)
        counts[key] = freq
    return counts


def write_results(outputfile, counts, n):
    """ Write ngram counts greater than n """
    with open(outputfile, 'w', encoding='utf-8', errors='ignore') as f:
        fieldnames = ['pa', 'city', 'topic', 'gram', 'frequency']
        writer = csv.DictWriter(f, delimiter=',', fieldnames=fieldnames)
        writer.writeheader()
        for key, results in counts.items():
            keys = key.split('|')
            pa = keys[0]
            city = keys[1]
            topic = keys[2]
            for result in results:
                gram = result[0]
                freq = result[1]
                if freq > n:
                    writer.writerow({'pa': pa, 'city': city, 'topic': topic, 'gram': gram, 'frequency': freq})


if __name__ == '__main__':
    # Assignments
    start_time = time.time()
    current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    outputfile = 'results_' + current_time + '.csv'

    # Function Calls
    questions = get_questions(inputfile='questions.csv')
    words = get_words(questions, stopwords='stopwords.txt')
    ngrams = generate_ngrams(words, ngram_size=int(sys.argv[1]))
    counts = get_counts(ngrams, top_n=int(sys.argv[2]))
    write_results(outputfile, counts, n=2)

    # Runtime
    end_time = time.time()
    print('Runtime: {} seconds'.format(round(end_time - start_time, 2)))
