import csv
import re
import time
from datetime import datetime


def get_keywords(KEYWORDS_FILE):
    """ Read keywords and specialties from csv file and return dict tuples """
    keywords = {}
    with open(KEYWORDS_FILE, 'r') as f:
        reader = csv.reader(f, delimiter=',')
        next(reader)
        for row in reader:
            keyword, specialty_id, specialty_name = row
            if keyword not in keywords:
                keywords[keyword] = tuple((specialty_id, specialty_name))
    return keywords


def get_reviews(REVIEWS_FILE):
    """ Read reviews and return a list """
    reviews = []
    with open(REVIEWS_FILE, 'r', encoding='utf-8', errors='ignore') as f:
        reader = csv.reader(f, delimiter=',')
        next(reader)
        for row in reader:
            professional_id, review_id, city_id, city_name, state_id, state_code, review_date, overall_rating, body = row
            reviews.append((professional_id, review_id, city_id, city_name, state_id, state_code, review_date, overall_rating, body))

    return reviews


def tag_reviews(keywords, reviews):
    """
    Search for keyword in review body, if found bold keyword and
    only select 10 words before keyword and 15 words after keyword
    with a maximum of 200 total characters. Return list of dicts.
    """
    tagged = []
    processed = set()
    for review in reviews:
        professional_id, review_id, city_id, city_name, state_id, state_code, review_date, overall_rating, body = review
        for keyword, pa in keywords.items():
            specialty_id = pa[0]
            specialty_name = pa[1]
            match_obj = re.search(keyword, body, re.IGNORECASE)
            if match_obj:
                keyword_found = match_obj.group()
                body_kw_bold = re.sub(keyword_found, '<b>' + keyword_found + '</b>', body)
                body_kw_partitions = body_kw_bold.partition(keyword_found)
                words_before_kw = body_kw_partitions[0].split()
                before_start_idx = 0 if len(words_before_kw) - 10 < 0 else len(words_before_kw) - 10
                before_end_idx = len(words_before_kw)
                before_text = words_before_kw[before_start_idx:before_end_idx]
                words_after_kw = body_kw_partitions[2].split()
                after_start_idx = 0
                after_end_idx = None if 15 > len(words_after_kw) - 1 else 15
                after_text = words_after_kw[after_start_idx:after_end_idx]
                keyword_text = body_kw_partitions[1]
                body_trimmed = '...' + ' '.join(before_text) + keyword_text + ' '.join(after_text)
                body_revised = body_trimmed[0:197] + '...' if len(body_trimmed) > 197 else body_trimmed + '...'
                unique_id = review_id + '-' + specialty_id
                if unique_id not in processed:
                    tagged.append({'professional_id': professional_id,
                                   'review_id': review_id,
                                   'city_id': city_id,
                                   'city_name': city_name,
                                   'state_id': state_id,
                                   'state_code': state_code,
                                   'review_date': review_date,
                                   'overall_rating': overall_rating,
                                   'specialty_id_tagged': specialty_id,
                                   'specialty_name_tagged': specialty_name,
                                   'keyword_searched': keyword,
                                   'keyword_found': keyword_found,
                                   'body': body,
                                   'body_revised': body_revised})
                    processed.add(review_id + '-' + specialty_id)

    return tagged


def write_tagged(tagged, outputfile):
    """ Write tagged reviews to csv file """
    with open(outputfile, 'w', encoding='utf-8', errors='ignore') as f:
        fieldnames = ['professional_id',
                      'review_id',
                      'city_id',
                      'city_name',
                      'state_id',
                      'state_code',
                      'review_date',
                      'overall_rating',
                      'specialty_id_tagged',
                      'specialty_name_tagged',
                      'keyword_searched',
                      'keyword_found',
                      'body',
                      'body_revised']
        writer = csv.DictWriter(f, delimiter=',', fieldnames=fieldnames)
        writer.writeheader()
        for row in tagged:
            writer.writerow({'professional_id': row['professional_id'],
                             'review_id': row['review_id'],
                             'city_id': row['city_id'],
                             'city_name': row['city_name'],
                             'state_id': row['state_id'],
                             'state_code': row['state_code'],
                             'review_date': row['review_date'],
                             'overall_rating': row['overall_rating'],
                             'specialty_id_tagged': row['specialty_id_tagged'],
                             'specialty_name_tagged': row['specialty_name_tagged'],
                             'keyword_searched': row['keyword_searched'],
                             'keyword_found': row['keyword_found'],
                             'body': row['body'],
                             'body_revised': row['body_revised']})

if __name__ == '__main__':
    # Assignments
    start_time = time.time()
    current_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    outputfile = './results/results_' + current_time + '.csv'
    KEYWORDS_FILE = 'specialty_keywords.csv'
    REVIEWS_FILE = 'reviews.csv'

    # Function Calls
    keywords = get_keywords(KEYWORDS_FILE)
    reviews = get_reviews(REVIEWS_FILE)
    tagged = tag_reviews(keywords, reviews)
    write_tagged(tagged, outputfile)

    # Runtime
    end_time = time.time()
    runtime = round((end_time - start_time)/60, 2)
    print('Runtime: {} minutes'.format(runtime))
