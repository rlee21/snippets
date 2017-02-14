# create empty list and append data with split on \n

# create a clean version of list above (findAll symbols replace with "")

# create empty dictionary and append word with logic to increment by 1 if word in dictionary else 1

# sort list by value in descending order

# refactor to include prof id 
# by using nested dict {"prof_id": {"word":count}}
# 	or
# iterate by prof_id...
import requests
from bs4 import BeautifulSoup
import operator

# url = "http://www.imdb.com/genre/drama/?ref_=gnr_mn_dr_mp"
url = "http://www.imdb.com/showtimes/title/tt3521164/?ref_=shlc_li_i"

def get_data(url):
    word_list = []
    request = requests.get(url).text
    soup = BeautifulSoup(request)
    # for content_raw in soup.find_all("span", class_="outline"):
    for content_raw in soup.find_all(itemprop="description"):
        content = content_raw.string
        words = content.split()
        for each_word in words:
            print(each_word)
            word_list.append(words)
    clean_data(word_list)


def clean_data(words_list):
    clean_word_list = []
    for word in words_list:
        symbols = "!@#$%^&*()_+=-[]{}|;':\"<>?,./"
        for i in range(0, len(symbols)):
            word.replace(symbols[i], "")
        if len(word) > 0:
            print(word)
            clean_word_list.append(word)
    count_word(clean_word_list)

def count_word(clean_word_list):
    word_count = {}
    for word in clean_word_list:
        if word in word_count:
            word_count[word] += 1
        else:
            word_count[word] = 1
    for key, value in sorted(word_count.items(), key=operator.itemgetter(1)):
        print(key, value)
    # type(word_count)


if __name__ == "__main__":
    get_data(url)