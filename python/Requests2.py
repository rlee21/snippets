import requests
from bs4 import BeautifulSoup
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords


url = "https://en.m.wikipedia.org/wiki/Python_(programming_language)"
request = requests.get(url)
soup = BeautifulSoup(request.content, "html.parser")
# print(soup.prettify())
# print(soup.get_text())
stop_words = set(stopwords.words("english"))

for item in soup.find_all("p"):
    # with open("output.txt","w") as outfile:
    #     outfile.writelines(item.text)
    text1 = item.text.split()
    # text2 = [i for i in text1]
    print(text1)
#     text2 = text1.split()
#     for i in text2:
#         my_list.append(i)
# print(my_list)
    # my_list.append(item.text)
# print(my_list)
    # for i in item.text:
    #     text = []
    #     text.append(i)
    #     # word_source = item.text
    # print(text)
###############################################
    # words = word_tokenize(word_source.lower())
    # print(words)
# for word in words:
    # filtered_words = [word for word in words if word not in stop_words]
#     filtered_words = []
#     if word not in stop_words:
#         filtered_words.append(word)
# print(filtered_words)
# word_count = {}
# for word in filtered_words:
#     # print(word)
#     if word in word_count:
#         word_count[word] += 1
#     else:
#         word_count[word] = 1
#     # for k,v in word_count.items():
#     #     print(k,v)
# print(word_count)
