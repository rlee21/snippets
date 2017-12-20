from nltk.tokenize import sent_tokenize, word_tokenize
input = "I am an attorney with a perfect bar record but don't want to advertise with AVVO. If I have my rating removed (they won't remove the profile), they replace it with boilerplate language saying I have requested that it be removed, insinuating I have done something wrong. The rating is based on their parameters, (colleagues reviews, judge reviews) not just client reviews, which in my case are all positive. This site is a SCAM!"
################################
# tokenizing sentences and words
#################################
input_lower = input.lower()

# sentences = sent_tokenize(input_lower)
words = word_tokenize(input_lower)
# print(stop_words)

# for sentence in sentences:
#     print(sentence)

# for word in words:
#     print(word)

# for word in words:
#     if words.count(word) >= 3 and word not in stop_words:
#         print(words.count(word), word)
# #         # print(words.count(word.lower()))

# print(words.count("AVVO"))
#################################
# stop words
#################################
from nltk.corpus import stopwords
# stop_words = set(stopwords.words("english"))
# filtered_sentence = []
# for word in words:
#     if word not in stop_words:
#         filtered_sentence.append(word)

# filtered_sentence = [word for word in words if word not in stop_words]
# # list comprehension - exp (word), source (words), filter (if statement)
# print(filtered_sentence)
#################################
# stemming (wordnet, sen set is alternative)
#################################
# from nltk.stem import PorterStemmer
# ps = PorterStemmer()
# stem_input = ["python", "pythonic", "pythoner", "pythoning"]

# for i in stem_input:
#     print(ps.stem(i))
#################################
# part of speech tagging
#################################
from nltk.tokenize import PunktSentenceTokenizer
from nltk.corpus import state_union
import nltk
train_text = state_union.raw("1999-Clinton.txt")
sample_text = state_union.raw("2000-Clinton.txt")

custom_sentence_tokenizer = PunktSentenceTokenizer(train_text)
tokenized = custom_sentence_tokenizer.tokenize((sample_text))


def process_content():
    try:
        for i in tokenized:
            words = nltk.word_tokenize(i)
            tagged = nltk.pos_tag(words)
            print(tagged)
    except Exception as e:
        print(str(e))

process_content()
#################################
# lemmatizing
#################################
# from nltk.stem import WordNetLemmatizer
#
# lemmatizer = WordNetLemmatizer()
# print(lemmatizer.lemmatize("geese"))
# # better as an adjective
# print(lemmatizer.lemmatize("better", pos="a"))
# # run as a noun (note default pos="n")
# print(lemmatizer.lemmatize("run"))
# # run as a verb
# print(lemmatizer.lemmatize("running","v"))