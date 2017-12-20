#import sys
#from textblob import TextBlob
from nltk import sent_tokenize, word_tokenize, pos_tag, RegexpParser

document = 'document.txt'
with open(document, 'r') as doc:
    text = doc.read()

#blob = TextBlob(text)
#tags = blob.tags
#np = blob.noun_phrases
#n_grams = blob.ngrams(n=3)
#
#print('blob:', blob)
#print('\ntags:', tags)
#print('\nnoun phrases:', np)
#print('\nn-grams:', n_grams)

sents = sent_tokenize(text)
words = [word_tokenize(sent) for sent in sents]
tags = [pos_tag(word) for word in words]
#print(pos)

for tag in tags:
    #pattern = r"""chunk >>>: {<DT>?<JJ>*<NN>}"""
    pattern = r"""
        CHUNK: {<JJ>*<NN>}   # chunk determiner/possessive, adjectives and noun
            {<NNP>+}                # chunk sequences of proper nouns
    """
    chunk_parser = RegexpParser(pattern)
    chunk = chunk_parser.parse(tag)
    print(chunk)

#print(sents)
#print(len(sents))
#for sent in sents:
#    words = word_tokenize(sent) 
#    pos = pos_tag(words)
#    #print(pos)
#
#    #chunk_pattern = r"""chunk: {<RB.?>*<VB.?>*<NNP>+<NN>?}"""
#    chunk_pattern = r"""chunk: {<DT>?<JJ>*<NN>}"""
#    chunk_parser = RegexpParser(chunk_pattern)
#    chunk = chunk_parser.parse(pos)
#    print(chunk)
    #chunk.draw()






if __name__ == '__main__':
    document = 'document.txt'
