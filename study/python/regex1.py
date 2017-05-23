import re

text = 'python ....An example of an interpreted programming language is python'
#text = 'An example of an interpreted programming language is python'
pattern = r'python'
#pattern = r'pyt.o.'

#if re.match(pattern, text):
#if re.search(pattern, text):
if re.findall(pattern, text):
    print('True')

else:
    print('False')


