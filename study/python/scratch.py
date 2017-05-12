import json

with open("PivotTableData.json","r") as file:
	data = json.load(file)
print data["Name"], data["Device Type"]

##################3

text = """ 
Python is a widely used high-level programming language used for general-purpose programming, created by Guido van Rossum and first released in 1991. An interpreted language, Python has a design philosophy which emphasizes code readability (notably using whitespace indentation to delimit code blocks rather than curly braces or keywords), and a syntax which allows programmers to express concepts in fewer lines of code than possible in languages such as C++ or Java.[22][23] The language provides constructs intended to enable writing clear programs on both a small and large scale.
 """
words = text.split()
# print(words)
word_count = {}
for word in words:
    # print(word)
    if word in word_count:
        word_count[word] += 1
    else:
        word_count[word] = 1 
print(word_count)
# result = max(word_count, key=word_count.get)
result = sorted(word_count, key=word_count.get, reverse=True)[:5]
#print(type(result))

