# import datetime
# now =  (datetime.datetime.now())
# today = (datetime.date.today())
# # print now
# now_new = now.strftime('%Y-%m-%d %H:%M:%S')
# # print now.strftime('%Y-%m-%d')
# print now
# print datetime.datetime.isoformat(now)
####################
################################
# from math import pi
# radius = float(input('Enter radius: '))
# area = pi * radius**2
# print('The area of a circle with a radius of ' + str(radius) + ' is ' + str(area))
################################
# import datetime
# now =  (datetime.datetime.now())
# today = (datetime.date.today())
# # print now
# now_new = now.strftime('%Y-%m-%d %H:%M:%S')
# # print now.strftime('%Y-%m-%d')
# print now
# print datetime.datetime.isoformat(now)
################################
# firstname = input("Enter first name: ")
# lastname = input("Enter last name: ")
# print(lastname + " " + firstname)
################################
# input = 3, 5, 7, 23
# print(tuple(input))
# my_list = []
# for i in input:
#     my_list.append(i)
# print(my_list)
#################################
# import random
# x = int(input('Enter a number: '))
# y = random.randint(1,2)
# if x == y:
#     print("YOU WON!! The number was " + str(y))
# else:
#     print("The number was " + str(y) + ", please try again Isabella.")
#################################
# filename = 'foo.txt'
# file_split = filename.split(".")
# print "The extension of the file is : " + str(file_split[-1])
# print "The extension of the file is : " + repr(file_split[-1])
##################################
# color_list = ["Red","Green","White" ,"Black"]
# print color_list[0], color_list[-1]
# print( "%s %s"%(color_list[0],color_list[-1]))
# print( "{} {}" .format(color_list[0],color_list[-1]) )
###################################
# exam_st_date = (11, 12, 2014)
# mm = exam_st_date[0] 
# dd = exam_st_date[1]
# yyyy = exam_st_date[2]
# print str(mm) + "/" + str(dd) + "/" + str(yyyy)
# print("The start of the exam will be from {} / {} / {}" .format(exam_st_date[0],exam_st_date[1],exam_st_date[2]))
# print("The start of the exam will be from %i / %i /%i" %exam_st_date)
###################################
# import os
# print os.system("pwd") + os.system("ls -lrt")
###################################
# import os.path
# import time
# dir(time)
# # print (os.path.isfile("./tmp.py"))
# file = "./tmp.yp"
# wait_time = 15
# if os.path.isfile(file) != True:
# 	print ("file does not exist, waiting for " + str(wait_time) + " seconds")
# 	time.sleep(wait_time)
# 	print("file does not exist, program exited")
# else:
# 	print("file found!!")
#######################################
# import pandas as pd

# df = pd.read_csv('/Users/relee/PythonProjects/BigQuery/outputs/BigQuery.csv')
# print(df)
# with open('/Users/relee/PythonProjects/BigQuery/outputs/BigQuery.csv','r') as infile:
    # print(infile.read())
# #     # pd.DataFrame(read_csv(infile))
# import matplotlib.pyplot as plt
# squares = [1, 4, 9, 16, 25]
# plt.plot(squares)
# plt.show()
############
# key - animal_name : value - location 
# zoo_animals = { 'Unicorn' : 'Cotton Candy House',
# 'Sloth' : 'Rainforest Exhibit',
# 'Bengal Tiger' : 'Jungle House',
# 'Atlantic Puffin' : 'Arctic Exhibit',
# 'Rockhopper Penguin' : 'Arctic Exhibit'}
# del zoo_animals["Sloth"], zoo_animals["Bengal Tiger"]
# print zoo_animals
###############
# from nltk.tokenize import PunktSentenceTokenizer
# from nltk.corpus import state_union
# import nltk
# train_text = state_union.raw("1999-Clinton.txt")
# sample_text = state_union.raw("2000-Clinton.txt")

# custom_sentence_tokenizer = PunktSentenceTokenizer(train_text)
# tokenized = custom_sentence_tokenizer.tokenize((sample_text))


# def process_content():
#     try:
#         for i in tokenized:
#             words = nltk.word_tokenize(i)
#             tagged = nltk.pos_tag(words)
#             print(tagged)
#     except Exception as e:
#         print(str(e))

# process_content()
#####################
# languages = {"python" : 5, "ruby" : 4, "java" : 3, "html" : 2, "scala" : 1}
# languages = dict(python = 5, ruby = 4, java = 3, html = 2, scala = 1)
# # for k in languages:
# # 	print k, languages[k]
# import csv
# help(csv)

# for k, v in languages.items():
# 	print(k, v)
##############
# my_list = []
# for number in range(1,11):
# 	my_list.append(number)
# print(my_list)

# my_list = [number for number in range(1,11)]
# print(my_list)

# my_list = [1,2,3,4,5]
# my_list.reverse()
# print(my_list)

# for i in range(1,21):
# 	if i % 3 == 0 and i % 5 == 0:
# 		print("HelloWorld")
# 	elif i % 3 == 0:
# 		print("Hello")
# 	elif i % 5 == 0:
# 		print("World")
# 	else:
# 		print(i)
##########
# input = "abcdef"
# print(input [::-1])
####################
# lloyd = {
#     "name": "Lloyd",
#     "homework": [90.0, 97.0, 75.0, 92.0],
#     "quizzes": [88.0, 40.0, 94.0],
#     "tests": [75.0, 90.0]
# }
# alice = {
#     "name": "Alice",
#     "homework": [100.0, 92.0, 98.0, 100.0],
#     "quizzes": [82.0, 83.0, 91.0],
#     "tests": [89.0, 97.0]
# }
# tyler = {
#     "name": "Tyler",
#     "homework": [0.0, 87.0, 75.0, 22.0],
#     "quizzes": [0.0, 75.0, 78.0],
#     "tests": [100.0, 100.0]
# }
# students = [lloyd, alice, tyler]
# for i in students:
# 	print i["name"]
# 	print i["homework"]
# 	print i["quizzes"]
# 	print i["tests"]
#######################
# my_list = [1,2,3,4,4,5]
# my_dict = {}
# for i in my_list:
#     if i in my_dict:
#         my_dict[i] += 1
#     else:
#         my_dict[i] = 1 
# # print (my_dict)
# max = max(my_dict, key=my_dict.get)
# most_freq = sorted(my_dict,key=my_dict.get, reverse=True)[:2]
# result = most_freq[0]
# print max
# print most_freq
# print result
# lloyd = {
#     "name": "Lloyd",
#     "homework": [90.0, 97.0, 75.0, 92.0],
#     "quizzes": [88.0, 40.0, 94.0],
#     "tests": [75.0, 90.0]
# }
# alice = {
#     "name": "Alice",
#     "homework": [100.0, 92.0, 98.0, 100.0],
#     "quizzes": [82.0, 83.0, 91.0],
#     "tests": [89.0, 97.0]
# }
# tyler = {
#     "name": "Tyler",
#     "homework": [0.0, 87.0, 75.0, 22.0],
#     "quizzes": [0.0, 75.0, 78.0],
#     "tests": [100.0, 100.0]
# }

# # Add your function below!
# def average(numbers):
#     total = sum(numbers)
#     return float(total) / len(numbers)
    
    
# def get_average(student):
#     homework = average(student["homework"])
#     quizzes = average(student["quizzes"])
#     tests = average(student["tests"])
#     return (homework * 0.10) + (quizzes * 0.30) + (tests * 0.60)
    

# def get_letter_grade(score):
#     if score >= 90:
#         return "A"
#     elif score >= 80:
#         return "B"
#     elif score >= 70:
#         return "C"
#     elif score >= 60:
#         return "D"
#     else:
#         return "F"


# print get_letter_grade(get_average(lloyd))

# def get_class_average(students):
#     results = []
#     for student in students:
#         results.append(get_average(student))
#         return average(results)
        
# print results
# students = [lloyd, alice, tyler] 
# print get_class_average([lloyd, alice, tyler])       

# for student in students:
# 	print student        
# print get_average(lloyd)
# print get_average(alice)
# print get_average(tyler)
###########
# battle ship game
# from random import randint
#
# board = []
#
# for x in range(5):
#     board.append(["O"] * 5)
#
# def print_board(board):
#     for row in board:
#         print " ".join(row)
#
# print "Let's play Battleship!"
# print_board(board)
#
# def random_row(board):
#     return randint(0, len(board) - 1)
#
# def random_col(board):
#     return randint(0, len(board[0]) - 1)
#
# ship_row = random_row(board)
# ship_col = random_col(board)
# print ship_row
# print ship_col
#
# # Everything from here on should go in your for loop!
# for turn in range(4):
#     print "Turn", turn + 1
#     guess_row = int(raw_input("Guess Row:"))
#     guess_col = int(raw_input("Guess Col:"))
#     if guess_row == ship_row and guess_col == ship_col:
#         print "Congratulations! You sunk my battleship!"
#         break
#     else:
#         if (guess_row < 0 or guess_row > 4) or (guess_col < 0 or guess_col > 4):
#             print "Oops, that's not even in the ocean."
#         elif(board[guess_row][guess_col] == "X"):
#             print "You guessed that one already."
#         else:
#             print "You missed my battleship!"
#             board[guess_row][guess_col] = "X"
#             if turn == 3:
#                 print "Game Over"
# # Print (turn + 1) here!
# print(turn + 1)
#
### string slicing ###
def front_back(str):
    if len(str) <= 1:
        return str

#     mid = str[1:len(str) - 1]  # can be written as str[1:-1]
    mid = str[1:-1]  # can be written as str[1:-1]
    # print(mid)
    # last + mid + first
    return str[len(str) - 1] + mid + str[0]

print(front_back('code'))
# front_back('code')

# str = 'code'
# print(str[1:3])
