""" Write a program that outputs all possibilities to put + or - or 
nothing between the numbers 1, 2, ..., 9 (in this order) such that 
the result is always 100. For example: 1 + 2 + 34 – 5 + 67 – 8 + 9 = 100. """


# sudo code
# sum series of numbers starting with 1, 2 and ending with 9 equals 100
# subract 12 (1+2+9) from 100 (88)
# determine permutations of numbers that in which the addition or subtraction equals 88


def example():
    return (1 + 2 + 34 – 5 + 67 – 8 + 9)




if __name__ == "__main__":
    print(example())




