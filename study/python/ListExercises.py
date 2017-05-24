"""
Write a function that returns the elements on odd positions (0 based) in a list
"""
### sudo code ###
# create empty list called output
# iterate thru range of length of input list
# check to see if position divided by 2 does not equal zero then append to output list

def solution1(input):
    output = []
    for i in range(len(input)):
        if i % 2 != 0:
            output.append(input[i]) 

    return output


"""
Write a function that returns the cumulative sum of elements in a list
"""
### sudo code ###
# create var called sum and init to zero
# create empty list called output
# iterate thru list and add element to current value of sum
# append to output

def solution2(input):
    sum = 0
    output = []
    for i in input:
        sum += i
        output.append(sum)

    return output


"""
Write a function that takes a number and returns a list of its digits
"""

### sudo code ###
# create empty list called output
# convert nbr arg to string and iterate over this string value 
# convert each element of string to int and append to output list

def solution3(input):
    output = []
    for i in str(input):
        output.append(int(i))

    return output


"""
From: http://codingbat.com/prob/p126968
Return the "centered" average of an array of ints, which we'll say is 
the mean average of the values, except ignoring the largest and 
smallest values in the array. If there are multiple copies of the 
smallest value, ignore just one copy, and likewise for the largest 
value. Use int division to produce the final average. You may assume 
that the array is length 3 or more.
"""

### sudo code ###
# create an empty list called results
# sort the list in ascending order
# iterate over range 1 to length of list less 1 in order to exclude one copy of both smallest and largest element
# append item to results list
# divide length of results list by 2, if type equal float then use int value as index else int value as index


def solution4(input):
    results = []
    input_sorted = sorted(input)
    print(input_sorted)
    #for i in range(1, 5):
    for i in range(1,len(input_sorted) - 1):
        results.append(input_sorted[i])
    print(results)
    value = len(results) / 2
    print(type(value))
    print(value)
    if value == float(value):
        return results[int(value)]
    else:
        return (results[value -1] + results[value]) / 2
    #output = results[position]

    #return output 



if __name__ == "__main__":
    #list1 = [0,1,2,3,4,5] 
    #list1 = [1,-1,2,-2] 
    #print(solution1(list1))
    #list2 = [1,1,1]
    #list2 = [1,-1,3]
    #print(solution2(list2))
    #nbr = 123
    #nbr = 400
    #print(solution3(nbr))
    #list3 = [1, 2, 3, 4, 100]
    #list3 = [1, 1, 5, 5, 10, 8, 7]
    list3 = [-10, -4, -2, -4, -2, 0]
    print(solution4(list3))
