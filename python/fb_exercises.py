"""
Write a function that returns the elements on odd positions (0
based) in a list
"""
# create empty list
# get len of input list
# iter for range from 0 to len of input list
# condition where ith in for loop is odd using modulo
# append to new list

def solution(input):
    output = []
    length = len(input)
    for i in range(0, length):
        if i % 2 != 0:
           output.append(input[i])
    return output

#print(solution([0,1,2,3,4,5]))
#print(solution([1,-1,2,-2]))
assert solution([0,1,2,3,4,5]) == [1,3,5]
assert solution([1,-1,2,-2]) == [-1,-2]


def solution2(input):
    return [input[i] for i in range(len(input)) if i % 2 != 0]

#print(solution2([0,1,2,3,4,5]))
#print(solution2([1,-1,2,-2]))
assert solution2([0,1,2,3,4,5]) == [1,3,5]
assert solution2([1,-1,2,-2]) == [-1,-2]

"""
Write a function that returns the cumulative sum of elements in a
list
"""
# create empty list
# assign sum var with val of 0
# for item in list += sum

def solution3(input):
    output = []
    sum = 0
    for i in input:
        sum += i
        output.append(sum)
    return output

#print(solution3([1,1,1]))
#print(solution3([1,-1,3]))
assert solution3([1,1,1]) == [1,2,3]
assert solution3([1,-1,3]) == [1,0,3]


"""
Write a function that takes a number and returns a list of its
digits
"""
# convert num to str
# convert each item to int

def solution4(input):
    input_str = str(input)
    output = []
    for i in input_str:
        output.append(int(i))
    return output
assert solution4(123) == [1,2,3]
assert solution4(400) == [4,0,0]


"""
Write a function that takes list and returns element that that occurs more than once
"""
# create empty dict
# iter thru list
# if item is key in dict then increment val in dict else 0
def solution5(input):
    count = {}
    output = []
    for i in input:
        if i in count:
            count[i] += 1
        else:
            count[i] = 1
    for k, v in count.items():
        if v > 1:
            output.append(k)
    return output

assert solution5([0,0,0,3,6,6,7]) == [0,6]
assert solution5([1,2,3,3,4,5,5]) == [3,5]


def solution6(input):
    output = set()
    for i in input:
        if input.count(i) > 1:
            output.add(i)
    return list(output)

assert solution6([0,0,0,3,6,6,7]) == [0,6]
assert solution6([1,2,3,3,4,5,5]) == [3,5]



def solution7(input):
    seen = set()
    dups = set()
    for i in input:
        if i in seen:
            dups.add(i)
        else:
            seen.add(i)
    return list(dups)

assert solution7([0,0,0,3,6,6,7]) == [0,6]
assert solution7([1,2,3,3,4,5,5]) == [3,5]


"""
Given a non-negative integer, add the digits until if become a one digit integer
"""
def solution8(input):
    is_list = isinstance(input, list)
    if not is_list:
        nums = [int(i) for i in str(input)]
    else:
        sum = 0
        if len(nums) == 1:
            sum = nums[0]
            return sum
        else:
            sum = nums[0] + solution8(nums[1:])
            return sum


#print(solution8(38))
#assert solution8(38) == 2

# def sum(list):
#     if len(list) == 1:
#         return list[0]
#     else:
#         return list[0] + sum(list[1:])
#
#
# print(sum([5, 7, 3, 8, 10]))


"""
Given a sorted list if integers, remove any duplicates that occur more than twice.
"""
# def solution9(input):
#     cnt = 1
#     for i in range(len(input)-1):
#         if input[i] == input[i+1]:
#             cnt += 1
#             if cnt > 2:
#                 input.remove(input[i])
#                 cnt = 1
#         else:
#             cnt = 1
#
#     return input

def solution9(input):
    output = []
    cnt = 0
    for i in range(len(input)):
        if input[i] == input[i-1]:
            cnt += 1
            if cnt > 1:
                # input.remove(i)
                cnt = 1
            else:
                output.append(input[i])
        else:
            output.append(input[i])
            cnt = 1

    return output

# print(solution9([1,1]))
# print(solution9([1,1,1,2,2,3,4]))
# assert solution9([1,1]) == [1,1]
# assert solution9([1,1,1,2,2,3,4]) == [1,1,2,2,3,4]

"""
Given two lists, return a list containing common elements between both lists
"""
def solution10(nums1, nums2):
    nums1_set = set(nums1)
    nums2_set = set(nums2)
    #return list(nums1_set.difference(nums2_set))
    return list(nums1_set.intersection(nums2_set))

#print(solution10([1,1,1,2,2,3,4], [0,2,9,8,7,3,5]))
#assert solution10([1,1,1,2,2,3,4], [0,2,9,8,7,3,5]) == [1, 4]
assert solution10([1,1,1,2,2,3,4], [0,2,9,8,7,3,5]) == [2, 3]

"""
Given two lists, return a list containing common elements between both lists
"""
# init empty list
# init empty set
# walk list if i in set then append to new list

#def solution11(nums1, nums2):
#    output = []
#    seen = set()


#print(solution11([1,1,1,2,2,3,4], [0,2,9,8,7,3,5]))


"""
Write a function that returns the elements on odd positions (0 based) in a list
"""
def solution12(input):
    return [input[i] for i in range(len(input)) if i % 2 != 0]

assert solution12([0,1,2,3,4,5]) == [1,3,5]
assert solution12([1,-1,2,-2]) == [-1,-2]

"""
Write a function that returns the cumulative sum of elements in a list
"""
def solution13(input):
    output = []
    sum = 0
    for i in input:
        sum += i
        output.append(sum)
    return output

assert solution13([1,1,1]) == [1,2,3]
assert solution13([1,-1,3]) == [1,0,3]


"""
Write a function that takes a number and returns a list of its digits
"""
def solution14(input):
    s = str(input)
    return [int(i) for i in list(s)]

assert solution14(123) == [1,2,3]
assert solution14(400) == [4,0,0]


"""
From: http://codingbat.com/prob/p126968
Return the "centered" average of an array of ints, which we'll say is
the mean average of the values, except ignoring the largest and
smallest values in the array. If there are multiple copies of the
smallest value, ignore just one copy, and likewise for the largest
value. Use int division to produce the final average. You may assume
that the array is length 3 or more.
"""

def solution15(input):
    l_sort = sorted(input)
    l_new = l_sort[1:-1]
    return int(sum(l_new) / len(l_new))

# print(solution15([1, 2, 3, 4, 100]))
assert solution15([1, 2, 3, 4, 100]) == 3
assert solution15([1, 1, 5, 5, 10, 8, 7]) == 5
assert solution15([-10, -4, -2, -4, -2, 0]) == -3


def check_ip(input):
    valid_nums = [i for i in range(0, 256)]
    result = []
    if not input:
        return False
    else:
        ips = input.split('.')
        for ip in ips:
            if int(ip) in valid_nums:
                result.append(int(ip))
        if len(result) == 4:
            return True
        else:
            return False

assert check_ip('123.456.789.001') == False
assert check_ip('123.001.001.001') == True
assert check_ip('123.001.001.257') == False
