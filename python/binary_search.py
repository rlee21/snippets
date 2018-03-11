#!/usr/bin/python


def binary_search(arr, tgt):
    min = 0
    max = len(arr) - 1
    while True:
        mid = (min + max) // 2
        if max <  min:
            return -1
        elif arr[mid] > tgt:
            max -= 1
        elif arr[mid] < tgt:
            min += 1
        else:
            return mid



if __name__ == '__main__':
    arr = [1, 2, 3, 4, 5]
    tgt = 7
    print(binary_search(arr, tgt))

tgt = 9
nums = [1,4,18,9,10,12,21]

# sort nums
# set found var = false
# set first var = 0
# set last var = len(nums) - 1
# while condition first <= last and not found
# set mid = (first + last) // 2
# if sorted nums[mid] == tgt then found true
# else:
#      if tgt < sorted nums[mid] then set last to mid less 1
#      else set first to mid plus 1

def binary_search(input, tgt):
    input_sorted = sorted(input)
    found = False
    first = 0
    last = len(input) - 1
    while first <= last and not found:
        mid =  (first + last) // 2
        if tgt == input_sorted[mid]:
            found = True
        else:
            if tgt < input_sorted[mid]:
                last = mid - 1
            else:
                first = mid + 1
    return found

#print(binary_search([1,21,4,6,9,10], 9))
assert binary_search([1,21,4,6,9,10], 9) == False
