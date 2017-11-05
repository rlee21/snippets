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
