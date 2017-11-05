#!/usr/bin/python


"""
given a sorted array and a target element,
write a function that returns the number
of times the target element occurs
"""

## brute force option 1
def arr_freq1(arr, tgt):
    d = {}
    for i in arr:
        if i not in d:
            d[i] = 1
        else:
            d[i] += 1
    return d[tgt]


## brute force option 2
def arr_freq2(arr, tgt):
    arr_len = len(arr)
    tgt_idx = []
    for i in range(arr_len):
        if arr[i] == tgt:
            tgt_idx.append(i)

    return len(tgt_idx)


## binary search
# iter thru arr
# id start and end of tgt indices
# condition: when proceeding tgt indice slice != tgt



if __name__ == '__main__':
    arr = [1, 2, 2, 3, 3, 3]
    tgt = 2
    # print(arr_freq1(arr, tgt))
    # print(arr_freq2(arr, tgt))
    arr_len = len(arr)
    mid = arr_len/2
    start_idx = 0
    end_idx = 0
    for i in range(0, arr_len):
        print(arr[i])
        if arr[i] == tgt:
            start_idx = i
            end_idx = i
            #print(i)
            continue
