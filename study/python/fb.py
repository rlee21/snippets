

def array_unique(input_list):
    unique_list = []
    for i in input_list:
        if i not in unique_list:
            unique_list.append(i)

    return unique_list
 
assert cmp(array_unique([1,2,3,5,6,1,2,4]), [1,2,3,5,6,4]) == 0
assert cmp(array_unique([1,2]), [1,2]) == 0
assert cmp(array_unique([]), []) == 0
assert cmp(array_unique([1,1,1,1]), [1]) == 0
print('passed')

#print(array_unique([1,2,3,5,6,1,2,4]))
