"""Write a function that combines two lists by alternatingly taking elements. 
   For example: given the two lists [a, b, c] and [1, 2, 3], the function 
   should return [a, 1, b, 2, c, 3]."""


# sudo code
# init new_list
# add 1st element from l1 to new_list
# add 1st element from l2 to new_list
# add 2nd element from l1 to new_list
# add 2nd element from l2 to new_list
# add 3rd element from l1 to new_list
# add 3rd element from l2 to new_list
# return new_list 


def combine_lists(l1, l2):
    new_list = []
    for i, j in zip(l1, l2):
        new_list.append(i)
        new_list.append(j)
   
    return new_list




if __name__ == "__main__":
    l1 = ["a", "b", "c"]
    l2 = [1, 2, 3]
    print(combine_lists(l1, l2))

