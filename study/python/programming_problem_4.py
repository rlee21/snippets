"""Write a function that given a list of non negative integers,
arranges them such that they form the largest possible number.
For example, given [50, 2, 1, 9], the largest formed number is 95021."""


import itertools

def largest_nbr(l):
    new_list = [int("".join(map(str, i))) for i in itertools.permutations(l)]
    return sorted(new_list, reverse=True)[0]



if __name__ == "__main__":
    l = [50, 2, 1, 9]     
    print(largest_nbr(l))

