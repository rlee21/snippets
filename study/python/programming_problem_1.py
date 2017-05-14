"""Write three functions that compute the sum of the numbers in
   a given list using a for-loop, a while-loop, and recursion."""


def for_loop_sum(numbers):
    x = 0
    for number in numbers:
        x += number

    return x 


def while_loop_sum(numbers):
    x = 0
    y = 0
    while x < len(numbers):
        y += numbers[x]
        x += 1
    
    return y
    
        
def recursion_sum(numbers):
    if not numbers:
        return 0
    else:
        return numbers[0] + recursion_sum(numbers[1:])
        


if __name__ == "__main__":
    l = [1, 2, 3, 4, 5]
    print(for_loop_sum(l))
    print(while_loop_sum(l))    
    print(recursion_sum(l)) 
