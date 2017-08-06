"""Write a function that computes the list of the first 100 Fibonacci numbers. 
By definition, the first two numbers in the Fibonacci sequence are 0 and 1, and 
each subsequent number is the sum of the previous two. As an example, here are 
the first 10 Fibonnaci numbers: 0, 1, 1, 2, 3, 5, 8, 13, 21, and 34."""



# sudo code
#range from 0 to 100
#starting with 0 and 1
#each subsequent nbr is sum of previous two


def fib_seq_for_loop():
    x = 0
    y = 1
    for _ in range(100):
        x, y = y, x + y 
        print(x)


def fib_seq_recursion(n):
    if n < 2:
        return 1
    else:
        return fib_seq_recursion(n-1) +fib_seq_recursion(n-2)




if __name__ == "__main__":
    fib_seq_for_loop()
    n = 5
    print(fib_seq_recursion(n))

