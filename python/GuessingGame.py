import random
import sys

start_nbr = 1
end_nbr = 9
actual = random.randint(start_nbr, end_nbr)
guess = int(input("Enter number between " + str(start_nbr) + " and " + str(end_nbr) + ":  "))
counter = 0

while True:
    if counter == 4:
        print("Game over! You have exceeded the number of attempts allowed.")
        sys.exit(1)
    elif guess >= start_nbr and guess <= end_nbr:
        break
    else:
        print("The number you entered is not between " + str(start_nbr) + " and " + str(end_nbr) + " please try again")
        counter += 1
        actual = random.randint(1, 3)
        guess = int(input("Enter number between " + str(start_nbr) + " and " + str(end_nbr) + ":  "))

while True:
    if counter == 4:
        print("Game over! You have exceeded the maximum number of attempts allowed.")
        sys.exit(1)
    elif guess == actual:
        print("You won!! The number was " + str(actual) + " and you guessed " + str(guess))
        break
    else:
        print("Try again. The number was " + str(actual) + " and you guessed " + str(guess))
        counter += 1
        actual = random.randint(1, 3)
        guess = int(input("Enter number between " + str(start_nbr) + " and " + str(end_nbr) + ":  "))