class Animals:
#    __name=""
#    __color=""
#    __size=""
#    __sound=""
#   

    def __init__(self, __name, __color, __size, __sound):
       self.__name = __name 
       self.__color = __color 
       self.__size = __size 
       self.__sound = __sound 


    def set_name(self, name):
        self.__name = name
        

    def get_name(self):
        return self.__name


    def set_color(self, color):
        self.__color = color


    def get_color(self):
        return self.__color


    def set_size(self, size):
        self.__size = size 


    def get_size(self):
        return self.__size


    def set_sound(self, sound):
        self.__sound = sound


    def get_sound(self):
        return self.__sound


dog = Animals("Maile", "Black", "small", "ruff")

print(dog.get_name())
print(dog.get_color())
print(dog.get_size())
print(dog.get_sound())
print("-------------")

dog.set_name("Sparky")
dog.set_color("Brown")
dog.set_size("Large")
dog.set_sound("grrr")

print(dog.get_name())
print(dog.get_color())
print(dog.get_size())
print(dog.get_sound())
print("-------------")

class Elephants(Animals):

    def __init__(self, __name, __color, __size, __sound, __weight):
        self.__weight = __weight
        super(Elephants, self).__init__(__name, __color, __size, __sound)

    def set_weight(self, weight):
        self.__weight = weight

    def get_weight(self):
        return self.__weight

test_weight = Elephants("Hadoop", "Gray", "large", "blah blah", 1000)


print(test_weight.get_name())
print(test_weight.get_color())
print(test_weight.get_size())
print(test_weight.get_sound())
print(test_weight.get_weight())
print("-------------")

test_weight.set_weight(2000)
print(test_weight.get_weight())
