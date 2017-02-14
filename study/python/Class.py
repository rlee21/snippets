class Dog:

    def __init__(self, size='large', color='black'):
        self.size = size
        self.color = color

    def sound(self):
        print ("ruff")

# class cat(Dog):
#
#     def size(self):
#         return 'small'
#
#     def color(self):
#         return 'white'
#
#     def sound(self):
#         print 'meow'


my_object = Dog()

print my_object.sound()
print my_object.size
print my_object.color
