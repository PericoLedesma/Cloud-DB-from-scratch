import shelve
import os
import argparse
from collections import defaultdict, OrderedDict

# Open the shelf file
# with shelve.open('storage.db') as shelf:
#     # Print all key-value pairs in the shelf
#     print("All key-value pairs:")
#     counter = 1
#     for key, value in shelf.items():
#         print(f"Item {counter}==> {key} | {value}")
#         counter += 1

# test_example.py

class MyClass:


    def __init__(self, name):
        self.name = name
        MyClass.instances.append(self)

    def delete_self(self):
        index = MyClass.instances.index(self)
        del MyClass.instances[index]

instances = []
# Create some instances of MyClass
obj1 = MyClass("Object 1")
obj2 = MyClass("Object 2")
obj3 = MyClass("Object 3")

print(MyClass.instances)  # Output: [<__main__.MyClass object at 0x...>, <__main__.MyClass object at 0x...>, <__main__.MyClass object at 0x...>]

# Delete obj2 from the list
obj2.delete_self()

print(MyClass.instances)  # Output: [<__main__.MyClass object at 0x...>, <__main__.MyClass object at 0x...>]
