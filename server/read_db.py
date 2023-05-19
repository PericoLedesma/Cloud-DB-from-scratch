import shelve
import os
import argparse

# Open the shelf file
# with shelve.open('storage.db') as shelf:
#     # Print all key-value pairs in the shelf
#     print("All key-value pairs:")
#     counter = 1
#     for key, value in shelf.items():
#         print(f"Item {counter}==> {key} | {value}")
#         counter += 1
#

parser = argparse.ArgumentParser(description='Load balancer server')
parser.add_argument('-d', '--directory', default='data', type=str, help='Storage directory')

args = parser.parse_args()

os.makedirs(args.directory, exist_ok=True)


# import argparse
#
# # Create an argument parser
# parser = argparse.ArgumentParser(description='Script for performing a specific task.')
#
# # Add arguments
# parser.add_argument('-n', '--name', help='Input file path')
# parser.add_argument('-i', '--id', type=int, help='Number argument')
#
# # Parse the command-line arguments
# args = parser.parse_args()
#
# # Access the parsed arguments
# name = args.name
# id = args.id
#
# # Perform the task based on the provided arguments
# if name:
#     print(f'Input file path: {name}')
# else:
#     print('No input file path provided.')
#
# if id is not None:
#     print(f'Number argument: {id}')
# else:
#     print('No number argument provided.')
