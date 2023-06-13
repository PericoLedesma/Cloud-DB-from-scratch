import socket
import argparse



# # sys will allow us to access the passed arguments
# import sys
#
# # sys.argv[0] access the first argument passed that is the python script name
# print("\nFile or Script Name is :", sys.argv[0])
#
# # print arguments other than the file name
# print("\nArguments passed:", end = " ")
# for i in range(1, len(sys.argv)):
#    print(sys.argv[i], end = " ")
#
# # Lowercase operation on the passed arguments
# for i in range(1, len(sys.argv)):
#    print(sys.argv[i].lower(), end = " ")




message= ' '

print(message)

if message is None:
    print("hello")


if message == "":
    print("hello2")

if not message:
    print("hello3")

# class ParentClass:
#
#     def __init__(self):
#         self.child = ChildClass(self)
#     def parent_function(self):
#         print("This is a function from the parent class")
#
#
# class ChildClass(ParentClass):
#     def __init__(self, parent):
#         parent.parent_function()
#     def child_function(self):
#         print("This is a function from the child class")
#
#
# parent = ParentClass()
# # Create an instance of the ChildClass



#
# response2 = b'get_error apple123@# \r\n ppl'
#
#
# request, *args = response2.decode().split('\r\n')
# print(request)
# print(args)
#
# print('-----')
# print(repr(response2))
# buffer = response2.decode().split()
# print(buffer)


# # Create an argument parser
# parser = argparse.ArgumentParser(description='Script for performing a specific task.')
#
# # Add arguments
# parser.add_argument('-f', '--file', help='Input file path')
# parser.add_argument('-n', '--number', type=int, help='Number argument')
#
# # Parse the command-line arguments
# args = parser.parse_args()
#
# # Access the parsed arguments
# file_path = args.file
# number = args.number
#
# # Perform the task based on the provided arguments
# if file_path:
#     print(f'Input file path: {file_path}')
# else:
#     print('No input file path provided.')
#
# if number is not None:
#     print(f'Number argument: {number}')
# else:
#     print('No number argument provided.')
#
#
#
# # Server configuration
# HOST = '0.0.0.0'  # Listen on all available network interfaces
# PORT = 8000      # Choose a free port for the kvserver
#
# # Create a socket object
# server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#
# # Bind the socket to the host and port
# server_socket.bind((HOST, PORT))
#
# # Listen for incoming connections
# server_socket.listen()
#
# print(f"Server listening on {HOST}:{PORT}")
#
# while True:
#     # Accept a kvclient connection
#     client_socket, client_address = server_socket.accept()
#     print(f"Connected by {client_address}")
#
#     response = "Hi kvclient!"
#     client_socket.sendall(response.encode())
#
#     # Receive kv_data from the kvclient
#     kv_data = client_socket.recv(1024).decode()
#     print(f"Received message: {kv_data}")
#
#     # Send a response back to the kvclient
#     response = "Message received successfully!"
#     client_socket.sendall(response.encode())
#
#     # Close the kvclient socket connection
#     client_socket.close()