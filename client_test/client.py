import sys
import socket
import time
import pytest
from test_request_generator import *


def read_file_lines(filename):
    lines = []
    with open(filename, 'r') as file:
        for line in file:
            # Remove trailing newline character
            line = line.rstrip('\n')
            lines.append(line)
    return lines




def tried(request):
    assert True

def test_server_response():
    host = "127.0.0.1"
    port = 8000


    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Connect to the server
    client_socket.connect((host, port))
    print(f"Connected to {host}:{port}")

    # # Send a test message to the server
    # message = "Test message"
    # client_socket.sendall(message.encode())

    response = client_socket.recv(1024).decode().split()
    assert response[0] == "hello", "Response is not 'hello'"

    # Lets start with the test

    # Read test file
    lines = read_file_lines('test_requests.txt')
    print(lines)


    # Generate 10 requests
    requests_data = generate_requests(10)

    # Simulate server responses
    responses_data = simulate_server(requests_data)

    # Print the requests and responses
    for request, response in zip(requests_data, responses_data):

        print(f'-------Simulation data -------')
        print(f'Request: {request}')
        print(f'Response: {response}')
        print('-------- Server answer ----------')

        tried(request)




    # print(f"Server response: {repr(response)}")
    # print(f"Server response: {repr(response)}")

    print(f"AssertionError: {str(AssertionError)}")

    print(f"Error: {str(Exception)}")

    # finally:
        # Close the socket
        # print('this should fail!')
        # assert False
    client_socket.close()



def main():
    test_server_response()



if __name__ == '__main__':
    print('Hello Pedro, server tester is starting.. ')
    # pytest.main([__file__])
    main()
    print('Shuting up client. Adios!')

