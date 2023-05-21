import sys
import socket
import time
import pytest
from test_request_generator import *



def tried(client_socket, request,response_expect):
    client_socket.sendall(request.encode())
    response = client_socket.recv(1024).decode().split()
    response = ' '.join(response)

    print(f"Server response: {repr(response)}")

    assert response_expect == response, "Response is not the same"

    # print(f"AssertionError: {str(AssertionError)}")
    # print(f"Error: {str(Exception)}")

    return response


def test_server_response():
    host = "127.0.0.1"
    port = 8000

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((host, port))
    print(f"Connected to {host}:{port}")



    response = client_socket.recv(1024).decode().split()
    assert response[0] == "hello", "Response is not 'hello'"


    requests_data = generate_requests(50)
    responses_data = simulate_server(requests_data)

    request_num = 0
    with open('test_log.txt', 'w') as file:
        for request, response_expect in zip(requests_data, responses_data):
            request_num +=1
            print(f'-------Simulation {request_num} -------')
            print(f'Request: {request}')
            print(f'Response: {response_expect}')
            response = tried(client_socket, request, response_expect)

            log = f'{request_num}-Request: {request} |Expected: {response_expect} |Server: {response}'
            file.write(log + '\n')

    client_socket.close()



def main():
    test_server_response()
if __name__ == '__main__':
    # pytest.main([__file__])
    main()

