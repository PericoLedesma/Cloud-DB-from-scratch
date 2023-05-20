import sys
import socket
import time

def send_put_request(host, port, key, value):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:

        client.connect((host, port))
        response = client.recv(1024)
        print(response.decode('utf-8'))

        # Send a PUT request
        #request = f'PUT {key} HTTP/1.1\r\nContent-Length: {len(value)}\r\n\r\n{value}'.encode('utf-8')
        request = f'put {key} {value}'.encode('utf-8')


        for i in range(10):
            print("Request to send: ", request)
            client.sendall(request)
            response = client.recv(1024)
            print(response.decode('utf-8'))


        request = f'delete {key}'.encode('utf-8')

        for i in range(10):
            print("Request to send: ", request)
            client.sendall(request)
            response = client.recv(1024)
            print(response.decode('utf-8'))


        time.sleep(10)  # Pause execution for 3 seconds
        request = f'close'.encode('utf-8')

        print("Request to send: ", request)
        client.sendall(request)
        response = client.recv(1024)
        print(response.decode('utf-8'))




if __name__ == '__main__':
    print('Hello Pedro, client is starting.. ')
    mykey = 'Key5'
    myvalue = 'Pedro'

    send_put_request('localhost', 8000, mykey, myvalue)

    print('Shuting up client. Adios!')

