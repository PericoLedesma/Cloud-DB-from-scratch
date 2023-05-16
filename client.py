import socket
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def send_request(server_socket, request):
    server_socket.sendall(request.encode())
    response = server_socket.recv(1024)
    return response.decode()


def main():
    host = '127.0.0.1'
    port = 65432

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect((host, port))

        while True:
            command = input("Enter command (put/get): ")
            key = input("Enter key: ")

            if command in ('put', 'get', 'delete'):
                if command == 'put':
                    value = input("Enter value: ")
                    request = f"{command}|{key}|{value}"
                elif command == 'get':
                    request = f"{command}|{key}"
                else:
                    print("Invalid command")
                    continue

            response = send_request(client_socket, request)
            logging.info(f"Server response: {response}")


if __name__ == '__main__':
    main()
