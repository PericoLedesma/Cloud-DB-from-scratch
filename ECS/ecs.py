import subprocess
import os
import time
import argparse
import socket
import select


class ECS:
    def __init__(self, host, port, directory, num_kvservers):
        print(f'Starting ECS server on port[{host}:{port}]')

        self.cli = f'[ECS]>'
        self.host = host
        self.port = port

        self.num_kvservers = num_kvservers
        self.kv_sockets = []
        self.kv_sockets_e = []

        self.timeout = 10

        self.server_bootstrap()
        self.listen_to_kvservers()

        time.sleep(5)
        print(f'{self.cli}Sending msg')
        for sock in self.kv_sockets:
            self.handle_RESPONSE(f'Hey kvserver', sock)

        time.sleep(10)
        print(f'{self.cli}Sending msg')
        for sock in self.kv_sockets:
            self.handle_RESPONSE(f'Hey KVSERVER', sock)

        print(f'{self.cli}Closing ECS')

    def handle_RESPONSE(self, response, sock):
        sock.sendall(bytes(response, encoding='utf-8'))

    def listen_to_kvservers(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_fd:
            # server_fd.settimeout(5)
            server_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_fd.bind((self.host, self.port))
            server_fd.listen()
            print(f'{self.cli}Listening on {self.host}:{self.port}')
            start_time = time.time()

            while True:
                # time.sleep(2)
                print(f'{self.cli} Waiting')
                ready_sockets, _, error_sockets = select.select([server_fd] + self.kv_sockets, [], self.kv_sockets_e, 10)
                print(f'{self.cli} Kvservers:{len(self.kv_sockets)}| Sockets ready:{len(ready_sockets)}| Sockets error:{len(error_sockets)}')

                for sock in ready_sockets:
                    print(f'{self.cli}Ready sockets. Restart time')
                    start_time = time.time()

                    if sock is server_fd:
                        conn, addr = sock.accept()
                        print(f'{self.cli}Connection accepted: {addr}')
                        self.kv_sockets.append(conn)
                        self.handle_REQUEST(conn)
                    elif sock is self.kv_sockets:
                        print(f'{self.cli}Socket from store ')
                        self.handle_REQUEST(sock)
                    else:
                        try:
                            if sock.getpeername() is not None:
                                print(f'{self.cli}Socket checked conn.')
                                self.handle_REQUEST(sock)
                        except:
                            print("Socket is not connected to a remote endpoint. Deleting socket")
                            self.kv_sockets.remove(sock)
                            sock.close()

                for sock in error_sockets:
                    print("Error occurred on socket:")

                elapsed_time = time.time() - start_time
                if (time.time() - start_time) >= self.timeout:
                    print(f'{self.cli}Time out.Stop listening')
                    break

    def handle_REQUEST(self, sock):
        print(f'{self.cli}Handling the request')
        try:
            data = sock.recv(128 * 1024)
            if data:
                print(f'{self.cli}Received message: {data.decode()}')
                response = "[ECS] Message received: " + data.decode()
                sock.send(response.encode())

        except ConnectionResetError:
            print(f'{self.cli}EXCEPTION: Connection reset by peer.')
            if sock is self.kv_sockets:
                self.kv_sockets.remove(sock)
            # self.kv_sockets_e.append(sock)

        except socket.error as e:
            print(f'{self.cli}EXCEPTION: Socket error: {e}')
            if sock is self.kv_sockets:
                self.kv_sockets.remove(sock)
            # self.kv_sockets_e.append(sock)



    def server_bootstrap(self):
        print(f'{self.cli}Server bootstrap...')
        # Paths
        current_dir = os.path.abspath(os.path.dirname(__file__))
        parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
        script_path = os.path.join(parent_dir, 'kvserver', 'kvserver.py')

        port = 8000
        bootstrap = f'{self.host}:{self.port}'

        for n in range(self.num_kvservers):
            print(f'{self.cli}Starting kvserver {n}')
            command = ['python', script_path, f'-i {n}', f'-p {port + n}', f'-b {bootstrap}']

            result = subprocess.Popen(command)

            if result.returncode == 0:
                print(f'{self.cli}The script ran successfully.')
            else:
                print(f'{self.cli}The script encountered an error.')
        print(f'{self.cli}Server bootstrap END')


def main():
    parser = argparse.ArgumentParser(description='ECS Server')
    parser.add_argument('-a', '--address', default='127.0.0.1', help='Server address')
    parser.add_argument('-p', '--port', default='8000', type=int, help='Server port')
    parser.add_argument('-d', '--directory', default='.', type=str, help='Storage directory')
    parser.add_argument('-n', '--num-kvservers', default=2, type=int, help='Number of kvservers')
    # parser.add_argument("-h", "--help", required=True, help="Help")

    args = parser.parse_args()

    ECS(host=args.address,
        port=args.port,
        directory=args.directory,
        num_kvservers=args.num_kvservers)


if __name__ == '__main__':
    main()
