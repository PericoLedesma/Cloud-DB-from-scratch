import subprocess
import os
import time
import argparse
import socket
import select
import time

class ECS:
    def __init__(self, host, port, directory, num_kvservers):
        print(f'Starting ECS server on port[{host}:{port}]')

        self.cli = f'\tECS>'
        self.host = host
        self.port = port

        self.num_kvservers = num_kvservers
        self.kvserver_list = []


        self.server_bootstrap()
        self.listen_to_connections()



    def listen_to_connections(self):
        time.sleep(10)
        print(f'{self.cli }Listining to kvservers...')
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.settimeout(5)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.host, self.port))
            server.listen()

            print(f'{self.cli}Listening on {self.host}:{self.port}')

            while True:
                ready_sockets, _, error_sockets = select.select([server], [], [server], 3)
                print(f'{self.cli }ready_sockets >{ready_sockets}')
                print(f'{self.cli }error_sockets >{error_sockets}')

                if ready_sockets:
                    for sock in ready_sockets:
                        print(f'{self.cli}Socket >{sock}')
                        if sock in self.kvserver_list:
                            print(f'{self.cli}Socket > already connnected')
                            try:
                                data = sock.recv(1024)
                                if data:
                                    print(f'{self.cli }Received', data)
                                    response = "Response"
                                    sock.sendall(response.encode())
                            except sock.error as e:
                                print("Socket error:", e)
                        else:
                            print(f'{self.cli}Socket >{sock} NOT connnected')
                            client_socket, addr = sock.accept()
                            print(f'{self.cli }Connection accepted:, {addr}')
                            self.kvserver_list.append(client_socket)
                            try:
                                data = sock.recv(1024)
                                if data:
                                    print(f'{self.cli }Received', data)
                                    response = "Response"
                                    sock.sendall(response.encode())
                            except sock.error as e:
                                print("Socket error:", e)


    def server_bootstrap(self):
        print(f'{self.cli }Server bootstrap...')
        # Paths
        current_dir = os.path.abspath(os.path.dirname(__file__))
        parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
        script_path = os.path.join(parent_dir, 'kvserver', 'kvserver.py')

        port = 8000

        bootstrap = f'{self.host}:{self.port}'
        print(f'{self.cli }ECS ADDRESS {bootstrap}')

        for n in range(self.num_kvservers):
            print(f'{self.cli }Starting kvserver {n}')
            command = ['python', script_path, f'-i {n}', f'-p {port+n}', f'-b {bootstrap}']

            result = subprocess.Popen(command)

            if result.returncode == 0:
                print(f'{self.cli }The script ran successfully.')
            else:
                print(f'{self.cli }The script encountered an error.')
        print(f'{self.cli}Server bootstrap END')



def main():
    parser = argparse.ArgumentParser(description='ECS Server')
    parser.add_argument('-a', '--address', default='127.0.0.1', help='Server address')
    parser.add_argument('-p', '--port', default='8000', type=int, help='Server port')
    parser.add_argument('-d', '--directory', default='.', type=str, help='Storage directory')
    parser.add_argument('-n', '--num-kvservers', default=1, type=int, help='Number of kvservers')
    # parser.add_argument("-h", "--help", required=True, help="Help")

    args = parser.parse_args()

    ECS(host=args.address,
          port=args.port,
          directory=args.directory,
          num_kvservers=args.num_kvservers)


if __name__ == '__main__':
    main()