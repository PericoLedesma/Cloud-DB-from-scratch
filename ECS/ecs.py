# from hashing_function import *
import subprocess
import os
import time
import argparse
import socket
import select
import hashlib
import threading
import json

class ECS:
    def __init__(self, host, port, directory, num_kvservers):
        print(f' ==> ECS server on port[{host}:{port}]')

        self.cli = f'[ECS]>'
        self.host = host
        self.port = port


        self.kv_sockets = []
        self.kv_sockets_e = []

        self.timeout = 20


        self.num_kvservers = num_kvservers
        self.server_names = {}



        self.server_bootstrap()

        thread = threading.Thread(target=self.listen_to_kvservers())
        thread.start()

        print('-----lines continuees---')

        # time.sleep(5)
        # print(f'{self.cli}Sending msg')
        # for sock in self.kv_sockets:
        #     self.handle_RESPONSE(f'Hey kvserver', sock)
        #
        # time.sleep(10)
        # print(f'{self.cli}Sending msg')
        # for sock in self.kv_sockets:
        #     self.handle_RESPONSE(f'Hey KVSERVER', sock)

        print(f'{self.cli}Closing ECS')


    def handle_RESPONSE(self, response, sock):
        sock.sendall(bytes(response, encoding='utf-8'))


    def listen_to_kvservers(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            # server.settimeout(5)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.host, self.port))
            server.listen()
            print(f'{self.cli}Listening on {self.host}:{self.port}')
            start_time = time.time()

            while True:
                time.sleep(2)
                print(f'{self.cli}->Listening..... KVservers:{self.kv_sockets}')
                readable, writable, errors = select.select([server] + self.kv_sockets, [], self.kv_sockets_e, 10)
                print(f'{self.cli}Kvservers:{len(self.kv_sockets)}|Readable:{len(readable)}|Writable:{len(writable)}|Errors:{len(errors)}')

                for sock in readable:
                    if sock is server:
                        conn, addr = sock.accept()
                        print(f'{self.cli}Connection accepted: {addr}')
                        self.kv_sockets.append(conn)
                        self.handle_REQUEST(conn)
                        print(f'{self.cli}Timeout restarted')
                        start_time = time.time()

                    elif sock == self.kv_sockets:
                        print(f'{self.cli}Socket from kvstore store')
                        try: self.handle_REQUEST(sock)
                        except: self.kv_sockets.remove(sock)
                        print(f'{self.cli}Timeout restarted')
                        start_time = time.time()

                    else:
                        print(f'{self.cli}Socket checked conn of socket out of list.')
                        try:
                            if sock.getpeername() is not None:
                                self.handle_REQUEST(sock)
                                print(f'{self.cli}Timeout restarted')
                                start_time = time.time()
                            else:
                                raise Exception('No connection. Delete socket')
                        except Exception as e:
                            print(f'{self.cli}Exception outside: {e}. Closing socket')
                            sock.close()

                for s in self.kv_sockets:
                    if sock.fileno() < 0:
                        self.kv_sockets.remove(sock)
                        print(f'{self.cli}Deleted socket from list of kvservers')

                if (time.time() - start_time) >= self.timeout:
                    print(f'{self.cli}Time out.Stop listening')
                    break


    def handle_REQUEST(self, sock):
        print(f'{self.cli}Handling the request')
        try:
            data = sock.recv(128 * 1024).decode()
            if data:
                recv_data = json.loads(data)
                formatted_json = json.dumps(recv_data, indent=4)
                print(f'{self.cli}Received message: {formatted_json}')

            else:
                print(f'{self.cli}No data')
                raise Exception('Error while handling the request. No data. Closing socket as')

        except ConnectionResetError:
            raise Exception('Error while handling the request. Connection reset by peer')
        except:
            raise Exception('Error while handling the request.')

    def store_kvserver(self):
        my_dict = {('id', '1'): {'name': f'kvserver' , 'vnodes':[list]}}

        # for num in range(0, num + 1):
        #     key = ('id', f'kvserver{num}')
        #     map_to_ring()
        #     value = ('nodes',))

    def server_bootstrap(self):
        print(f'{self.cli}Server bootstrap...')
        # Paths
        current_dir = os.path.abspath(os.path.dirname(__file__))
        parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
        script_path = os.path.join(parent_dir, 'kvserver', 'kvserver.py')

        port = 3000 #todo
        bootstrap = f'{self.host}:{self.port}'

        for n in range(self.num_kvservers):
            print(f'{self.cli}Starting kvserver {n}')
            command = ['python', script_path, f'-i {n}', f'-p {port + n}', f'-b {bootstrap}']

            result = subprocess.Popen(command)

            if result.returncode == 0:
                print(f'{self.cli}The script ran successfully.')
            # else:
            #     print(f'{self.cli}The script encountered an error.')
        print(f'{self.cli}Server bootstrap DONE')



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
