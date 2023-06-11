from hashing_function import *
import subprocess
import os
import time
import argparse
import socket
import select
import hashlib
import threading
import json
import os
os.system('cls' if os.name == 'nt' else 'clear')

class ECS:
    def __init__(self, host, port, directory, num_kvservers):
        print(f' ==> ECS server on port {host}:{port} ')

        self.cli = f'[ECS]>'
        self.host = host
        self.port = port
        self.timeout = 10

        self.num_kvservers = num_kvservers
        self.kvs_inputs = []
        self.kvs_outputs = []

        self.kv_data = {}

        self.server_bootstrap()
        self.hashing_class = ConsistentHashing(self.kv_data, num_kvservers)


        # print('\n')
        # formatted_dict = json.dumps(self.kvdata_part, indent=4)
        # print(formatted_dict)
        # print('\n')

        thread = threading.Thread(target=self.listen_to_kvservers())
        thread.start()

        print('-----lines continuees---')

        # # time.sleep(5)
        # self.ecsprint(f'Sending msg')
        # for sock in self.kvs_inputs:
        #     self.handle_RESPONSE(f' BYE KvSERVER', sock)

        self.ecsprint(f'Closing ECS')


    def listen_to_kvservers(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.settimeout(5)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.host, self.port))
        server_socket.listen()
        # server_socket.setblocking(0)

        inputs = [server_socket]
        self.kv_sockets = {}
        self.ecsprint(f'Listening on {self.host}:{self.port}')
        start_time = time.time()
        while True:
            self.ecsprint(f'Listening...')
            readable, _, _ = select.select(inputs, [], [], 5)
            self.ecsprint(f'Readable:{len(readable)}|inputs:{len(inputs)}')

            for sock in readable:
                if sock is server_socket:
                    kv_sock, kv_addr = server_socket.accept()
                    self.ecsprint(f'New kvserver connected: {kv_addr}')

                    # Storing data
                    self.kv_sockets[kv_sock] = {'addr': kv_addr, 'sock': kv_sock, 'last_activity': time.time()}
                    inputs.append(kv_sock)

                    # Asking for data
                    self.handle_json_RESPONSE(kv_sock, 'kvserver_data')
                    start_time = time.time()

                else:
                    try:
                        data = sock.recv(128 * 1024).decode()
                        if data:
                            self.ecsprint(f'Received data from {self.kv_id(sock)}')
                            self.handle_REQUEST(data, sock)
                            start_time = time.time()
                        else:
                            self.ecsprint(f'No data. Disconnected {self.kv_id(sock)} --> Closing socket', c='r')
                            inputs.remove(sock)
                            sock.close()
                            del self.kv_sockets[sock]

                    except Exception as e:
                        self.ecsprint(f'Exception: {e} --> Closing socket', c='r')
                        sock.close()
                        if sock in inputs:
                            inputs.remove(sock)
                            self.ecsprint(f'\tDeleted socket from list of inputs', c='r')

            if (time.time() - start_time) >= self.timeout:
                self.ecsprint(f'Time out.Stop listening')
                break
        server_socket.close()




    def handle_REQUEST(self, data, sock):
        messages = data.replace('\\r\\n', '\r\n').split('\r\n')
        for msg in messages:
            if msg is None or msg == " " or not msg:
                break
            else:
                self.ecsprint(f'Received message: {repr(msg)}')
                try:
                    parsedata = json.loads(msg)
                    request = parsedata.get('request')
                    # self.ecsprint(f'Request: {repr(request)}')
                    if request == 'kvserver_data':
                        id = parsedata.get('data', {}).get('id')
                        host = parsedata.get('data', {}).get('host')
                        port = parsedata.get('data', {}).get('port')

                        if self.kv_data[id]['port'] != port or \
                                self.kv_data[id]['host'] != host:
                            self.ecsprint(f'Error with data received. Data dont match', c='r')
                        else:
                            self.handle_json_RESPONSE(sock, 'kvserver_hash_key', self.kv_data[id])

                    else:
                        self.ecsprint(f'error unknown command!')

                except Exception as e:
                    self.ecsprint(f'Error handling request\parsing JSON: {str(e)}', c='r')
                    self.handle_RESPONSE(f'{self.cli}Message received: {msg}', sock)


    def handle_RESPONSE(self, response, sock):
        sock.sendall(bytes(f'{response}\r\n', encoding='utf-8'))


    def handle_json_RESPONSE(self, sock, request, kvserver=None):
        try:
            json_data = json.dumps(self.messages_templates(request, kvserver))
            sock.sendall(bytes(f'{json_data}\r\n', encoding='utf-8'))
            # self.ecsprint(f'Resquest sent: [{method}]')
        except Exception as e:
            self.ecsprint(f'Error while sending the data: {e}', c='r')


    def messages_templates(self, request, kvserver):
        if request == 'kvserver_data':
            return {
                'request': 'kvserver_data'
            }
        elif request == 'kvserver_hash_key':
            return {
                'request': 'kvserver_hash_key',
                'data': {
                    'id': kvserver['id'],
                    'name': kvserver['name'],
                    'hash_key': kvserver['hash_key']
                }
            }

        else:
            self.ecsprint(f'{self.cli}Message templates. Request not found:{request}')



    def kv_id(self, sock):
        try:
            for server in self.kv_data.values():
                if server['sock'] == sock:
                    return server['name']
        except:
            return sock.getpeername()


    def ecsprint(self, *args, c=None):
        COLORS = {
            'r': '\033[91m',
            'g': '\033[92m',
            'w': '\033[97m',
            'reset': '\033[0m'
        }
        c = COLORS['w'] if c is None else COLORS[c]

        message = ' '.join(str(arg) for arg in args)
        message = c + self.cli + message + COLORS['reset']
        print(message)

        # if log == 'd':
        #     self.log.debug(f'{self.cli}{message}')
        # if log == 'i':
        #     self.log.info(f'{self.cli}{message}')


    def server_bootstrap(self):
        self.ecsprint(f'Server bootstrap...')

        # Paths
        current_dir = os.path.abspath(os.path.dirname(__file__))
        parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
        script_path = os.path.join(parent_dir, 'kvserver', 'kvserver.py')

        port = 3000 #todo
        addr = '127.0.0.1'

        bootstrap = f'{self.host}:{self.port}'

        self.ecsprint(f'Starting kvservers... Bootstrap addr:{bootstrap}')
        for n in range(self.num_kvservers):

            try:
                command = ['python',
                           script_path,
                           f'-i {n}',
                           f'-b {bootstrap}',
                           f'-a {addr}',
                           f'-p {(port + n)}'
                           ]
                result = subprocess.Popen(command)
                self.kv_data[n] = {'id': n,
                                       'name': f'kvserver{n}',
                                       'port': (port + n),
                                       'host': addr}

            except Exception as e:
                self.ecsprint(f'Error starting server {n}: {str(e)}')

            # if result.returncode == 0:
            #     self.ecsprint(f'The script ran successfully.')
            # else:
            #     self.ecsprint(f'The script encountered an error.')
        self.ecsprint(f'Server bootstrap done. Number of server started: {len(self.kv_data)}/{self.num_kvservers}')



def main():
    parser = argparse.ArgumentParser(description='ECS Server')
    parser.add_argument('-a', '--address', default='127.0.0.1', help='Server address')
    parser.add_argument('-p', '--port', default='7000', type=int, help='Server port')
    parser.add_argument('-d', '--directory', default='.', type=str, help='Storage directory')
    parser.add_argument('-n', '--num-kvservers', default=1, type=int, help='Number of kvservers')
    # parser.add_argument('-h', '--help', required=True, help='Help')

    args = parser.parse_args()

    ECS(host=args.address,
        port=args.port,
        directory=args.directory,
        num_kvservers=args.num_kvservers)


if __name__ == '__main__':
    main()
