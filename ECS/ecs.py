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

        self.tictac = time.time()
        self.timeout = 30  # Note: It has to be bigger than the kvserver one.

        self.kvs_connected = {}  # Here we store the sockets of the connected kvservers
        self.kvs_data = {}  # Data to store all the information of the kvs

        self.server_bootstrap(num_kvservers)
        self.hash_class = ConsistentHashing(self.kvs_data)

        thread = threading.Thread(target=self.listen_to_kvservers())
        thread.start()

        self.ecsprint(f'Closing ECS')
        exit(0)

    def listen_to_kvservers(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.settimeout(5)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.host, self.port))
        server_socket.listen()
        # server_socket.setblocking(0)
        self.ecsprint(f'Listening on {self.host}:{self.port}')
        self.heartbeat()

        while True:
            readable, _, _ = select.select([server_socket] + list(self.kvs_connected.keys()), [], [], 5)
            for sock in readable:
                if sock is server_socket:
                    kv_sock, kv_addr = server_socket.accept()
                    self.ecsprint(f'New kvserver connected: {kv_addr}')
                    self.handle_json_REPLY(kv_sock, 'kvserver_data')  # Asking for kv_data. We need to know the host

                    self.kvs_connected[kv_sock] = {'addr': kv_addr,
                                                   'sock': kv_sock,
                                                   'last_activity': time.time()}
                    self.heartbeat()
                else:
                    try:
                        data = sock.recv(128 * 1024).decode()
                        if data:
                            self.ecsprint(f'Received data from {self.kv_id(sock)}')
                            self.handle_RECV(data, sock)
                            self.heartbeat()
                        else:
                            self.ecsprint(f'No data. Disconnected {self.kv_id(sock)} --> Closing socket', c='r')
                            sock.close()
                            del self.kvs_connected[sock]
                    except Exception as e:
                        self.ecsprint(f'Exception: {e} --> Closing socket', c='r')
                        sock.close()
                        del self.kvs_connected[sock]
            if (time.time() - self.tictac) >= self.timeout:
                self.ecsprint(f'Time out.Stop listening')
                break
            else:
                self.ecsprint(f'Waiting... Active kvservers:{len(self.kvs_connected)}')
        server_socket.close()

    def handle_RECV(self, data, sock):
        messages = data.replace('\\r\\n', '\r\n').split('\r\n')
        for msg in messages:
            if msg is None or msg == " " or not msg:
                break
            else:
                self.ecsprint(f'Received message: {repr(msg)}')
                try:
                    parsedata = json.loads(msg)
                    request = parsedata.get('request')
                    if request == 'kvserver_data':  # Welcome msg
                        data = parsedata.get('data', {})
                        id = data.get('id')
                        host = data.get('host')
                        port = data.get('port')
                        if id in self.kvs_data:
                            if self.kvs_data[id]['port'] != port or \
                                    self.kvs_data[id]['host'] != host:
                                print('ERROR. Metadata of kvserver doesnt match')
                        else:
                            self.ecsprint(f'New kvserver!, Current ids:[{list(self.kvs_data)}]|New:[{id}]', c='r')
                            self.kvs_data[id] = {
                                'id': id,
                                'name': f'kvserver{id}',
                                'host': host,
                                'port': port
                            }
                        self.kvs_data[id]['sock'] = sock
                        self.kvs_data[id]['active'] = True
                        self.kvs_connected[sock] = {'addr': sock.getpeername(),
                                                    'sock': sock,
                                                    'last_activity': time.time()}
                        self.hash_class.new_node(self.kvs_data[id], host, port)
                        print(
                            f'Connected socket = {len(self.kvs_connected.keys())}| Ring nodes: {len(self.hash_class.RING_metadata)}')
                        if len(self.hash_class.RING_metadata) == len(self.kvs_connected.keys()):
                            self.broadcast('ring_metadata')
                        else:
                            print('Waiting all kvserver connected to be added to ring')
                    elif request == 'heartbeat':
                        self.heartbeat()
                    else:
                        self.ecsprint(f'error unknown command!', c='r')

                except Exception as e:
                    self.ecsprint(f'Error handling request\parsing JSON: {str(e)}', c='r')
                    self.handle_REPLY(f'{self.cli}Message received: {msg}', sock)

    def broadcast(self, request):
        self.ecsprint(f'Broadcasting [{request}]...  ')
        for sock in self.kvs_connected:
            self.handle_json_REPLY(sock, request)

    def handle_REPLY(self, response, sock):
        sock.sendall(bytes(f'{response}\r\n', encoding='utf-8'))

    def handle_json_REPLY(self, sock, request):
        try:
            json_data = json.dumps(self.REPLY_templates(request))
            sock.sendall(bytes(f'{json_data}\r\n', encoding='utf-8'))
        except Exception as e:
            self.ecsprint(f'Error while sending the data: {e}', c='r')

    def REPLY_templates(self, request):
        if request == 'kvserver_data':
            return {
                'request': 'kvserver_data'
            }
        elif request == 'ring_metadata':
            list_kvservers = []
            for key, value in self.hash_class.RING_metadata.items():
                for id, server in self.kvs_data.items():
                    if value[0] == server['host'] and value[1] == server['port']:
                        list_kvservers.append(id)
                        continue
            self.ecsprint(f'Sending ring to {list_kvservers} . Number of kv_servers in ring: {len(self.hash_class.RING_metadata)}')
            return {
                'request': 'ring_metadata',
                'data': self.hash_class.RING_metadata
            }
        else:
            self.ecsprint(f'{self.cli}Message templates. Request not found:{request}')

    def heartbeat(self):
        self.tictac = time.time()
        # self.ecsprint(f'Pum pum', c='g')

    def kv_id(self, sock):
        try:
            for key, values in self.kvs_data.items():
                if values['sock'] == sock:
                    return values['name']
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

    def server_bootstrap(self, num_kvservers):
        current_dir = os.path.abspath(os.path.dirname(__file__))
        parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
        script_path = os.path.join(parent_dir, 'kvserver', 'kvserver.py')

        port = 3000  # todo
        addr = '127.0.0.1'
        bootstrap = f'{self.host}:{self.port}'

        self.ecsprint(f'Starting kvservers... Bootstrap addr:{bootstrap}')
        for n in range(num_kvservers):
            try:
                command = ['python',
                           script_path,
                           f'-i {n}',
                           f'-b {bootstrap}',
                           f'-a {addr}',
                           f'-p {(port + n)}'
                           ]
                result = subprocess.Popen(command)
                self.kvs_data[n] = {'id': n,
                                    'name': f'kvserver{n}',
                                    'port': (port + n),
                                    'host': addr,
                                    'active': False}
            except Exception as e:
                self.ecsprint(f'Error starting server {n}: {str(e)}')
            # if result.returncode == 0:
            #     self.ecsprint(f'The script ran successfully.')
            # else:
            #     self.ecsprint(f'The script encountered an error.')
        self.ecsprint(f'Server bootstrap done. Number of server started: {len(self.kvs_data)}/{num_kvservers}')


def main():
    parser = argparse.ArgumentParser(description='ECS Server')
    parser.add_argument('-a', '--address', default='127.0.0.1', help='Server address')
    parser.add_argument('-p', '--port', default='7000', type=int, help='Server port')
    parser.add_argument('-d', '--directory', default='.', type=str, help='Storage directory')
    parser.add_argument('-n', '--num-kvservers', default=2, type=int, help='Number of kvservers')
    # parser.add_argument('-h', '--help', required=True, help='Help')

    args = parser.parse_args()

    ECS(host=args.address,
        port=args.port,
        directory=args.directory,
        num_kvservers=args.num_kvservers)


if __name__ == '__main__':
    main()
