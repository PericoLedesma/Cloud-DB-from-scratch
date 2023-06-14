from hashing_class import *
import subprocess
import time
import argparse
import socket
import select
import json
import os
import logging

os.system('cls' if os.name == 'nt' else 'clear')


class ECS:
    def __init__(self,log_level, log_file, host, port, directory, num_kvservers):
        self.cli = f'[ECS]>'
        self.host = host
        self.port = port

        # Time parameters
        self.timeout = 15  # Note: It has to be bigger than the kvserver one.
        self.tictac = time.time()

        # Data structures
        self.kvs_connected = {}  # Here we store the sockets of the connected kvservers
        self.kvs_data = {}  # Data to store all the information of the kvs

        # START
        self.init_log(log_level, log_file, directory)
        self.ecsprint(f' ==> Active ECS server on port {host}:{port}')
        # self.server_bootstrap(num_kvservers)
        self.hash_class = ConsistentHashing(self.kvs_data)
        self.listen_to_kvservers()
        self.ecsprint(f'Closing ECS')
        exit(0)

    def listen_to_kvservers(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.settimeout(5)
        # server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.host, self.port))
        server_socket.listen()
        # server_socket.setblocking(0)
        self.ecsprint(f'Listening on {self.host}:{self.port}')
        self.heartbeat()
        try:
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
                                # Todo we can lose data -> self.hash_class.remove_node() reorganize
                                sock.close()
                                del self.kvs_connected[sock]
                        except Exception as e:
                            self.ecsprint(f'Exception: {e} --> Closing socket', c='r')
                            # Todo we can lose data -> self.hash_class.remove_node() reorganize
                            sock.close()
                            del self.kvs_connected[sock]
                if (time.time() - self.tictac) >= self.timeout:
                    self.ecsprint(f'Time out.Stop listening')
                    break
                else:
                    self.ecsprint(f'Waiting... Active kvservers:{len(self.kvs_connected)}')
            server_socket.close()
        except KeyboardInterrupt:
            print("Closing ECS server")
            server_socket.close()

    def handle_RECV(self, data, sock):
        messages = data.replace('\\r\\n', '\r\n').split('\r\n')
        for msg in messages:
            if msg is None or msg == " " or not msg:
                break
            else:
                self.handle_REQUEST(msg, sock)

    def handle_REQUEST(self, msg, sock):
        self.ecsprint(f'Received message from {self.kv_id(sock)}: {repr(msg)}')
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
                    self.ecsprint(f'New kvserver{id}!, Current ids:{list(self.kvs_data)}', c='r')
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

                self.ecsprint(
                    f'Connected socket = {len(self.kvs_connected.keys())}| Ring nodes: {len(self.hash_class.RING_metadata)}')
                if len(self.hash_class.RING_metadata) == len(self.kvs_connected.keys()):
                    self.broadcast('ask_ring_metadata')
                    self.broadcast('write_lock_deact')
                else:
                    self.ecsprint(f'Waiting all kvserver connected to be added to ring')
                    self.broadcast('write_lock_act')

            elif request == 'heartbeat':
                self.heartbeat()
            elif request == 'kvserver_shutdown':
                self.broadcast('write_lock_act')

                kvdata = parsedata.get('data', {})
                message = self.hash_class.remove_node(kvdata)
                self.handle_json_REPLY(sock, f'reorganize_ring', message)


            else:
                self.ecsprint(f'error unknown command!', c='r')

        except Exception as e:
            self.ecsprint(f'Error handling request\parsing JSON: {str(e)}', c='r')
            self.handle_REPLY(f'{self.cli}Message received: {msg}', sock)




    def broadcast(self, request):
        for sock in self.kvs_connected:
            self.handle_json_REPLY(sock, request)

    def handle_REPLY(self, response, sock):
        sock.sendall(bytes(f'{response}\r\n', encoding='utf-8'))

    def handle_json_REPLY(self, sock, request, *args):
        try:
            json_data = json.dumps(self.REPLY_templates(request, args))
            sock.sendall(bytes(f'{json_data}\r\n', encoding='utf-8'))
        except Exception as e:
            self.ecsprint(f'Error while sending the data: {e}', c='r')

    def REPLY_templates(self, request, args):
        if request == 'kvserver_data':
            return {
                'request': 'kvserver_data'
            }
        elif request == 'ask_ring_metadata':
            return {
                'request': 'ask_ring_metadata',
                'data': self.hash_class.RING_metadata
            }
        elif request == 'write_lock_act':
            return {
                'request': 'write_lock_act'
            }
        elif request == 'write_lock_deact':
            return {
                'request': 'write_lock_deact'
            }
        elif request == 'reorganize_ring':
            return {
                'request': 'reorganize_ring',
                'data': args
            }
        else:
            self.ecsprint(f'{self.cli}Message templates. Request not found:{request}')

    def heartbeat(self):
        self.tictac = time.time()
        # self.ecsprint(f'Pum pum', c='g')

    def kvserver_shutdown(self, id, kvdata):
        id = kvdata.get('id')
        previous_hash = kvdata.get('previous_hash')
        hash_key = kvdata.get('hash_key')

    def kv_id(self, sock):
        try:
            for key, values in self.kvs_data.items():
                if values['sock'] == sock:
                    return values['name']
        except:
            print('HERE', sock.getpeername())
            return sock.getpeername()

    def ecsprint(self, *args, c=None, log='d'):
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

        if log == 'd':
            self.log.debug(f'{self.cli}{message}')
        if log == 'i':
            self.log.info(f'{self.cli}{message}')

    def init_log(self, log_level, log_file, directory):
        # if directory is None or directory == '.':
        #     # self.directory = f'ecs'

        if directory == '.':
            directory = os.getcwd()  # Use current path
        os.makedirs(directory, exist_ok=True)
        log_dir = os.path.join(directory, log_file)
        if log_level == 'INFO' or log_level == 'ALL':
            logging.basicConfig(filename=log_dir,
                                filemode='w',
                                level=logging.INFO,
                                format='%(asctime)s - %(levelname)s - %(message)s')
        elif log_level == 'DEBUG':
            logging.basicConfig(filename=log_dir,
                                filemode='w',
                                level=logging.DEBUG,
                                format='%(asctime)s - %(levelname)s - %(message)s')
        self.log = logging.getLogger(__name__)

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
    parser.add_argument('-l', '--log-file', default='ecs.log', help='Log file')
    parser.add_argument('-ll', '--log-level', default='DEBUG', help='Log level:DEBUG or INFO')
    parser.add_argument('-d', '--directory', default='.', type=str, help='Storage directory')
    parser.add_argument('-a', '--address', default='127.0.0.1', help='Server address')
    parser.add_argument('-p', '--port', default='5002', type=int, help='Server port')
    parser.add_argument('-n', '--num-kvservers', default=2, type=int, help='Number of kvservers')

    # parser.add_argument('-h', '--help', required=True, help='Help')

    args = parser.parse_args()
    # args, unknown = parser.parse_known_args()


    ECS(log_level=args.log_level,
        log_file=args.log_file,
        host=args.address,
        port=args.port,
        directory=args.directory,
        num_kvservers=args.num_kvservers)


if __name__ == '__main__':
    main()
