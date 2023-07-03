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
    def __init__(self, log_level, log_file, host, port, directory, num_kvservers):
        self.cli = f'[ECS]>'
        self.host = host
        self.port = port

        # Time parameters
        self.timeout = 25  # Note: It has to be bigger than the kvserver one.
        self.tictac = time.time()

        # Data structures
        self.kvs_connected = {}  # Here we store the sockets of the connected kvservers
        self.kvs_data = {}  # Data to store all the information of the kvs
        self.shutting_down_queue = []
        self.shutting_down = []

        # START
        self.init_log(log_level, log_file, directory)
        self.ecsprint(f'------------------------------------------------------------------')
        self.ecsprint(f'                 ECS server on port {host}:{port}                 ')
        self.ecsprint(f'------------------------------------------------------------------')

        # self.server_bootstrap(num_kvservers)
        self.hash_class = ConsistentHashing()
        self.listen_to_kvservers()

        self.ecsprint(f'------------------------------------------------------------------')
        self.ecsprint(f'                         Closing ECS                              ')
        self.ecsprint(f'------------------------------------------------------------------')
        exit(0)

    def listen_to_kvservers(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.settimeout(self.timeout)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.host, self.port))
        server_socket.listen()
        # server_socket.setblocking(0)
        self.ecsprint(f'Listening on {self.host}:{self.port}')
        self.heartbeat(broadcast=False)
        try:
            while True:
                readable, _, _ = select.select([server_socket] + list(self.kvs_connected.keys()), [], [], 10)
                for sock in readable:
                    if sock is server_socket:
                        kv_sock, kv_addr = server_socket.accept()
                        self.ecsprint(f'New kvserver connected: {kv_addr}')
                        self.kvs_connected[kv_sock] = {'addr': kv_addr,
                                                       'sock': kv_sock}
                        self.heartbeat()
                    else:
                        try:
                            data = sock.recv(128 * 1024).decode()
                            if data:
                                self.handle_RECV(data, sock)
                                self.heartbeat()
                            else:
                                self.ecsprint(f'Socket => No data. Disconnected KVS{self.kv_id(sock)} --> Closing socket', log='e')
                                self.closing_kvserver(sock)
                        except Exception as e:
                            self.ecsprint(f'Exception Socket: {e} --> Closing socket', log='e')
                            self.closing_kvserver(sock)
                if (time.time() - self.tictac) >= self.timeout:
                    self.ecsprint(f'Tic tac Time out. Stop listening', log='e')
                    break
                # else:
                    # self.ecsprint(f'Waiting... Active kvservers: [{len(self.kvs_connected)}]')

            server_socket.close()
        except KeyboardInterrupt:
            server_socket.close()


    def handle_RECV(self, data, sock):
        messages = data.replace('\\r\\n', '\r\n').split('\r\n')
        for msg in messages:
            if msg is None or msg == " " or not msg:
                break
            else:
                self.handle_REQUEST(msg, sock)

    def handle_REQUEST(self, msg, sock):
        try:
            parsedata = json.loads(msg)
            request = parsedata.get('request')
            self.ecsprint(f'\tReceived message from KVS{self.kv_id(sock)}: {request}')
            # if request != 'heartbeat':
            #     pass
            # REQUESTS
            if request == 'kvserver_data':  # New kvserver sending personal data
                self.broadcast('write_lock_act')
                data = parsedata.get('data', {})
                id = data.get('id')
                # Storing data
                self.kvs_data[id] = { #todo CAREFULL IF IDS are all equal!!
                    'id': id,
                    'name': f'kvserver{id}',
                    'host': data.get('host'),
                    'port': data.get('port'),
                    'sock': sock,
                    'active': True
                }
                self.hash_class.new_node(self.kvs_data, id, self.handle_json_REPLY, self.ecsprint)
                self.ecsprint(f'New kvserver{id}!, Current ids:{list(self.kvs_data)}')
                self.ecsprint(f'Added. Connected[{len(self.kvs_connected.keys())}]|Ring nodes:{len(self.hash_class.RING_metadata)}')
                self.broadcast('ring_metadata')
            elif request == 'heartbeat':
                pass
            elif request == 'ring_metadata':
                self.broadcast('ring_metadata')
            elif request == 'kvserver_shutdown':
                data = parsedata.get('data', {}) #Todo is kvserver is not in the ring
                if self.shutting_down is None or self.shutting_down == []:
                    self.broadcast('write_lock_act')
                    self.hash_class.remove_node(self.kvs_data, data['id'], sock, self.handle_json_REPLY, self.ecsprint)
                    self.broadcast('ring_metadata')
                    self.shutting_down.append(data['id'])
                else:
                    self.shutting_down_queue.append((data['id'], sock))
                    self.ecsprint(f'WAITING there is a kvserver shutting down')
            elif request == 'kvserver_shutdown_now': #Todo
                data = parsedata.get('data', {})
                self.shutting_down.remove(data['id'])
                sock.close()
                del self.kvs_connected[sock]
            else:
                self.ecsprint(f'error unknown command!')
            if self.shutting_down_queue and self.shutting_down == []:
                print('Cleaning shutdown queue')
                id = self.shutting_down_queue[0][0]
                socket = self.shutting_down_queue[0][1]
                del self.shutting_down_queue[0]
                message =  {
                    'request': 'kvserver_shutdown',
                    'data': {'id': id}
                }
                self.handle_REQUEST(self, message, socket)

        except Exception as e:
            self.ecsprint(f'Error handling request\parsing JSON: {str(e)}', log='e')

    def broadcast(self, request):
        for sock in self.kvs_connected:
            self.handle_json_REPLY(sock, request)

    def handle_REPLY(self, response, sock):
        self.ecsprint(f'Normal Message sent: {response}')
        sock.sendall(bytes(f'{response}\r\n', encoding='utf-8'))

    def handle_json_REPLY(self, sock, request, data=None):
        try:
            # if request != 'heartbeat' and request != 'write_lock_act':
            self.ecsprint(f'MSG sent to {self.kv_id(sock)}: {request}')
            json_data = json.dumps(self.REPLY_templates(request, data))
            sock.sendall(bytes(f'{json_data}\r\n', encoding='utf-8'))
        except Exception as e:
            self.ecsprint(f'Error while sending request {request}: {e}', log='e')

    def REPLY_templates(self, request, data):
        if request == 'kvserver_data':
            return {
                'request': 'kvserver_data'
            }
        elif request == 'ring_metadata':
            return {
                'request': 'ring_metadata',
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
        elif request == 'heartbeat':
            return {
                'request': 'heartbeat'
            }
        elif request == 'arrange_ring':
            return {
                'request': 'arrange_ring',
                'data': data
            }
        else:
            self.ecsprint(f'Message templates. Request not found:{request}')

    def heartbeat(self, broadcast=True):
        self.tictac = time.time()
        # print(f'Time restarted: {self.tictac -time.time()}')
        if broadcast is True:
            self.broadcast('heartbeat')

    def kv_id(self, sock):
        try:
            for key, values in self.kvs_data.items():
                if values['sock'] == sock:
                    return key
        except:
            self.ecsprint(f'Error kv_id  {sock.getpeername()}')
            return sock.getpeername()


    def closing_kvserver(self, sock): # Closing without backup process
        print('Closing kvserver...')
        print('\t Searching for id')
        self.broadcast('write_lock_act')
        for id, values in self.kvs_data.items():
            if values['sock'] == sock:
                id = values['id']
                break
        print('\t If kvserver as node in ring, delete it and updating the ring')
        if self.kvs_data[id]['hash_key'] in self.hash_class.RING_metadata:
            del self.hash_class.RING_metadata[self.kvs_data[id]['hash_key']]
            self.hash_class.update_ring_intervals(self.kvs_data)
        else:
            print('\tNot found as node in the ring. Closing socket')
        sock.close()
        del self.kvs_connected[sock]
        self.broadcast('ring_metadata')


    def ecsprint(self, *args, log='d'):
        message = ' '.join(str(arg) for arg in args)
        message = self.cli + message
        if log == 'd':
            self.log.debug(message)
        if log == 'i':
            self.log.info(message)
        if log == 'e':
            self.log.error(message)

    def init_log(self, log_level, log_file, directory):
        if directory == '.':
            directory = os.getcwd()  # Use current path
        os.makedirs(directory, exist_ok=True)
        log_dir = os.path.join(directory, log_file)
        if log_level == 'INFO':
            logging.basicConfig(filename=log_dir,
                                filemode='w',
                                level=logging.DEBUG,
                                format='%(asctime)s - INFO - %(message)s')
        elif log_level == 'DEBUG' or log_level == 'FINEST' or log_level == 'ALL':
            logging.basicConfig(filename=log_dir,
                                filemode='w',
                                level=logging.DEBUG,
                                format='%(asctime)s - %(levelname)s - %(message)s')
        self.log = logging.getLogger(__name__)
        stream_handler = logging.StreamHandler()
        self.log.addHandler(stream_handler)

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
                                    'host': addr}
            except Exception as e:
                self.ecsprint(f'Error starting server {n}: {str(e)}', log='e')
        self.ecsprint(f'Server bootstrap done. Number of server started: {len(self.kvs_data)}/{num_kvservers}')


def main():
    parser = argparse.ArgumentParser(description='ECS Server')
    parser.add_argument('-l', '--log-file', default='log.log', help='Log file')
    parser.add_argument('-ll', '--log-level', default='DEBUG', type=str, help='Log level:DEBUG or INFO')
    parser.add_argument('-d', '--directory', default='.', type=str, help='Storage directory')
    parser.add_argument('-a', '--address', default='0.0.0.0', help='Server address')
    parser.add_argument('-p', '--port', default='5002', type=int, help='Server port')
    parser.add_argument('-n', '--num-kvservers', default=2, type=int, help='Number of kvservers')

    # parser.add_argument('-h', '--help', required=True, help='Help')

    args = parser.parse_args()
    # data, unknown = parser.parse_known_args()
    # print(f'Commands: {data}')

    ECS(log_level=args.log_level,
        log_file=args.log_file,
        host=args.address,
        port=args.port,
        directory=args.directory,
        num_kvservers=args.num_kvservers)


if __name__ == '__main__':
    main()
