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
    def __init__(self, log_level, log_file, host, port, directory):
        self.cli = f'[ECS]>'
        self.host = host
        self.port = port

        # Time parameters
        self.kvs_timeout = 25  # When we declare that a kvserver is not active
        self.ecs_timeout = 35  # To control when to exit while loop.
        self.sock_timeout = 5  # To control the while loop
        self.tictac = time.time()

        # Data structures
        self.kvs_connected = {}  # Here we store the sockets of the connected kvservers
        self.kvs_data = {}  # Data to store all the information of the kvs
        self.shutting_down_queue = []
        self.shuttingdown_kvservers = []

        # START
        self.init_log(log_level, log_file, directory)
        self.ecsprint(f'{"-" * 60}')
        self.ecsprint(f'{" " * 20}ECS server on port {host}:{port} ')
        self.ecsprint(f'{"-" * 60}')

        self.hash_class = ConsistentHashing(self.ecsprint)
        self.listen_to_kvservers()

        self.ecsprint(f'{"-" * 60}')
        self.ecsprint(f'{" " * 20}Stopping ECS')
        self.ecsprint(f'{"-" * 60}')
        exit(0)

    def listen_to_kvservers(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.settimeout(self.sock_timeout)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((self.host, self.port))
        server_socket.listen()
        # server_socket.setblocking(0)
        self.ecsprint(f'Listening to KVServers at {self.host}:{self.port}')
        self.heartbeat()
        try:
            while True:
                readable, _, _ = select.select([server_socket] + list(self.kvs_connected.keys()), [], [],
                                               self.sock_timeout)
                for sock in readable:
                    if sock is server_socket:
                        kv_sock, kv_addr = server_socket.accept()
                        self.ecsprint(f'++ New socket connected: {kv_addr}. Waiting to receive KVS data...')
                        self.kvs_connected[kv_sock] = {'addr': kv_addr,
                                                       'sock': kv_sock}
                        self.heartbeat()
                    else:
                        try:
                            data = sock.recv(128 * 1024).decode()  # Todo endwith(\r\n)
                            if data:
                                self.handle_RECV(data, sock)
                            else:
                                self.ecsprint(f'Error. No data --> Closing socket KVS{self.kv_id(sock)}', log='e')
                                self.closing_kvserver(sock)
                        except Exception as e:
                            self.ecsprint(f'Exception recv: {e} --> Closing socket', log='e')
                            self.closing_kvserver(sock)

                if self.shutting_down_queue and self.shuttingdown_kvservers == []:
                    self.ecsprint(f'Cleaning shutdown queue...')
                    id = self.shutting_down_queue[0][0]
                    socket_closing = self.shutting_down_queue[0][1]
                    del self.shutting_down_queue[0]
                    message = {
                        'request': 'starting_shutdown_process',
                        'data': {'id': id}
                    }
                    self.handle_REQUEST(socket_closing, 'starting_shutdown_process', message)

                if (time.time() - self.tictac) >= self.ecs_timeout and self.check_active_kvservers() is False:
                    self.ecsprint(f'Tic tac, Time out and no kvs actives. Stop listening --> Closing ECS', log='e')
                    break
            self.broadcast('shutdown_ecs')
            server_socket.close()
        except KeyboardInterrupt:
            self.broadcast('shutdown_ecs')
            server_socket.close()

    def handle_RECV(self, data, sock):
        messages = data.replace('\\r\\n', '\r\n').split('\r\n')
        for msg in messages:
            if msg is None or msg == " " or not msg:
                break
            else:
                try:
                    parsedata = json.loads(msg)
                    request = parsedata.get('request')
                    self.handle_REQUEST(sock, request, parsedata)
                except Exception as e:
                    self.ecsprint(f'Error handling request\parsing JSON of KVS{self.kv_id(sock)}: {str(e)}', log='e')

    def handle_REQUEST(self, sock, request, parsedata):
        if request != 'heartbeat':
            self.ecsprint(f'\tReceived message from KVS{self.kv_id(sock)}: {request}')
        # REQUESTS
        if request == 'kvserver_data':  # New kvserver sending personal data
            self.broadcast('write_lock_act')
            data = parsedata.get('data', {})
            id = data.get('id')
            # Storing data
            self.kvs_data[id] = {  # todo CAREFULL IF IDS are all equal!!
                'id': id,
                'name': f'kvserver{id}',
                'host': data.get('host'),
                'port': data.get('port'),
                'sock': sock,
                'active': False  # If it has clients, last active heartbeat
            }
            self.hash_class.new_node(self.kvs_data, id, self.handle_json_REPLY)
            self.ecsprint(f'++ New kvserver{id} saved!, Current ids:{list(self.kvs_data)}')
            self.ecsprint(
                f'Num. KVS con.[{len(self.kvs_connected.keys())}]|Num. Ring nodes:{len(self.hash_class.ring_coordinators)}')
            self.broadcast('ring_metadata', f'-> trigger {request}')
        elif request == 'heartbeat':
            id = self.kv_id(sock)
            data = parsedata.get('data', {})
            if data.get('active'):
                self.kvs_data[id]['active'] = time.time()
                self.heartbeat()
        elif request == 'ring_metadata':
            self.broadcast('ring_metadata', f'-> trigger {request}')
        elif request == 'starting_shutdown_process':
            data = parsedata.get('data', {})
            if data['id'] in self.kvs_data.keys():
                if self.shuttingdown_kvservers is None or self.shuttingdown_kvservers == []:
                    self.broadcast('write_lock_act')
                    self.hash_class.remove_node(self.kvs_data, data['id'], sock, self.handle_json_REPLY)
                    self.broadcast('ring_metadata', f'-> trigger {request}')
                    self.shuttingdown_kvservers.append(data['id'])
                else:
                    self.shutting_down_queue.append((data['id'], sock))
                    self.ecsprint(f'WAITING there is a kvserver shutting down')
            else:
                self.ecsprint(f'Error. KVSERVER{data["id"]} not found ')
        elif request == 'kvserver_shutdown':
            data = parsedata.get('data', {})
            if data['id'] in self.shuttingdown_kvservers:
                self.shuttingdown_kvservers.remove(data['id'])
                self.ecsprint(f'Removing kvserver from shutting down ')
            else:
                self.ecsprint(f'Error. Kvserver not in shutdown process. CHECK ')
            sock.close()
            del self.kvs_connected[sock]
        else:
            self.ecsprint(f'error unknown command!')

    def broadcast(self, request, data=None):
        self.ecsprint(f'Broadcasting {request} {data}')
        for sock in self.kvs_connected:
            self.handle_json_REPLY(sock, request, data)

    def handle_REPLY(self, response, sock):
        self.ecsprint(f'Normal Message sent: {response}')
        sock.sendall(bytes(f'{response}\r\n', encoding='utf-8'))

    def handle_json_REPLY(self, sock, request, data=None):
        MAX_BYTES = 128 * 1024  # 128 kilobytes in bytes #TODO

        # else:
        #     # If the message size exceeds the limit, split it into multiple messages
        #     num_chunks = (message_length + MAX_BYTES_TO_SEND - 1) // MAX_BYTES_TO_SEND
        #     for i in range(num_chunks):
        #         start = i * MAX_BYTES_TO_SEND
        #         end = min((i + 1) * MAX_BYTES_TO_SEND, message_length)
        #         chunk = message_bytes[start:end]
        #         sock.sendall(chunk)

        try:

            json_data = json.dumps(self.REPLY_templates(request, data))
            message_bytes = bytes(f'{json_data}\r\n', encoding='utf-8')

            if len(message_bytes) > MAX_BYTES:
                self.ecsprint(f'ERROR handle_json_REPLY with message_bytes too big ', log='e')

            if request != 'heartbeat' and request != 'write_lock_act' and request != 'ring_metadata':
                self.ecsprint(f'MSG sent to {self.kv_id(sock)}(SIZE {len(message_bytes)}/{MAX_BYTES}): {request}')

            # if request == 'ring_metadata':
            #     self.ecsprint(f"Sending ring_metadata")
            #     # formatted_json = json.dumps(data, indent=4)
            #     self.ecsprint(json_data)

            sock.sendall(message_bytes)
        except Exception as e:
            self.ecsprint(f'Error while sending request {request} to KVS{self.kv_id(sock)}: {e}', log='e')

    def REPLY_templates(self, request, data):
        if request == 'kvserver_data':
            return {
                'request': request
            }
        elif request == 'ring_metadata':
            return {
                'request': f'{request} {data}',
                'data': self.hash_class.complete_ring
            }
        elif request == 'write_lock_act':
            return {
                'request': request
            }
        elif request == 'write_lock_deact':
            return {
                'request': request
            }
        elif request == 'shutdown_ecs':
            return {
                'request': request
            }
        elif request == 'arrange_ring':
            return {
                'request': request,
                'data': data
            }
        else:
            self.ecsprint(f'Message templates. Request not found:{request}')

    def heartbeat(self):
        self.tictac = time.time()

    def kv_id(self, sock):
        try:
            for key, values in self.kvs_data.items():
                if values['sock'] == sock:
                    return key
        except:
            self.ecsprint(f'Error kv_id  {sock.getpeername()}')
            return sock.getpeername()

    def check_active_kvservers(self):
        for key, item in self.kvs_data.items():
            if item['active']:
                elapsed_time = time.time() - item['active']
                if elapsed_time < self.kvs_timeout:
                    return True
                else:
                    item['active'] = False
        return False

    def closing_kvserver(self, sock):  # Closing without backup process
        id = self.kv_id(sock)
        self.ecsprint(f'Removing KVS{id}... ')
        try:
            del self.kvs_connected[sock]
        except:
            self.ecsprint(f'Error. Check, deleted socket not in connected socket')

        if self.kvs_data[id]['to_hash'] in self.hash_class.ring_coordinators:
            self.ecsprint(f'KVS{id} in coordinator ring. Removing it')
            self.broadcast('write_lock_act')
            del self.hash_class.ring_coordinators[self.kvs_data[id]['to_hash']]
            self.hash_class.update_ring_intervals()
            self.broadcast('ring_metadata')
        else:
            self.ecsprint(f'Error. Not found as node in the ring. Just removing and closing socket')
        sock.close()
        self.ecsprint(f'Socket closet. Successfully removed KVS{id}')

    def ecsprint(self, *args, log='d'):
        message = ' '.join(str(arg) for arg in args)
        # message = self.cli + message
        message = message
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


def main():
    parser = argparse.ArgumentParser(description='ECS Server')
    parser.add_argument('-l', '--log-file', default='log.log', help='Log file')
    parser.add_argument('-ll', '--log-level', default='DEBUG', type=str, help='Log level:DEBUG or INFO')
    parser.add_argument('-d', '--directory', default='.', type=str, help='Storage directory')
    parser.add_argument('-a', '--address', default='0.0.0.0', help='Server address')
    parser.add_argument('-p', '--port', default='5002', type=int, help='Server port')

    # parser.add_argument('-h', '--help', required=True, help='Help')

    args = parser.parse_args()
    # data, unknown = parser.parse_known_args()
    # print(f'Commands: {data}')

    ECS(log_level=args.log_level,
        log_file=args.log_file,
        host=args.address,
        port=args.port,
        directory=args.directory)


if __name__ == '__main__':
    main()
