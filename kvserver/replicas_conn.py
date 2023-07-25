from replicas_conn import *

import socket
import json
import shelve
import time


class Replicas_handler:
    def __init__(self, kv_data, storage_dir, ring_structures, printer_config, sock_timeout):

        # Printing parameters
        self.cli = f'[Replicas]>'
        self.print_cnfig = printer_config
        self.storage_dir = storage_dir

        # Data structures
        self.kv_data = kv_data
        self.ring_mine_replicas = {}
        self.connected_replicas = {}

        # Some function to ask for data. todo rethink
        self.ask_ring = ring_structures[0]
        self.ask_lock_write_value = ring_structures[1]
        # self.ask_lock_ecs = ring_structures[2]

        # Timeout and heartbeat parameters
        self.sock_timeout = sock_timeout

        self.kvprint(f'Running Replicas handler...')

    def update_replicas(self, replicas_list):
        self.kvprint(f'Updating replicas ....')

        # Current replicas
        if self.ring_mine_replicas:
            self.kvprint(f'We have already replicas')

            old_replicas = self.ring_mine_replicas
            self.ring_mine_replicas = {}

            for value in replicas_list:
                if value['from'] == self.kv_data['from'] and value['to_hash'] == self.kv_data['to_hash']:
                    if self.ring_mine_replicas[value['type']] != value:
                        # self.close_replica(value['type'])
                        self.ring_mine_replicas[value['type']] = value
                        # self.connect_to_replica(value)
                    else:
                        self.kvprint(f'Replica already connected to')
        else:
            self.kvprint(f'No previous replicas')
            for value in replicas_list:
                if value['from'] == self.kv_data['from'] and value['to_hash'] == self.kv_data['to_hash']:
                    self.ring_mine_replicas[value['type']] = value
                    # self.connect_to_replica(value)

        # self.kvprint(f'KVS data')
        # self.kvprint(self.kv_data)
        # self.kvprint(f'My replicas data')
        # self.kvprint(self.ring_mine_replicas)


    def close_replica(self, type):
        self.kvprint(f'Closing old replica {type}')
        key = None
        for sock, rep in self.connected_replicas.items():
            if rep['type'] is type:
                sock.close()
                key = sock
                break
        del self.connected_replicas[key]


    def connect_to_replica(self, rep):
        RETRY_INTERVAL = 0.5
        connect_to_try = 0
        self.kvprint(f'Connecting to replica {rep["host"]}:{rep["port"]} ....')
        while True:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((rep['host'], rep['port']))
                addr, port = sock.getsockname()
                sock.settimeout(self.sock_timeout)
                self.kvprint(f'Connected to rep {rep["host"]}:{rep["port"]}. Connection addr: {addr}:{port}')
                self.connected_replicas[sock] = {
                    'sock': sock,
                    'addr': addr,
                    'port': port,
                    'host': f'{rep["host"]}',
                    'host_port': f'{rep["port"]}',
                    'type': rep["type"]
                }
                self.handle_json_REPLY(sock, 'you_are_my_replica')
                break
            except socket.error as e:
                # self.kvprint(f'Error connecting:{e}. Retrying..')
                if connect_to_try > 40:
                    self.kvprint(f'Tried to connect to Replica unsuccessfully ')
                    break
                else:
                    connect_to_try += 1
                    time.sleep(RETRY_INTERVAL)


    def handle_CONN(self):
        while True:
            try:
                data = self.sock.recv(128 * 1024).decode()
                if data:
                    messages = data.replace('\\r\\n', '\r\n').split('\r\n')
                    for msg in messages:
                        if msg is None or msg == " " or not msg:
                            break
                        elif msg == 'null':
                            self.kvprint(f'handle_RECV --> null. Reconnecting')
                            self.connect_to_ECS()
                        else:
                            self.handle_RECV(msg)
                else:
                    self.kvprint(f'No data. --> Reconnecting')
                    self.connect_to_ECS()
                    break
            except socket.timeout:
                # self.kvprint(f'Time out handle_CONN --> Continue', log='e')
                pass
            except Exception as e:
                self.kvprint(f'Exception handle_CONN: {e} --> Continue')
                continue
        self.kvprint(f'Exiting handle_CONN with replicas ...| {self.ecs_connected}')
        self.sock.close()
        self.kvprint(f'{"-" * 60}')
        self.kvprint(f'{" " * 20}Replicas Handler Stopped')
        self.kvprint(f'{"-" * 60}')
        del self
        exit(0)

    def handle_RECV(self, msg):
        try:
            parsedata = json.loads(msg)
            request = parsedata.get('request')
            self.kvprint(f'Received message: {request}')
            # REQUESTS
            if request == 'kvserver_data':
                self.handle_json_REPLY(request)

            else:
                self.kvprint(f'error unknown command!')

        except json.decoder.JSONDecodeError as e:
            self.kvprint(f'Error handling received: {str(e)} |MSG {msg}')

    def handle_REPLY(self,sock,  response):
        sock.sendall(bytes(f'{response}\r\n', encoding='utf-8'))

    def handle_json_REPLY(self, sock, request, data=None):
        json_data = json.dumps(self.REPLY_templates(request, data))
        sock.sendall(bytes(f'{json_data}\r\n', encoding='utf-8'))
        self.kvprint(f'Response sent:{request}')

    def REPLY_templates(self, request, data):
        if request == 'you_are_my_replica':
            return f' you_are_my_replica'
        elif request == 'you_are_my_replica_put':
            return f'you_are_my_replica_put KEY VALUE'

        elif request == 'you_are_my_replica_delete':
            return {
                'request': request,
                'data': data
            }
        else:
            self.kvprint(f'Error Message templates. Request not found:{request}')

    def kvprint(self, *args, log='d'):
        message = ' '.join(str(arg) for arg in args)
        message = self.cli + message
        # message = self.print_cnfig[0] + self.cli + message
        if log == 'd':
            self.print_cnfig[1].debug(message)
        elif log == 'i':
            self.print_cnfig[1].info(message)
        elif log == 'e':
            self.print_cnfig[1].error(message)
