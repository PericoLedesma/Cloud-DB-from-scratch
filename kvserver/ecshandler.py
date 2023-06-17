import socket
import time
import json


class ECS_handler:
    def __init__(self, kv_data, socket_pool, printer_config, timeout_config):
        # ECS Server connection parameters
        self.ecs_addr, self.ecs_port = kv_data['ecs_addr'].split(':')
        self.ecs_addr = self.ecs_addr.replace(" ", "")
        self.ecs_port = int(self.ecs_port)
        self.socket_pool = socket_pool

        # Printing parameters
        self.cli = f'[ECS handler]>'
        self.print_cnfig = printer_config

        # Data structures
        self.kv_data = kv_data
        self.ring_metadata = {}

        # Lock write parameter
        self.write_lock = True

        # To turn of the ecs handler and the thread
        self.ON = True

        # Timeout and heartbeat parameters
        self.heartbeat = timeout_config[0]
        self.tictac = timeout_config[1]
        self.timeout = timeout_config[2]

    def connect_to_ECS(self):
        RETRY_INTERVAL = 1
        self.kvprint(f'Connecting to ECS {self.ecs_addr}:{self.ecs_port}....')
        while self.ON:
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect((self.ecs_addr, self.ecs_port))
                self.addr, self.port = self.sock.getsockname()
                # self.socket_pool.append(self.sock)
                self.sock.settimeout(10)
                self.kvprint(
                    f'Connected to ECS[{self.ecs_addr}:{self.ecs_port}]. Connection addr: {self.addr}:{self.port}')
                self.handle_json_REPLY('kvserver_data')
                break
            except socket.error as e:
                # self.kvprint(f'Error connecting:{e}. Retrying..')
                time.sleep(RETRY_INTERVAL)
        self.handle_CONN()

    def handle_CONN(self):
        while self.ON:
            try:
                data = self.sock.recv(128 * 1024).decode()
                if data:
                    self.handle_RECV(data)
                    self.tictac = time.time()
                else:
                    self.kvprint(f'No data. --> Closing socket')
                    self.ON = False
                    self.sock.close()
            except Exception as e:
                self.kvprint(f'Exception: {e} --> Closing socket')
                self.ON = False
                self.sock.close()

    def handle_RECV(self, data):
        messages = data.replace('\\r\\n', '\r\n').split('\r\n')
        time.sleep(3)
        for msg in messages:
            if msg is None or msg == " " or not msg:
                break
            else:
                try:
                    parsedata = json.loads(msg)
                    request = parsedata.get('request')
                    # REQUESTS
                    if request == 'kvserver_data':
                        self.handle_json_REPLY(request)
                    elif request == 'ring_metadata':
                        self.kvprint(f'Received message: {request}')
                        self.ring_metadata = parsedata.get('data', {})
                        for key, values in self.ring_metadata.items():  # TODO delete when we just use str
                            if len(values) != 4:
                                raise Exception('Error recv ring metadata. Not complete table')
                            values[-2] = str(values[-2])
                            values[-1] = str(values[-1])
                        self.kvprint(f'UPDATED RING. Number of kv_servers: {len(self.ring_metadata)}')
                        self.write_lock = False
                        for key, value in self.ring_metadata.items():
                            if value[0] == self.kv_data['host'] and value[1] == self.kv_data['port']:
                                self.kv_data['hash_key'] = value[2]
                                self.kv_data['previous_hash'] = value[3]
                                break
                    elif request == 'write_lock_act':
                        self.kvprint(f'Received message: {request}')
                        self.write_lock = True
                    elif request == 'write_lock_deact':
                        self.kvprint(f'Received message: {request}')
                        self.write_lock = False
                    elif request == 'heartbeat':
                        # self.kvprint(f'Received message: {request}')
                        self.heartbeat(ecs=False)
                        pass
                    elif request == 'reorganize_ring':
                        self.kvprint(f'Received message: {request}')
                        data = parsedata.get('data', {})
                        print('reorganize_ring')
                        print(data)
                    else:
                        self.kvprint(f'Received message: {request}')
                        self.kvprint(f'error unknown command!')
                except json.decoder.JSONDecodeError as e:
                    self.kvprint(f'Error handling received. Not a json or in the logic?: {str(e)}.')

    def handle_REPLY(self, response):
        self.kvprint(f'Sending normal answer')
        self.sock.sendall(bytes(f'{response}\r\n', encoding='utf-8'))

    def handle_json_REPLY(self, method):
        try:
            json_data = json.dumps(self.REPLY_templates(method))
            self.sock.sendall(bytes(f'{json_data}\r\n', encoding='utf-8'))
            self.kvprint(f'Response sent:{method}')
        except Exception as e:
            self.kvprint(f'Error while sending the data: {e}')

    def REPLY_templates(self, request):
        if request == 'kvserver_data':
            return {
                'request': 'kvserver_data',
                'data': {
                    'id': self.kv_data['id'],
                    'name': self.kv_data['name'],
                    'host': self.kv_data['host'],
                    'port': self.kv_data['port'],
                }
            }
        elif request == 'heartbeat':
            return {
                'request': 'heartbeat'
            }
        elif request == 'kvserver_shutdown':
            print(self.kv_data)
            return {
                'request': 'kvserver_shutdown',
                'data': self.kv_data
            }
        else:
            self.kvprint(f'Message templates. Request not found:{request}')

    def send_heartbeat(self):
        if self.sock.fileno() >= 0:
            self.handle_json_REPLY('heartbeat')
        else:
            self.kvprint(f'send_heartbeat. Not sent, Not connected')

    def kvprint(self, *args, log='d'):
        message = ' '.join(str(arg) for arg in args)
        message = self.print_cnfig[0] + self.cli + message
        if log == 'd':
            self.print_cnfig[1].debug(message)
        if log == 'i':
            self.print_cnfig[1].info(message)
        if log == 'e':
            self.print_cnfig[1].info(message)

    def ask_ring_metadata(self): # For the client handler to get the ring
        return self.ring_metadata

    def ask_lock_write_value(self): # For the client too
        return self.write_lock
