import socket
import time
import json


class ECS_handler:
    def __init__(self, kv_data, printer_config, timeout_config):
        self.ecs_addr, self.ecs_port = kv_data['ecs_addr'].split(':')
        self.ecs_addr = self.ecs_addr.replace(" ", "")
        self.ecs_port = int(self.ecs_port)

        self.cli = f'[ECS handler]>'
        self.print_cnfig = printer_config

        self.kv_data = kv_data
        self.ring_metadata = {}

        self.heartbeat = timeout_config[0]
        self.tictac = timeout_config[1]
        self.timeout = timeout_config[2]

        self.connect_to_ECS()

    def connect_to_ECS(self):
        self.kvprint(f'Connecting to bootstrap {self.ecs_addr}:{self.ecs_port}...')
        RETRY_INTERVAL = 3

        while True:
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect((self.ecs_addr, self.ecs_port))
                self.addr, self.port = self.sock.getsockname()
                self.kvprint(f'Connected to ECS. Connection addr: {self.addr}:{self.port}')
                break
            except socket.error as e:
                self.kvprint(f'Connection error:{e}. Retrying in {RETRY_INTERVAL} seconds...')
                time.sleep(RETRY_INTERVAL)

            if (time.time() - self.tictac) >= self.timeout:
                self.kvprint(f'Stop retrying to connect to ECS', c='r')
                break

    def handle_CONN(self):
        try:
            data = self.sock.recv(128 * 1024).decode()
            if data:
                self.handle_RECV(data)
                self.heartbeat()
            else:
                self.kvprint(f'No data. --> Closing socket', c='r')
                self.sock.close()

        except Exception as e:
            self.kvprint(f'Exception: {e} --> Closing socket', c='r')
            self.sock.close()

    def handle_RECV(self, data):
        messages = data.replace('\\r\\n', '\r\n').split('\r\n')
        time.sleep(3)
        for msg in messages:
            if msg is None or msg == " " or not msg:
                break
            else:
                self.kvprint(f'Received message: {repr(msg)}')
                try:
                    parsedata = json.loads(msg)
                    request = parsedata.get('request')
                    if request == 'kvserver_data':
                        self.handle_json_REPLY(request)

                    elif request == 'ring_metadata':
                        self.ring_metadata = parsedata.get('data', {})
                        self.kvprint(f'Updated RING. Number of kv_servers: {len(self.ring_metadata)}')
                        for key, value in self.ring_metadata.items():
                            if value[0] == self.kv_data['host'] and value[1] == self.kv_data['port']:
                                self.kv_data['hash_key'] = value[2]
                                self.kv_data['previous_hash'] = value[3]
                                break
                    else:
                        self.kvprint(f'error unknown command!', c='r')

                except json.decoder.JSONDecodeError as e:
                    self.kvprint(f'Error handling received. Not a json?: {str(e)}.', c='r')

    def handle_REPLY(self, response):
        print('sending answer')
        self.sock.sendall(bytes(f'{response}\r\n', encoding='utf-8'))

    def handle_json_REPLY(self, method):
        try:
            json_data = json.dumps(self.REPLY_templates(method))
            self.sock.sendall(bytes(f'{json_data}\r\n', encoding='utf-8'))
            # self.kvprint(f'Response sent:{json_data}')
        except Exception as e:
            self.kvprint(f'Error while sending the data: {e}', c='r')

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
        else:
            self.kvprint(f'Message templates. Request not found:{request}', c='r')

    def send_heartbeat(self):
        self.handle_json_REPLY('heartbeat')

    def kvprint(self, *args, c=None, log='d'):
        COLORS = {
            'r': '\033[91m',
            'g': '\033[92m',
            'y': '\033[93m',
            'b': '\033[94m',
            'reset': '\033[0m'
        }
        c = self.print_cnfig[1] if c is None else COLORS[c]

        message = ' '.join(str(arg) for arg in args)
        message = c + self.print_cnfig[0] + self.cli + message + COLORS['reset']
        print(message)

        if log == 'd':
            self.print_cnfig[2].debug(f'{self.print_cnfig[0]}{self.cli}{message}')
        if log == 'i':
            self.print_cnfig[2].info(f'{self.print_cnfig[0]}|{self.cli}{message}')
