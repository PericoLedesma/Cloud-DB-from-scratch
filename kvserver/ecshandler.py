import socket
import time
import json

class ECS_handler:
    def __init__(self, addr, kv_data, printer_config):
        self.ecs_addr, self.ecs_port = addr.split(':')

        self.ecs_addr = self.ecs_addr.replace(" ", "")
        self.ecs_port = int(self.ecs_port)

        self.cli = f'[ECS handler]>'
        self.print_cnfig = printer_config

        self.kv_data = kv_data
        self.timeout = 10

        self.connect_to_ECS()


    def connect_to_ECS(self):
        self.kvprint(f'Connecting to bootstrap {self.ecs_addr}:{self.ecs_port}')
        RETRY_INTERVAL = 3
        start_time = time.time()

        while True:
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect((self.ecs_addr, self.ecs_port))
                self.addr, self.port = self.sock.getsockname()
                self.kvprint(f'Connected to ECS. Connectiona addr: {self.addr}:{self.port }')
                break
            except socket.error as e:
                self.kvprint(f'Connection error:{e}. Retrying in {RETRY_INTERVAL} seconds...')
                time.sleep(RETRY_INTERVAL)

            if (time.time() - start_time) >= self.timeout:
                self.kvprint(f' Closing kvserver')
                break



    def handle_RECV(self):
        self.kvprint(f'Handling the recv of ECS')
        try:
            data = self.sock.recv(128 * 1024).decode()
            # if data is not None and data != 'null' and data !="":
            if data:
                # self.kvprint(f'Data received:', repr(data))
                self.handle_REQUEST(data)
            else:
                self.kvprint(f'No data. --> Closing socket')
                self.sock.close()

        except Exception as e:
            self.kvprint(f'Exception: {e} --> Closing socket')
            self.sock.close()



    def handle_REQUEST(self, data):
        messages = data.replace('\\r\\n', '\r\n').split('\r\n')
        for msg in messages:
            if msg is None or msg == " " or not msg:
                break
            else:
                self.kvprint(f'Received message: {repr(msg)}')
                try:
                    parsedata = json.loads(msg)
                    method = parsedata.get('request')
                    # self.kvprint(f'Method: {method}. Sending answer')
                    if method == 'kvserver_data':
                        self.handle_json_RESPONSE(method)
                    elif method == 'kvserver_hash_key':
                        self.kv_data['has_key'] = parsedata.get('data', {}).get('hash_key')
                    else:
                        self.kvprint(f'error unknown command!')

                except json.decoder.JSONDecodeError as e:
                    self.kvprint(f'Error parsing JSON: {str(e)}.')
                    # self.handle_RESPONSE(f'{self.cli}Message received: {msg}')

    def handle_RESPONSE(self, response):
        print('sending answer')
        self.sock.sendall(bytes(f'{response}\r\n', encoding='utf-8'))

    def handle_json_RESPONSE(self, method):
        try:
            json_data = json.dumps(self.messages_templates(method))
            self.sock.sendall(bytes(f'{json_data}\r\n', encoding='utf-8'))
            # self.kvprint(f'Response sent:{json_data}')
        except Exception as e:
            self.kvprint(f'Error while sending the data: {e}')


    def messages_templates(self, request):
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
        else:
            self.kvprint(f'Message templates. Request not found:{request}')

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





