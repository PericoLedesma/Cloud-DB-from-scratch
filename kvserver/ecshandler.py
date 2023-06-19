import socket
import time
import json
import shelve
import threading
import hashlib


class ECS_handler:
    def __init__(self, kv_data, clients_conn, storage_dir, printer_config, timeout_config):
        # ECS Server connection parameters
        self.ecs_addr, self.ecs_port = kv_data['ecs_addr'].split(':')
        self.ecs_addr = self.ecs_addr.replace(" ", "")
        self.ecs_port = int(self.ecs_port)

        # Printing parameters
        self.cli = f'[ECS]>'
        self.print_cnfig = printer_config
        self.storage_dir = storage_dir

        # Data structures
        self.kv_data = kv_data
        self.clients_conn = clients_conn
        self.ring_metadata = {}
        self.list_kvs_addr = []

        # Lock write parameter
        self.write_lock = True

        # To turn of the ecs handler and the thread
        self.ON = True
        self.shutdown = False

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
                self.heartbeat()
                break
            except socket.error as e:
                # self.kvprint(f'Error connecting:{e}. Retrying..')
                time.sleep(RETRY_INTERVAL)
        self.handle_CONN()
        self.sock.close()

    def handle_CONN(self):
        while self.ON:
            try:
                data = self.sock.recv(128 * 1024).decode()
                if data:
                    self.handle_RECV(data)
                    self.heartbeat(ecs=False)
                else:
                    self.kvprint(f'No data. --> Closing socket')
                    self.ON = False
                    self.sock.close()
            except Exception as e:
                # self.kvprint(f'Exception handle_CONN: {e}')
                continue


    def handle_RECV(self, data):
        messages = data.replace('\\r\\n', '\r\n').split('\r\n')
        for msg in messages:
            if msg is None or msg == " " or not msg:
                break
            else:
                try:
                    parsedata = json.loads(msg)
                    request = parsedata.get('request')

                    # REQUESTS
                    if request == 'heartbeat':
                        self.heartbeat(ecs=False)
                        pass
                    elif request == 'kvserver_data':
                        self.handle_json_REPLY(request)
                    elif request == 'ring_metadata':
                        self.kvprint(f'Received message: {request}')
                        self.ring_metadata = parsedata.get('data', {})
                        self.list_kvs_addr = []
                        for key, values in self.ring_metadata.items():  # TODO delete when we just use str
                            values[0] = str(values[0])
                            values[1] = str(values[1])
                            self.list_kvs_addr.append(f'{values[2]}:{values[3]}')
                        self.kvprint(f'UPDATED RING. Number of kv_servers: {len(self.ring_metadata)}')
                        for key, value in self.ring_metadata.items():
                            if value[2] == self.kv_data['host'] and value[3] == self.kv_data['port']:
                                self.kv_data['hash_key'] = value[1]
                                self.kv_data['previous_hash'] = value[0]
                                break
                        self.write_lock = False
                    elif request == 'write_lock_act':
                        self.write_lock = True
                    elif request == 'write_lock_deact':
                        self.write_lock = False
                    elif request == 'arrange_ring':
                        self.kvprint(f'Received message: {request}')
                        data = parsedata.get('data', {})
                        if data is not None:
                            thread = threading.Thread(target=self.send_data_kvserver, args=(data,))
                            thread.start()
                        else:
                            self.kvprint(f'Error. No node to send data')
                    else:
                        self.kvprint(f'error unknown command!')
                except json.decoder.JSONDecodeError as e:
                    self.kvprint(f'Error handling received: {str(e)} |MSG {msg}')

    def handle_REPLY(self, response):
        self.sock.sendall(bytes(f'{response}\r\n', encoding='utf-8'))
        self.kvprint(f'Sending normal msg: {response}')

    def handle_json_REPLY(self, request):
        try:
            json_data = json.dumps(self.REPLY_templates(request))
            self.sock.sendall(bytes(f'{json_data}\r\n', encoding='utf-8'))
            self.kvprint(f'Response sent:{request}')
        except Exception as e:
            self.kvprint(f'Error handle_json_REPLY while sending {request}: {e}')

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
        elif request == 'ring_metadata':
            return {
                'request': 'ring_metadata'
            }
        elif request == 'kvserver_shutdown':
            return {
                'request': 'kvserver_shutdown',
                'data': self.kv_data
            }
        elif request == 'kvserver_shutdown_now':
            return {
                'request': 'kvserver_shutdown_now',
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
        message = self.cli + message
        # message = self.print_cnfig[0] + self.cli + message
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
    def ask_lock_ecs(self): # For the client too
        self.handle_json_REPLY('ring_metadata')

    def send_data_kvserver(self, data):
        low_hash, up_hash = str(data['interval'][0]), str(data['interval'][1])
        addr, port = data['responsable'].split(':')
        addr = addr.replace(" ", "")
        port = int(port)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((addr, port))
        sock.settimeout(10)

        RETRY_INTERVAL = 1
        self.kvprint(f'Connecting to KVserver responsable {addr}:{port}....')

        while True:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((addr, port))
                sock.settimeout(10)
                # self.socket_pool.append(self.sock)
                sock.settimeout(10)
                self.kvprint(f'Connected to KVserver [{addr}:{port}].')
                # Todo something with the first msg so the other kvserver knows he is a kvserer
                sock.sendall(bytes(f'This is {self.kv_data["name"]} rearranging data.\r\n', encoding='utf-8'))
                break
            except socket.error as e:
                # self.kvprint(f'Error connecting:{e}. Retrying..')
                time.sleep(RETRY_INTERVAL)

        while self.write_lock:
            time.sleep(1)
            self.kvprint(f'Waiting to lock deacti')
        self.kvprint(f'Starting sending data... ')
        if up_hash > low_hash:
            with shelve.open(self.storage_dir, writeback=True) as db:
                for key, value in db.items():  # Todo have a follow of which one are correctly traspased
                    md5_hash = hashlib.md5(key.encode()).hexdigest()
                    # md5_hash = md5_hash[:1] #todo CAREFUL
                    if md5_hash < up_hash and md5_hash > low_hash:
                        message = f'put {key} {value}'
                        sock.sendall(bytes(f'{message}\r\n', encoding='utf-8'))
                        self.kvprint(f'Sended (hash {md5_hash} - {message}')
                        del db[key]
        elif up_hash < low_hash:
            with shelve.open(self.storage_dir, writeback=True) as db:
                for key, value in db.items(): #Todo have a follow of which one are correctly traspased
                    md5_hash = hashlib.md5(key.encode()).hexdigest()
                    # md5_hash = md5_hash[:1]#todo CAREFUL
                    if md5_hash > up_hash or md5_hash < low_hash:
                        message = f'put {key} {value}'
                        sock.sendall(bytes(f'{message}\r\n', encoding='utf-8'))
                        self.kvprint(f'\tSended hash {md5_hash} - {message}')
                        del db[key]
        while True:
            try:
                data = sock.recv(128 * 1024).decode()
                if data:
                    messages = data.replace('\\r\\n', '\r\n').split('\r\n')
                    for msg in messages:
                        if msg is None or msg == " " or not msg:
                            break
                        else:
                            self.kvprint(f'Arranging data:Message ', msg)
                else:
                    self.kvprint(f'Arranging data.No data. --> Closing socket')
                    break
            except Exception as e:
                self.kvprint(f'Exception handle_CONN: {e}')
                break
        self.kvprint(f'Successfully rearrange data. Closing clients sockets..')
        sock.close()

        if self.shutdown:
            self.handle_json_REPLY('kvserver_shutdown_now')
            self.closing_all()

    def closing_all(self):
        self.kvprint(f'------- CLOSING ALL ------')
        self.kvprint(f'Closing clients handlers...')
        if self.clients_conn:
            for client_handler in self.clients_conn.values():
                client_handler.conn_status = False
        time.sleep(2)
        self.kvprint(f'Closing ECS handler...')
        self.ON = False

