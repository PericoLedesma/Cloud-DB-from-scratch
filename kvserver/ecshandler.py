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

        # Lock write parameter. While server starting locked
        self.write_lock = True

        # To turn of the ecs handler and the thread
        self.ecs_connected = True
        self.kvs_ON = True
        self.shutdown = False # To shutdown after arranging data.

        # Timeout and heartbeat parameters
        self.sock_timeout = timeout_config[0]

    def connect_to_ECS(self):
        self.ecs_connected = False
        RETRY_INTERVAL = 1
        connect_to_ECS_try = 0
        self.kvprint(f'Connecting to ECS {self.ecs_addr}:{self.ecs_port}....')
        while True:
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect((self.ecs_addr, self.ecs_port))
                self.addr, self.port = self.sock.getsockname()
                # self.socket_pool.append(self.sock)
                self.sock.settimeout(self.sock_timeout)
                self.kvprint(f'Connected to ECS. Connection addr: {self.addr}:{self.port}')
                self.handle_json_REPLY('kvserver_data')
                self.ecs_connected = True
                break
            except socket.error as e:
                # self.kvprint(f'Error connecting:{e}. Retrying..')
                if connect_to_ECS_try > 10:
                    self.kvprint(f'Tried to connect to ECS unsuccessfully ')
                    self.ecs_connected = False
                    break
                else:
                    connect_to_ECS_try += 1
                    time.sleep(RETRY_INTERVAL)


    def handle_CONN(self):
        self.connect_to_ECS()
        while self.ecs_connected:
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
                self.kvprint(f'Time out handle_CONN --> Continue', log='e')
            except Exception as e:
                self.kvprint(f'Exception handle_CONN: {e} --> Continue')
                continue
        self.kvprint(f'Exiting handle_CONN with ECS ...| {self.ecs_connected}')
        self.closing_all()
        self.sock.close()
        self.kvprint(f'{"-"*60}')
        self.kvprint(f'{" " * 20}ECS Handler Stopped')
        self.kvprint(f'{"-"*60}')

    def handle_RECV(self, msg):
        try:
            parsedata = json.loads(msg)
            request = parsedata.get('request')
            self.kvprint(f'Received message: {request}')
            # REQUESTS
            if request == 'kvserver_data':
                self.handle_json_REPLY(request)
            elif request == 'ring_metadata':
                self.write_lock = True
                self.ring_metadata = parsedata.get('data', {})
                for key, value in self.ring_metadata.items():  # TODO delete when we just use str
                    # value['from'] = str(value['from']) #todo take out when normal hash
                    # value['to_hash'] = str(value['to_hash'])
                    if value['host'] == self.kv_data['host'] and value['port'] == self.kv_data['port']:
                        self.kv_data['to_hash'] = str(value['to_hash'])
                        self.kv_data['from'] = str(value['from'])
                self.kvprint(f'UPDATED RING. Number of kv_servers: {len(self.ring_metadata)}')
                message = ''
                self.kvprint(f'------ KEYRANGE -----')
                for v in self.ask_ring_metadata().values():  # Posible problem por el orden
                    row = f'{v["from"]},{v["to_hash"]},{v["host"]}:{v["port"]},{v["type"]};'
                    message = f'{message} {row}'
                    self.kvprint(f'{row}')
                self.kvprint(f'--------------------')
                self.write_lock = False
            elif request == 'write_lock_act':
                self.write_lock = True
            elif request == 'write_lock_deact':
                self.write_lock = False
            elif request == 'shutdown_kvserver_now':
                print('HERE2')
                self.closing_all()
            elif request == 'arrange_ring':
                data = parsedata.get('data', {})
                if data is not None:
                    thread = threading.Thread(target=self.send_data_kvserver, args=(data,))
                    thread.start()
                else:
                    self.kvprint(f'Error in arrange_ring request. No node to send data')
            else:
                self.kvprint(f'error unknown command!')

        except json.decoder.JSONDecodeError as e:
            self.kvprint(f'Error handling received: {str(e)} |MSG {msg}')

    def handle_REPLY(self, response):
        self.sock.sendall(bytes(f'{response}\r\n', encoding='utf-8'))
        self.kvprint(f'Sending normal msg: {response}')

    def handle_json_REPLY(self, request, data=None):
        json_data = json.dumps(self.REPLY_templates(request, data))
        self.sock.sendall(bytes(f'{json_data}\r\n', encoding='utf-8'))
        if request != 'heartbeat':
            self.kvprint(f'Response sent:{request}')


    def REPLY_templates(self, request, data):
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
                'request': 'heartbeat',
                'data': data
            }
        elif request == 'ring_metadata':
            return {
                'request': 'ring_metadata'
            }
        elif request == 'starting_shutdown_process':
            return {
                'request': 'starting_shutdown_process',
                'data': self.kv_data
            }
        elif request == 'kvserver_shutdown_now':
            return {
                'request': 'kvserver_shutdown_now',
                'data': self.kv_data
            }
        else:
            self.kvprint(f'Error Message templates. Request not found:{request}')

    def send_heartbeat(self, active):
        try:
            if self.ecs_connected:
                active_dict = {'active': active}
                self.handle_json_REPLY('heartbeat', active_dict)
            else:
                self.kvprint(f'Not connected, not hearbeat sent.')
        except Exception as e:
            self.kvprint(f'Failed send_heartbeat: {e}. Check if parameter')

    def kvprint(self, *args, log='d'):
        message = ' '.join(str(arg) for arg in args)
        message = '\t' + self.cli + message
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
        sock.settimeout(self.sock_timeout)

        RETRY_INTERVAL = 1
        self.kvprint(f'Connecting to KVserver responsable {addr}:{port}....')

        while True:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((addr, port))
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
                    md5_hash = md5_hash[:3] #todo CAREFUL
                    if md5_hash < up_hash and md5_hash > low_hash:
                        message = f'put {key} {value}'
                        sock.sendall(bytes(f'{message}\r\n', encoding='utf-8'))
                        self.kvprint(f'Sended (hash {md5_hash} - {message}')
                        del db[key]
        elif up_hash < low_hash:
            with shelve.open(self.storage_dir, writeback=True) as db:
                for key, value in db.items(): #Todo have a follow of which one are correctly traspased
                    md5_hash = hashlib.md5(key.encode()).hexdigest()
                    md5_hash = md5_hash[:3]#todo CAREFUL
                    if md5_hash > up_hash or md5_hash < low_hash:
                        message = f'put {key} {value}'
                        sock.sendall(bytes(f'{message}\r\n', encoding='utf-8'))
                        self.kvprint(f'\tSended to hash {md5_hash} - {message}')
                        del db[key]
        # while True:
        #     try:
        #         data = sock.recv(128 * 1024).decode()
        #         if data:
        #             messages = data.replace('\\r\\n', '\r\n').split('\r\n')
        #             for msg in messages:
        #                 if msg is None or msg == " " or not msg:
        #                     break
        #                 else:
        #                     self.kvprint(f'Arranging data:Message ', msg)
        #         else:
        #             self.kvprint(f'Arranging data.No data. --> Closing socket')
        #             break
        #     except Exception as e:
        #         self.kvprint(f'Exception handle_CONN: {e}')
        #         break
        self.kvprint(f'Successfully rearrange data. Closing kv-kv socket')
        sock.close()

        if self.shutdown:
            self.handle_json_REPLY('kvserver_shutdown_now')
            print('HERE3')
            self.closing_all()

    def closing_all(self):
        self.kvprint(f'-------> Stopping ALL <------')
        self.kvprint(f'Asking clients handler to stop...')
        if self.clients_conn:
            for client_handler in self.clients_conn.values():
                if client_handler is not None:
                    client_handler.conn_status = False
        time.sleep(2)
        self.kvprint(f'Asking ECS handler to stop...')
        self.kvprint(f'ecs_connected --> OFF')
        self.ecs_connected = False
        self.kvprint(f'kvs_ON --> OFF')
        self.kvs_ON = False

