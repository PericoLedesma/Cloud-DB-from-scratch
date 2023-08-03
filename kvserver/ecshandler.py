from replicas_conn import *

import socket
import time
import json
import shelve
import threading
import hashlib


class ECS_handler:
    def __init__(self, kv_data, clients_conn, storage_dir, printer_config, sock_timeout):
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

        # Ring metadata
        self.ring_metadata = {}
        self.ring_replicas = []
        self.ring_mine_replicas = []
        self.complete_ring = {}

        # Lock write parameter. While server starting locked
        self.write_lock = True

        # To turn of the ecs handler and the thread
        self.ecs_connected = True
        self.kvs_ON = True
        self.shutdown = False  # To shutdown after arranging data.

        # Timeout and heartbeat parameters
        self.sock_timeout = sock_timeout

        self.kvprint(f'Running ECS handler...')

        # ECS handler thread starter
        self.rep = Replicas_handler(kv_data,
                                    storage_dir,
                                    self.ring_mine_replicas,
                                    printer_config,
                                    sock_timeout)

    def connect_to_ECS(self):
        self.ecs_connected = False
        RETRY_INTERVAL = 0.5
        connect_to_ECS_try = 0
        self.kvprint(f'Connecting to ECS {self.ecs_addr}:{self.ecs_port}....')
        while True:
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect((self.ecs_addr, self.ecs_port))
                self.addr, self.port = self.sock.getsockname()
                self.sock.settimeout(self.sock_timeout)
                self.kvprint(f'Connected to ECS. Connection addr: {self.addr}:{self.port}')
                self.handle_json_REPLY('kvserver_data')
                self.ecs_connected = True
                break
            except socket.error as e:
                # self.kvprint(f'Error connecting:{e}. Retrying..')
                if connect_to_ECS_try > 40:
                    self.kvprint(f'Tried to connect to ECS unsuccessfully ')
                    self.ecs_connected = False
                    break
                else:
                    connect_to_ECS_try += 1
                    time.sleep(RETRY_INTERVAL)

    def handle_CONN(self):
        self.connect_to_ECS()
        incomplete_msg = None
        while self.ecs_connected:
            try:
                data = self.sock.recv(128 * 1024).decode()  # todo max size 128 * 1024
                if data:
                    if data.endswith('\r\n'): # If it ends, it is a completed msg
                        messages = data.replace('\\r\\n', '\r\n').split('\r\n')
                        if incomplete_msg:
                            messages[0] = incomplete_msg + messages[0]
                            incomplete_msg = None
                    else: # Msg not complete
                        messages = data.replace('\\r\\n', '\r\n').split('\r\n')
                        if incomplete_msg:
                            messages[0] = incomplete_msg + messages[0]
                            incomplete_msg = None
                        incomplete_msg = messages[-1]
                        del messages[-1]

                    for msg in messages:
                        if msg is None or msg == " " or not msg:
                            break
                        elif msg == 'null':
                            self.kvprint(f'handle_RECV --> null. Reconnecting')
                            self.connect_to_ECS()
                        else:
                            try:
                                # self.kvprint(f'MSG {msg}')
                                parsedata = json.loads(msg)
                                request = parsedata.get('request')
                                self.kvprint(f'ECS MSG: {request}')
                                self.handle_RECV(parsedata, request)
                            except json.decoder.JSONDecodeError as e:
                                self.kvprint(f'Error handling received: {str(e)} |MSG {msg}')
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
        self.kvprint(f'Exiting handle_CONN with ECS ...| {self.ecs_connected}')
        self.closing_all()
        self.sock.close()
        self.kvprint(f'{"-" * 60}')
        self.kvprint(f'{" " * 20}ECS Handler Stopped')
        self.kvprint(f'{"-" * 60}')
        del self
        exit(0)

    def handle_RECV(self, parsedata, request):
        if request == 'kvserver_data':
            self.handle_json_REPLY(request)
        elif 'ring_metadata' in request:
            if self.shutdown is False:
                self.kvprint(f'Updating Ring ... Extracting coordinators and replicas...')
                self.write_lock = True

                # Storing data
                self.complete_ring = []
                self.ring_metadata = {}
                self.ring_replicas = []

                data = parsedata.get('data', {})
                for item in data:
                    self.complete_ring.append({
                        'from': str(item["from"]),
                        'to_hash': str(item["to_hash"]),
                        'host': str(item["host"]),
                        'port': str(item["port"]),
                        'type': str(item["type"]),
                    })
                    if item['type'] == 'C':
                        self.ring_metadata[item['to_hash']] = {
                            'from': str(item["from"]),
                            'to_hash': str(item["to_hash"]),
                            'host': str(item["host"]),
                            'port': str(item["port"]),
                            'type': str(item["type"]),
                        }
                        if item['host'] == self.kv_data['host'] and item['port'] == self.kv_data['port']:
                            self.kv_data['to_hash'] = str(item['to_hash'])
                            self.kv_data['from'] = str(item['from'])
                    else:
                        self.ring_replicas.append({
                            'from': str(item["from"]),
                            'to_hash': str(item["to_hash"]),
                            'host': str(item["host"]),
                            'port': str(item["port"]),
                            'type': str(item["type"]),
                        })

                # self.kvprint(f'--------------complete_ring--------')
                # for values in self.complete_ring:
                #     self.kvprint(values)
                #
                # self.kvprint(f'--------------ring_metadata--------')
                # for key, values in self.ring_metadata.items():
                #     self.kvprint(key,'|', values)
                #
                # self.kvprint(f'--------------ring_replicas--------')
                # for values in self.ring_replicas:
                #     self.kvprint(values)

                self.kvprint(f'Ring data extracted. C = {len(self.ring_metadata)} | R = {len(self.ring_replicas)}')
                if self.ring_replicas:
                    self.kvprint(f'Updating my replicas ...')
                    self.rep.update_replicas(self.ring_replicas)
                else:
                    self.kvprint(f'No replicas. Closing connections in case of before having rep..(TODO)')
                    #Todo delete my store of other replicas
                    # Close connections with my replicas

                self.kvprint(f'Updated ring. Number of nodes: {len(self.ring_metadata)}')
                self.write_lock = False

            else:
                self.kvprint(f'Shutdown in process. Ring not updated')
        elif 'write_lock_act' in request:
            self.write_lock = True
        elif request == 'write_lock_deact':
            self.write_lock = False
            self.kvprint(f'LOCK OFF')
        elif request == 'shutdown_kvserver' or request == 'shutdown_ecs':
            self.closing_all()
        elif request == 'arrange_ring':
            data = parsedata.get('data', {})
            if data is not None:
                self.kvprint(f'Init send_data_to_reponsable thread...')
                thread = threading.Thread(target=self.send_data_to_reponsable, args=(data,))
                thread.start()
            else:
                self.kvprint(f'arrange_ring  -> No node to send data')
                if self.shutdown:
                    self.handle_json_REPLY('kvserver_shutdown')
                    self.closing_all()
        else:
            self.kvprint(f'error unknown command! -> {request}')


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
                'request': request,
                'data': data
            }
        elif request == 'ring_metadata':
            return {
                'request': request
            }
        elif request == 'starting_shutdown_process':
            # So the ECS knows and sends to who send the data
            return {
                'request': request,
                'data': self.kv_data
            }
        elif request == 'kvserver_shutdown':
            # So the ECS knows we have already finish with the shutting down process
            return {
                'request': request,
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
        message = self.cli + message
        # message = self.print_cnfig[0] + self.cli + message

        if log == 'd':
            self.print_cnfig[1].debug(message)
        elif log == 'i':
            self.print_cnfig[1].info(message)
        elif log == 'e':
            self.print_cnfig[1].error(message)

    def ask_ring(self):  # For the client handler to get the ring
        return self.ring_metadata
    def ask_replicas(self):  # For the client handler to get the ring
        return self.rep.ring_mine_replicas


    def ask_lock(self):  # For the client too
        return self.write_lock

    def ask_lock_ecs(self): # For the client too # Todo rethink
        self.handle_json_REPLY('ring_metadata')

    def send_data_to_reponsable(self, data):
        cli = '[send_data_to_reponsable]'
        with shelve.open(self.storage_dir, writeback=True) as db:
            # Check if the database is empty
            if not list(db.keys()):
                self.kvprint(f'{cli}Empty database. Nothing to send.')
                return  # TODO check if we store everything or sometimes just in cache

        self.kvprint(f'{cli}--->     Sending data to KVSERVER responsable    <-----')

        low_hash, up_hash = str(data['interval'][0]), str(data['interval'][1])  # TODO when ahsh back to normal
        low_hash = int(low_hash, 16)
        up_hash = int(up_hash, 16)

        addr, port = data['responsable'].split(':')
        addr = addr.replace(" ", "")
        port = int(port)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((addr, port))
        sock.settimeout(self.sock_timeout)

        RETRY_INTERVAL = 0.5
        connecting_try = 0
        self.kvprint(f'{cli}Connecting to KVserver responsable {addr}:{port}....')
        while True:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((addr, port))
                self.kvprint(f'{cli}Connected to KVserver [{addr}:{port}].')
                # Todo something with the first msg so the other kvserver knows he is a kvserer
                sock.sendall(bytes(f'{self.kv_data["name"]} This is {self.kv_data["name"]} rearranging data.\r\n', encoding='utf-8'))
                # Get the local address
                # print('OLALA') # todo
                # print(sock.getsockname())
                # print(sock.getpeername())

                break
            except:
                if connecting_try > 20:
                    self.kvprint(f'{cli}Tried to connect to KVSERVER unsuccessfully. Data not sent.  ')
                    break
                else:
                    connecting_try += 1
                    time.sleep(RETRY_INTERVAL)

        self.kvprint(f'{cli}Starting sending data... ')
        if up_hash > low_hash:
            with shelve.open(self.storage_dir, writeback=True) as db:
                for key, value in db.items():  # Todo have a follow of which one are correctly traspased
                    md5_hash = hashlib.md5(key.encode()).hexdigest()
                    # md5_hash = md5_hash[:3] #todo CAREFUL
                    md5_hash = int(md5_hash, 16)
                    if up_hash > md5_hash > low_hash:
                        message = f'put {key} {value}'
                        sock.sendall(bytes(f'{message}\r\n', encoding='utf-8'))
                        self.kvprint(f'Sended (hash {md5_hash} - {message}')
                        del db[key]
        else:
            with shelve.open(self.storage_dir, writeback=True) as db:
                for key, value in db.items():  # Todo have a follow of which one are correctly traspased
                    md5_hash = hashlib.md5(key.encode()).hexdigest()
                    # md5_hash = md5_hash[:3]#todo CAREFUL
                    md5_hash = int(md5_hash, 16)
                    if md5_hash > up_hash or md5_hash < low_hash:
                        message = f'put {key} {value}'
                        sock.sendall(bytes(f'{message}\r\n', encoding='utf-8'))
                        self.kvprint(f'\tSended to hash {md5_hash} - {message}')
                        del db[key]

        message = f'organise completed'
        sock.sendall(bytes(f'{message}\r\n', encoding='utf-8'))

        while True:
            try:
                data = sock.recv(128 * 1024).decode()
                if data:
                    messages = data.replace('\\r\\n', '\r\n').split('\r\n')
                    for msg in messages:
                        if msg is None or msg == " " or not msg:
                            break
                        elif msg == 'organise received':
                            self.kvprint(f'{cli}Arranging data completed successfully ', msg)
                            break
                        else:
                            self.kvprint(f'{cli}Back msg: ', msg)
                else:
                    self.kvprint(f'{cli}Arranging data.No data. --> Closing socket')
                    break
            except Exception as e:
                self.kvprint(f'{cli}Exception handle_CONN: {e}')
                break
        self.kvprint(f'{cli}Successfully rearrange data. Closing kv-kv socket')
        sock.close()

        if self.shutdown:
            self.kvprint(f'{cli}MSG send: kvserver_shutdown ')
            self.handle_json_REPLY('kvserver_shutdown')
            self.kvprint(f'{cli} self.closing_all()')
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
