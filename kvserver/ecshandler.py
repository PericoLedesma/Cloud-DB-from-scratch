from replicas_conn import *

import socket
import time
import json
import shelve
import threading
import hashlib
import datetime


class ECS_handler:
    def __init__(self, kvserver):
        self.kvs = kvserver

        # ECS Server connection parameters
        self.ecs_addr, self.ecs_port = self.kvs.kv_data['ecs_addr'].split(':')
        self.ecs_addr = self.ecs_addr.replace(" ", "")
        self.ecs_port = int(self.ecs_port)

        # Printing parameters
        self.cli = f'[ECS]>'

        # To turn of the ecs handler and the thread
        self.ecs_connected = True

        self.kvprint(f'Running ECS handler...')

        # ECS handler thread starter
        self.rep = Replicas_handler(self, kvserver)


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
                self.sock.settimeout(self.kvs.sock_timeout)
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
        self.kvs.closing_all()
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
            if self.kvs.shutdown is False:
                self.kvprint(f'Updating Ring ... Extracting coordinators and replicas...')
                self.kvs.write_lock = True

                # Storing data
                self.kvs.complete_ring = []
                self.kvs.ring_metadata = {}
                self.kvs.ring_replicas = []

                data = parsedata.get('data', {})
                for item in data:
                    self.kvs.complete_ring.append({
                        'from': str(item["from"]),
                        'to_hash': str(item["to_hash"]),
                        'host': str(item["host"]),
                        'port': str(item["port"]),
                        'type': str(item["type"]),
                    })
                    if item['type'] == 'C':
                        self.kvs.ring_metadata[item['to_hash']] = {
                            'from': str(item["from"]),
                            'to_hash': str(item["to_hash"]),
                            'host': str(item["host"]),
                            'port': str(item["port"]),
                            'type': str(item["type"]),
                        }
                        if item['host'] == self.kvs.kv_data['host'] and item['port'] == self.kvs.kv_data['port']:
                            self.kvs.kv_data['to_hash'] = str(item['to_hash'])
                            self.kvs.kv_data['from'] = str(item['from'])
                    else:
                        self.kvs.ring_replicas.append({
                            'from': str(item["from"]),
                            'to_hash': str(item["to_hash"]),
                            'host': str(item["host"]),
                            'port': str(item["port"]),
                            'type': str(item["type"]),
                        })

                self.kvprint(f'Ring data extracted. C = {len(self.kvs.ring_metadata)} | R = {len(self.kvs.ring_replicas)}')
                if self.kvs.ring_replicas:
                    self.kvprint(f'Updating my replicas ...')
                    self.rep.update_replicas()
                else:
                    self.kvprint(f'No replicas. Closing connections in case of before having rep..')
                    self.kvs.i_am_replica_of = {}
                    self.kvs.ring_mine_replicas = {}
                    # if self.rep.connected_replicas:
                    #     for sock, replica in self.rep.connected_replicas.items():
                    #         sock.close()
                    #     self.rep.connected_replicas = {}


                self.kvprint(f'Updated ring. Number of nodes: {len(self.kvs.ring_metadata)}')
                self.kvs.write_lock = False

            else:
                self.kvprint(f'Shutdown in process. Ring not updated')
        elif 'write_lock_act' in request:
            self.kvs.write_lock = True
        elif request == 'write_lock_deact':
            self.kvs.write_lock = False
            self.kvprint(f'LOCK OFF')
        elif request == 'shutdown_kvserver' or request == 'shutdown_ecs':
            self.kvs.closing_all()
        elif request == 'arrange_ring':
            data = parsedata.get('data', {})
            if data is not None:
                self.kvprint(f'Init send_data_to_reponsable thread...')
                thread = threading.Thread(target=self.send_data_to_reponsable, args=(data,))
                thread.start()
            else:
                self.kvprint(f'arrange_ring  -> No node to send data')
                if self.kvs.shutdown:
                    self.handle_json_REPLY('kvserver_shutdown')
                    self.kvs.closing_all()
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
                    'id': self.kvs.kv_data['id'],
                    'name': self.kvs.kv_data['name'],
                    'host': self.kvs.kv_data['host'],
                    'port': self.kvs.kv_data['port'],
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
                'data': self.kvs.kv_data
            }
        elif request == 'kvserver_shutdown':
            # So the ECS knows we have already finish with the shutting down process
            return {
                'request': request,
                'data': self.kvs.kv_data
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
        # formatted_time = datetime.datetime.now().strftime("%H:%M:%S")
        # message = f'[{formatted_time}] {self.cli} {message}'

        if log == 'd':
            self.kvs.log.debug(message)
        elif log == 'i':
            self.kvs.log.info(message)
        elif log == 'e':
            self.kvs.log.error(message)


    # def ask_lock_ecs(self): # For the client too # Todo rethink
    #     self.handle_json_REPLY('ring_metadata')

    def send_data_to_reponsable(self, data):
        cli = '[send_data_to_reponsable]'
        with shelve.open(self.kvs.storage_dir, writeback=True) as db:
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
        sock.settimeout(self.kvs.sock_timeout)

        RETRY_INTERVAL = 0.5
        connecting_try = 0
        self.kvprint(f'{cli}Connecting to KVserver responsable {addr}:{port}....')
        while True:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((addr, port))
                self.kvprint(f'{cli}Connected to KVserver [{addr}:{port}].')
                # Todo something with the first msg so the other kvserver knows he is a kvserer
                sock.sendall(bytes(f'{self.kvs.kv_data["name"]} This is {self.kvs.kv_data["name"]} rearranging data.\r\n', encoding='utf-8'))
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
            with shelve.open(self.kvs.storage_dir, writeback=True) as db:
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
            with shelve.open(self.kvs.storage_dir, writeback=True) as db:
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
                            self.kvprint(f'{cli} Arranging data completed successfully ', msg)
                            break
                        else:
                            self.kvprint(f'{cli} Back msg: ', msg)
                else:
                    self.kvprint(f'{cli} No data. --> Closing socket', log='e')
                    break
            except Exception as e:
                self.kvprint(f'{cli}Exception handle_CONN: {e}', log='e')
                break
        self.kvprint(f'{cli}Successfully rearrange data. Closing kv-kv socket')
        sock.close()

        if self.kvs.shutdown:
            self.kvprint(f'{cli}MSG send: kvserver_shutdown ')
            self.handle_json_REPLY('kvserver_shutdown')
            self.kvprint(f'{cli} self.kvs.closing_all()')
            self.kvs.closing_all()

