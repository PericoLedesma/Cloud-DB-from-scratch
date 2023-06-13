import sys
import shelve
import os
from cache_classes import *
import socket
import json
import hashlib


# ------------------------------------------------------------------------
class Client_handler:
    def __init__(self, client_data, clients_conn, ring_metadata, cache_config, lock, storage_dir, printer_config,
                 timeout_config):

        self.kv_data = client_data[0]
        self.client_id = client_data[1]
        self.client_fd = client_data[2]
        self.addr = client_data[3]

        self.heartbeat = timeout_config[0]
        self.tictac = timeout_config[1]
        self.timeout = timeout_config[2]

        self.client_fd.settimeout(self.timeout)

        self.ring_metadata = ring_metadata

        self.conn_status = True
        self.lock = lock
        self.storage_dir = storage_dir

        self.welcome_msg = f'Connection to KVServer established: /{self.client_fd.getsockname()[0]} / {self.client_fd.getsockname()[1]}'
        # self.welcome_msg = 'hello'
        self.cli = f'[Handler C{self.client_id}]>'
        self.print_cnfig = printer_config

        clients_conn[self.client_id] = self

        self.cache_init(cache_config)
        self.handle_CONN()

        clients_conn[self.client_id] = None
        del self

    def handle_CONN(self):
        self.kvprint(f' Connected')
        self.handle_RESPONSE(self.welcome_msg)
        while self.conn_status:
            try:
                data = self.client_fd.recv(128 * 1024)
                if data:
                    request = data.replace(b'\\r\\n', b'\r\n')
                    messages = request.decode().split('\r\n')
                    for msg in messages:
                        if msg is None or msg == " " or not msg:
                            break
                        else:
                            self.handle_RECV(msg)
                            self.heartbeat()
                else:
                    self.kvprint(f'No data --> Closing socket', c='r')
                    break

            except socket.timeout:
                self.kvprint(f'Time out client --> Closing socket', c='r')
                break
            except Exception as e:
                self.kvprint(f'Exception: {e} --> Closing socket', c='r')
        self.client_fd.close()

    def handle_RECV(self, msg):
        method, *args = msg.split()
        self.cache.print_cache()
        if method in ['put', 'get', 'delete']: # Check if the key is for this server:
            key = args[0]
            if self.key_checker(key) is False:
                print('Not for this server')  # Todo
                kvs_target = self.search_interval(key)
            else:
                self.handle_REQUEST(method, *args)
        else:
            self.handle_REQUEST(method, *args)

    def handle_REQUEST(self, request, *args):
        if request == 'put' and len(args) > 1:
            key, value = args[0], ' '.join(args[1:])
            self.cache.put(key, value)
            with self.lock:
                self.PUT_request(key, value)
        elif request == 'get' and len(args) == 1:
            key = args[0]
            if self.cache.get(key):
                self.kvprint(f' {key} at CACHE')
                self.handle_RESPONSE(f'get_success {key} {self.cache.get(key)}')
            else:
                self.kvprint(f'{key} not in cache. Checking STORAGE')
                self.GET_request(key)
        elif request == 'delete' and len(args) == 1:
            key = args[0]
            self.cache.delete(key)  # Todo error, the updated value is in the cache not in the storage
            self.DELETE_request(key)
        elif request == 'show':
            self.kvprint(f'Request => show db')
            self.handle_RESPONSE(self.print_storage())
        elif request == 'close':
            self.kvprint(f'Request => close')
            self.conn_status = False
            self.handle_RESPONSE('End connection with client')
        else:  # ERRORS
            if request == 'pass':  # Logic when the
                pass
            elif request == 'put' and len(args) < 2:
                self.handle_RESPONSE('error not enough arguments')
            elif request == 'get' and len(args) != 1:
                self.handle_RESPONSE('error only 1 arguments')
            elif request == 'delete' and len(args) != 1:
                self.handle_RESPONSE('error only 1 arguments')
            else:
                self.handle_RESPONSE('error unknown command!')

    def PUT_request(self, key, value):
        self.kvprint(f'Request => put {key} {value}')
        try:
            with shelve.open(self.storage_dir, writeback=True) as db:
                if key in db:
                    if db.get(key) == value:
                        self.kvprint(f'{key} |{value} already exists with same values')
                        self.handle_RESPONSE(f'put_update {key}')  # Todo creo que esta respuesta me la he inventado
                    else:
                        self.kvprint(f' Key>{key} already exists. Overwriting value.')
                        db[key] = value
                        self.handle_RESPONSE(f'put_update {key}')
                else:
                    db[key] = value
                    self.kvprint(f'{key}Data stored: key={key}, value={value}')
                    self.handle_RESPONSE(f'put_success {key}')
        except:
            self.handle_RESPONSE('put_error')

    def GET_request(self, key):
        self.kvprint(f'{key}Request => get {key}')
        try:
            with shelve.open(self.storage_dir, flag='r') as db:
                value = db.get(key)
                if value is not None:
                    self.kvprint(f'Key {key} found. Value {value}')
                    self.handle_RESPONSE(f'get_success {key} {value}')
                    self.cache.put(key, value)
                else:
                    self.kvprint(f'Key {key} not found')
                    self.handle_RESPONSE(f'get_error {key}')
        except:
            self.handle_RESPONSE(f'get_error {key}')

    def DELETE_request(self, key):  # TODO
        self.kvprint(f'Request => delete {key}')
        # self.cache.print_cache()
        try:
            with shelve.open(self.storage_dir, writeback=True) as db:
                if key in db:
                    self.kvprint(f'Key {key} found.')
                    value = db.get(key)
                    del db[key]
                    self.handle_RESPONSE(f'delete_success {key} {value}')
                else:
                    self.kvprint(f'Key {key} not found')
                    self.handle_RESPONSE(f'delete_error {key}')
        except:
            self.handle_RESPONSE(f'delete_error {key}')

    def handle_RESPONSE(self, response):
        self.kvprint(f'Reply sent:{response}')
        self.client_fd.sendall(bytes(response, encoding='utf-8'))

    def key_checker(self, key):
        print('RING')
        for key, value in self.ring_metadata.items():
            print(key, '|', value)
        if len(self.ring_metadata) == 1:
            self.kvprint('key_checker: Just one server. Proceed')
            return True
        elif len(self.ring_metadata) > 1:
            list_hash = list(self.ring_metadata.keys()).sort()
            print(list_hash)
            hash = self.hash(key)
            if list_hash[0] == self.kv_data['hash_key']: # If it is the last range
                if list_hash[-1] < hash or list_hash[0] > hash:
                    print('In the last interval')
                    return True
                else:
                    print('KVserver in last interval, but key hash not')
                    return False
            else:
                print(f'key_checker: kvs_interval[{self.kv_data["previous_hash"]}|{hash}|{self.kv_data["hash_key"]}')
                if hash > self.kv_data['previous_hash'] and hash < self.kv_data['hash_key']:
                    print('True')
                    return True
                else:
                    print('False')
                    return False
        elif self.ring_metadata is None or self.ring_metadata == {}:
            self.kvprint('Error in key_checker. self.ring_metadata EMPTY', c='r')
        else:
            self.kvprint('Error in key_checker. Outside the logic. Check. ', c='r')

    def search_interval(self, hash):
        for key, value in self.ring_metadata.items():
            if hash > value['previous_hash'] and hash < value['hash_key']:
                print(f'Interval founded [{value["previous_hash"]}|{hash}|{value["hash_key"]}')
                print('Target kvserver:', value)
                return value

    def hash(self, key):
        md5_hash = hashlib.md5(key.encode()).hexdigest()
        md5_hash = int(md5_hash[:3], 16)
        return md5_hash

    def cache_init(self, cache_config):
        cache_type, cache_cap = cache_config[0], cache_config[1]
        if cache_type == 'FIFO':
            self.cache = FIFOCache(cache_cap)
        elif cache_type == 'LRU':
            self.cache = LRUCache(cache_cap)
        elif cache_type == 'LFU':
            self.cache = LFUCache(cache_cap)
        else:
            self.kvprint(f'error cache selection')

    def print_storage(self):
        with shelve.open(self.storage_dir, flag='r') as db:
            message = f"\n------------------\n"
            message += f'All key-value pairs\n'
            counter = 1
            for key, value in db.items():
                message += f"Item {counter}==> {key} | {value}\n"
                counter += 1
            message += f"------------------\n"
            return message

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
