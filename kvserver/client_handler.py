import sys
import shelve
import os
from cache_classes import *
import socket
import json


# ------------------------------------------------------------------------
class Client_handler:
    def __init__(self, client_fd, client_id, clients_conn, cache_type, cache_cap, lock, storage_dir, printer_config):
        self.client_id = client_id
        self.cli = f'[Handler{self.client_id}]>'
        self.print_cnfig = printer_config

        self.client_fd = client_fd
        # self.client_fd.settimeout(100)
        self.conn_status = True
        self.lock = lock
        self.storage_dir = storage_dir

        # self.welcome_msg = f'Connection to KVServer established: /{self.client_fd.getsockname()[0]} / {self.client_fd.getsockname()[1]}'
        self.welcome_msg = 'hello'

        clients_conn[client_id] = self

        self.cache_init(cache_cap, cache_type)
        self.handle_conn()

        clients_conn[client_id] = None
        del self

    def handle_conn(self):
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
                            response = self.handle_REQUEST(msg)
                            self.kvprint(f' {response}')
                            self.handle_RESPONSE(response)
                            if response == 'End connection':
                                break
                else:
                    self.kvprint(f'No data --> Closing socket', c='r')
                    break

            except socket.timeout:
                self.kvprint(f'Time out client --> Closing socket', c='r')
                break
            except Exception as e:
                self.kvprint(f'Exception: {e} --> Closing socket', c='r')
        self.client_fd.close()


    def handle_REQUEST(self, msg):
        method, *args = msg.split()
        self.cache.print_cache()
        if method == 'put' and len(args) > 1:
            key, value = args[0], ' '.join(args[1:])
            self.cache.put(key, value)
            with self.lock:
                return self.PUT_request(key, value)

        elif method == 'get' and len(args) == 1:
            key = args[0]
            if self.cache.get(key):
                self.kvprint(f' {key} at CACHE')
                return f'get_success {key} {self.cache.get(key)}'
            else:
                self.kvprint(f'{key} checking STORAGE')
                value = self.GET_request(key)
                self.cache.put(key, value)
                return value

        elif method == 'delete' and len(args) == 1:
            key = args[0]
            self.cache.delete(key) #Todo error, the updated value is in the cache not in the storage
            return self.DELETE_request(key)

        elif method == 'show':
            self.kvprint(f'Request => show db')
            return self.print_storage()

        elif method == 'close':
            self.kvprint(f'Request => close')
            self.conn_status = False
            return 'End connection with client'

        else: # ERRORS
            if method == 'put' and len(args) < 2:
                return 'error not enough arguments'
            elif method == 'get' and len(args) != 1:
                return 'error only 1 arguments'
            elif method == 'delete' and len(args) != 1:
                return 'error only 1 arguments'
            else:
                return 'error unknown command!'


    def PUT_request(self, key, value):
        self.kvprint(f'Request => put {key} {value}')
        try:
            with shelve.open(self.storage_dir, writeback=True) as db:
                if key in db:
                    if db.get(key) == value:
                        self.kvprint(f'{key} |{value} already exists with same values')
                        return f'put_update {key}' #Todo creo que esta respuesta me la he inventado
                    else:
                        self.kvprint(f' Key>{key} already exists. Overwriting value.')
                        db[key] = value
                        return f'put_update {key}'
                else:
                    db[key] = value
                    self.kvprint(f'{key}Data stored: key={key}, value={value}')
                    return f'put_success {key}'
        except:
            return 'put_error'


    def GET_request(self, key):
        self.kvprint(f'{key}Request => get {key}')
        try:
            with shelve.open(self.storage_dir, flag='r') as db:
                value = db.get(key)
                if value is not None:
                    self.kvprint(f'Key {key} found. Value {value}')
                    return f'get_success {key} {value}'
                else:
                    self.kvprint(f'Key {key} not found')
                    return f'get_error {key}'
        except:
            return f'get_error {key}'


    def DELETE_request(self, key):  # TODO
        self.kvprint(f'Request => delete {key}')
        # self.cache.print_cache()
        try:
            with shelve.open(self.storage_dir, writeback=True) as db:
                if key in db:
                    self.kvprint(f'Key {key} found.')
                    value = db.get(key)
                    del db[key]
                    return f'delete_success {key} {value}'
                else:
                    self.kvprint(f'Key {key} not found')
                    return f'delete_error {key}'
        except:
            return f'delete_error {key}'

    def handle_RESPONSE(self, response):
        self.client_fd.sendall(bytes(response, encoding='utf-8'))


    def cache_init(self, cache_cap, cache_type):
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