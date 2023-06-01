import sys
import shelve
import os
from cache_classes import *
import socket
import json


# ------------------------------------------------------------------------
class Client_handler:
    def __init__(self, client_fd, client_id, clients_conn, cache_type, cache_cap, lock, logger, storage_dir):
        self.client_id = client_id
        self.cli = f'\t\tHandler{self.client_id}>'

        self.client_fd = client_fd
        # self.client_fd.settimeout(100)
        self.conn_status = True
        self.log = logger
        self.lock = lock
        self.storage_dir = storage_dir
        # self.welcome_msg = f'Connection to KVServer established: /{self.client_fd.getsockname()[0]} / {self.client_fd.getsockname()[1]}'
        self.welcome_msg = 'hello'

        clients_conn[client_id] = self

        self.cache_init(cache_cap, cache_type)
        self.handle_conn()

        self.log.info(f'{self.cli} Closing handler ')

        clients_conn[client_id] = None
        del self

    def handle_conn(self):
        self.log.info(f'{self.cli} Connected')
        self.handle_RESPONSE(self.welcome_msg)

        while self.conn_status:
            try:
                request = self.client_fd.recv(128 * 1024)
                request = request.replace(b'\\r\\n', b'\r\n')
                messages = request.decode().split('\r\n')

                for msg in messages:
                    if msg is None or msg == " " or not msg:
                        break
                    else:
                        response = self.handle_REQUEST(msg)
                        self.log.info(f'{self.cli} {response}')
                        self.handle_RESPONSE(response)
                        if response == 'End connection':
                            break

            except socket.timeout:
                self.log.debug(f'{self.cli}Time out client')
                break
        self.log.debug(f'{self.cli} End connection')
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
                self.log.debug(f'{self.cli} {key} at CACHE')
                return f'get_success {key} {self.cache.get(key)}'
            else:
                self.log.debug(f'{self.cli}{key} checking STORAGE')
                value = self.GET_request(key)
                self.cache.put(key, value)
                return value

        elif method == 'delete' and len(args) == 1:
            key = args[0]
            self.cache.delete(key) #Todo error, the updated value is in the cache not in the storage
            return self.DELETE_request(key)

        elif method == 'show':
            self.log.debug(f'{self.cli}Request => show db')
            return self.print_storage()


        elif method == 'close':
            self.log.debug(f'{self.cli}Request => close')
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
        self.log.debug(f'{self.cli}Request => put {key} {value}')
        try:
            with shelve.open(self.storage_dir, writeback=True) as db:
                if key in db:
                    if db.get(key) == value:
                        self.log.debug(f"{self.cli}{key} |{value} already exists with same values")
                        return f'put_update {key}' #Todo creo que esta respuesta me la he inventado
                    else:
                        self.log.debug(f"{self.cli} Key>{key} already exists. Overwriting value.")
                        db[key] = value
                        return f'put_update {key}'
                else:
                    db[key] = value
                    self.log.debug(f'{self.cli}{key}Data stored: key={key}, value={value}')
                    return f'put_success {key}'
        except:
            return 'put_error'


    def GET_request(self, key):
        self.log.debug(f'{self.cli}{key}Request => get {key}')
        try:
            with shelve.open(self.storage_dir, flag='r') as db:
                value = db.get(key)
                if value is not None:
                    self.log.debug(f'{self.cli}Key {key} found. Value {value}')
                    return f'get_success {key} {value}'
                else:
                    self.log.debug(f'{self.cli}Key {key} not found')
                    return f'get_error {key}'
        except:
            return f'get_error {key}'


    def DELETE_request(self, key):  # TODO
        self.log.debug(f'{self.cli}Request => delete {key}')
        # self.cache.print_cache()
        try:
            with shelve.open(self.storage_dir, writeback=True) as db:
                if key in db:
                    self.log.debug(f'{self.cli}Key {key} found.')
                    value = db.get(key)
                    del db[key]
                    return f'delete_success {key} {value}'
                else:
                    self.log.debug(f'{self.cli}Key {key} not found')
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
            self.log.info(f'{self.cli}error cache selection')

    def print_storage(self):
        with shelve.open(self.storage_dir, flag='r') as db:
            message = f"\n------------------\n"
            message += f'All key-value pairs\n'

            counter = 1

            for key, value in db.items():
                message +=f"Item {counter}==> {key} | {value}\n"
                counter += 1
            message += f"------------------\n"
            return message


