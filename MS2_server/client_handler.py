import sys
import shelve
import os
from cache import *
import socket


# ------------------------------------------------------------------------
class Client_handler:
    def __init__(self, client_fd, client_id, clients_list, clients_conn, cache_type, cache_cap, lock, logger):
        self.client_id = client_id
        self.cli = f'\t\tHandler{self.client_id}>'

        self.client_fd = client_fd
        self.client_fd.settimeout(10)
        self.log = logger
        self.lock = lock
        # self.welcome_msg = f'Connection to KVServer established: /{self.kvs_fd.getsockname()[0]} / {self.kvs_fd.getsockname()[1]}'
        self.welcome_msg = f'hello pedro: /{self.client_fd.getsockname()[0]} / {self.client_fd.getsockname()[1]}'

        clients_conn[client_id] = self

        self.cache_init(cache_cap, cache_type)
        self.handle_conn()

        self.log.info(f'{self.cli} Closing handler ')
        del clients_conn[client_id]
        del self

    def handle_conn(self):
        self.log.info(f'{self.cli} Connected')
        # self.handle_response('Welcome client. You are connected to the server')
        self.handle_response(self.welcome_msg)
        while True:
            try:
                request = self.client_fd.recv(128 * 1024)
                # print('request before deco >', repr(request))
                # print('request  deco >', repr(request.decode()))
                # print('request  deco split>', repr(request.decode().split('\r\n')))
                request = request.replace(b'\\r\\n', b'\r\n')
                messages = request.decode().split('\r\n')

                for msg in messages:
                    if msg is None or msg == " " or not msg:
                        break
                    response = self.handle_request(msg)
                    self.handle_response(response)
                    if response == 'End connection':
                        break
            except socket.timeout:
                self.log.debug(f'{self.cli}Time out client')
                break

        self.log.debug(f'{self.cli} End connection')
        self.client_fd.close()

    def handle_request(self, msg):
        method, *args = msg.split()
        # print('request', request)
        # print(len(data))

        if method == 'put' and len(args) > 1:
            key, value = args[0], ' '.join(args[1:])
            # print('key', key)
            # print('value', value)s
            self.cache.put(key, value)
            with self.lock:
                return self.PUT_request(key, value)

        elif method == 'get' and len(args) == 1:
            key = args[0]
            if self.cache.get(key):
                self.log.debug(f'{self.cli}{key} {self.cache.get(key)} at CACHE')
                return self.cache.get(key)
            else:
                self.log.debug(f'{self.cli}{key} checking STORAGE')
                value = self.GET_request(key)
                self.cache.put(key, value)
                return value

        elif method == 'keyrange' and len(args) == 1:
            return f'keyrange'

        elif method == 'delete' and len(args) == 1:
            key = args[0]
            self.cache.print_cache()

            self.cache.delete(key)
            self.cache.print_cache()

            return self.DELETE_request(key)


        elif method == 'close':
            self.log.debug(f'{self.cli}Request => close')
            return 'End connection'
        else:
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
            with shelve.open('storage.db', writeback=True) as db:
                if key in db:
                    if db.get(key) == value:
                        self.log.debug(f"{self.cli}{key} |{value} already exists with same values")
                        self.log.info(f'{self.cli}put_not_update {key}')
                        return f'put_not_update {key}'
                    else:
                        self.log.debug(f"{self.cli} Key>{key} already exists. Overwriting value.")
                        db[key] = value
                        self.log.info(f'{self.cli}put_update {key}')
                        return f'put_update {key}'
                else:
                    db[key] = value
                    self.log.debug(f'{self.cli}{key}Data stored: key={key}, value={value}')
                    self.log.info(f'{self.cli}{key}put_success {key}')
                    return f'put_success {key}'
        except:
            self.log.info(f'{self.cli}{key}put_error')
            return 'put_error'

    def GET_request(self, key):
        self.log.debug(f'{self.cli}{key}Request => get {key}')
        try:
            with shelve.open('storage.db') as db:
                value = db.get(key)
                if value is not None:
                    self.log.debug(f'{self.cli}Key {key} found. Value {value}')
                    self.log.info(f'{self.cli}get_sucess {value}')
                    return f'get_success {key} {value}'
                else:
                    self.log.debug(f'{self.cli}Key {key} not found')
                    self.log.info(f'{self.cli}get_error {key}')
                    return f'get_error {key}'
        except:
            self.log.info(f'{self.cli}get_error {key}')
            return f'get_error {key}'

    def DELETE_request(self, key):  # TODO
        self.log.debug(f'{self.cli}Request => delete {key}')
        # self.cache.print_cache()
        try:
            with shelve.open('storage.db') as db:
                if key in db:
                    self.log.debug(f'{self.cli}Key {key} found.')
                    value = db.get(key)
                    del db[key]
                    self.log.info(f'{self.cli}delete_success {key} {value}')
                    # self.cache.print_cache()
                    return f'delete_success {key} {value}'
                else:
                    self.log.debug(f'{self.cli}Key {key} not found')
                    self.log.info(f'{self.cli}delete_error {key}')
                    # self.cache.print_cache()
                    return f'delete_error {key}'
        except:
            self.log.info(f'{self.cli}delete_error {key}')
            return f'delete_error {key}'

    def handle_response(self, response):
        chunk_size = 128 * 1024  # 128 KBytes
        # Split the message into chunks
        chunks = [response[i:i + chunk_size] for i in range(0, len(response), chunk_size)]

        # Add a newline character to the last chunk
        if chunks:
            chunks[-1] += " \r\n"
        # print('number of chunks: ', chunks)
        # Send each chunk
        for chunk in chunks:
            self.client_fd.sendall(bytes(chunk, encoding='utf-8'))


    def cache_init(self, cache_cap, cache_type):
        if cache_type == 'FIFO':
            self.cache = FIFOCache(cache_cap)
        elif cache_type == 'LRU':
            self.cache = LRUCache(cache_cap)
        elif cache_type == 'LFU':
            self.cache = LFUCache(cache_cap)
        else:
            self.log.info(f'{self.cli}error cache selection')
