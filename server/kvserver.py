import sys
import csv
import time
import threading
import shelve

from cache import *


# ------------------------------------------------------------------------
class KVServer:
    def __init__(self, client_fd, id, cache_type, cache_cap):
        self.id = id
        self.client_fd = client_fd
        self.lock = threading.Lock()

        # Cache initialization
        if cache_type == 'fifo':
            self.cache = FIFOCache(cache_cap)
        elif cache_type == 'lru':
            self.cache = LRUCache(cache_cap)
        elif cache_type == 'lfu':
            self.cache = LFUCache(cache_cap)


        else:
            self.cache = FIFOCache(cache_cap)

        self.handle_conn()

    def handle_conn(self):
        self.KVS_print(f' --> Connected')
        while True:
            request = self.client_fd.recv(1024)
            response = self.handle_request(request)
            self.handle_response(response)
            if response == 'End connection':
                break

        self.KVS_print(f' --> End connection')
        self.client_fd.close()

    def handle_request(self, request):
        method, *args = request.decode().split()

        if method == 'put':
            key, value = args
            self.cache.print_cache()
            self.cache.put(key, value)
            with self.lock:
                return self.PUT_request(key, value)

        elif method == 'get':
            key = args[0]
            self.cache.print_cache()
            if self.cache.get(key):
                self.KVS_print(f'{key} {self.cache.get(key)} at CACHE')
                return self.cache.get(key)
            else:
                self.KVS_print(f'{key} checking STORAGE')
                return self.GET_request(key)

        elif method == 'delete':
            pass
        elif method == 'close':
            return 'End connection'
        else:
            return 'Server>Invalid method'

    def PUT_request(self, key, value):
        self.KVS_print(f'Processing => put {key} {value}')
        try:
            with shelve.open('storage.db', writeback=True) as db:
                if key in db:
                    if db.get(key) == value:
                        self.KVS_print(f"{key} |{value} already exists")
                        return f'put_not_update {key}'
                    else:
                        self.KVS_print(f" Key>{key} already exists. Overwriting value.")
                        return f'put_update {key}'
                else:
                    db[key] = value
                    self.KVS_print(f"Data stored: key={key}, value={value}")
                    return f'put_success {key}'
        except:
            return 'put_error'

    def GET_request(self, key):
        with shelve.open('storage.db') as db:
            value = db.get(key)
            if value is not None:
                self.KVS_print(f'Key {key}found. Value {value}')
                return value
            else:
                self.KVS_print(f'Key {key} not found')
                return 'no key'


    def DELETE_request(self, request):  # TODO
        pass

    def handle_response(self, response):
        self.client_fd.sendall(bytes(response, encoding='utf-8'))

    def KVS_print(self, *args):
        if len(args) != 0:
            sys.stdout.write(
                f"\tKVServer {self.id} {self.client_fd.getsockname()}> {' '.join(str(arg) for arg in args)}")
            sys.stdout.write('\n')
