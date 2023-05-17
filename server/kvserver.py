import sys
import csv
import time
import threading
import shelve

from cache import *


# ------------------------------------------------------------------------
class KVServer:
    def __init__(self, client_fd, id, clients_list, cache_type, cache_cap, lock):
        self.id = id
        self.lock = lock
        self.client_fd = client_fd
        clients_list.append(self.id)

        # Cache initialization
        if cache_type == 'fifo':
            self.cache = FIFOCache(cache_cap)
        elif cache_type == 'lru':
            self.cache = LRUCache(cache_cap)
        elif cache_type == 'lfu':
            self.cache = LFUCache(cache_cap)
        else:
            self.KVS_print(f'error cache selection')

        self.handle_conn()

        if self.id in clients_list:
            clients_list.remove(self.id)
        else:
            print(f"Client id is not found in the list.")


    def handle_conn(self):
        self.KVS_print(f' --> Connected')
        self.handle_response('Welcome client. You are connected to the server')
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
        self.cache.print_cache()

        if method == 'put' and len(args) == 2:
            key, value = args
            self.cache.put(key, value)
            with self.lock:
                return self.PUT_request(key, value)

        elif method == 'get' and len(args) == 1:
            key = args[0]
            # self.cache.print_cache()
            if self.cache.get(key):
                self.KVS_print(f'{key} {self.cache.get(key)} at CACHE')
                return self.cache.get(key)
            else:
                self.KVS_print(f'{key} checking STORAGE')
                value = self.GET_request(key)
                self.cache.put(key, value)
                return value

        elif method == 'delete' and len(args) == 0:
            pass
        elif method == 'close':
            self.KVS_print(f'Request => close')
            return 'End connection'
        else:
            return 'Server>Invalid method'



    def PUT_request(self, key, value):
        self.KVS_print(f'Request => put {key} {value}')
        try:
            with shelve.open('storage.db', writeback=True) as db:
                if key in db:
                    if db.get(key) == value:
                        self.KVS_print(f"{key} |{value} already exists with same values")
                        return f'put_not_update {key}'
                    else:
                        self.KVS_print(f" Key>{key} already exists. Overwriting value.")
                        db[key] = value
                        return f'put_update {key}'
                else:
                    db[key] = value
                    self.KVS_print(f"Data stored: key={key}, value={value}")
                    return f'put_success {key}'
        except:
            return 'put_error'

    def GET_request(self, key):
        self.KVS_print(f'Request => get {key}')
        try:
            with shelve.open('storage.db') as db:
                value = db.get(key)
                if value is not None:
                    self.KVS_print(f'Key {key}found. Value {value}')
                    return value
                else:
                    self.KVS_print(f'Key {key} not found')
                    return f'get_error {key}'
        except:
            return 'get_error'


    def DELETE_request(self, request):  # TODO
        pass

    def handle_response(self, response):
        response += " \r\n"
        self.client_fd.sendall(bytes(response, encoding='utf-8'))

    def KVS_print(self, *args):
        if len(args) != 0:
            sys.stdout.write(
                f"\tKVServer {self.id} {self.client_fd.getsockname()}> {' '.join(str(arg) for arg in args)}")
            sys.stdout.write('\n')
