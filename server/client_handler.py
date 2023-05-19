import sys
import shelve
import os
from cache import *


# ------------------------------------------------------------------------
class Client_handler:
    def __init__(self, client_fd, client_id, clients_list, cache_type, cache_cap, lock, logger):
        self.client_id = client_id
        self.lock = lock
        self.client_fd = client_fd
        self.log = logger

        clients_list.append(self.client_id)

        self.cache_init(cache_cap, cache_type)

        self.handle_conn()

        # Once the conn ended
        if self.client_id in clients_list:
            clients_list.remove(self.client_id)



    def handle_conn(self):
        self.log.debug(self.hprint(f' --> Connected'))
        self.handle_response('Welcome client. You are connected to the server')
        while True:
            request = self.client_fd.recv(1024)
            response = self.handle_request(request)
            self.handle_response(response)
            if response == 'End connection':
                break

        self.log.debug(self.hprint(f' --> End connection'))
        self.client_fd.close()

    def handle_request(self, request):
        method, *args = request.decode().split()

        if method == 'put' and len(args) == 2:
            key, value = args
            self.cache.put(key, value)
            with self.lock:
                return self.PUT_request(key, value)

        elif method == 'get' and len(args) == 1:
            key = args[0]
            if self.cache.get(key):
                self.log.debug(self.hprint(f'{key} {self.cache.get(key)} at CACHE'))
                return self.cache.get(key)
            else:
                self.log.debug(self.hprint(f'{key} checking STORAGE'))
                value = self.GET_request(key)
                self.cache.put(key, value)
                return value

        elif method == 'delete' and len(args) == 1:
            key = args[0]
            self.cache.delete(key)
            return self.DELETE_request(key)


        elif method == 'close':
            self.log.debug(self.hprint(f'Request => close'))
            return 'End connection'
        else:
            if method == 'put' and len(args) != 2:
                return 'error only 2 arguments'
            elif method == 'get' and len(args) != 1:
                return 'error only 1 arguments'
            elif method == 'delete' and len(args) != 1:
                return 'error only 1 arguments'
            else:
                return 'error invalid method'



    def PUT_request(self, key, value):
        self.log.debug(self.hprint(f'Request => put {key} {value}'))
        try:
            with shelve.open('storage.db', writeback=True) as db:
                if key in db:
                    if db.get(key) == value:
                        self.log.debug(self.hprint(f"{key} |{value} already exists with same values"))
                        self.log.info(self.hprint(f'put_not_update {key}'))
                        return 'SUCCESS'
                    else:
                        self.log.debug(self.hprint(f" Key>{key} already exists. Overwriting value."))
                        db[key] = value
                        self.log.info(self.hprint(f'put_update {key}'))
                        return 'SUCCESS'
                else:
                    db[key] = value
                    self.log.debug(self.hprint(f"Data stored: key={key}, value={value}"))
                    self.log.info(self.hprint(f'put_success {key}'))
                    return 'SUCCESS'
        except:
            self.log.info(self.hprint('put_error'))
            return 'ERROR'

    def GET_request(self, key):
        self.log.debug(self.hprint(f'Request => get {key}'))
        try:
            with shelve.open('storage.db') as db:
                value = db.get(key)
                if value is not None:
                    self.log.debug(self.hprint(f'Key {key}found. Value {value}'))
                    self.log.info(self.hprint(f'get_sucess {value}'))
                    return 'SUCCESS'
                else:
                    self.log.debug(self.hprint(f'Key {key} not found'))
                    self.log.info(self.hprint(f'get_error {key}'))
                    return 'SUCCESS'
        except:
            self.log.info(self.hprint('get_error {key}'))
            return 'ERROR'


    def DELETE_request(self, key):  # TODO
        self.log.debug(self.hprint(f'Request => delete {key}'))
        try:
            with shelve.open('storage.db') as db:
                if key in db:
                    self.log.debug(self.hprint(f'Key {key} found.'))
                    value = db.get(key)
                    del db[key]
                    self.log.info(self.hprint(f'delete_success {key} {value}'))
                    return 'SUCCESS'
                else:
                    self.log.debug(self.hprint(f'Key {key} not found'))
                    self.log.info(self.hprint(f'delete_error {key}'))
                    return 'ERROR'
        except:
            self.log.info(self.hprint(f'delete_error {key}'))
            return 'ERROR'



    def handle_response(self, response):
        response += " \r\n"
        self.client_fd.sendall(bytes(response, encoding='utf-8'))


    def hprint(self, *args):
        # if len(args) != 0:
        #     sys.stdout.write(
        #         f"\tHandler {self.client_id} {self.client_fd.getsockname()}> {' '.join(str(arg) for arg in args)}")
        #     sys.stdout.write('\n')
        if len(args) != 0:
            return f"\t\tHandler {self.client_id} {self.client_fd.getsockname()}> {' '.join(str(arg) for arg in args)}"

    def cache_init(self, cache_cap, cache_type):
        if cache_type == 'fifo':
            self.cache = FIFOCache(cache_cap)
        elif cache_type == 'lru':
            self.cache = LRUCache(cache_cap)
        elif cache_type == 'lfu':
            self.cache = LFUCache(cache_cap)
        else:
            self.log.DEBUG(self.hprint(f'error cache selection'))