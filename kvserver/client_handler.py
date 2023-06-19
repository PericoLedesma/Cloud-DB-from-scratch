from cache_classes import *
import shelve
import socket
import hashlib

# ------------------------------------------------------------------------
class Client_handler:
    def __init__(self,
                 clients_conn,
                 client_data,
                 ask_ring_metadata,
                 cache_config,
                 lock,
                 ask_lock_write_value,
                 storage_dir,
                 printer_config,
                 timeout_config):

        self.clients_conn = clients_conn
        self.kv_data = client_data[0]
        self.client_id = client_data[1]
        self.client_fd = client_data[2]
        self.addr = client_data[3]

        self.heartbeat = timeout_config[0]
        self.tictac = timeout_config[1]
        self.timeout = timeout_config[2]

        self.client_fd.settimeout(self.timeout)

        self.ask_ring_metadata = ask_ring_metadata
        self.ask_lock_write_value = ask_lock_write_value

        self.conn_status = True
        self.lock = lock
        self.storage_dir = storage_dir

        self.welcome_msg = f'Hi! Connection to {self.kv_data["name"]} established'
        self.cli = f'[Handler C{self.client_id}]>'
        self.print_cnfig = printer_config

        # START
        self.cache_init(cache_config)
        self.kvprint(f' Connected')
        self.handle_RESPONSE(self.welcome_msg)


    def handle_CONN(self, shutdown=False):
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
                            print('Message recv: ', msg)
                            if len(self.ask_ring_metadata()) > 0:
                                self.handle_RECV(msg, shutdown)
                            elif len(self.ask_ring_metadata()) == 0:
                                self.handle_RESPONSE('server_stopped')
                            self.heartbeat()
                else:
                    self.kvprint(f'No data --> Closing socket', log='e')
                    break
            except socket.timeout:
                self.kvprint(f'Time out client --> Closing socket', log='e')
                break
            except Exception as e:
                self.kvprint(f'Exception: {e} --> Closing socket', log='e')
                break
        self.clients_conn[self.client_id] = None
        self.client_fd.close()
        self.kvprint(f'----- Closing handler ------')
        del self


    def handle_RECV(self, msg, shutdown):
        method, *args = msg.split()
        if method in ['put', 'delete'] and shutdown is False:  # Some checks
            if self.ask_lock_write_value() is False:
                key = args[0]
                hash = self.hash(key)
                if self.key_checker(hash) is False:
                    self.handle_RESPONSE(f'server_not_responsible') # TODO delete data
                else:
                    self.handle_REQUEST(method, *args)
            elif self.ask_lock_write_value():
                self.handle_RESPONSE('server_write_lock')
        elif method in ['get']:
            if shutdown:
                self.handle_REQUEST(method, *args)
            else:
                hash = self.hash(args[0])
                if self.key_checker(hash) is False:
                    self.handle_RESPONSE(f'server_not_responsible') # TODO delete data
                else:
                    self.handle_REQUEST(method, *args)
        elif method in ['keyrange']:
            self.handle_REQUEST(method, *args)
        elif method in ['show', 'close']:
            self.handle_REQUEST(method, *args)
        else:
            if method in ['put', 'delete'] and shutdown:
                self.handle_RESPONSE('server_stopped')
            else:
                self.handle_RESPONSE('error unknown command!')

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
        elif request == 'keyrange':
            self.kvprint(f'Request => keyrange')
            message = ''
            print(f'------ Keyranges -----')
            for value in self.ask_ring_metadata().values():  # Posible problem por el orden
                message = f'{message}{value[0]},{value[1]},{value[2]}:{value[3]};'
                print(f'{value[0]},{value[1]},{value[2]}:{value[3]};')
            print(f'------ Keyranges -----')
            self.handle_RESPONSE(message)
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
        except Exception as e:
            self.kvprint(f'Exception in put request: {e} ', log='e')
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
        except Exception as e:
            self.kvprint(f'Exception in get request: {e} ')
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
        except Exception as e:
            self.kvprint(f'Exception in delete request: {e} ')
            self.handle_RESPONSE(f'delete_error {key}')

    def handle_RESPONSE(self, response):
        self.kvprint(f'Reply sent:{response}')
        self.client_fd.sendall(bytes(f'{response}\r\n', encoding='utf-8'))

    def key_checker(self, hash):
        if len(self.ask_ring_metadata()) == 1:
            if self.ask_ring_metadata()[self.kv_data['hash_key']]:
                return True
            else:
                raise Exception('Key checker. Check this flow')
        elif len(self.ask_ring_metadata()) > 1:
            list_hash = list(self.ask_ring_metadata()).copy()
            sorted_hash_list = sorted(list_hash, key=lambda x: int(x, 16))
            if sorted_hash_list[0] == self.kv_data['hash_key']:  # If it is the last range
                if sorted_hash_list[-1] < hash or sorted_hash_list[0] > hash:
                    # self.kvprint(f'key_checker: Last interval match')
                    return True
                else:
                    # self.kvprint(f'key_checker: KVserver in last interval, but key hash not')
                    return False
            else:
                # self.kvprint(f'key_checker: kvs_interval[{self.kv_data["previous_hash"]}|{hash}|{self.kv_data["hash_key"]}')
                if hash > self.kv_data['previous_hash'] and hash < self.kv_data['hash_key']:
                    return True
                else:
                    return False
        elif self.ask_ring_metadata() is None or self.ask_ring_metadata() == {}:
            self.kvprint('Error in key_checker. ring_metadata EMPTY', log='e')
            return None
        else:
            self.kvprint('Error in key_checker. Outside the logic. Check. ', log='e')
            return None


    def hash(self, key):
        md5_hash = hashlib.md5(key.encode()).hexdigest()
        #md5_hash = int(md5_hash[:3], 16)
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
            self.kvprint(f'error cache selection', log='e')

    def print_storage(self):
        with shelve.open(self.storage_dir, flag='r') as db:
            message = f"\n------------------\n"
            message += f'All key-value pairs\n'
            counter = 1
            for key, value in db.items():
                message += f"Hash {self.hash(key)}==> {key} | {value}\n"
                counter += 1
            message += f"------------------\n"
            return message

    def kvprint(self, *args, log='d'):
        message = ' '.join(str(arg) for arg in args)
        message = self.cli + message
        # message = self.print_cnfig[0] + self.cli + message
        if log == 'd':
            self.print_cnfig[1].debug(message)
        if log == 'i':
            self.print_cnfig[1].info(message)
        if log == 'e':
            self.print_cnfig[1].info(message)
