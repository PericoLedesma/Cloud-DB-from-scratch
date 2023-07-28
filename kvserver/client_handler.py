from cache_classes import *
import shelve
import socket
import hashlib

# ------------------------------------------------------------------------
class Client_handler:
    def __init__(self,
                 clients_conn,
                 client_data,
                 ring_structures,
                 connected_replicas,
                 cache_config,
                 lock,
                 storage_dir,
                 printer_config,
                 timeout_config):

        self.clients_conn = clients_conn
        self.kv_data = client_data[0]
        self.client_id = client_data[1]
        self.client_fd = client_data[2]
        self.addr = client_data[3]

        self.tictac = timeout_config[0]
        self.client_fd.settimeout(timeout_config[1])
        self.coordinator = False # Storing the type of replica that we are for the coord.

        # Some function to ask for data. todo rethink
        self.ask_ring = ring_structures[0]
        self.ask_replicas = ring_structures[1]
        self.ask_lock = ring_structures[2]
        self.ask_lock_ecs = ring_structures[3]

        # Connection to rep
        self.connected_replicas = connected_replicas

        self.conn_status = True
        self.lock = lock
        self.storage_dir = storage_dir

        self.welcome_msg = f'Hi! You are connected to {self.kv_data["name"]}.'
        self.cli = f'[Client{self.client_id}]>' # Todo change name is it is a replica
        self.print_cnfig = printer_config

        # START
        self.cache_init(cache_config)
        self.kvprint(f'> Running Client handler {self.client_id}')

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
                            self.kvprint(f'MSG recv: [{msg}]')
                            if len(self.ask_ring()) > 0:
                                self.handle_RECV(msg, shutdown)
                            elif len(self.ask_ring()) == 0:
                                self.kvprint(f'MSG sent: server_stopped')
                                self.handle_RESPONSE('server_stopped')
                else:
                    self.kvprint(f'No data handle_CONN --> Closing socket', log='e')
                    break
            except socket.timeout:
                if self.coordinator:
                    self.kvprint(f'Time out handle_CONN coordinator --> Continue', log='e')
                else:
                    self.kvprint(f'Time out handle_CONN client --> Closing socket', log='e')
                    break
            except Exception as e:
                if self.coordinator:
                    self.kvprint(f'Exception handle_CONN coordinator: {e} --> Closing socket', log='e')
                    self.kvprint(f'Deleting me as replica of the coordinator..', log='e')
                    del self.kv_data[self.coordinator]
                    break
                else:
                    self.kvprint(f'Exception handle_CONN client: {e} --> Closing socket', log='e')
                    break
        self.clients_conn[self.client_id] = None
        self.client_fd.close()
        self.kvprint(f'Stopped')
        del self
        exit(0)


    def handle_RECV(self, msg, shutdown):
        method, *args = msg.split()
        # self.kvprint(f'HEREEEE method : {method} => *args  {args}')

        if shutdown is False:
            if method in ['put', 'delete'] and self.ask_lock() is False:
                key = args[0]
                if self.key_check_coordinators(self.hash(key)) is False:
                    self.handle_RESPONSE(f'server_not_responsible') # TODO delete data
                else:
                    self.handle_REQUEST(method, *args)
            elif method in ['put', 'delete'] and self.ask_lock():
                self.handle_RESPONSE('server_write_lock')
                self.ask_lock_ecs() #todo check
            elif method in ['get']:
                key = self.hash(args[0])
                if self.key_check_coordinators(key) is False: #todo and self.key_check_replicas(hash) is False:
                    self.handle_RESPONSE(f'server_not_responsible')
                else:
                    self.handle_REQUEST(method, *args)
            elif method in ['keyrange', 'keyrange_read', 'show', 'close']:
                self.handle_REQUEST(method, *args)
            elif method in ['you_are_my_replica', 'coordinator_order']: # Todo put coordinator_order with get and check with replicas
                # self.kvprint(f'method : {method} => *args  {args}')
                self.handle_REQUEST(method, *args)
            else:
                self.handle_RESPONSE(f'error unknown command! (shutdown OFF)| Method {repr(method)}')
        elif shutdown:
            if method in ['get', 'show', 'close']:
                self.handle_REQUEST(method, *args)
            elif method in ['put', 'delete', 'keyrange', 'keyrange_read']:
                self.handle_RESPONSE('server_stopped')
            else:
                self.handle_RESPONSE(f'error unknown command! (shutdown ON)| Method {repr(method)}')

        if method in ['organise']:
            method, args = args[0], args[1:]
            self.handle_REQUEST(method, *args)


    def handle_REQUEST(self, request, *args):
        # self.kvprint(f'Request => [{request}]')
        if request == 'put' and len(args) > 1:
            key, value = args[0], ' '.join(args[1:])
            self.kvprint(f'NORMAL PUT   [{key}] [{value}]')
            self.cache.put(key, value)
            with self.lock:
                self.PUT_request(key, value)
            # Replicas

            for sock, rep in self.connected_replicas.items():
                # self.kvprint(f'Sending data to REPLICAS123  {rep["type"]} | put |{key} |{value}')
                self.handle_rep_RESPONSE(rep["sock"], f'{rep["type"]} put {key} {value}')
        elif request == 'get' and len(args) == 1:
            key = args[0]
            if self.cache.get(key):
                # self.kvprint(f' {key} at CACHE')
                self.handle_RESPONSE(f'get_success {key} {self.cache.get(key)}')
            else:
                # self.kvprint(f'{key} not in cache. Checking STORAGE')
                self.GET_request(key)
        elif request == 'delete' and len(args) == 1:
            key = args[0]
            self.cache.delete(key)  # Todo error, the updated value is in the cache not in the storage
            self.DELETE_request(key)
        elif request == 'show':
            self.kvprint(f'Request => show db')
            self.handle_RESPONSE(self.print_storage())
        elif request == 'completed':
            self.kvprint(f'Request => organise completed')
            self.handle_RESPONSE('organise received')
        elif request == 'keyrange':
            self.kvprint(f'Request => keyrange')
            message = ''
            # self.kvprint(self.ask_ring())
            # self.kvprint('-------')
            # self.kvprint(self.ring_metadata2)
            for key, v in self.ask_ring().items():
                if v['type'] == 'C':
                    row = f'{v["from"]},{v["to_hash"]},{v["host"]}:{v["port"]};'
                    message = f'{message}{row}'
            self.handle_RESPONSE(message)
        elif request == 'keyrange_read':
            self.kvprint(f'Request => keyrange_read')
            message = ''
            for key, v in self.ask_ring().items():
                row = f'{v["from"]},{v["to_hash"]},{v["host"]}:{v["port"]};'
                message = f'{message}{row}'
            self.handle_RESPONSE(message)
        elif request == 'close':
            self.kvprint(f'Request => close')
            self.conn_status = False
            self.handle_RESPONSE('End connection with client')
        elif request == 'you_are_my_replica': # Todo a check if we are responsable
            rep_type, interval = args[0], args[1:]
            self.kvprint(f'Request => you_are_my_replica {rep_type}. Changing promtp {self.cli}> [Coordinator{rep_type}]')
            self.cli = f'[Coordinator{rep_type}]>' # Todo delete the coordinator when new
            self.coordinator = rep_type
            self.kv_data[rep_type] = {
                "from": interval[0],
                "to_hash": interval[1]
            }
            # self.kvprint(f'My interval as replica {rep_type} stored')
            # self.kvprint(f'MY DATA')
            # self.kvprint(self.kv_data)
            # self.kvprint(f'-------')

        elif request == 'coordinator_order': # Todo a check if we are responsable
            rep_type, method, key, value = args[0], args[1], args[2], ' '.join(args[3:])
            self.kvprint(f'Request => coordinator_order to {rep_type} (me) => Request {method} |key {key}, value {value}')
            self.handle_REQUEST(method, *args)
            if method == 'put':
                self.cache.put(key, value) # Cache lock too
                with self.lock:
                    self.PUT_request(key, value)

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
                self.handle_RESPONSE(f'error unknown command! => {request} ')

    def PUT_request(self, key, value):
        self.kvprint(f'Request => put {key} {value}')
        try:
            with shelve.open(self.storage_dir, writeback=True) as db:
                if key in db:
                    if db.get(key) == value:
                        # self.kvprint(f'{key} |{value} already exists with same values')
                        self.handle_RESPONSE(f'put_update {key}')  # Todo creo que esta respuesta me la he inventado
                    else:
                        # self.kvprint(f' Key>{key} already exists. Overwriting value.')
                        db[key] = value
                        self.handle_RESPONSE(f'put_update {key}')
                else:
                    db[key] = value
                    # self.kvprint(f'{key}Data stored: key={key}, value={value}')
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
                    # self.kvprint(f'Key {key} found. Value {value}')
                    self.handle_RESPONSE(f'get_success {key} {value}')
                    self.cache.put(key, value)
                else:
                    # self.kvprint(f'Key {key} not found')
                    self.handle_RESPONSE(f'get_error {key}')
        except Exception as e:
            # self.kvprint(f'Exception in get request: {e} ')
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
                    # self.kvprint(f'Key {key} not found')
                    self.handle_RESPONSE(f'delete_error {key}')
        except Exception as e:
            self.kvprint(f'Exception in delete request: {e} ')
            self.handle_RESPONSE(f'delete_error {key}')

    def handle_RESPONSE(self, response):
        self.kvprint(f'MSG sent:{response}')
        self.client_fd.sendall(bytes(f'{response}\r\n', encoding='utf-8'))

    def handle_rep_RESPONSE(self, sock, response):
        message = f'coordinator_order {response}'
        self.kvprint(f'MSG to rep sent:{message}')
        sock.sendall(bytes(f'{message}\r\n', encoding='utf-8'))

    def key_check_coordinators(self, hash):
        # Here we check just coordinators nodes
        int_hash = int(hash, 16)
        self.kvprint(f'key_check_coordinators: len(ring_metadata) =  {len(self.ask_ring())}')
        if len(self.ask_ring()) == 1:
            if self.ask_ring()[self.kv_data['to_hash']] is not None:
                return True
            else:
                raise Exception('Key checker ERROR. Just one node and it is not me')
        elif len(self.ask_ring()) > 1:
            list_hash = list(self.ask_ring()).copy()
            sorted_hash_list = sorted(list_hash, key=lambda x: int(x, 16))
            if sorted_hash_list[0] == self.kv_data['to_hash']:  # If it is the last range
                if int(sorted_hash_list[-1], 16) < int_hash or int_hash < int(sorted_hash_list[0], 16):
                    return True
                else:
                    return False
            else:
                if int_hash > int(self.kv_data['from'], 16) and int_hash < int(self.kv_data['to_hash'], 16):
                    return True
                else:
                    return False
        elif self.ask_ring() is None or self.ask_ring() == {}:
            self.kvprint('ERROR in key_check_coordinators. ring_metadata EMPTY', log='e')
            return None
        else:
            self.kvprint('ERROR in key_check_coordinators. Outside the logic. Check. ', log='e')
            return None

    def key_check_replicas(self, hash):
        int_hash = int(hash, 16)
        self.kvprint(f'key_check_replicas: len(ring_metadata) =  {len(self.ask_replicas())}')

        if len(self.ask_ring()) == 1:
            if self.ask_ring()[self.kv_data['to_hash']] is not None:
                return True
            else:
                raise Exception('Key checker ERROR. Just one node and it is not me')
        elif len(self.ask_ring()) > 1:
            list_hash = list(self.ask_ring()).copy()
            sorted_hash_list = sorted(list_hash, key=lambda x: int(x, 16))
            if sorted_hash_list[0] == self.kv_data['to_hash']:  # If it is the last range
                if int(sorted_hash_list[-1], 16) < int_hash or int_hash < int(sorted_hash_list[0], 16):
                    return True
                else:
                    return False
            else:

                if int_hash > int(self.kv_data['from'], 16) and int_hash < int(self.kv_data['to_hash'], 16):
                    return True
                else:
                    return False
        elif self.ask_ring() is None or self.ask_ring() == {}:
            self.kvprint('Error in key_check_replicas. ring_metadata EMPTY', log='e')
            return None
        else:
            self.kvprint('Error in key_check_replicas. Outside the logic. Check. ', log='e')
            return None

    def hash(self, key):
        md5_hash = hashlib.md5(key.encode()).hexdigest()
        # md5_hash = int(md5_hash[:3], 16)
        # return str(md5_hash)
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
        message = '\t' + self.cli + message
        # message = self.print_cnfig[0] + self.cli + message
        if log == 'd':
            self.print_cnfig[1].debug(message)
        elif log == 'i':
            self.print_cnfig[1].info(message)
        elif log == 'e':
            self.print_cnfig[1].error(message)
