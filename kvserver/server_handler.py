from cache_classes import *
import shelve
import socket
import hashlib

# ------------------------------------------------------------------------
class Server_handler:
    def __init__(self,
                 kv_data,
                 kvs_socket,
                 ask_ring_metadata,
                 ask_lock_write_value,
                 lock,
                 storage_dir,
                 printer_config,
                 timeout_config):

        self.kv_data = kv_data
        self.kvs_fd = kvs_socket[0]
        self.addr = kvs_socket[1]

        self.heartbeat = timeout_config[0]
        self.tictac = timeout_config[1]
        self.timeout = timeout_config[2]

        self.kvs_fd.settimeout(self.timeout)

        self.ask_ring_metadata = ask_ring_metadata
        self.ask_lock_write_value = ask_lock_write_value

        self.lock = lock
        self.storage_dir = storage_dir

        self.welcome_msg = f'Hi KVSERVER! Connection to {self.kv_data["name"]} established'
        self.cli = f'[Handler KVS]>'
        self.print_cnfig = printer_config

        # START
        self.kvprint(f'Connected')
        self.handle_RESPONSE(self.welcome_msg)


    def handle_CONN(self):
        while True:
            try:
                data = self.kvs_fd.recv(128 * 1024)
                if data:
                    request = data.replace(b'\\r\\n', b'\r\n')
                    messages = request.decode().split('\r\n')
                    for msg in messages:
                        if msg is None or msg == " " or not msg:
                            break
                        else:
                            print('Message recv: ', msg)
                            if len(self.ask_ring_metadata()) > 0:
                                self.handle_RECV(msg)
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
        self.kvs_fd.close()
        self.kvprint(f'----- Closing handler ------')
        del self


    def handle_RECV(self, msg):
        method, *args = msg.split()
        if method in ['put']:  # Some checks
            if self.ask_lock_write_value() is False:
                self.handle_REQUEST(method, *args)
            elif self.ask_lock_write_value():
                self.handle_RESPONSE('server_write_lock')
        else:
            self.handle_RESPONSE('error unknown command!')

    def handle_REQUEST(self, request, *args):
        if request == 'put' and len(args) > 1:
            key, value = args[0], ' '.join(args[1:])
            with self.lock:
                self.PUT_request(key, value)

        else:  # ERRORS
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


    def handle_RESPONSE(self, response):
        self.kvprint(f'Reply sent:{response}')
        self.kvs_fd.sendall(bytes(f'{response}\r\n', encoding='utf-8'))

    def kvprint(self, *args, log='d'):
        message = ' '.join(str(arg) for arg in args)
        message = self.print_cnfig[0] + self.cli + message
        if log == 'd':
            self.print_cnfig[1].debug(message)
        if log == 'i':
            self.print_cnfig[1].info(message)
        if log == 'e':
            self.print_cnfig[1].info(message)
