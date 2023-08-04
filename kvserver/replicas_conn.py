import socket
import json
import shelve
import time


class Replicas_handler:
    def __init__(self, ecshandler, kvserver):
        self.ecsh = ecshandler
        self.kvserver = kvserver

        # Printing parameters
        self.cli = f'[ - Replicas - ]>'

        # Data structures
        self.connected_replicas = {}

        self.kvprint(f'Running Replicas handler...')

    def update_replicas(self):
        # Current replicas
        if self.kvserver.ring_mine_replicas:
            self.kvprint(f'Updating replicas .... We have already replicas')
            # self.kvprint(f'------')
            # for key, value in self.kvserver.ring_mine_replicas.items():
            #     self.kvprint(f'Mine Replicas already connected {key} {value}')
            # for key, value in self.connected_replicas.items():
            #     self.kvprint(f'Connected Replica  {key} {value}')
            # self.kvprint(f'------')

            # big todo, if it is connected but type is different

            for value in self.kvserver.ring_replicas:

                if value['from'] == self.kvserver.kv_data['from'] and value['to_hash'] == self.kvserver.kv_data['to_hash']:
                    # self.kvprint(f'This should be my Replica {value}')
                    if self.kvserver.ring_mine_replicas[value['type']]['host'] == value['host'] and self.kvserver.ring_mine_replicas[value['type']]['port'] == value['port']:
                        # self.kvprint(f'Replica already connected.')
                        pass
                    else:
                        # self.kvprint(f'New replica. Removing previous and connecting to new one. ')
                        self.close_replica(value)
                        self.kvserver.ring_mine_replicas[value['type']] = value
                        self.connect_to_replica(value)
        else:
            self.kvprint(f'Updating replicas .... No previous replicas')

            for value in self.kvserver.ring_replicas:
                if value['from'] == self.kvserver.kv_data['from'] and value['to_hash'] == self.kvserver.kv_data['to_hash']:
                    # self.kvprint(f'My Replica  {value}')
                    self.kvserver.ring_mine_replicas[value['type']] = value
                    self.connect_to_replica(value)

        self.kvprint(f'Finish updating replicas.')


        # self.kvprint(self.kvserver.kv_data)
        # self.kvprint(f'My replicas data')

        # self.kvprint(self.kvserver.ring_mine_replicas)

        # self.kvprint(f'Checking connection again')
        #
        #
        # self.kvprint(f'My replicas data')
        # self.kvprint(f'---connected_replicas ----')
        # for sock, rep in self.connected_replicas.items():
        #     self.kvprint(f'{rep} - sernding msg ')
        #     self.handle_REPLY(rep["sock"], f'you_are_my_replica2')
        # self.kvprint(f'-------')




    def connect_to_replica(self, rep):
        RETRY_INTERVAL = 0.5
        connect_to_try = 0
        self.kvprint(f'Connecting to replica {rep["type"]} {rep["host"]}:{rep["port"]} ....')
        while True:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((rep['host'], int(rep['port'])))
                addr, port = sock.getsockname()
                sock.settimeout(self.kvserver.sock_timeout)
                self.kvprint(f'Connected to rep {rep["host"]}:{rep["port"]}. Connection addr: {addr}:{port}')

                self.connected_replicas[sock] = {
                    'sock': sock,
                    'addr': addr,
                    'port': port,
                    'host': f'{rep["host"]}',
                    'host_port': f'{rep["port"]}',
                    'type': rep["type"]
                }
                self.handle_REPLY(sock, f'you_are_my_replica {rep["type"]} {self.kvserver.kv_data["from"]}  {self.kvserver.kv_data["to_hash"]}')
                break
            except socket.error as e:
                # self.kvprint(f'Error connecting:{e}. Retrying..')
                if connect_to_try > 40:
                    self.kvprint(f'Error. Tried to connect to Replica unsuccessfully ')
                    break
                else:
                    connect_to_try += 1
                    time.sleep(RETRY_INTERVAL)
        # self.kvprint(f'---connected_replicas ----')
        # for sock, rep in self.connected_replicas.items():
        #     self.kvprint(f'{rep}')
        # self.kvprint(f'-------')

    def close_replica(self, replica):
        key = None
        for sock, rep in self.connected_replicas.items():
            if rep['type'] == replica["type"]:
                # self.kvprint(f'Closing replica {replica["type"]}| {rep["host"]}')
                key = sock
                break
        try:
            del self.connected_replicas[key]
            key.close()

            self.kvprint(f'Closed successfully old replica {replica["type"]}')
        except Exception as e:
            self.kvprint(f'Error.Failed deleting replica connection  : {e}. ')



    def handle_CONN(self):
        while True:
            try:
                data = self.sock.recv(128 * 1024).decode()
                if data:
                    messages = data.replace('\\r\\n', '\r\n').split('\r\n')
                    for msg in messages:
                        if msg is None or msg == " " or not msg:
                            break
                        elif msg == 'null':
                            self.kvprint(f'handle_RECV --> null. Reconnecting')
                            self.connect_to_ECS()
                        else:
                            self.handle_RECV(msg)
                else:
                    self.kvprint(f'No data. --> Reconnecting')
                    self.connect_to_ECS()
                    break
            except socket.timeout:
                # self.kvprint(f'Time out handle_CONN --> Continue', log='e')
                pass
            except Exception as e:
                self.kvprint(f'Exception handle_CONN: {e} --> Continue')
                continue
        self.kvprint(f'Exiting handle_CONN with replicas ...| {self.ecs_connected}')
        self.sock.close()
        self.kvprint(f'{"-" * 60}')
        self.kvprint(f'{" " * 20}Replicas Handler Stopped')
        self.kvprint(f'{"-" * 60}')
        del self
        exit(0)

    def handle_RECV(self, msg):
        try:
            parsedata = json.loads(msg)
            request = parsedata.get('request')
            self.kvprint(f'Received message: {request}')
            # REQUESTS
            if request == 'kvserver_data':
                self.handle_json_REPLY(request)

            else:
                self.kvprint(f'error unknown command!')

        except json.decoder.JSONDecodeError as e:
            self.kvprint(f'Error handling received: {str(e)} |MSG {msg}')

    def handle_REPLY(self, sock,  response):
        sock.sendall(bytes(f'{response}\r\n', encoding='utf-8'))
        self.kvprint(f'MSG sent:{response}')

    def handle_json_REPLY(self, sock, request, data=None):
        json_data = json.dumps(self.REPLY_templates(request, data))
        sock.sendall(bytes(f'{json_data}\r\n', encoding='utf-8'))
        self.kvprint(f'MSG sent:{request}')

    def REPLY_templates(self, request, data):
        if request == 'you_are_my_replica':
            return f'you_are_my_replica'
        elif request == 'you_are_my_replica_put':
            return f'you_are_my_replica_put KEY VALUE'

        elif request == 'you_are_my_replica_delete':
            return {
                'request': request,
                'data': data
            }
        else:
            self.kvprint(f'Error Message templates. Request not found:{request}')

    def kvprint(self, *args, log='d'):
        message = ' '.join(str(arg) for arg in args)
        message = self.cli + message
        # message = self.kvserver.cli + self.cli + message
        if log == 'd':
            self.kvserver.log.debug(message)
        elif log == 'i':
            self.kvserver.log.info(message)
        elif log == 'e':
            self.kvserver.log.error(message)
