import socket
import json
import time
import datetime

class Replicas_handler:
    def __init__(self, ecshandler, kvserver):
        self.ecsh = ecshandler
        self.kvs = kvserver

        # Printing parameters
        self.cli = f'[ - Replicas - ]>'

        # Data structures
        self.connected_replicas = {}

        self.kvprint(f'Running Replicas handler...')

    def update_replicas(self):
        if self.kvs.ring_mine_replicas:
            self.kvprint(f'Updating replicas .... We have already replicas')
            self.kvs.i_am_replica_of = {}

            # big todo, if it is connected but type is different

            for value in self.kvs.ring_replicas:
                if value['from'] == self.kvs.kv_data['from'] and value['to_hash'] == self.kvs.kv_data['to_hash']:
                    if self.kvs.ring_mine_replicas[value['type']]['host'] == value['host'] and self.kvs.ring_mine_replicas[value['type']]['port'] == value['port']:
                        pass
                    else:
                        self.close_replica(value)
                        self.kvs.ring_mine_replicas[value['type']] = value
                        self.connect_to_replica(value)
                elif value['host'] == self.kvs.kv_data['host'] and int(value['port']) == int(self.kvs.kv_data['port']):
                    self.kvs.i_am_replica_of[value['type']] = value
                else:
                    pass
        else:

            self.kvprint(f'Updating replicas .... No previous replicas')
            for value in self.kvs.ring_replicas:
                if value['from'] == self.kvs.kv_data['from'] and value['to_hash'] == self.kvs.kv_data['to_hash']:
                    self.kvs.ring_mine_replicas[value['type']] = value
                    self.connect_to_replica(value)
                elif value['host'] == self.kvs.kv_data['host'] and int(value['port']) == int(self.kvs.kv_data['port']):
                    self.kvs.i_am_replica_of[value['type']] = value
                else:
                    pass

        self.kvprint(f'Finish updating replicas.')
        if self.kvs.i_am_replica_of is None or self.kvs.i_am_replica_of == {}:
            raise Exception("Error update_replicas. I am not a replica of anyone.")


    def connect_to_replica(self, rep):
        RETRY_INTERVAL = 0.5
        connect_to_try = 0
        self.kvprint(f'Connecting to replica {rep["type"]} {rep["host"]}:{rep["port"]} ....')
        while True:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((rep['host'], int(rep['port'])))
                addr, port = sock.getsockname()
                sock.settimeout(self.kvs.sock_timeout)
                self.kvprint(f'Connected to rep {rep["host"]}:{rep["port"]}. Connection addr: {addr}:{port}')

                self.connected_replicas[sock] = {
                    'sock': sock,
                    'addr': addr,
                    'port': port,
                    'host': f'{rep["host"]}',
                    'host_port': f'{rep["port"]}',
                    'type': rep["type"]
                }
                self.handle_REPLY(sock, f'you_are_my_replica {rep["type"]} {self.kvs.kv_data["from"]}  {self.kvs.kv_data["to_hash"]}')
                break
            except socket.error as e:
                # self.kvprint(f'Error connecting:{e}. Retrying..')
                if connect_to_try > 40:
                    self.kvprint(f'Error. Tried to connect to Replica unsuccessfully ')
                    break
                else:
                    connect_to_try += 1
                    time.sleep(RETRY_INTERVAL)

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
        # message = self.cli + message

        message = f'[{datetime.datetime.now().strftime("%H:%M:%S")}] {self.cli} {message}'

        if log == 'd':
            self.kvs.log.debug(message)
        elif log == 'i':
            self.kvs.log.info(message)
        elif log == 'e':
            self.kvs.log.error(message)
