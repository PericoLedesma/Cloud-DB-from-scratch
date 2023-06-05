import socket
import time
import json

class ECS_handler:
    def __init__(self, addr, cli, kv_data):
        self.addr, self.port = addr.split(':')
        self.addr = self.addr.replace(" ", "")
        self.port = int(self.port)

        self.kv_data = kv_data

        self.cli = f'{cli}[ECS handler]>'

        self.connect_to_ECS()

    def connect_to_ECS(self):
        print(f'{self.cli}Connecting to bootstrap [{self.addr, self.port}]')
        RETRY_INTERVAL = 3
        while True:
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect((self.addr, self.port))
                print(f'{self.cli}Connected to ECS')
                break
            except socket.error as e:
                print(f'{self.cli}Connection error:{e}. Retrying in {RETRY_INTERVAL} seconds...')
                time.sleep(RETRY_INTERVAL)


    def handle_RECV(self, start_time):
        print(f'{self.cli}Handling the request of ECS')

        try:
            data = self.sock.recv(128 * 1024).decode()
            if data is not None and data != 'null':

                print(f'{self.cli}Received message: {repr(data)}')
                self.handle_REQUEST(data)

                print(f'{self.cli}Timeout restarted')
                start_time = time.time()
            else:
                print(f'{self.cli}No data')
                raise Exception('Error while handling the request. No data. Closing socket as')

        except ConnectionResetError:
            print(f'{self.cli}EXCEPTION: Connection reset by peer.')
        except:
            print(f'{self.cli}EXCEPTION: Socket error. Check exception')


    def handle_REQUEST(self, data):
        recv_data = json.loads(data)
        method = recv_data.get('request')
        data = recv_data.get('data')

        formatted_json = json.dumps(recv_data, indent=4)

        if method == 'kvserver_Data':
            self.handle_json_RESPONSE(method)
        else:
            print(f'{self.cli}error unknown command!')


    def handle_json_RESPONSE(self, method):
        try:
            json_data = json.dumps(self.messages_templates(method))
            self.sock.sendall(bytes(json_data, encoding='utf-8'))
        except:
            raise Exception('Error while sending the data.')


    def messages_templates(self, method):
        if method == 'kvserver_data':
            self.data = {
                'request': 'kvserver_data',
                'data': {
                    'id': self.kv_data['id'],
                    'name': self.kv_data['name'],
                    'host': self.kv_data['host'],
                    'port': self.kv_data['port'],
                }
            }
