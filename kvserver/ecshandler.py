import socket
import time
import json

class ECS_handler:
    def __init__(self, addr, cli):
        self.addr, self.port = addr.split(':')
        self.addr = self.addr.replace(" ", "")
        self.port = int(self.port)

        self.cli = f'{cli}[ECS handler]>'

        self.connect_to_ECS()


    def connect_to_ECS(self):
        print(f'{self.cli}Connecting to bootstrap [{self.addr, self.port}]')
        RETRY_INTERVAL = 3
        while True:
            try:
                self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.sock.connect((self.addr, self.port))
                while True:
                    if self.sock.fileno() != -1:
                        break
                print(f'{self.cli}Connected to ECS')
                break

            except socket.error as e:
                print(f'{self.cli}Connection error:{e}. Retrying in {RETRY_INTERVAL} seconds...')
                time.sleep(RETRY_INTERVAL)


    def handle_REQUEST(self):
        print(f'{self.cli}Handling the request of ECS')
        try:
            data = self.sock.recv(128 * 1024)
            if data:
                recv_data = json.loads(data.decode('utf-8'))
                parsed_data = json.loads(recv_data)
                formatted_json = json.dumps(parsed_data, indent=4)
                print(f'{self.cli}Received message: {data.decode()}')
            else:
                print(f'{self.cli}No data')

        except ConnectionResetError:
            print(f'{self.cli}EXCEPTION: Connection reset by peer.')

        except:
            print(f'{self.cli}EXCEPTION: Socket error. Check exception')


    def handle_json_RESPONSE(self, response):


        json_data = json.dumps(response)
        self.sock.sendall(bytes(json_data, encoding='utf-8'))

    def handle_RESPONSE(self, response):
        self.sock.sendall(bytes(response, encoding='utf-8'))

    def END_socket(self):
        # Close the client socket
        self.sock.close()
        print(f'{self.cli}Client socket closed')