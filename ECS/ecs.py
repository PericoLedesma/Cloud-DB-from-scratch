# from hashing_function import *
import subprocess
import os
import time
import argparse
import socket
import select
import hashlib
import threading
import json

class ECS:
    def __init__(self, host, port, directory, num_kvservers):
        print(f' ==> ECS server on port[{host}:{port}]')

        self.cli = f'[ECS]>'
        self.host = host
        self.port = port
        self.timeout = 20

        self.num_kvservers = num_kvservers
        self.kvs_inputs = []
        self.kvs_outputs = []
        self.kvs_data = {}

        # self.server_bootstrap()
        # thread = threading.Thread(target=self.listen_to_kvservers())
        # thread.start()

        host = 'localhost'
        port = 12345
        timeout = 10

        self.listen_to_kvservers()

        print('-----lines continuees---')

        # # Get the local and remote addresses
        # local_address = sock.getsockname()
        # remote_address = sock.getpeername()

        # time.sleep(5)
        print(f'{self.cli}Sending msg')
        for sock in self.kvs_inputs:
            self.handle_RESPONSE(f' BYE KvSERVER', sock)
        #
        # time.sleep(10)
        # print(f'{self.cli}Sending msg')
        # for sock in self.kv_sockets:
        #     self.handle_RESPONSE(f'Hey KVSERVER', sock)

        print(f'{self.cli}Closing ECS')


    def handle_RESPONSE(self, response, sock):
        sock.sendall(bytes(f'{response}\r\n', encoding='utf-8'))

    def ecsprint(self, *args, c=None):
        COLORS = {
            'r': '\033[91m',
            'g': '\033[92m',
            'y': '\033[93m',
            'reset': '\033[0m'
        }
        message = ' '.join(str(arg) for arg in args)
        if c in COLORS:
            message = COLORS[c] + self.cli + message + COLORS['reset']
        print(message)


    def listen_to_kvservers(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen()

        inputs = [server_socket]
        outputs = []
        kv_sockets = {}

        while True:
            self.ecsprint(f'Listening...')
            readable, _, _ = select.select(inputs, [], [], self.timeout)
            self.ecsprint(f'Readable:{len(readable)}|inputs:{len(inputs)}|outputs{len(outputs)}', c='g')


            for sock in readable:
                if sock is server_socket:
                    kv_sock, client_addr = server_socket.accept()
                    self.ecsprint(f"New client connected: {client_addr}")
                    kv_sockets[kv_sock] = {'addr': client_addr, 'sock': kv_sock, 'last_activity': time.time()}
                    inputs.append(kv_sock)

                    message = "Server message: Hello client!"
                    kv_sock.sendall(message.encode())

                else:
                    try:
                        data = sock.recv(1024).decode().strip()
                        if data:
                            self.ecsprint(f"Received data from {kv_sockets[sock]['addr']}: {data}")
                            if sock not in outputs:
                                outputs.append(sock)

                            message = "Server message: Hello client!"
                            sock.sendall(message.encode())

                        else:
                            # No data received, client disconnected
                            self.ecsprint(f"Client disconnected: {kv_sockets[sock]}")
                            inputs.remove(sock)
                            if sock in outputs:
                                outputs.remove(sock)
                            sock.close()
                            del kv_sockets[sock]

                    except Exception as e:
                        self.ecsprint(f'{self.cli}Exception: {e} --> Closing socket', c='r')
                        sock.close()
                        if sock in inputs:
                            inputs.remove(sock)
                            self.ecsprint(f'{self.cli}\tDeleted socket from list of inputs', c='r')
                        if sock in outputs:
                            outputs.remove(sock)
                            self.ecsprint(f'{self.cli}\tDeleted socket from list of outputs', c='r')


            # for sock in writable:
            #     # Handle writable sockets if needed
            #     # For example, sending data back to clients
            #     message = "Server message: Hello client!"
            #     sock.sendall(message.encode())
            #     outputs.remove(sock)



        server_socket.close()


    def handle_RECV(self, sock, start_time):
        print(f'{self.cli}Handling the recv')
        try:
            data = sock.recv(128 * 1024).decode()
            print(f'{self.cli}Received data: {data}')
            if data is not None and data != 'null' and data !='':
                self.handle_REQUEST(data)
                print(f'{self.cli}Timeout restarted')
                start_time = time.time()
            else:
                print(f"Client disconnected: KVSERVER1")
                self.kvs_inputs.remove(sock)
                if sock in outputs:
                    outputs.remove(sock)
                sock.close()
                del client_sockets[sock]

                print(f'{self.cli}No data')
                # raise Exception('Error while handling the request. No data. Closing socket as')
        except ConnectionResetError:
            raise Exception('Error while handling the request. Connection reset by peer')
        except:
            raise Exception('Error while handling the request.')



    def handle_REQUEST(self, data):
        data = data.replace('\\r\\n', '\r\n')
        # print(f'{self.cli}Received data after replace and decoded: {repr(data)}')
        messages = data.split('\r\n')

        for msg in messages:
            if msg is None or msg == " " or not msg:
                break
            else:
                print(f'{self.cli}Received message: {repr(msg)}')
                try:
                    recv_data = json.loads(msg)
                    method = recv_data.get('request')
                    if method == 'kvserver_data':
                        self.kvs_data[data.get('id')] = {
                            'name': data.get('name'),
                            'host': data.get('host'),
                            'port': data.get('port')
                        }
                        print('Check of data stored', self.kvs_data)
                    else:
                        print(f'{self.cli}error unknown command!')

                except json.decoder.JSONDecodeError as e:
                    print(f'{self.cli}Error parsing JSON: {str(e)}')



    def handle_json_RESPONSE(self, sock, method):
        try:
            json_data = json.dumps(self.messages_templates(method))
            sock.sendall(bytes(f'{json_data}\r\n', encoding='utf-8'))
            # sock.sendall(bytes(json_data, encoding='utf-8'))
            print(f'{self.cli}Resquest sent: [{method}]')
        except:
            raise Exception('Error while sending the data.')

    def messages_templates(self, method):
        if method == 'kvserver_data':
            return {
                'request': 'kvserver_data'
            }


    def server_bootstrap(self):
        print(f'{self.cli}Server bootstrap...')
        # Paths
        current_dir = os.path.abspath(os.path.dirname(__file__))
        parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))
        script_path = os.path.join(parent_dir, 'kvserver', 'kvserver.py')

        port = 3000 #todo
        bootstrap = f'{self.host}:{self.port}'

        for n in range(self.num_kvservers):
            print(f'{self.cli}Starting kvserver {n}')
            command = ['python', script_path, f'-i {n}', f'-p {port + n}', f'-b {bootstrap}']

            result = subprocess.Popen(command)

            if result.returncode == 0:
                print(f'{self.cli}The script ran successfully.')
            # else:
            #     print(f'{self.cli}The script encountered an error.')
        print(f'{self.cli}Server bootstrap DONE')



def main():
    parser = argparse.ArgumentParser(description='ECS Server')
    parser.add_argument('-a', '--address', default='127.0.0.1', help='Server address')
    parser.add_argument('-p', '--port', default='8000', type=int, help='Server port')
    parser.add_argument('-d', '--directory', default='.', type=str, help='Storage directory')
    parser.add_argument('-n', '--num-kvservers', default=1, type=int, help='Number of kvservers')
    # parser.add_argument("-h", "--help", required=True, help="Help")

    args = parser.parse_args()

    ECS(host=args.address,
        port=args.port,
        directory=args.directory,
        num_kvservers=args.num_kvservers)


if __name__ == '__main__':
    main()
