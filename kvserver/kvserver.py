from client_handler import *
from ecshandler import *

import time
import os
import socket
import threading
import argparse
import logging
import select


class KVServer:
    def __init__(self, host, port, ecs_addr, id, cache_strategy, cache_size, log_level, log_file, directory, max_conn):
        # Server parameters
        self.id = id
        self.name = f'kvserver{id}'

        self.host = host
        self.port = port
        self.ecs_addr = ecs_addr

        self.data = {'id': self.id, 'name': self.name, 'host': self.host, 'port': self.port}

        self.cli = f'\t[{self.name}]>'

        self.max_conn = max_conn
        self.timeout = 30
        self.lock = threading.Lock()

        #Cache
        self.c_strg = cache_strategy
        self.c_size = cache_size

        # To store connections/sockets with clients
        self.clients_conn = {key: None for key in range(1, self.max_conn + 1)}
        self.active_clients = int

        # START
        self.init_log(log_level, log_file, directory)
        self.init_storage()

        print(f'{self.cli}---- KVSSERVER {id} ACTIVE -----')
        self.log.info(f'{self.cli}---- KVSSERVER {id} ACTIVE -----')


        self.ecs = ECS_handler(ecs_addr, self.cli, self.data)



        # time.sleep(3)
        # print(f'{self.cli} Sending ')
        # self.ecs.handle_RESPONSE(f'Hi from KVSSERVER{id}')
        # time.sleep(3)
        # print(f'{self.cli} Sending msg')
        # self.ecs.handle_RESPONSE(f'Second msg')
        # time.sleep(12)


        self.listen_to_connections()

        self.log.info(f'{self.cli}---- KVSSERVER {id} SLEEP -----')
        print(f'{self.cli}---- KVSSERVER {id} SLEEP -----')


    def listen_to_connections(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.settimeout(10)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.host, self.port))
            server.listen()

            self.log.debug(f'{self.cli}Listening on {self.host}:{self.port}')
            print(f'{self.cli}Listening on {self.host}:{self.port}')

            start_time = time.time()

            while True:
                time.sleep(2)
                print(f'{self.cli}->Listening...')
                readable, writable, errors = select.select([server] + [self.ecs.sock], [], [], 10)
                print(f'{self.cli}ECS:{len({self.ecs.sock})}|Readable:{len(readable)}|Writable:{len(writable)}|Errors:{len(errors)}')

                for sock in readable:
                    if sock is server:
                        conn, addr = sock.accept()
                        print(f'{self.cli}Connection accepted: {addr}')
                        self.log.info(f'{self.cli}Connection accepted:, {addr}')

                        client_thread = threading.Thread(target=self.init_client_handler, args=(conn, addr))
                        client_thread.start()

                        print(f'{self.cli}Timeout restarted')
                        start_time = time.time()

                    elif sock == self.ecs.sock:
                        print(f'{self.cli}Message from ECS. Msg addr: {sock.getsockname()}.| Real: {self.ecs_addr}')
                        self.ecs.handle_RECV(start_time)

                    else:
                        try:
                            if sock.getpeername() is not None:
                                print(f'{self.cli}Socket checked conn. Socket not used. CHECK THIS END')
                                print(f'{self.cli}Timeout restarted')
                                start_time = time.time()
                            else:
                                raise socket.error('No connection. Delete socket')

                        except socket.error as e:
                            print(f"Exception:{e}. Deleting socket")
                            sock.close()

                        except:
                            print("Socket is not connected to a remote endpoint. Deleting socket")
                            sock.close()
                        print('Check this error. Socket out of list')


                if not self.check_active_clients() and (time.time() - start_time) >= self.timeout:
                    self.log.debug(f'{self.cli} Closing kvserver')
                    break
                else:
                    # self.log.debug(f'{self.cli} Still active. Reset while.')
                    continue


    def init_client_handler(self, conn, addr):
        print('Init client handler')
        client_id = None #TODO rethink client organization
        for key, value in self.clients_conn.items():
            if value is None:
                client_id = key
                break
        if client_id is None:
            self.log.debug(f'{self.cli} ERROR client id selections')

        Client_handler(client_fd=conn,
                        client_id=client_id,
                        clients_conn=self.clients_conn,
                        cache_type=self.c_strg,
                        cache_cap=self.c_size,
                        lock=self.lock,
                        logger=self.log,
                       storage_dir=self.storage_dir)


    def check_active_clients(self):
        # print('Clients: ', self.clients_conn)
        count = sum(value is not None for value in self.clients_conn.values())
        active_ids = list()
        for key, value in self.clients_conn.items():
            if value is None:
                continue
            else:
                active_ids.append(value.client_id)
        self.log.debug(f'{self.cli} Active clients: {count}, Ids: {active_ids}')
        return count


    def init_storage(self):
        self.storage_dir = os.path.join(self.directory, f'kserver{self.id}_storage')
        shelve.open(filename=self.storage_dir)


    def init_log(self, log_level, log_file, directory):
        if directory is None or directory == '.':
            self.directory = f'kserver{self.id}'
        else:
            self.directory = f'kserver{self.id}_{directory}'
        os.makedirs(self.directory, exist_ok=True)

        log_dir = os.path.join(self.directory, log_file)
        if log_level == 'INFO':
            logging.basicConfig(filename=log_dir,
                                filemode='w',
                                level=logging.INFO,
                                format='%(asctime)s - %(levelname)s - %(message)s')
        elif log_level == 'DEBUG':
            logging.basicConfig(filename=log_dir,
                                filemode='w',
                                level=logging.DEBUG,
                                format='%(asctime)s - %(levelname)s - %(message)s')
        self.log = logging.getLogger(__name__)




# ------------------------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Load balancer kvserver')
    parser.add_argument('-i', '--id', default=1, type=int, help='Server id')
    # parser.add_argument('-a', '--address', default='0.0.0.0', help='Server address')
    parser.add_argument('-a', '--address', default='127.0.0.1', help='Server address')
    parser.add_argument('-b', '--ecs-addr', default='127.0.0.1:8000', help='ECS Server address')
    parser.add_argument('-p', '--port', default='2000', type=int, help='Server port')
    parser.add_argument('-s', '--cache-strategy', default='LFU', type=str, help='Cache strategy: fifo, lru, lfu')
    parser.add_argument('-c', '--cache-size', default=3, type=int, help='Cache size')
    parser.add_argument('-ll', '--log-level', default='DEBUG', help='Log level:DEBUG or INFO')
    parser.add_argument('-l', '--log-file', default='kvserver.log', help='Log file')
    parser.add_argument('-d', '--directory', default='.', type=str, help='Storage directory')
    parser.add_argument('-m', '--max-conn', default=5, type=int, help='Number of clients that the server can handle')
    # parser.add_argument("-h", "--help", required=True, help="Help")

    args = parser.parse_args()

    # print(f'ID {args.id}, PORT {args.port}')

    KVServer(host=args.address,
              port=args.port,
             ecs_addr=args.ecs_addr,
              id=args.id,
              cache_strategy=args.cache_strategy,
              cache_size=args.cache_size,
              log_level=args.log_level,
              log_file=args.log_file,
              directory=args.directory,
              max_conn=args.max_conn)


if __name__ == '__main__':
    main()
