from client_handler import *
from ecshandler import *

import time
import os
import socket
import threading
import argparse
import logging
import select
import random

class KVServer:
    def __init__(self, addr, port, ecs_addr, id, cache_strategy, cache_size, log_level, log_file, directory, max_conn):
        # Server parameters
        self.id = id
        self.name = f'kvserver{id}'

        self.addr = "".join(addr.split())
        self.port = port


        self.ecs_addr = ecs_addr

        self.data = {'id': self.id, 'name': self.name, 'host': self.addr, 'port': self.port}

        self.cli = f'\t[{self.name}]>'
        self.cli_color = f"\033[38;5;{random.randint(1, 254)}m"

        self.max_conn = max_conn
        self.timeout = 5
        self.lock = threading.Lock()
        self.write_lock = True

        #Cache
        self.c_strg = cache_strategy
        self.c_size = cache_size

        # To store connections/sockets with clients
        self.clients_conn = {key: None for key in range(1, self.max_conn + 1)}
        self.active_clients = int

        # START
        self.init_log(log_level, log_file, directory)
        self.init_storage()

        self.kvprint(f'---- KVSSERVER {id} ACTIVE -----Hosting in {self.addr}:{self.port}')

        self.ecs = ECS_handler(ecs_addr, self.data, [self.cli, self.cli_color, self.log])
        self.listen_to_connections()

        self.kvprint(f'---- KVSSERVER {id} SLEEP -----')
        exit(0)


    def listen_to_connections(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.settimeout(10)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.addr, self.port))
        server.listen()

        self.kvprint(f'Listening on {self.addr}:{self.port}')
        start_time = time.time()

        while True:
            # time.sleep(2)
            self.kvprint(f'->Listening...')
            readable, _, _ = select.select([server] + [self.ecs.sock], [], [], 5)
            self.kvprint(f'ECS:{len({self.ecs.sock})}|Readable:{len(readable)}')

            for sock in readable:
                if sock is server:
                    conn, addr = sock.accept()
                    self.kvprint(f'Connection accepted: {addr}', log='i')

                    client_thread = threading.Thread(target=self.init_client_handler, args=(conn, addr))
                    client_thread.start()
                    start_time = time.time()

                elif sock == self.ecs.sock:
                    self.kvprint(f' Message from ECS..', log='i')
                    self.ecs.handle_CONN()
                else:
                    self.kvprint(f'OUTSIDE SOCKET NOT FOLLOW.CHECK. Socket out of list')

            if not self.check_active_clients() and (time.time() - start_time) >= self.timeout:
                self.kvprint(f' Closing kvserver')
                break


    def init_client_handler(self, conn, addr):
        client_id = None #TODO rethink client organization
        for key, value in self.clients_conn.items():
            if value is None:
                client_id = key #Todo store client by addr -> id
                break
        if client_id is None:
            self.kvprint(f' ERROR client id selections')

        Client_handler(client_fd=conn,
                        client_id=client_id,
                        clients_conn=self.clients_conn,
                        cache_type=self.c_strg,
                        cache_cap=self.c_size,
                        lock=self.lock,
                       storage_dir=self.storage_dir,
                       printer_config=[self.cli, self.cli_color, self.log])


    def check_active_clients(self):
        count = sum(value is not None for value in self.clients_conn.values())
        active_ids = list()
        for key, value in self.clients_conn.items():
            if value is None:
                continue
            else:
                active_ids.append(value.client_id)
        self.kvprint(f' Active clients: {count}, Ids: {active_ids}')
        return count

    def init_storage(self):
        self.storage_dir = os.path.join(self.directory, f'kserver{self.id}_storage')
        shelve.open(filename=self.storage_dir)

    def kvprint(self, *args, c=None, log='d'):
        COLORS = {
            'r': '\033[91m',
            'g': '\033[92m',
            'y': '\033[93m',
            'b': '\033[94m',
            'reset': '\033[0m'
        }
        c = self.cli_color if c is None else COLORS[c]

        message = ' '.join(str(arg) for arg in args)
        message = c + self.cli + message + COLORS['reset']
        print(message)

        if log == 'd':
            self.log.debug(f'{self.cli}{message}')
        if log == 'i':
            self.log.info(f'{self.cli}{message}')




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
    parser.add_argument('-i', '--id', default=9, type=int, help='Server id')
    parser.add_argument('-b', '--ecs-addr', default='127.0.0.1:7000', help='ECS Server address')
    parser.add_argument('-a', '--address', default='127.0.0.1', help='Server address')
    parser.add_argument('-p', '--port', default='5000', type=int, help='Server port')
    parser.add_argument('-s', '--cache-strategy', default='LFU', type=str, help='Cache strategy: fifo, lru, lfu')
    parser.add_argument('-c', '--cache-size', default=3, type=int, help='Cache size')
    parser.add_argument('-ll', '--log-level', default='DEBUG', help='Log level:DEBUG or INFO')
    parser.add_argument('-l', '--log-file', default='kvserver.log', help='Log file')
    parser.add_argument('-d', '--directory', default='.', type=str, help='Storage directory')
    parser.add_argument('-m', '--max-conn', default=5, type=int, help='Number of clients that the server can handle')
    # parser.add_argument("-h", "--help", required=True, help="Help")

    args = parser.parse_args()
    KVServer(addr=args.address,
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


# python kvserver.py -i 0 -p 4001 -b 127.0.0.1:8000
# python kvserver.py -i 1 -p 4002 -b 127.0.0.1:8000