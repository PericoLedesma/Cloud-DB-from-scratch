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

os.system('cls' if os.name == 'nt' else 'clear')


class KVServer:
    def __init__(self, addr, port, ecs_addr, id, cache_strategy, cache_size, log_level, log_file, directory, max_conn):
        # Server parameters

        # self.id = id
        self.id = addr[-1]
        self.name = f'kvserver{self.id}'


        self.timeout = 20
        self.tictac = time.time()

        self.addr = "".join(addr.split())
        self.port = port
        self.ecs_addr = ecs_addr
        self.kv_data = {'id': self.id,
                        'name': self.name,
                        'host': self.addr,
                        'port': self.port,
                        'ecs_addr': ecs_addr}

        # Printing parameters
        self.cli = f'\t[{self.name}]>'
        self.cli_color = f"\033[38;5;{random.randint(1, 254)}m"

        self.lock = threading.Lock()

        # Cache
        self.c_strg = cache_strategy
        self.c_size = cache_size

        # To store connections/sockets with clients
        self.clients_conn = {}
        self.pool = []

        self.init_log(log_level, log_file, directory)
        self.init_storage(directory)
        self.RUN_kvserver()



    def RUN_kvserver(self):
        self.kvprint(f'---- KVSSERVER {self.id} ACTIVE -----  Hosting in {self.addr}:{self.port}')
        self.ecs = ECS_handler(self.kv_data,
                               self.pool,
                               [self.cli, self.cli_color, self.log],
                               [self.heartbeat, self.tictac, self.timeout])

        ecs_thread = threading.Thread(target=self.connect_to_ecs, args=())
        ecs_thread.start()

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.settimeout(10)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.addr, self.port))
        server.listen()
        self.pool.append(server)

        try:
            self.listen_to_connections(server)
        except KeyboardInterrupt:
            try:
                # Todo if it is not connect or never connected
                print("\n ----> Init backup process <----")
                clients_thread = {}
                for id, client_handler in self.clients_conn.items():
                    client_handler.write_lock = True
                    clients_thread[id] = threading.Thread(target=client_handler.handle_CONN())
                    clients_thread[id].start()

                self.ecs.handle_json_REPLY('kvserver_shutdown')

                self.listen_to_connections(server)

            except KeyboardInterrupt:
                print('Shutdown of the KVServer. ')

        self.kvprint(f'---- KVSSERVER {self.id} SLEEP -----')
        server.close()
        exit(0)


    def listen_to_connections(self, server):
        self.kvprint(f'Listening on {self.addr}:{self.port}')
        self.heartbeat()
        while True:
            readable, _, _ = select.select(self.pool, [], [], 5)
            for sock in readable:
                if sock is server:
                    conn, addr = sock.accept()
                    self.kvprint(f'Connection accepted: {addr}', log='i')
                    self.init_client_handler(conn, addr)
                    conn.sendall(bytes(f'Hi client!!! This is the {self.name}\r\n', encoding='utf-8'))
                    self.heartbeat()
                elif sock == self.ecs.sock:
                    self.ecs.handle_CONN()  # Todo rethink becuase it thinks all the time it is connected
                    self.heartbeat()
                else:
                    self.kvprint(f'OUTSIDE, SOCKET NOT FOLLOW. CHECK. Socket out of list')
            if not self.check_active_clients() and (time.time() - self.tictac) >= self.timeout:
                self.kvprint(f'Time out and no clients. Closing kvserver')
                # TODO send shutdown mesage to ECS
                break
            if self.ecs.sock.fileno() < 0:
                self.kvprint(f'ECS connection lost or close. Retrying to connect')
                self.pool.remove(self.ecs.sock)
                self.ecs.connect_to_ECS()
        server.close()


    def heartbeat(self):
        self.tictac = time.time()
        # self.kvprint(f'Pum pum ', c='g')
        if self.ecs.sock in self.pool:
            self.ecs.send_heartbeat()
        # else:
        #     self.kvprint(f'No heartbeat, not connected to ecs')

    def connect_to_ecs(self):
        self.ecs.connect_to_ECS()

    def init_client_handler(self, conn, addr):
        client_id = 0
        while True:
            if client_id in self.clients_conn:
                client_id += 1
                continue
            else:
                break
        self.clients_conn[client_id] = Client_handler(clients_conn = self.clients_conn,
                                                      client_data=[self.kv_data, client_id, conn, addr],
                                                      ask_ring_metadata=self.ecs.ask_ring_metadata,
                                                      cache_config=[self.c_strg, self.c_size],
                                                      lock=self.lock,
                                                      ask_lock_write_value=self.ecs.ask_lock_write_value,
                                                      storage_dir=self.storage_dir,
                                                      printer_config=[self.cli, self.cli_color, self.log],
                                                      timeout_config=[self.heartbeat, self.tictac, self.timeout])
        client_thread = threading.Thread(target=self.clients_conn[client_id].handle_CONN())
        client_thread.start()

    def check_active_clients(self):
        count = sum(value is not None for value in self.clients_conn.values())
        active_ids = list()
        for key, value in self.clients_conn.items():
            if value is None:
                continue
            else:
                active_ids.append(value.client_id)
        self.kvprint(f'Waiting... Active clients: {count}, Ids: {active_ids}.')
        return count

    def init_storage(self, directory):
        self.storage_dir = os.path.join(directory, f'kserver{self.id}_storage')
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
        # if directory is None or directory == '.':
        #     # self.directory = f'kserver{self.id}'
        if directory == '.':
            directory = os.getcwd()  # Use current path
        os.makedirs(directory, exist_ok=True)
        log_dir = os.path.join(directory, log_file)
        if log_level == 'INFO' or log_level == 'ALL':
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

    # def init_reorganization(self):


# ------------------------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Load balancer kvserver')
    parser.add_argument('-i', '--id', default=9, type=int, help='Server id')
    parser.add_argument('-b', '--ecs-addr', default='127.0.0.1:5002', help='ECS Server address')
    parser.add_argument('-a', '--address', default='127.0.0.1', help='Server address')
    parser.add_argument('-p', '--port', default='3001', type=int, help='Server port')
    parser.add_argument('-s', '--cache-strategy', default='LFU', type=str, help='Cache strategy: fifo, lru, lfu')
    parser.add_argument('-c', '--cache-size', default=3, type=int, help='Cache size')
    parser.add_argument('-l', '--log-file', default='kvserver.log', help='Log file')
    parser.add_argument('-ll', '--log-level', default='DEBUG', help='Log level:DEBUG or INFO')
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
