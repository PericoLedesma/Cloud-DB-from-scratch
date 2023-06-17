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

        # Time parameters
        self.timeout = 15
        self.tictac = time.time()

        # Server Socket parameters
        self.addr = "".join(addr.split())
        self.port = port
        self.ecs_addr = ecs_addr
        self.kv_data = {'id': self.id,
                        'name': self.name,
                        'host': self.addr,
                        'port': self.port,
                        'ecs_addr': ecs_addr}

        # Printing parameters
        self.cli = f'[{self.name}]>'

        # So threads doesnt enter same time to the put process
        self.lock = threading.Lock()

        # Cache
        self.c_strg = cache_strategy
        self.c_size = cache_size

        # To store connections/sockets with clients
        self.clients_conn = {}
        self.pool = []

        # Start processes
        self.init_log(log_level, log_file, directory)
        self.init_storage(directory)

        # ECS handler thread starter
        self.ecs = ECS_handler(self.kv_data,
                               self.pool,
                               [self.cli, self.log],
                               [self.heartbeat, self.tictac, self.timeout])
        ecs_thread = threading.Thread(target=self.ecs.connect_to_ECS, args=())
        ecs_thread.start()

        self.RUN_kvserver()

        # Shutdown process
        self.ecs.ON = False  # To stop the ECS handler thread
        self.kvprint(f'Closing KVServer. ')
        exit(0)

    def RUN_kvserver(self):
        self.kvprint(f'---- KVSSERVER {self.id} ACTIVE -----  Hosting in {self.addr}:{self.port}')
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.settimeout(10)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.addr, self.port))
        server.listen()
        # self.pool.append(server)
        try:
            self.listen_to_connections(server)
        except Exception as e:
            self.kvprint(f'Exception: {e} --> Break', log='e')
        except KeyboardInterrupt:
            # Todo if it is not connect or never connected
            print("\n ----> Init backup process <----")
            # clients_thread = {}
            # for id, client_handler in self.clients_conn.items():
            #     client_handler.write_lock = True
            #     clients_thread[id] = threading.Thread(target=client_handler.handle_CONN())
            #     clients_thread[id].start()

            self.ecs.handle_json_REPLY('kvserver_shutdown')
            self.listen_to_connections(server)
        self.kvprint(f'---- KVSSERVER {self.id} SLEEP -----')
        server.close()

    def listen_to_connections(self, server):
        self.kvprint(f'Listening on {self.addr}:{self.port}')
        self.heartbeat(ecs=False)
        while True:
            readable, _, _ = select.select([server], [], [], 10)
            for sock in readable:
                if sock is server:
                    conn, addr = sock.accept()
                    self.kvprint(f'Client accepted: {addr}')
                    self.init_client_handler(conn, addr)
                    self.heartbeat()
                else:
                    self.kvprint(f'OUTSIDE, SOCKET NOT FOLLOW. CHECK. Socket out of list', log='e')
            if not self.check_active_clients() and (time.time() - self.tictac) >= self.timeout:
                self.kvprint(f'Time out and no clients. Closing kvserver')
                # TODO send shutdown mesage to ECS
                break
        server.close()

    def heartbeat(self, ecs=True):
        self.tictac = time.time()
        if ecs is True:
            try:
                self.ecs.send_heartbeat()
            except Exception as e:
                self.kvprint(f'Exception in sending heartbeat to ecs: {e}', log='e')

    def init_client_handler(self, conn, addr):
        client_id = 0 # Todo we are using new id for each new client
        while True:
            if client_id in self.clients_conn:
                client_id += 1
                continue
            else:
                break
        self.clients_conn[client_id] = Client_handler(clients_conn=self.clients_conn,
                                                      client_data=[self.kv_data, client_id, conn, addr],
                                                      ask_ring_metadata=self.ecs.ask_ring_metadata,
                                                      cache_config=[self.c_strg, self.c_size],
                                                      lock=self.lock,
                                                      ask_lock_write_value=self.ecs.ask_lock_write_value,
                                                      storage_dir=self.storage_dir,
                                                      printer_config=[self.cli, self.log],
                                                      timeout_config=[self.heartbeat, self.tictac, self.timeout])
        client_thread = threading.Thread(target=self.clients_conn[client_id].handle_CONN())
        client_thread.start()

    def check_active_clients(self):
        count = sum(value is not None for value in self.clients_conn.values())
        active_ids = list()
        ECS = 0
        for key, value in self.clients_conn.items():
            if value is None:
                continue
            else:
                active_ids.append(value.client_id)
        if self.ecs.sock in self.pool:
            ECS += 1
        self.kvprint(f'Waiting... Active clients: {count}, Ids: {active_ids}.| ECS: {ECS}')
        return count

    def init_storage(self, directory):
        self.storage_dir = os.path.join(directory, f'kserver{self.id}_storage')
        shelve.open(filename=self.storage_dir)

    def kvprint(self, *args, log='d'):
        message = ' '.join(str(arg) for arg in args)
        message = self.cli + message
        if log == 'd':
            self.log.debug(message)
        if log == 'i':
            self.log.info(message)
        if log == 'r':
            self.log.error(message)

    def init_log(self, log_level, log_file, directory):
        if directory == '.':
            directory = os.getcwd()  # Use current path
        os.makedirs(directory, exist_ok=True)
        log_dir = os.path.join(directory, log_file)
        if log_level == 'INFO':
            logging.basicConfig(filename=log_dir,
                                filemode='w',
                                level=logging.DEBUG,
                                format='%(asctime)s - INFO - %(message)s')
        elif log_level == 'DEBUG':
            logging.basicConfig(filename=log_dir,
                                filemode='w',
                                level=logging.DEBUG,
                                format='%(asctime)s - %(levelname)s - %(message)s')
        elif log_level == 'FINEST':
            logging.basicConfig(filename=log_dir,
                                filemode='w',
                                level=logging.DEBUG,
                                format='%(asctime)s - FINEST - %(message)s')
        elif log_level == 'ALL':
            logging.basicConfig(filename=log_dir,
                                filemode='w',
                                level=logging.DEBUG,
                                format='%(asctime)s - ALL - %(message)s')
        self.log = logging.getLogger(__name__)
        stream_handler = logging.StreamHandler()
        self.log.addHandler(stream_handler)


# ------------------------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='KVServer')
    parser.add_argument('-i', '--id', default=9, type=int, help='Server id')
    parser.add_argument('-b', '--ecs-addr', default='127.0.0.1:5002', help='ECS Server address')
    parser.add_argument('-a', '--address', default='127.0.0.1', help='Server address')
    parser.add_argument('-p', '--port', default='40823', type=int, help='Server port')
    parser.add_argument('-s', '--cache-strategy', default='LFU', type=str, help='Cache strategy: fifo, lru, lfu')
    parser.add_argument('-c', '--cache-size', default=3, type=int, help='Cache size')
    parser.add_argument('-l', '--log-file', default='kv-server.log', help='Log file')
    parser.add_argument('-ll', '--log-level', default='DEBUG', help='Log level:DEBUG or INFO')
    parser.add_argument('-d', '--directory', default='data', type=str, help='Storage directory')
    parser.add_argument('-m', '--max-conn', default=5, type=int, help='Number of clients that the server can handle')
    # parser.add_argument("-h", "--help", required=True, help="Help")

    args = parser.parse_args()
    # print(f'Commands:  {args}')

    KVServer(addr=args.address,
             port=args.port,
             ecs_addr=args.ecs_addr,
             id=args.id,
             cache_strategy=args.cache_strategy,
             cache_size=args.cache_size,
             log_level=args.log_level,
             log_file=args.address,
             directory=args.directory,
             max_conn=args.max_conn)


if __name__ == '__main__':
    main()

# python kvserver.py -i 0 -p 4001 -b 127.0.0.1:8000
# python kvserver.py -i 1 -p 4002 -b 127.0.0.1:8000
