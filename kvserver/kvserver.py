from client_handler import *
from ecshandler import *

import time
import os
import socket
import threading
import argparse
import logging
import select

os.system('cls' if os.name == 'nt' else 'clear')


class KVServer:
    def __init__(self, addr, port, ecs_addr, id, cache_strategy, cache_size, log_level, log_file, directory, max_conn):
        # Server parameters
        # self.id = id
        self.id = addr[-1]
        # self.id = str(port)[-1]
        self.name = f'KV{self.id}'

        # Time parameters
        self.kvs_timeout = 20
        self.sock_timeout = 15  # For client and ECS
        self.heartbeat_interval = 5  # kvs Socket sock_timeout that will control more or less the heartbeat time
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

        # Start processes
        self.init_log(log_level, log_file, directory)
        self.init_storage(directory)
        self.kvprint(f'{"-" * 60}')
        self.kvprint(f'{" " * 20}KVSSERVER {self.id} Hosting in {self.addr}:{self.port}')
        self.kvprint(f'{"-" * 60}')

        # ECS handler thread starter
        self.ecs = ECS_handler(self.kv_data,
                               self.clients_conn,
                               self.storage_dir,
                               [self.cli, self.log],
                               self.sock_timeout)
        ecs_thread = threading.Thread(target=self.ecs.handle_CONN, args=())
        ecs_thread.start()
        self.RUN_kvserver()

    def RUN_kvserver(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.settimeout(self.heartbeat_interval)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.addr, self.port))
        server.listen()
        try:
            self.listen_to_connections(server)
        except Exception as e:
            self.kvprint(f'Exception RUN_kvserver: {e} --> Closing kvserver', log='e')
        except KeyboardInterrupt:
            # Todo if it is not connect or never connected
            self.ecs.handle_json_REPLY('starting_shutdown_process')
            self.kvprint(" --------------------------------")
            self.kvprint(" ----> Init backup process <----")
            self.kvprint(" --------------------------------")
            self.ecs.write_lock = True
            self.ecs.shutdown = True
            self.listen_to_connections(server)
            if self.clients_conn:
                for id, client_handler in self.clients_conn.items():
                    if client_handler:
                        thread = threading.Thread(target=client_handler.handle_CONN, args=(True,))
                        thread.start()
        server.close()
        self.kvprint(f'{"-" * 60}')
        self.kvprint(f'{" " * 20}KVServer{self.id} Stopped')
        self.kvprint(f'{"-" * 60}')
        del self
        exit(0)

    def listen_to_connections(self, server):
        self.kvprint(f'Listening on {self.addr}:{self.port}')
        while self.ecs.kvs_ON:
            try:
                readable, _, _ = select.select([server], [], [], self.heartbeat_interval)
                for sock in readable:
                    if sock is server:
                        conn, addr = sock.accept()
                        self.kvprint(f'Client accepted: {addr}')
                        self.init_client_handler(conn, addr)
                    else:
                        self.kvprint(f'OUTSIDE, SOCKET NOT FOLLOW. CHECK. Socket out of list', log='e')
                if self.check_active_clients():  # Depending if the kvserver has active clients or not
                    self.ecs.send_heartbeat(active=True)
                else:
                    self.ecs.send_heartbeat(active=False)

                # MY DATA
                # self.kvprint(f'MY DATA')
                # self.kvprint(self.kv_data)
                # self.kvprint(f'-------')
                # self.kvprint(f"------------------")
                # self.kvprint(f"All key-value pairs")


                # # Storage check
                # with shelve.open(self.storage_dir, flag='r') as db:
                #     message = f"\n------------------\n"
                #     message += f'All key-value pairs\n'
                #     counter = 1
                #     for key, value in db.items():
                #         message += f"Hash {self.hash(key)}==> {key} | {value}\n"
                #         counter += 1
                #     message += f"------------------\n"
                #     return message


            except Exception as e:
                self.kvprint(f'Exception listen_to_connections: {e}. Continue')
        self.kvprint(f'Stop listening')

    def init_client_handler(self, conn, addr):
        client_id = 0  # Todo check
        while True:
            if client_id in self.clients_conn:
                client_id += 1
                continue
            else:
                break
        self.kvprint(f'Init client handler....')
        self.clients_conn[client_id] = Client_handler(clients_conn=self.clients_conn,
                                                      client_data=[self.kv_data, client_id, conn, addr],
                                                      ring_structures=[self.ecs.ask_ring, self.ecs.ask_replicas, self.ecs.ask_lock, self.ecs.ask_lock_ecs],
                                                      connected_replicas=self.ecs.rep.connected_replicas,
                                                      cache_config=[self.c_strg, self.c_size],
                                                      lock=self.lock,
                                                      storage_dir=self.storage_dir,
                                                      printer_config=[self.cli, self.log],
                                                      timeout_config=[self.tictac, self.sock_timeout])

        client_thread = threading.Thread(target=self.clients_conn[client_id].handle_CONN, args=())
        client_thread.start()



    def check_active_clients(self):
        return sum(value is not None for value in self.clients_conn.values())

    def init_storage(self, directory):
        self.storage_dir = os.path.join(directory, f'kserver{self.id}_storage')
        shelve.open(filename=self.storage_dir)

    def kvprint(self, *args, log='d'):
        message = ' '.join(str(arg) for arg in args)
        message = self.cli + message
        if log == 'd':
            self.log.debug(message)
        elif log == 'i':
            self.log.info(message)
        elif log == 'e':
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
    # print(f'Commands:  {data}')

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
