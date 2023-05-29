from client_handler import *
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
        self.cli = f'KVServer[{self.id}]>'

        self.host = host
        self.port = port
        self.ecs_addr = ecs_addr
        self.max_conn = max_conn
        self.timeout = 10
        self.lock = threading.Lock()

        #Cache
        self.c_strg = cache_strategy
        self.c_size = cache_size

        # To store connections/sockets with clients
        self.clients_conn = {key: None for key in range(1, self.max_conn + 1)}
        self.active_clients = int

        self.init_log(log_level, log_file, directory)
        self.init_storage()


        print(f'{self.cli}---- KVSSERVER {id} ACTIVE -----')
        self.log.info(f'{self.cli}---- KVSSERVER {id} ACTIVE -----')
        self.connect_to_ECS()
        # self.listen_to_connections()
        self.log.info(f'{self.cli}---- KVSSERVER {id} SLEEP -----')
        print(f'{self.cli}---- KVSSERVER {id} SLEEP -----')


    def connect_to_ECS(self):
        print(f'{self.cli}Connecting to bootstrap.Address {self.ecs_addr}')
        RETRY_INTERVAL = 3
        host = "127.0.0.1"
        port = 8000
        while True:
            try:
                self.ecs_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                ecs_address, ecs_port = self.ecs_addr.split(':')
                # self.ecs_sock.connect((ecs_address, int(ecs_port)))
                self.ecs_sock.connect((host, port))
                # Wait until the connection is established
                while True:
                    if self.ecs_sock.fileno() != -1:
                        break

                print(f'{self.cli}Connected to {host}:{port}')
                self.ecs_sock.sendall(bytes('HI ECS', encoding='utf-8'))
                print(f'{self.cli}MESSAGE SENT to {host}:{port}')
                break
            except socket.error as e:
                print(f'{self.cli}Connection error:{e}')
                print(f'{self.cli}Retrying in {RETRY_INTERVAL} seconds...')
                time.sleep(RETRY_INTERVAL)
            finally:
                # Close the client socket
                self.ecs_sock.close()
                print(f'{self.cli}Client socket closed')

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
                print('Waiting ')
                ready_sockets, _, _ = select.select([server], [], [], 2)
                print('done ')
                if ready_sockets:
                    conn, addr = server.accept()
                    if self.check_active_clients() < self.max_conn:
                        self.log.info(f'{self.cli}Connection accepted:, {addr}')
                        client_thread = threading.Thread(target=self.init_client_handler, args=(conn, addr))
                        client_thread.start()
                    else:
                        self.log.info(f'{self.cli} Max number of clients ')
                        # todo
                    start_time = time.time()

                elapsed_time = time.time() - start_time
                print('Listening.Checking exit conditions(clients, time)')

                if not self.check_active_clients() and elapsed_time >= self.timeout:
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
    parser.add_argument('-p', '--port', default='8000', type=int, help='Server port')
    parser.add_argument('-s', '--cache-strategy', default='LFU', type=str, help='Cache strategy: fifo, lru, lfu')
    parser.add_argument('-c', '--cache-size', default=3, type=int, help='Cache size')
    parser.add_argument('-ll', '--log-level', default='DEBUG', help='Log level:DEBUG or INFO')
    parser.add_argument('-l', '--log-file', default='kvserver.log', help='Log file')
    parser.add_argument('-d', '--directory', default='.', type=str, help='Storage directory')
    parser.add_argument('-m', '--max-conn', default=5, type=int, help='Number of clients that the server can handle')
    # parser.add_argument("-h", "--help", required=True, help="Help")

    args = parser.parse_args()

    print(f'ID {args.id}, PORT {args.port}')

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
