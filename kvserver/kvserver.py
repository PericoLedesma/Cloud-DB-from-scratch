from client_handler import *
import time
import os
import socket
import threading
import argparse
import logging
import select


class KVServer:
    def __init__(self, host, port, id, cache_strategy, cache_size, log_level, log_file, directory):
        # Server parameters
        self.id = id
        self.cli = f'KVServer{self.id}>'
        self.host = host
        self.port = port
        self.timeout = 100

        #Cache
        self.c_strg = cache_strategy
        self.c_size = cache_size

        self.log_config(log_level, log_file, directory)

        # To store connections/sockets with clients
        self.clients_conn = dict()
        self.clients_list = []
        self.max_clients = 5

        self.lock = threading.Lock()

        self.log.info(f'{self.cli}---- KVSSERVER {id} ACTIVE -----')
        self.listen_to_connections()
        self.log.info(f'{self.cli}---- KVSSERVER {id} SLEEP -----')


    def listen_to_connections(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.settimeout(2)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((self.host, self.port))
            server.listen()

            self.log.debug(f'{self.cli}Listening on {self.host}:{self.port}')

            start_time = time.time()

            while True:
                ready_sockets, _, _ = select.select([server], [], [], server.gettimeout())

                if ready_sockets:
                    conn, addr = server.accept()
                    self.check_active_clients()

                    if len(self.clients_conn) < self.max_clients:
                        self.log.info(f'{self.cli}Connection accepted:, {addr}')
                        client_thread = threading.Thread(target=self.init_handler, args=(conn, addr))
                        client_thread.start()
                    else:
                        self.log.info(f'{self.cli} Max number of clients ')
                        # todo

                    start_time = time.time()
                elapsed_time = time.time() - start_time

                self.check_active_clients()
                if not self.clients_conn and elapsed_time >= self.timeout:
                    self.log.debug(f'{self.cli} Closing kvserver')
                    break
                else:
                    # self.log.debug(f'{self.cli} Still active. Reset while.')
                    continue


    def init_handler(self, conn, addr):
        client_id = self.next_client_id()
        Client_handler(client_fd=conn,
                        client_id=client_id,
                        clients_list=self.clients_list,
                        clients_conn=self.clients_conn,
                        cache_type=self.c_strg,
                        cache_cap=self.c_size,
                        lock=self.lock,
                        logger=self.log)


    def check_active_clients(self):
        active = list()
        for key, value in self.clients_conn.items():
            if value is None:
                del self.clients_conn[key]
            else:
                active.append(value.client_id)
        self.log.debug(f'{self.cli} Active clients: {len(self.clients_conn)}, Ids: {active}')



    def next_client_id(self):
        self.clients_list.sort()
        if not self.clients_list or min(self.clients_list) > 1:
            # If the list is empty or does not contain 1, return 1
            return 1
        numbers_set = set(self.clients_list)
        max_number = max(self.clients_list)
        for i in range(1, max_number + 2):
            if i not in numbers_set:
                return i

    def log_config(self, log_level, log_file, directory):
        os.makedirs(directory, exist_ok=True)
        log_dir = os.path.join(directory, log_file)

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
    parser.add_argument('-a', '--address', default='0.0.0.0', help='Server address')
    parser.add_argument('-p', '--port', default='8000', type=int, help='Server port')
    parser.add_argument('-s', '--cache-strategy', default='LFU', type=str, help='Cache strategy: fifo, lru, lfu')
    parser.add_argument('-c', '--cache-size', default=3, type=int, help='Cache size')
    parser.add_argument('-ll', '--log-level', default='DEBUG', help='Log level:DEBUG or INFO')
    parser.add_argument('-l', '--log-file', default='kvserver.log', help='Log file')
    parser.add_argument('-d', '--directory', default='.', type=str, help='Storage directory')
    # parser.add_argument("-h", "--help", required=True, help="Help")

    args = parser.parse_args()

    lb_server1 = KVServer(host=args.address,
                          port=args.port,
                          id=args.id,
                          cache_strategy=args.cache_strategy,
                          cache_size=args.cache_size,
                          log_level=args.log_level,
                          log_file=args.log_file,
                          directory=args.directory)


if __name__ == '__main__':
    main()
