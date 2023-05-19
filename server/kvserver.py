from client_handler import *

import os
import socket
import threading
import argparse
import logging


class KVServer:
    def __init__(self, host, port, id, cache_strategy, cache_size, log_level, log_file, directory):
        # Server parameters
        self.id = id
        self.host = host
        self.port = port

        #Cache
        self.c_strg = cache_strategy
        self.c_size = cache_size

        self.log_config(log_level, log_file, directory)

        # To store connections/sockets with clients
        self.clients_conn = []
        self.clients_list = []
        self.max_clients = 5

        self.lock = threading.Lock()

        self.log.debug(self.sprint("---- KVSSERVER ", id, " ACTIVE -----"))
        self.listen_to_connections()


    def listen_to_connections(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((self.host, self.port))
            server_socket.listen()

            self.log.debug(self.sprint(f'Listening on {self.host}:{self.port}'))

            while True:
                conn, addr = server_socket.accept()
                client_id = self.next_client_id()

                if len(self.clients_list) < self.max_clients:
                    self.log.debug(self.sprint(f'New client '))
                    client_thread = threading.Thread(target=self.init_handler, args=(conn, client_id))
                    client_thread.start()
                else:
                    self.log.debug(self.sprint(f'Max number of clients '))
                    #todo

                # todo Check active connections

                self.log.debug(self.sprint(f'Active clients {self.clients_list}'))


    def init_handler(self, conn, client_id):
        self.clients_conn.append(Client_handler(client_fd=conn,
                                                client_id=client_id,
                                                clients_list=self.clients_list,
                                                cache_type=self.c_strg,
                                                cache_cap=self.c_size,
                                                lock=self.lock,
                                                logger=self.log))

    def sprint(self, *args):
        # if len(args) != 0:
        #     sys.stdout.write(f"LBServer> {' '.join(str(arg) for arg in args)}")
        #     sys.stdout.write('\n')
        if len(args) != 0:
            return f"KVServer> {' '.join(str(arg) for arg in args)}"


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
        elif log_level == 'debug':
            logging.basicConfig(filename=log_dir,
                                filemode='w',
                                level=logging.DEBUG,
                                format='%(asctime)s - %(levelname)s - %(message)s')
        self.log = logging.getLogger(__name__)






def main():
    parser = argparse.ArgumentParser(description='Load balancer server')
    parser.add_argument('-i', '--id', default=1, type=int, help='Server id')
    parser.add_argument('-a', '--address', default='0.0.0.0', help='Server address')
    parser.add_argument('-p', '--port', default='8000', type=int, help='Server port')
    parser.add_argument('-s', '--cache-strategy', default='fifo', type=str, help='Cache strategy: fifo, lru, lfu')
    parser.add_argument('-c', '--cache-size', default=3, type=int, help='Cache size')
    parser.add_argument('-ll', '--log-level', default='INFO', help='Log level:debug or INFO')
    parser.add_argument('-l', '--log-file', default='server.log', help='Log file')
    parser.add_argument('-d', '--directory', default='data', type=str, help='Storage directory')
    # parser.add_argument("-h", "--help", required=True, help="Help")

    args = parser.parse_args()
    print(args.id)
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
