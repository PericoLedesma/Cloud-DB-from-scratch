from kvserver import *

import os
import socket
import threading
import argparse
import time


class LoadBalancerServer:
    def __init__(self, host, port, id, cache_strategy):
        print("---- Load balancer ", id, " created-----")

        # Server parameters
        self.id = id
        self.host = host
        self.port = port
        self.c_strg = cache_strategy

        # To store connections/sockets with clients
        self.clients_list = []
        self.max_clients = 5

        self.lock = threading.Lock()
        self.listen_to_connections()



    def server_print(self, *args):
        if len(args) != 0:
            sys.stdout.write(f"LBServer> {' '.join(str(arg) for arg in args)}")
            sys.stdout.write('\n')


    def listen_to_connections(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((self.host, self.port))
            server_socket.listen()
            self.server_print(f'Listening on {self.host}:{self.port}')
            # todo not active connections

            while True:
                conn, addr = server_socket.accept()
                client_id = self.next_client_id()

                if len(self.clients_list) < self.max_clients:
                    self.server_print(f'New client ')
                    client_thread = threading.Thread(target=self.run_kvserver, args=(conn, client_id))
                    client_thread.start()
                else:
                    self.server_print(f'Max number of clients ')
                    #todo

                self.server_print(f'Active clients {self.clients_list}')

    def run_kvserver(self, conn, client_id):
        KVServer(client_fd=conn,
                 id=client_id,
                 clients_list=self.clients_list,
                 cache_type=self.c_strg,
                 cache_cap=2,
                 lock=self.lock)


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

def main():
    parser = argparse.ArgumentParser(description='Load balancer server')
    parser.add_argument('-a', '--address', default='127.0.0.1', help='Server address')
    parser.add_argument('-p', '--port', default='8000', type=int, help='Server port')
    parser.add_argument('-c', '--cache-strategy', default='lru', type=str, help='Cache strategy: fifo, lru, lfu')

    args = parser.parse_args()
    server1 = LoadBalancerServer(args.address, args.port, 1, args.cache_strategy)

if __name__ == '__main__':
    print("Hello, lets go...")
    main()
    print('End script')
