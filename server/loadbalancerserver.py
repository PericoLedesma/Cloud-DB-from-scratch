from kvserver import *

import os
import socket
import threading
import argparse
import time




# ------------------------------------------------------------------------
class LoadBalancerServer:
    def __init__(self, host, port, id):
        print("---- Load balancer  ", id, " created-----")

        # Server parameters
        self.id = id
        self.host = host
        self.port = port

        # To store connections/sockets with clients
        self.client_id = 0
        self.num_clients = 0
        self.max_clients = 5


        # self.starting_storage_cache()

        self.listen_to_connections()
        self.server_print("Load balancer Server 1 active")



    def server_print(self, *args):
        if len(args) != 0:
            sys.stdout.write(f"LBServer {self.id}> {' '.join(str(arg) for arg in args)}")
            sys.stdout.write('\n')


    def listen_to_connections(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((self.host, self.port))
            server_socket.listen()

            self.server_print(f'Listening on {self.host}:{self.port}')

            while True:
                conn, addr = server_socket.accept()
                self.client_id += 1
                self.num_clients += 1
                self.server_print(f'Number of clients {self.num_clients}')

                if self.num_clients < self.max_clients:
                    self.server_print(f'New client ')
                    client_thread = threading.Thread(target=self.run_kvserver, args=(conn, addr))
                    client_thread.start()
                else:
                    self.server_print(f'Max number of worker ')



    def run_kvserver(self, conn, addr):
        KVServer(client_fd=conn,
                 id=self.client_id,
                 cache_type='fifo',
                 cache_cap=2)
        self.num_clients -= 1



def main():
    parser = argparse.ArgumentParser(description='Load balancer server')
    parser.add_argument('-a', '--address', default='127.0.0.1', help='Server address')
    parser.add_argument('-p', '--port', default='8000', type=int, help='Server port')

    args = parser.parse_args()
    server1 = LoadBalancerServer(args.address, args.port, 1)

if __name__ == '__main__':
    print("Hello, lets go...")
    main()
    print('End script')
