from worker import *

import os
import socket
import threading

class Socket:
    def __init__(self, conn, addr, id):
        self.id = id
        self.conn = conn
        self.addr = addr
# ------------------------------------------------------------------------
def start_worker(client_fd, client_id, worker_id, num_workers):

    print("Starting up worker...")
    my_object = Worker(client_fd,
                       client_id,
                       worker_id,
                       num_workers)


# ------------------------------------------------------------------------
class Server:
    def __init__(self, host, port, id):
        print("----Server ", id, " created-----")

        # Server parameters
        self.id = id
        self.host = host
        self.port = port
        self.address_family = socket.AF_INET
        self.socket_type = socket.SOCK_STREAM

        # To store connections/sockets with clients
        self.client_id = 0
        self.clients_fd = {}

        self.storage = {}

        # Workers/threads parameters
        self.workers_max = 5
        self.workers_counter = 0
        self.worker_id = 0
        self.workers = {}

        # Pool connections
        self.pool_fd = []
        self.disk_file_check()
        self.listen_to_connections()
        self.server_print("Server 1 active")


    def server_print(self, *args):
        # Just to print server prompts

        if len(args) != 0:
            sys.stdout.write(f"Server {self.id}> {' '.join(str(arg) for arg in args)}")
            sys.stdout.write('\n')

    def disk_file_check(self):
        # Check if data.csv exists and creates it if not

        if not os.path.exists('data.csv'):
            # Create data.csv if it doesn't exist
            with open('data.csv', 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(['Key', 'Value'])

    def listen_to_connections(self):
        # Server on listen mode and deploys a thread everytime a clients connects

        with socket.socket(self.address_family, self.socket_type) as server:
            server.bind((self.host, self.port))
            server.listen(
                5)  # listen arg it specifies the number of unaccepted connections that the system will allow before refusing new connections

            self.server_print(f'Listening on {self.host}:{self.port}.....')

            while True:
                conn, addr = server.accept()
                self.client_id += 1
                self.clients_fd[self.client_id] = Socket(conn, addr, self.client_id)

                if self.workers_counter < self.workers_max:
                    my_thread = threading.Thread(target=start_worker(client_fd=self.clients_fd[self.client_id],
                                                                     client_id=self.client_id,
                                                                     worker_id=self.workers_counter,
                                                                     num_workers=self.workers_counter))
                    my_thread.start()

                else:
                    self.pool_fd.append(self.client_id)
