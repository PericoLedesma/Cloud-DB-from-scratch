import socket
import shelve
import threading
import logging
import os
import argparse
from collections import OrderedDict, deque
from functools import lru_cache


class KeyValueServer:
    def __init__(self, host, port, storage_dir, cache_size, log_file, log_level):
        self.host = host
        self.port = port
        self.storage_dir = storage_dir
        self.cache_size = cache_size
        self.log_file = log_file
        self.log_level = log_level
        self.storage = {}

        # Set up logging
        logging.basicConfig(filename=self.log_file, level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')

        # Cache displacement strategies
        self.FIFO = 'fifo'
        self.LRU = 'lru'
        self.LFU = 'lfu'

        # Cache settings
        self.cache_size = 0
        self.cache_strategy = self.LRU

        # Cache data structures
        self.fifo_cache = OrderedDict()
        self.lru_cache = lru_cache(maxsize=self.cache_size)
        self.lfu_cache = {}

        # Cache access lock
        self.cache_lock = threading.Lock()

    def handle_client(self, client_socket):
        while True:
            data = client_socket.recv(1024).decode()
            if not data:
                break

            command, *args = data.strip().split('|')

            if command == 'put':
                key, value = args
                if key in self.storage:
                    response = f"put_update {key}"
                else:
                    response = f"put_success {key}"
                self.storage[key] = value
            elif command == 'get':
                key = args[0]
                if key in self.storage:
                    response = f"get_success {key}"
                else:
                    response = f"get_error {key}"
            elif command == 'delete':
                key = args[0]
                if key in self.storage:
                    value = self.storage.pop(key)
                    response = f"delete_success {key} {value}"
                else:
                    response = f"delete_error {key}"
            else:
                response = f"error Invalid command: {command}"

            client_socket.sendall((response + "\r\n").encode())

        client_socket.close()

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((self.host, self.port))
            server_socket.listen()

            while True:
                client_socket, _ = server_socket.accept()
                client_thread = threading.Thread(target=self.handle_client, args=(client_socket,))
                client_thread.start()



@lru_cache
def get_lru_value(key):
    return None


def put_lru_value(key, value):
    get_lru_value.cache_clear()
    get_lru_value(key)


def handle_client(client_socket):
    with client_socket:
        while True:
            data = client_socket.recv(1024)
            if not data:
                break

            command, *args = data.decode().split('|')

            if command == 'put':
                key, value = args
                with cache_lock:
                    if cache_strategy == FIFO:
                        if len(fifo_cache) >= cache_size:
                            fifo_cache.popitem(last=False)
                        fifo_cache[key] = value
                    elif cache_strategy == LRU:
                        put_lru_value(key, value)
                    elif cache_strategy == LFU:
                        if key in lfu_cache:
                            lfu_cache[key] = (lfu_cache[key][0] + 1, value)
                        else:
                            if len(lfu_cache) >= cache_size:
                                min_key = min(lfu_cache, key=lambda k: lfu_cache[k][0])
                                del lfu_cache[min_key]
                            lfu_cache[key] = (1, value)

                with shelve.open('storage.db') as db:
                    db[key] = value
                client_socket.sendall(b'OK')
            elif command == 'get':
                key = args[0]
                with cache_lock:
                    if cache_strategy == FIFO:
                        result = fifo_cache.get(key)
                    elif cache_strategy == LRU:
                        result = get_lru_value(key)
                    elif cache_strategy == LFU:
                        result = lfu_cache.get(key, (None, None))[1]

            if result is None:
                with shelve.open('storage.db') as db:
                    result = db.get(key, 'Key not found')
            else:
                logging.info(f'Cache hit for key: {key}')
            client_socket.sendall(result.encode())


def main():
    host = '127.0.0.1'
    port = 65431

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((host, port))
        server_socket.listen()

        while True:
            client_socket, addr = server_socket.accept()
            logging.info(f'Connected by {addr}')
            threading.Thread(target=handle_client, args=(client_socket,)).start()


if __name__ == '__main__':
    main()


def main():
    parser = argparse.ArgumentParser(description="Key-Value Storage Server")
    parser.add_argument("-p", "--port", type=int, required=True, help="Server port")
    parser.add_argument("-a", "--address", default="127.0.0.1", help="Server address")
    parser.add_argument("-d", "--directory", required=True, help="Storage directory")
    parser.add_argument("-c", "--cache-size", type=int, required=True, help="Cache size")
    parser.add_argument("-l", "--log-file", required=True, help="Log file")
    parser.add_argument("-ll", "--log-level", default="INFO", help="Log level")

    args = parser.parse_args()

    server = KeyValueServer(args.address, args.port, args.directory, args.cache_size, args.log_file, args.log_level)
    server.start()

if __name__ == "__main__":
    main()