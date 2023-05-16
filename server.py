import argparse
import logging
import shelve
import socket
import threading
from collections import OrderedDict


class LRUCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = OrderedDict()

    def get(self, key):
        if key not in self.cache:
            return None
        self.cache.move_to_end(key)
        return self.cache[key]

    def put(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)


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
        self.cache_strategy = self.FIFO

        # Cache data structures
        self.fifo_cache = OrderedDict()
        # self.lru_cache = lru_cache(maxsize=self.cache_size)
        self.lru_cache = LRUCache(cache_size)
        self.lfu_cache = {}

        # Cache access lock
        self.cache_lock = threading.Lock()

    def handle_client(self, client_socket):
        while True:
            data = client_socket.recv(1024)
            if not data:
                break

            command, *args = data.decode().split('|')

            if command == 'put':
                key, value = args
                with self.cache_lock:
                    if self.cache_strategy == self.FIFO:
                        if len(self.fifo_cache) >= self.cache_size:
                            self.fifo_cache.popitem(last=False)
                        self.fifo_cache[key] = value
                    elif self.cache_strategy == self.LRU:
                        self.lru_cache.put(key, value)
                    elif self.cache_strategy == self.LFU:
                        if key in self.lfu_cache:
                            self.lfu_cache[key] = (self.lfu_cache[key][0] + 1, value)
                        else:
                            if len(self.lfu_cache) >= self.cache_size:
                                min_key = min(self.lfu_cache, key=lambda k: self.lfu_cache[k][0])
                                del self.lfu_cache[min_key]
                            self.lfu_cache[key] = (1, value)
                    with shelve.open('storage.db') as db:
                        if key in self.storage:
                            response = f"put_update {key}"
                        else:
                            response = f"put_success {key}"
                        db[key] = value
            elif command == 'get':
                key = args[0]
                with self.cache_lock:
                    if self.cache_strategy == self.FIFO:
                        result = self.fifo_cache.get(key)
                    elif self.cache_strategy == self.LRU:
                        result = self.lru_cache.get(key)
                    elif self.cache_strategy == self.LFU:
                        result = self.lfu_cache.get(key, (None, None))[1]

                if result is None:
                    with shelve.open('storage.db') as db:
                        if key in db:
                            result = db.get(key)
                            response = f"get_success {key} {result}"
                        else:
                            response = f"get_error {key}"
                else:
                    response = f'get_success (Cache hit for key): {key} {result}'
            elif command == 'delete':
                key = args[0]
                with shelve.open('storage.db') as db:
                    if key in db:
                        result = db.pop(key)
                        response = f"delete_success {key} {result}"
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
