# # Check for timeouts and close inactive connections
# for sock in kv_sockets.keys():
#     if sock not in readable and sock not in writable:
#         print(f"Client connection timed out: {kv_sockets[sock]}")
#         delete.append(sock)
# for sock in delete:
#     sock.close()
#     if sock in inputs:
#         inputs.remove(sock)
#     if sock in outputs:
#         outputs.remove(sock)
#     del kv_sockets[sock]
# delete = []

# Inputs sockets, new socket + messages

def listen_to_kvservers3(self):
    ecs_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ecs_socket.bind((self.host, self.port))
    ecs_socket.listen(5)

    outputs = []  # List of sockets to write output to

    # start_time = time.time()

    client_sockets = {}
    delete = []

    while True:
        time.sleep(2)
        print(f'\n{self.cli}Listening on {self.host}:{self.port}')
        readable, writable, errors = select.select([ecs_socket] + self.kvs_inputs, outputs, [], 10)
        print \
            (f'{self.cli}Readable:{len(readable)}|Writable:{len(writable)}|Errors:{len(errors)}|IN:{len(self.kvs_inputs)}|OUT{len(outputs)}')

        print('readable:', end=" ")
        for item in readable:
            print(item.fileno(), end=" ")
        print('\n')
        print('Checking conn to delete')
        print \
            (f'{self.cli}Readable:{len(readable)}|Writable:{len(writable)}|Errors:{len(errors)}|IN:{len(self.kvs_inputs)}|OUT{len(outputs)}')
        print('readable:', end=" ")
        for item in readable:
            print(item.fileno(), end=" ")
        print('\n')

        for sock in self.kvs_inputs:
            if sock not in readable or sock not in writable:
                print('To delete:', sock.fileno())
                print(f"{self.cli}Client connection timed out.")
                sock.close()
                if sock in outputs:
                    outputs.remove(sock)
                del client_sockets[sock]
                self.kvs_inputs.remove(sock)

        for sock in readable:
            if sock is ecs_socket:
                kv_socket, kv_addr = sock.accept()
                self.kvs_inputs.append(kv_socket)
                client_sockets[kv_socket] = kv_addr
                print('Connection accepted:', kv_socket.fileno())
            else:
                try:
                    data = sock.recv(1024).decode().strip()
                    print(f"{self.cli}Received data from {sock.getpeername()}: {data}")
                    if data:
                        print(f"{self.cli}Received message: {data}")
                        if sock not in outputs:
                            print('append to outputs')
                            outputs.append(sock)
                    else:
                        print(f"{self.cli}No data. Client disconnected {sock}")
                        self.kvs_inputs.remove(sock)
                        if sock in outputs:
                            outputs.remove(sock)
                        sock.close()
                        del client_sockets[sock]
                except Exception as e:
                    sock.close()
                    del client_sockets[sock]
                    print(f'{self.cli}Exception when recv: {e}. Closing socket. Closing socket')


        for sock in writable:
            message = "Server message: Hello client!"
            sock.sendall(message.encode())
            outputs.remove(sock)
            print('MSG sent')


        # Deleting the closed sockets
        for sock in self.kvs_inputs:
            if sock.fileno() < 0:
                outputs.remove(sock)
                print(f'{self.cli}Deleted socket from list of outputs')


def listen_to_kvservers2(self):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        # server.settimeout(5)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((self.host, self.port))
        server.listen()


        start_time = time.time()

        outputs = []  # List of sockets to write output to

        while True:
            time.sleep(2)
            print(f'{self.cli}Listening on {self.host}:{self.port}')
            readable, writable, errors = select.select([server] + self.kvs_inputs, [], [], 10)
            print \
                (f'{self.cli}Kvservers:{len(self.kvs_inputs)}|Readable:{len(readable)}|Writable:{len(writable)}|Errors:{len(errors)}|OUTPUTS{outputs}')

            for sock in readable:
                if sock is server:
                    kvserver_socket, addr = sock.accept()
                    self.kvs_inputs.append(kvserver_socket)
                    print(f'{self.cli}Connection accepted from [{addr}]. Asking for data')
                    self.handle_json_RESPONSE(kvserver_socket, 'kvserver_data')
                    print(f'{self.cli}Timeout restarted')
                    start_time = time.time()

                else:
                    print(f'{self.cli}Socket already connected')
                    try:
                        if sock.getpeername() is not None:
                            self.handle_RECV(sock, start_time)
                        else:
                            raise Exception('No connection. sock.getpeername() = None. Delete socket')
                    except Exception as e:
                        print(f'{self.cli}Exception outside: {e}. Closing socket')
                        sock.close()

            if (time.time() - start_time) >= self.timeout:
                print(f'{self.cli}Time out.Stop listening')
                break

            # Deleting the closed sockets
            for s in self.kvs_inputs:
                if sock.fileno() < 0:
                    self.kvs_inputs.remove(sock)
                    print(f'{self.cli}Deleted socket from list of kvservers')
