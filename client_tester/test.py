def send_put_request(host, port, key, value, result):
    a = 1
    assert a == 1
    assert result == 5

    mykey = 'Key5'
    myvalue = 'Pedro'
    host = '127.0.0.1'
    port = 8000

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
        client.connect((host, port))
        response = client.recv(1024)
        response = process_reponse(response.decode('utf-8'))

        print(response)
        welcome_msg = f'Connection to KVServer established: /{host} / {port}'

        assertion = process_reponse(welcome_msg)
        print('Comparing')
        print(response)
        print(assertion)
        for index, item in enumerate(response):
            print('Cheching ', response[index], assertion[index])
            assert response[index] == assertion[index]

        #
        # request = f'put {key} {value}'.encode('utf-8')
        #
        #
        # for i in range(10):
        #     print("Request to send: ", request)
        #     client.sendall(request)
        #     response = client.recv(1024)
        #     print(response.decode('utf-8'))
        #
        #
        # request = f'delete {key}'.encode('utf-8')
        #
        # for i in range(10):
        #     print("Request to send: ", request)
        #     client.sendall(request)
        #     response = client.recv(1024)
        #     print(response.decode('utf-8'))
        #
        #
        # time.sleep(10)  # Pause execution for 3 seconds
        # request = f'close'.encode('utf-8')
        #
        # print("Request to send: ", request)
        # client.sendall(request)
        # response = client.recv(1024)
        # print(response.decode('utf-8'))
