import sys
import csv


# ------------------------------------------------------------------------
class Worker:
    def __init__(self, client_fd, client_id, worker_id, num_workers):
        num_workers += 1

        self.client_fd = client_fd
        self.client_id = client_id
        self.id = worker_id

        self.worker_print("Wake up")
        self.handle_conn()
        self.worker_print(f"Sleep")

        num_workers -= 1 # The num of workers in the server class should get updated. Not tested


    def handle_conn(self):
        self.worker_print(f'{self.client_fd.addr} --> Connected')

        while True:
            request = self.client_fd.conn.recv(1024)
            response = self.handle_request(request)
            self.handle_response(response)
            if response.decode('utf-8') == 'End connection':
                break

        self.client_fd.conn.close()
        self.worker_print(f'{self.client_fd.addr} --> End connection')


    def handle_request(self, request):
        print('Request:', request)
        parts = request.split()
        method = parts[0].decode('utf-8')
        if method == 'put':
            key, value = parts[1].decode('utf-8'), parts[2].decode('utf-8')
            return self.PUT_request(key, value)
        elif method == 'get':
            pass
        elif method == 'delete':
            pass
        elif method == 'close':
            return b'End connection'
        else:
            return b'Server>Invalid method'


    def PUT_request(self, key, value):
        self.worker_print(f'PUT {key} {value}')

        # To check if the key exits
        with open("data.csv", "r") as file:
            founded = False
            rows = list(csv.reader(file))

            for row in rows:
                if row[0] == key:
                    self.worker_print(f'Key found.')
                    row[1] = value
                    founded = True
                    response = f'put_update {key}'
                    break

            if founded is False:
                self.worker_print(f'Key not found')
                rows.append([key, value])
                response = f'put_success {key}'

        try:
            # Rewrite the CSV file with the updated or new record
            with open('data.csv', 'w', newline='') as f:
                writer = csv.writer(f)
                for row in rows:
                    writer.writerow(row)
            return bytes(response, encoding='utf-8')

        except:
            return b'put_error'


    def GET_request(self, key, value):  # TODO
        # open the file for reading
        with open("data.csv", "r") as f:
            # retrieve the data from the file as a list of lists
            reader = csv.reader(f)
            stored_data = list(reader)

        for row in reader:
            if row[0] == key:
                self.worker_print(f'Key {key}found. Value {value}')
                return value
            else:
                self.worker_print(f'Key {key} not found')
                return False


    def DELETE_request(self, request):  # TODO
        # Open the CSV file for reading
        with open('data.csv', 'r') as file:
            # Use csv.reader to read the file and convert it to a list of lists
            data = list(csv.reader(file))

        # Define the record you want to delete
        record_to_delete = ['John', '30', 'john@example.com']

        # Iterate through the list and find the index of the record to delete
        index_to_delete = None
        for i, record in enumerate(data):
            if record == record_to_delete:
                index_to_delete = i
                break

        # Delete the record from the list if it was found
        if index_to_delete is not None:
            del data[index_to_delete]

        # Open the CSV file for writing and write the updated data to it
        with open('data.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(data)


    def handle_response(self, response):
        self.client_fd.conn.sendall(response)


    def worker_print(self, *args):
        if len(args) != 0:
            sys.stdout.write(f"\tWorker {self.id}> {' '.join(str(arg) for arg in args)}")
            sys.stdout.write('\n')
