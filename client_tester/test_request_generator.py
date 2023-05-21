import random


def generate_requests(num_requests):
    requests = []
    keys = ['key1', 'key2', 'key3', 'key4', 'key5']
    values = ['value1', 'value2', 'value3', 'value4', 'value5']

    for _ in range(num_requests):
        operation = random.choice(['put', 'get', 'delete'])
        key = random.choice(keys)

        if operation == 'put':
            value = random.choice(values)
            request = f'{operation} {key} {value}'
        else:
            request = f'{operation} {key}'

        requests.append(request)

    return requests


def simulate_server(requests):
    storage = {}
    responses = []

    for request in requests:
        tokens = request.split()
        operation = tokens[0]
        key = tokens[1]

        if operation == 'put':
            value = tokens[2]
            if key in storage:
                response = f'put_update {key}'
            else:
                storage[key] = value
                response = f'put_success {key}'
        elif operation == 'get':
            if key in storage:
                value = storage[key]
                response = f'get_success {key} {value}'
            else:
                response = f'get_error {key}'
        elif operation == 'delete':
            if key in storage:
                del storage[key]
                response = f'delete_success {key} {value}'
            else:
                response = f'delete_error {key}'

        responses.append(response)

    return responses
