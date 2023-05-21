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
            request = {'operation': 'put', 'key': key, 'value': value}
        else:
            request = {'operation': operation, 'key': key}

        requests.append(request)

    return requests


def simulate_server(requests):
    storage = {}
    responses = []

    for request in requests:
        operation = request['operation']
        key = request['key']

        if operation == 'put':
            value = request['value']
            if key in storage:
                response = {'status': 'put_update', 'key': key}
            else:
                storage[key] = value
                response = {'status': 'put_success', 'key': key}
        elif operation == 'get':
            if key in storage:
                value = storage[key]
                response = {'status': 'get_success', 'key': key, 'value': value}
            else:
                response = {'status': 'get_error', 'key': key}
        elif operation == 'delete':
            if key in storage:
                del storage[key]
                response = {'status': 'delete_success', 'key': key}
            else:
                response = {'status': 'delete_error', 'key': key}

        responses.append(response)

    return responses



