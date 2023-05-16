

# ------------------------------------------------------------------------


class FIFOCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = []

    def get(self, key):
        for item in self.cache:
            if item['key'] == key:
                return item['value']
        return None

    def put(self, key, value):
        if len(self.cache) >= self.capacity:
            self.cache.pop(0)  # Remove the oldest item

        self.cache.append({'key': key, 'value': value})

    def print_cache(self):
        print("------Cache Contents:----")
        for item in self.cache:
            print(f"Key: {item['key']}, Value: {item['value']}")
        print("----------")


