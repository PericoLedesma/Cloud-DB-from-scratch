from collections import defaultdict, OrderedDict



class LFUCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = defaultdict(OrderedDict)
        self.frequency = defaultdict(int)

    def get(self, key):
        if key in self.cache:
            value = self.cache[key].pop(key)
            self.frequency[key] += 1
            self.cache[key][key] = value
            return value
        else:
            return None

    def put(self, key, value):
        if self.capacity <= 0:
            return

        if key in self.cache:
            self.cache[key].pop(key)
        elif len(self.cache) >= self.capacity:
            # Find the least frequently used key(s)
            min_frequency = min(self.frequency.values())
            least_frequent_keys = [
                k for k, v in self.frequency.items() if v == min_frequency
            ]

            # Remove the least frequently used key with the lowest timestamp
            least_frequent_key = self.cache[least_frequent_keys[0]].popitem(last=False)[0]
            del self.frequency[least_frequent_key]

        self.frequency[key] += 1
        self.cache[key][key] = value

    def print_cache(self):
        print("------Cache ----")
        for key, freq_map in self.cache.items():
            for inner_key, value in freq_map.items():
                print(f"Key: {inner_key}, Value: {value}")
        print("----------")



class LRUCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = OrderedDict()

    def get(self, key):
        if key in self.cache:
            # Move the accessed key to the end to mark it as the most recently used
            value = self.cache.pop(key)
            self.cache[key] = value
            return value
        else:
            return None

    def put(self, key, value):
        if key in self.cache:
            # If the key already exists, move it to the end to mark it as the most recently used
            self.cache.pop(key)
        elif len(self.cache) >= self.capacity:
            # If the cache is full, remove the least recently used (first) item
            self.cache.popitem(last=False)
        self.cache[key] = value

    def print_cache(self):
        print("------Cache ----")
        for key, value in self.cache.items():
            print(f"Key: {key}, Value: {value}")
        print("----------")



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
        for index, item in enumerate(self.cache):
            if item['key'] == key:
                self.cache[index]['value'] = value
                return

        if len(self.cache) >= self.capacity:
            self.cache.pop(0)  # Remove the oldest item
        self.cache.append({'key': key, 'value': value})


    def print_cache(self):
        print("------Cache----")
        for item in self.cache:
            print(f"Key: {item['key']}, Value: {item['value']}")
        print("----------")


