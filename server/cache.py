from collections import defaultdict, OrderedDict



class LFUCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = dict()
        self.frequency = defaultdict(int)

    def put(self, key, value):
        if self.capacity <= 0:
            return

        if key in self.cache:
            self.frequency[key] += 1

        else:
            if len(self.cache) >= self.capacity:
                # Find the least frequently used key(s)
                min_frequency = min(self.frequency.values())
                least_freq_key = [
                    k for k, v in self.frequency.items() if v == min_frequency
                ]
                del self.cache[least_freq_key[0]]
                del self.frequency[least_freq_key[0]]
                self.cache[key] = value
                self.frequency[key] += 1
            else:
                self.cache[key] = value
                self.frequency[key] += 1

    def get(self, key):
        if key in self.cache:
            value = self.cache[key]
            self.frequency[key] += 1
            return value
        else:
            return None

    def delete(self, key):
        if key in self.cache:
            del self.cache[key]
            del self.frequency[key]

    def print_cache(self):
        print("------Cache ----")
        for key, value in self.cache.items():
            print(f"Key: {key}, Value: {value}, Freq: {self.frequency[key]}")
        print("----------")
    # def print_cache(self):
    #     print("------Cache ----")
    #     for key, freq_map in self.cache.items():
    #         for inner_key, value in freq_map.items():
    #             print(f"Key: {inner_key}, Value: {value}")
    #     print("----------")



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

    def delete(self, key):
        if key in self.cache:
            del self.cache[key]

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

    def delete(self, key):
        for index, item in enumerate(self.cache):
            if item['key'] == key:
                del self.cache[index]
                return True

    def print_cache(self):
        print("------Cache----")
        for item in self.cache:
            print(f"Key: {item['key']}, Value: {item['value']}")
        print("----------")


