import hashlib
import json

class ConsistentHashing:
    def __init__(self, kv_data):
        self.kvs_data = kv_data
        self.RING_metadata = {}

    def new_node(self, kvserver):
        hash = self.hash(f'{kvserver["host"]}:{kvserver["port"]}')
        kvserver['hash_key'] = hash
        self.RING_metadata[hash] = [kvserver["host"], kvserver["port"], hash]
        self.update_ring()


    def hash(self, key):
        md5_hash = hashlib.md5(key.encode()).hexdigest()
        # md5_hash = int(md5_hash[:3], 16)
        if len(md5_hash) == 32:
            return md5_hash[:]
        else:
            raise Exception('Error in hash. Not getting all the character. Error when sorting probably')


    def update_ring(self):
        if len(self.RING_metadata) == 0: #Todo if it is call before filled
            raise Exception('ERROR in update_ring: Function should not be called if 0 nodes')
        else:
            self.RING_metadata = {k: self.RING_metadata[k] for k in sorted(self.RING_metadata)}
            # sorted_hash_list = sorted(list_hash, key=lambda x: int(x, 16)) # Todo use it
            previous_hash = list(self.RING_metadata.keys())[-1]
            for key, values in self.RING_metadata.items():
                if len(values) > 3:
                    values[-1] = previous_hash
                else:
                    values.append(previous_hash)
                previous_hash = key
            for data in self.kvs_data.values():
                data['previous_hash'] = self.RING_metadata[data['hash_key']][-1]

            # for index, hash_key in enumerate(list(self.RING_metadata.keys())):
            #     for server in self.kvs_data.values():
            #         if server['active'] is False:
            #             continue
            #         if server['hash_key'] == hash_key:
            #             server['previous_hash'] = list(self.RING_metadata.keys())[index - 1]
            #             continue


    def remove_node(self, kvdata):
        print('Remove node')
        if 'hash_key' in kvdata:
            if kvdata['hash_key'] in self.RING_metadata and len(self.RING_metadata) > 1:
                del self.RING_metadata[kvdata['hash_key']]
                self.update_ring()
                next_bigger = min(list(self.RING_metadata))
                for value in list(self.RING_metadata):
                    if value > kvdata['hash_key']:
                        if value < next_bigger:
                            next_bigger = value
                host = self.RING_metadata[next_bigger][0]
                port = self.RING_metadata[next_bigger][1]
                return f'send storage {host}:{port}'
            elif kvdata['hash_key'] in self.RING_metadata and len(self.RING_metadata) == 1:
                print('Error removing last node. Task not completed')
        else:
            print('KVSERVER has no hash')















