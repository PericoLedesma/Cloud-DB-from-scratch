import hashlib
import json

class ConsistentHashing:
    def __init__(self, kv_data):

        self.num_virtual_nodes = len(kv_data)

        self.kv_data = kv_data
        self.virtual_nodes = {}

        # print('Getting keys of kvservers..')

        # Assign each address to a partition in the hashing ring
        for server in self.kv_data.values():
            hash_key = self.hash(f"{server['host']}:{server['port']}")
            server['hash_key'] = hash_key
            self.virtual_nodes[hash_key] = (server['host'], server['port'])




        # # Create a new dictionary with updated keys
        # self.virtual_nodes = {new_key: self.virtual_nodes[old_key] for old_key, new_key in
        #             zip(self.virtual_nodes.keys(),
        #                 ['f0000000000000000000000000000000',
        #                  '10000000000000000000000000000000',
        #                  'a0000000000000000000000000000000',
        #                  '20000000000000000000000000000000'])}
        #
        # for server in self.kv_data.values():
        #     for key, value in self.virtual_nodes.items():
        #         if server['port'] == value[1]:
        #             server['hash_key'] = key
        #             break

        self.hash_intervals()
        # formatted_dict = json.dumps(self.kv_data, indent=4)
        # print(formatted_dict)


    def hash_intervals(self):
        self.virtual_nodes = {k: self.virtual_nodes[k] for k in sorted(self.virtual_nodes)}
        previous_hash = list(self.virtual_nodes.keys())[-1]
        for node in self.virtual_nodes:
            for server in self.kv_data.values():
                if server['hash_key'] == node:
                    server['previous_hash'] = previous_hash
                    previous_hash = server['hash_key']



    def find_server_for_key(self,kvdata_part, key):
        identifier, hash_key = self.hash(key)
        partition = kvdata_part[0]['hash_key']

        for server in kvdata_part.values():
            if server['hash_key'] <= hash_key:
                partition = server['id']
        return partition


    def hash(self, key):
        md5_hash = hashlib.md5(key.encode()).hexdigest()
        # identifier = int(md5_hash[:2], 16)

        return md5_hash









