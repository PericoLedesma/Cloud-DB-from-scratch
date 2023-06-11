import hashlib

class ConsistentHashing:
    def __init__(self, kvdata_part, n):
        self.num_virtual_nodes = n
        self.hash_ring = []
        self.virtual_nodes = {}# Dictionary to store partition assignments
        # print('Getting keys of kvservers..')

        # Assign each address to a partition in the hashing ring
        for server in kvdata_part.values():
            key = f"{server['host']}:{server['port']}"

            hash_key, identifier = self.hash(key)

            server['hash_int'] = identifier
            server['hash_key'] = hash_key


            self.hash_ring.append(hash_key)
            self.virtual_nodes[hash_key] = (server['host'], server['port'])

        self.hash_ring.sort()

        # print(f'Hash_key by order[{self.hash_ring}]')


    def find_server_for_key(self,kvdata_part, key):
        identifier, hash_key = self.hash(key)
        partition = kvdata_part[0]['hash_key']

        for server in kvdata_part.values():
            if server['hash_key'] <= hash_key:
                partition = server['id']
        return partition


    def hash(self, key):
        # Compute MD5 hash
        md5_hash = hashlib.md5(key.encode()).hexdigest()

        # Convert selected portion to integer
        identifier = int(md5_hash, 16)

        # print(f'Key [{key}] |Hash {md5_hash}|Identifier: {identifier}')

        return md5_hash, identifier









