import hashlib
#
# class ConsistentHashing:
#     def __init__(self, num_virtual_nodes):
#         self.num_virtual_nodes = num_virtual_nodes
#         self.hash_ring = []
#         self.virtual_nodes = {}
#
#         my_dict = {('id', 'kserver1'): {'vnodes':[list]}}
#
#         for num in range(0, num + 1):
#             key = ('id', f'kvserver{num}')
#             map_to_ring()
#             value = ('nodes',)))
#
#
#             self.server_names.append(f"")
#
#     def add_node(self, node_id):
#         for i in range(self.num_virtual_nodes):
#             virtual_node_id = f"{node_id}-vn{i}"
#             hash_value = self._hash(virtual_node_id)
#             self.hash_ring.append(hash_value)
#             self.virtual_nodes[hash_value] = node_id
#
#         self.hash_ring.sort()
#
#     def _hash(self, key):
#         return int(hashlib.md5(key.encode()).hexdigest(), 16)
#
#     def get_node(self, key):
#         if not self.hash_ring:
#             return None
#
#         hash_value = self._hash(key)
#         for node_hash in self.hash_ring:
#             if node_hash >= hash_value:
#                 return self.virtual_nodes[node_hash]
#
#         return self.virtual_nodes[self.hash_ring[0]]
#
#
#
#     def md5_hash_string(self, string):
#         # Create a hash object using the MD5 algorithm
#         hash_object = hashlib.md5()
#
#         # Convert the string to bytes and update the hash object
#         hash_object.update(string.encode('utf-8'))
#
#         # Get the hexadecimal representation of the hash digest
#         hash_value = hash_object.hexdigest()
#
#         return hash_value
#
#
#     def map_to_ring(self, server_id, total_servers):
#         # Hash the server identifier using a hash function (e.g., MD5)
#         hash_value = hashlib.md5(server_id.encode()).hexdigest()
#
#         # Convert the hash value to an integer
#         hash_int = int(hash_value, 16)
#
#         # Calculate the position on the ring based on the total number of servers
#         ring_position = (hash_int * 360) // total_servers
#
#         return ring_position
#
#






