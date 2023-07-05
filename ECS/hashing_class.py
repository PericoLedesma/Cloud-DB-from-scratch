import hashlib


class ConsistentHashing:
    def __init__(self):
        self.RING_metadata = {}
        self.RING_replicas = {}
        self.kvs_shuttingdown = {}

    def hash(self, key):
        md5_hash = hashlib.md5(key.encode()).hexdigest()
        # md5_hash = int(md5_hash[:3], 16)
        if len(md5_hash) == 32:
            md5_hash = int(md5_hash[:3], 16)
            return md5_hash  # For testing, just take one byte. Easier to check
        else:
            raise Exception('Error in hash. Not getting all the character. Error when sorting probably')

    def update_ring_intervals(self, ecsprint):
        ecsprint(f'Updating the ring. Len {len(self.RING_metadata)}')

        if len(self.RING_metadata) != 0:
            # We update the ring intervals if there is any node
            self.RING_metadata = {k: self.RING_metadata[k] for k in sorted(self.RING_metadata)}
            previous_hash = list(self.RING_metadata.keys())[-1]
            for key, values in self.RING_metadata.items():
                values['from'] = previous_hash
                values['type'] = 'C'
                previous_hash = key

            if len(self.RING_metadata) > 2:
                # If more than 2 nodes we create the replicas structure
                print('REPLICA ACTIVATED')

                coordinator = list(self.RING_metadata.keys())[-1]
                replica1 = list(self.RING_metadata.keys())[-2]
                for key, values in self.RING_metadata.items():
                    R1_KEY = 'R1_' + str(coordinator)
                    self.RING_replicas[R1_KEY] = {'from': self.RING_metadata[coordinator]['from'],
                                                  'to_hash': self.RING_metadata[coordinator]['to_hash'],
                                                  'host': self.RING_metadata[replica1]['host'],
                                                  'port': self.RING_metadata[replica1]['port'],
                                                  'type': 'R1'}
                    R2_KEY = 'R2_' + str(coordinator)
                    self.RING_replicas[R2_KEY] = {'from': self.RING_metadata[coordinator]['from'],
                                                  'to_hash': self.RING_metadata[coordinator]['to_hash'],
                                                  'host': self.RING_metadata[key]['host'],
                                                  'port': self.RING_metadata[key]['port'],
                                                  'type': 'R2'}
                    coordinator = replica1
                    replica1 = key
                for key, values in self.RING_replicas.items():
                    print(key, '+', values)
            else:
                ecsprint(f'No replica nodes.')
        else:
            ecsprint(f'No nodes. Ring not updated.')


    def new_node(self, kvs_data, id, handle_json_REPLY, ecsprint):
        ecsprint(f'Adding new node')
        new_hash = self.hash(f'{kvs_data[id]["host"]}:{kvs_data[id]["port"]}')
        kvs_data[id]['to_hash'] = new_hash

        if len(self.RING_metadata) > 0:
            old_ring = {}
            for key, value in self.RING_metadata.items():
                old_ring[key] = [value['from'], value['to_hash']]

            self.RING_metadata[new_hash] = {'from': None,
                                            'to_hash': new_hash,
                                            'host': kvs_data[id]["host"],
                                            'port': kvs_data[id]["port"],
                                            'type_replica': 'C'}
            self.update_ring_intervals(ecsprint)
            for data in kvs_data.values():
                data['from'] = self.RING_metadata[data['to_hash']]['from']

            old_hashes = list(old_ring.keys())
            next_hash = min(old_hashes)
            old_hashes.sort()
            for key in old_hashes:
                if key > new_hash:
                    next_hash = key
                    break
            for key, value in kvs_data.items():
                if value['to_hash'] == next_hash:
                    data = {
                        'interval': [old_ring[next_hash][0], new_hash],
                        'responsable': f'{kvs_data[id]["host"]}:{kvs_data[id]["port"]}'
                    }
                    handle_json_REPLY(sock=value['sock'], request=f'arrange_ring', data=data)
                    break

        elif len(self.RING_metadata) == 0:
            self.RING_metadata[new_hash] = {'from': None,
                                            'to_hash': new_hash,
                                            'host': kvs_data[id]["host"],
                                            'port': kvs_data[id]["port"],
                                            'type_replica': 'C'}
            self.update_ring_intervals(ecsprint)
            for data in kvs_data.values():
                data['from'] = self.RING_metadata[data['to_hash']]['from']
        else:
            ecsprint('Error. Node not added. Check')


    def remove_node(self, kvs_data, id, sock, handle_json_REPLY, ecsprint):
        ecsprint(f'Removing node {kvs_data[id]["name"]}')
        # self.kvs_shuttingdown[kvs_data[id]['to_hash']] = self.RING_metadata[kvs_data[id]['to_hash']]
        del self.RING_metadata[kvs_data[id]['to_hash']]

        if len(self.RING_metadata) > 0:
            self.update_ring_intervals(ecsprint)

            new_hash = list(self.RING_metadata)
            new_hash.sort()
            next_bigger = min(new_hash)

            for hash in new_hash:
                if hash > kvs_data[id]['to_hash']:
                    next_bigger = hash
                    break

            for key, value in kvs_data.items():
                if value['to_hash'] == next_bigger:
                    data = {
                        'interval': [kvs_data[id]['from'], kvs_data[id]['to_hash']],
                        'responsable': f'{value["host"]}:{value["port"]}'
                    }
                    print('====== Removing node--> Sending reorganization')
                    handle_json_REPLY(sock=sock, request=f'arrange_ring', data=data)
                    break
        else:
            ecsprint(f'No other node to send the data')
            handle_json_REPLY(sock=sock, request=f'arrange_ring', data=None)
