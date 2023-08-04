import hashlib


class ConsistentHashing:
    def __init__(self, ecsprint):
        self.ecsprint = ecsprint
        self.ring_coordinators = {}
        self.complete_ring = []
        self.ring_replicas = []

    def hash(self, key):
        md5_hash = hashlib.md5(key.encode()).hexdigest()
        # md5_hash = int(md5_hash[:3], 16)
        if len(md5_hash) == 32:
            # md5_hash = int(md5_hash[:3], 16) #todo CAREFUL
            return md5_hash  # For testing, just take one byte. Easier to check
        else:
            raise Exception('Ring > Error in hash. Not getting all the character. Error when sorting probably')

    def update_ring_intervals(self):
        self.ecsprint(f'Ring > Updating the ring. Num nodes: {len(self.ring_coordinators)}')
        if len(self.ring_coordinators) != 0:
            # We update the ring intervals if there is any node
            self.ring_coordinators = {k: self.ring_coordinators[k] for k in sorted(self.ring_coordinators)}
            previous_hash = list(self.ring_coordinators.keys())[-1]
            for key, values in self.ring_coordinators.items():
                values['from'] = previous_hash
                values['type'] = 'C'
                previous_hash = key

            self.ring_replicas = []

            if len(self.ring_coordinators) > 2:
                replica1 = list(self.ring_coordinators.keys())[-1]
                replica2 = list(self.ring_coordinators.keys())[-2]
                for key, values in self.ring_coordinators.items():
                    # self.ecsprint(f'\t\t C {key}| R1 {replica1} | R2 {replica2}')
                    self.ring_replicas.append({'from': self.ring_coordinators[key]['from'],
                                               'to_hash': self.ring_coordinators[key]['to_hash'],
                                               'host': self.ring_coordinators[replica1]['host'],
                                               'port': self.ring_coordinators[replica1]['port'],
                                               'type': 'R1'})
                    self.ring_replicas.append({'from': self.ring_coordinators[key]['from'],
                                               'to_hash': self.ring_coordinators[key]['to_hash'],
                                               'host': self.ring_coordinators[replica2]['host'],
                                               'port': self.ring_coordinators[replica2]['port'],
                                               'type': 'R2'})
                    replica2 = replica1
                    replica1 = key
                # if len(list(self.ring_coordinators.values())) == (len(self.ring_replicas) / 2):
                #     self.ecsprint(
                #         f'Ring > Successfully created replicas |C = {len(list(self.ring_coordinators.values()))}/ R = {len(self.ring_replicas)}')
                # else:
                #     self.ecsprint(
                #         f'Ring > Error C*2!=R |C = {len(list(self.ring_coordinators.values()))}/ R = {len(self.ring_replicas)}')
                self.complete_ring = list(self.ring_coordinators.values()) + self.ring_replicas
            else:
                self.complete_ring = list(self.ring_coordinators.values())
                self.ecsprint(f'Ring > No replica nodes.')
        else:
            self.ecsprint(f'Ring > No nodes. Ring not updated.')

    def new_node(self, kvs_data, id, handle_json_REPLY):
        self.ecsprint(f'Ring > ++ Adding new node')
        new_hash = self.hash(f'{kvs_data[id]["host"]}:{kvs_data[id]["port"]}')
        kvs_data[id]['to_hash'] = new_hash

        if len(self.ring_coordinators) > 0:
            old_ring = {}
            for key, value in self.ring_coordinators.items():
                old_ring[key] = [value['from'], value['to_hash']]

            self.ring_coordinators[new_hash] = {'from': None,
                                                'to_hash': new_hash,
                                                'host': kvs_data[id]["host"],
                                                'port': kvs_data[id]["port"],
                                                'type': 'C'}
            self.update_ring_intervals()
            for data in kvs_data.values():
                data['from'] = self.ring_coordinators[data['to_hash']]['from']
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

        elif len(self.ring_coordinators) == 0:
            self.ring_coordinators[new_hash] = {'from': None,
                                                'to_hash': new_hash,
                                                'host': kvs_data[id]["host"],
                                                'port': kvs_data[id]["port"],
                                                'type': 'C'}
            self.update_ring_intervals()
            for data in kvs_data.values():
                data['from'] = self.ring_coordinators[data['to_hash']]['from']
        else:
            self.ecsprint('Error. Node not added. Check')

    def remove_node(self, kvs_data, id, sock, handle_json_REPLY):
        self.ecsprint(f'Ring > -- Removing node {kvs_data[id]["name"]}')
        del self.ring_coordinators[kvs_data[id]['to_hash']]

        if len(self.ring_coordinators) > 0:
            self.update_ring_intervals()

            new_hash = list(self.ring_coordinators)
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
                    self.ecsprint(f'====== Removing node--> Sending reorganization')
                    handle_json_REPLY(sock=sock, request=f'arrange_ring', data=data)
                    break
        else:
            self.ecsprint(f'No other node to send the data')
            handle_json_REPLY(sock=sock, request=f'arrange_ring', data=None)
