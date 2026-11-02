import asyncio
import hashlib
import random
from collections import OrderedDict

from .protocol import to_hex


class RoutingTable:
    def __init__(self, my_id, k=20):
        self.my_id = my_id
        self.k = k
        self.buckets = [OrderedDict() for _ in range(160)]

    def _bucket(self, nid):
        return self.buckets[(self.my_id ^ nid).bit_length() - 1]

    def touch(self, nid, addr):
        # remember a peer; returns True if it is new to us
        if nid == self.my_id:
            return False
        bucket = self._bucket(nid)
        if nid in bucket:
            bucket[nid] = addr
            bucket.move_to_end(nid)       # most recently seen goes last
            return False
        if len(bucket) < self.k:
            bucket[nid] = addr
            return True
        return False    # full bucket: real Kademlia pings the oldest entry first

    def remove(self, nid):
        if nid != self.my_id:
            self._bucket(nid).pop(nid, None)

    def all(self):
        return [(nid, addr) for b in self.buckets for nid, addr in b.items()]

    def addr_of(self, nid):
        if nid == self.my_id:
            return None
        return self._bucket(nid).get(nid)

    def closest(self, target, n=20):
        return sorted(self.all(), key=lambda c: c[0] ^ target)[:n]


class DHT:
    def __init__(self, node):
        self.node = node
        self.cfg = node.cfg
        self.table = RoutingTable(node.id, node.cfg.k)
        self.store = {}              # small key/value store held by this node
        self._task = None
        node.on_contact.append(self._contact)
        node.on('FIND_NODE', self._on_find_node)
        node.on('STORE', self._on_store)
        node.on('FIND_VALUE', self._on_find_value)

    # ---- lifecycle ----
    async def start(self):
        await self.bootstrap()
        self._task = asyncio.create_task(self._refresh_loop())

    async def stop(self):
        if self._task:
            self._task.cancel()

    async def bootstrap(self):
        for addr in self.cfg.bootstrap:
            if tuple(addr) == tuple(self.node.addr):
                continue                  # that is us
            if await self.node.ping(addr) is not None:
                await self.lookup(self.node.id)    # fills our table, announces us
                return True
        return not self.cfg.bootstrap     # a seed node has nobody to contact

    async def _refresh_loop(self):
        while True:
            await asyncio.sleep(15)
            await self.lookup(random.getrandbits(160))   # keeps buckets fresh

    def _contact(self, nid, addr):
        if self.table.touch(nid, tuple(addr)):
            self.node.log_event('peer joined ' + to_hex(nid)[:8])

    # ---- the iterative lookup ----
    async def lookup(self, target):
        shortlist = dict(self.table.closest(target, self.cfg.k))
        queried = set()
        while True:
            best = sorted(shortlist, key=lambda n: n ^ target)[:self.cfg.k]
            batch = [n for n in best if n not in queried][:self.cfg.alpha]
            if not batch:
                break                     # the k closest have all been asked
            replies = await asyncio.gather(*(
                self.node.request(shortlist[n], 'FIND_NODE', {'target': to_hex(target)})
                for n in batch))
            for nid, reply in zip(batch, replies):
                queried.add(nid)
                if reply is None:         # no answer: treat the peer as gone
                    self.table.remove(nid)
                    shortlist.pop(nid, None)
                    continue
                for entry in reply['body'].get('nodes', []):
                    try:
                        hid, host, port = entry
                        found = int(hid, 16)
                    except (ValueError, TypeError):
                        continue          # ignore malformed entries
                    if found != self.node.id and found not in shortlist:
                        shortlist[found] = (host, port)
        return sorted(shortlist, key=lambda n: n ^ target)[:self.cfg.k]

    # ---- handlers other nodes call ----
    async def _on_find_node(self, msg, addr):
        target = int(msg['body']['target'], 16)
        nodes = [[to_hex(n), a[0], a[1]] for n, a in self.table.closest(target, self.cfg.k)]
        return 'NODES', {'nodes': nodes}

    async def _on_store(self, msg, addr):
        self.store[msg['body']['key']] = msg['body']['value']

    async def _on_find_value(self, msg, addr):
        key = msg['body']['key']
        return 'VALUE', {'found': key in self.store, 'value': self.store.get(key)}

    # ---- tiny distributed storage (network state without a database) ----
    def _key_id(self, key):
        return int(hashlib.sha1(key.encode()).hexdigest(), 16)

    async def put(self, key, value):
        self.store[key] = value
        for nid in (await self.lookup(self._key_id(key)))[:3]:     # 3 replicas
            addr = self.table.addr_of(nid)
            if addr:
                self.node.send(addr, 'STORE', {'key': key, 'value': value})

    async def get(self, key):
        if key in self.store:
            return self.store[key]
        for nid in await self.lookup(self._key_id(key)):
            addr = self.table.addr_of(nid)
            reply = addr and await self.node.request(addr, 'FIND_VALUE', {'key': key})
            if reply and reply['body'].get('found'):
                return reply['body']['value']
        return None
