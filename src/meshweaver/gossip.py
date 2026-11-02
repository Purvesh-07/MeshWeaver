import asyncio
import random
import time

from .metrics import Metrics

FIELDS = {'seq', 'score', 'cpu', 'mem', 'addr', 'tasks'}


class Gossip:
    def __init__(self, node, dht):
        self.node, self.dht, self.cfg = node, dht, node.cfg
        self.metrics = Metrics()
        self.seq = 0
        self.view = {}           # node hex id -> entry (what we believe about everyone)
        self.dead = set()        # ids proven dead (ids are random per run, so this is safe)
        self._task = None
        node.on('GOSSIP', self._on_gossip)

    async def start(self):
        self._task = asyncio.create_task(self._loop())

    async def stop(self):
        if self._task:
            self._task.cancel()

    def _local_entry(self):
        self.seq += 1
        cpu = self.metrics.cpu_percent()
        mem = self.metrics.mem_percent()
        busy = 100.0 * min(1.0, self.node.running_tasks / max(1, self.cfg.task_slots))
        return {
            'seq': self.seq, 'cpu': round(cpu, 1), 'mem': round(mem, 1),
            'tasks': self.node.running_tasks, 'addr': list(self.node.addr),
            'score': round(0.6 * cpu + 0.2 * mem + 0.2 * busy, 1),
        }

    def _payload(self):
        newest = sorted(self.view.items(), key=lambda kv: kv[1]['seen'], reverse=True)[:30]
        return {nid: {k: v for k, v in e.items() if k != 'seen'} for nid, e in newest}

    def _merge(self, entries):
        now = time.monotonic()
        for nid, e in entries.items():
            if nid == self.node.hex_id or nid in self.dead:
                continue
            if not isinstance(e, dict) or not FIELDS <= e.keys():
                continue                       # ignore malformed entries
            old = self.view.get(nid)
            if old is None or e['seq'] > old['seq']:
                self.view[nid] = dict(e, seen=now)

    def _expire(self):
        limit = time.monotonic() - 3 * self.cfg.gossip_interval
        silent = [n for n, e in self.view.items() if e['seen'] < limit and n != self.node.hex_id]
        for nid in silent:
            del self.view[nid]
            self.node.log_event('dropped ' + nid[:8] + ' (silent)')

    def mark_dead(self, nid):
        self.dead.add(nid)
        self.view.pop(nid, None)

    def ranked(self, excluded=(), inflight=None):
        # best node first; inflight = tasks we just sent that the node has not reported yet
        inflight = inflight or {}
        usable = [(n, e) for n, e in self.view.items() if n not in excluded]
        return sorted(usable, key=lambda ne: (ne[1]['score'] + 15 * inflight.get(ne[0], 0),
                                              ne[1]['tasks'], ne[0]))

    async def _on_gossip(self, msg, addr):
        self._merge(msg['body'].get('entries', {}))

    async def _loop(self):
        while True:
            self.view[self.node.hex_id] = dict(self._local_entry(), seen=time.monotonic())
            self._expire()
            peers = self.dht.table.all()
            for nid, addr in random.sample(peers, min(self.cfg.gossip_fanout, len(peers))):
                self.node.send(addr, 'GOSSIP', {'entries': self._payload()})
            await asyncio.sleep(self.cfg.gossip_interval + random.uniform(0, 0.5))
