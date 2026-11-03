import asyncio
import logging
import time
from collections import deque

from .config import Config
from .protocol import make_msg, verify, encode, decode, new_node_id, to_hex
from .security import ReplayGuard, RateLimiter, make_contexts

log = logging.getLogger('meshweaver')


class UDPProtocol(asyncio.DatagramProtocol):
    def __init__(self, on_data):
        self.on_data = on_data

    def datagram_received(self, data, addr):
        self.on_data(data, addr)


class Node:
    def __init__(self, config=None):
        self.cfg = config or Config()
        self.id = new_node_id()
        self.hex_id = to_hex(self.id)
        self.addr = (self.cfg.host, self.cfg.port)
        self.handlers = {}       # message type -> async function(msg, addr)
        self.pending = {}        # msg_id -> Future waiting for a reply
        self.on_contact = []     # callbacks run for every valid incoming message
        self.components = []     # DHT, Gossip, Tasks (started in this order)
        self.events = deque(maxlen=50)   # shown on the dashboard
        self.running_tasks = 0
        self.tls_server, self.tls_client = (
            make_contexts(self.cfg.tls)
            if self.cfg.tls
            else (None, None)
        )

        self.replay = ReplayGuard()
        self.limiter = RateLimiter()
        self.transport = None
        self.on('PING', self._on_ping)

    # ---- wiring helpers ----
    def attach(self, component):
        self.components.append(component)
        return component

    def on(self, type_, handler):
        self.handlers[type_] = handler

    def log_event(self, text):
        self.events.append(time.strftime('%H:%M:%S') + ' ' + text)

    # ---- lifecycle ----
    async def start(self):
        loop = asyncio.get_running_loop()
        self.transport, _ = await loop.create_datagram_endpoint(
            lambda: UDPProtocol(self._on_datagram),
            local_addr=(self.cfg.host, self.cfg.port))
        self.addr = tuple(self.transport.get_extra_info('sockname')[:2])
        for component in self.components:
            await component.start()

    async def stop(self):
        for component in reversed(self.components):
            await component.stop()
        if self.transport:
            self.transport.close()

    # ---- sending ----
    def send(self, addr, type_, body=None, reply_to=None):
        msg = make_msg(type_, self.hex_id, body or {}, self.cfg.key, reply_to)
        self.transport.sendto(encode(msg), tuple(addr))
        return msg['msg_id']

    async def request(self, addr, type_, body=None, timeout=2.0):
        # returns the reply message, or None if nobody answered in time
        fut = asyncio.get_running_loop().create_future()
        msg_id = self.send(addr, type_, body)
        self.pending[msg_id] = fut   # safe: no await happened since send
        try:
            return await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            self.pending.pop(msg_id, None)

    async def ping(self, addr):
        start = time.perf_counter()
        reply = await self.request(addr, 'PING')
        return None if reply is None else (time.perf_counter() - start) * 1000

    # ---- receiving ----
    def _on_datagram(self, data, addr):

        if not self.limiter.allow(addr[0]):
            return

        msg = decode(data)

        if msg is None or not verify(msg, self.cfg.key):
            return

        if self.replay.seen(msg.get('msg_id')):
            return

        msg = decode(data)
        if msg is None or not verify(msg, self.cfg.key):
            return                          # drop garbage and forgeries silently
        try:
            sender = int(msg['sender'], 16)
        except (KeyError, ValueError, TypeError):
            return
        for callback in self.on_contact:
            callback(sender, addr)
        fut = self.pending.get(msg.get('reply_to'))
        if fut is not None:
            if not fut.done():
                fut.set_result(msg)
            return
        handler = self.handlers.get(msg.get('type'))
        if handler:
            asyncio.get_running_loop().create_task(self._run_handler(handler, msg, addr))

    async def _run_handler(self, handler, msg, addr):
        try:
            result = await handler(msg, addr)
        except Exception:
            log.exception('handler for %s failed', msg.get('type'))
            return
        if result is not None:              # handlers return (reply_type, body)
            type_, body = result
            self.send(addr, type_, body, reply_to=msg['msg_id'])

    async def _on_ping(self, msg, addr):
        return 'PONG', {}
