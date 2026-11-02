import asyncio
import hashlib
import logging
import struct
import time
import traceback
import uuid
from collections import Counter
from concurrent.futures import ProcessPoolExecutor

import cloudpickle

from .protocol import make_msg, verify, encode, decode

log = logging.getLogger('meshweaver')


class RemoteTaskError(Exception):
    pass


class SecurityError(Exception):
    pass


def sha(data):
    return hashlib.sha256(data).hexdigest()


async def send_frame(writer, data):
    writer.write(struct.pack('!I', len(data)) + data)
    await writer.drain()


async def read_frame(reader, max_size):
    (size,) = struct.unpack('!I', await reader.readexactly(4))
    if size > max_size:
        raise ValueError('frame too large')
    return await reader.readexactly(size)


def _run_payload(payload):
    # this function runs INSIDE a worker process
    try:
        func, args, kwargs = cloudpickle.loads(payload)
        return True, cloudpickle.dumps(func(*args, **kwargs))
    except BaseException:
        return False, traceback.format_exc().encode()


class Tasks:
    def __init__(self, node, gossip, dht):
        self.node, self.gossip, self.dht, self.cfg = node, gossip, dht, node.cfg
        self.pool = None
        self.server = None
        self.records = {}            # task_id -> dict, used by the dashboard
        self.inflight = Counter()    # node id -> tasks we sent and are still waiting for
        node.on('TASK_OFFER', self._on_offer)      # used from Stage 6

    async def start(self):
        self.pool = ProcessPoolExecutor(max_workers=self.cfg.task_slots)
        self.server = await asyncio.start_server(
            self._on_connection, self.node.addr[0], self.node.addr[1], ssl=self.node.tls_server)

    async def stop(self):
        if self.server:
            self.server.close()
        if self.pool:
            self.pool.shutdown(wait=False, cancel_futures=True)

    # ================= worker side: someone sends us a task =================
    async def _on_connection(self, reader, writer):
        try:
            env = decode(await read_frame(reader, self.cfg.max_frame))
            if env is None or not verify(env, self.cfg.key) or env.get('type') != 'TASK_SUBMIT':
                return                                  # not from our cluster: hang up
            payload = await read_frame(reader, self.cfg.max_frame)
            if sha(payload) != env['body']['payload_hash']:
                return                                  # payload does not match the signed hash
            await self._execute(writer, env['body'], payload)
        except (asyncio.IncompleteReadError, ConnectionError, ValueError, KeyError):
            pass
        except Exception:
            log.exception('task connection failed')
        finally:
            writer.close()

    async def _execute(self, writer, body, payload):
        node = self.node
        loop = asyncio.get_running_loop()
        node.running_tasks += 1
        try:
            fut = loop.run_in_executor(self.pool, _run_payload, payload)
            deadline = loop.time() + float(body.get('timeout', 30))
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    fut.cancel()
                    ok, data = False, b'TimeoutError: task exceeded its time limit'
                    break
                done, _ = await asyncio.wait({fut}, timeout=min(self.cfg.heartbeat_interval, remaining))
                if done:
                    try:
                        ok, data = fut.result()
                    except Exception as exc:
                        ok, data = False, repr(exc).encode()
                    break
                beat = make_msg('HEARTBEAT', node.hex_id, {'task_id': body['task_id']}, self.cfg.key)
                await send_frame(writer, encode(beat))      # still working!
        finally:
            node.running_tasks -= 1
        result = make_msg('TASK_RESULT', node.hex_id,
                          {'task_id': body['task_id'], 'ok': ok, 'payload_hash': sha(data)}, self.cfg.key)
        await send_frame(writer, encode(result))
        await send_frame(writer, data)

    async def _on_offer(self, msg, addr):
        # answer: can you take one more task? (a small queue is allowed)
        busy = self.node.running_tasks >= self.cfg.task_slots * 2
        return ('TASK_REJECT' if busy else 'TASK_ACCEPT'), {}

    # ================= submitter side: we send a task =================
    async def run_remote(self, addr, func, args, kwargs, timeout=30.0, task_id=None):
        payload = cloudpickle.dumps((func, args, kwargs))
        task_id = task_id or uuid.uuid4().hex
        header = make_msg('TASK_SUBMIT', self.node.hex_id,
                          {'task_id': task_id, 'payload_hash': sha(payload), 'timeout': timeout},
                          self.cfg.key)
        reader, writer = await asyncio.open_connection(addr[0], addr[1], ssl=self.node.tls_client)
        try:
            await send_frame(writer, encode(header))
            await send_frame(writer, payload)
            while True:
                # a heartbeat or the result must arrive within heartbeat_timeout seconds
                env = decode(await asyncio.wait_for(read_frame(reader, self.cfg.max_frame),
                                                    self.cfg.heartbeat_timeout))
                if env is None or not verify(env, self.cfg.key):
                    raise SecurityError('bad signature from worker')
                if env['type'] == 'HEARTBEAT':
                    continue
                if env['type'] == 'TASK_RESULT':
                    data = await asyncio.wait_for(read_frame(reader, self.cfg.max_frame),
                                                  self.cfg.heartbeat_timeout)
                    break
                raise SecurityError('unexpected frame: ' + str(env.get('type')))
        finally:
            writer.close()
        if sha(data) != env['body']['payload_hash']:
            raise SecurityError('result hash mismatch')
        if env['body']['ok']:
            return cloudpickle.loads(data)           # safe: signature and hash were checked
        raise RemoteTaskError(data.decode(errors='replace'))
