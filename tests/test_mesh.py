import asyncio

import pytest

from meshweaver import create_node, Config
from meshweaver.node import Node
from meshweaver.tasks import RemoteTaskError


async def test_ping():
    a = Node(Config(port=19000))
    b = Node(Config(port=19001))
    await a.start()
    await b.start()
    assert await a.ping(('127.0.0.1', 19001)) is not None
    assert await a.ping(('127.0.0.1', 19999)) is None
    await a.stop()
    await b.stop()


async def test_mesh_forms_from_one_seed():
    nodes = []
    for i in range(6):
        boot = [('127.0.0.1', 19100)] if i else []
        node = create_node(Config(port=19100 + i, bootstrap=boot))
        await node.start()
        nodes.append(node)
    await asyncio.sleep(2)
    assert all(len(n.dht.table.all()) >= 4 for n in nodes)
    for n in nodes:
        await n.stop()


async def test_remote_task_and_remote_error():
    a = create_node(Config(port=19200))
    b = create_node(Config(port=19201, bootstrap=[('127.0.0.1', 19200)]))
    await a.start()
    await b.start()
    assert await a.tasks.run_remote(b.addr, lambda x: x * 2, (21,), {}, 10) == 42
    with pytest.raises(RemoteTaskError):
        await a.tasks.run_remote(b.addr, lambda: 1 / 0, (), {}, 10)
    await a.stop()
    await b.stop()
