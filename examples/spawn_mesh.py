import asyncio
from meshweaver.config import Config
from meshweaver.node import Node
from meshweaver.dht import DHT


async def main():
    nodes = []
    for i in range(10):
        boot = [('127.0.0.1', 9000)] if i else []     # only the seed address, no other IPs
        n = Node(Config(port=9000 + i, bootstrap=boot))
        n.dht = n.attach(DHT(n))
        await n.start()
        nodes.append(n)
    await asyncio.sleep(3)
    for n in nodes:
        print(n.hex_id[:8], 'knows', len(n.dht.table.all()), 'peers')
    await nodes[3].dht.put('hello', 'world')
    print('value found from node 8:', await nodes[8].dht.get('hello'))
    ok = all(len(n.dht.table.all()) >= 8 for n in nodes)
    print('AUDIT PASSED' if ok else 'AUDIT FAILED')
    for n in nodes:
        await n.stop()


asyncio.run(main())
