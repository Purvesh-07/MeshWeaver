import asyncio
from meshweaver.config import Config
from meshweaver.node import Node
from meshweaver.dht import DHT
from meshweaver.gossip import Gossip


async def main():
    nodes = []
    for i in range(6):
        boot = [('127.0.0.1', 9000)] if i else []
        n = Node(Config(port=9000 + i, bootstrap=boot, gossip_interval=1.0))   # fast for the demo
        n.dht = n.attach(DHT(n))
        n.gossip = n.attach(Gossip(n, n.dht))
        await n.start()
        nodes.append(n)
    await asyncio.sleep(6)
    for n in nodes:
        print(n.hex_id[:8], 'sees', len(n.gossip.view), 'nodes')
    print('best node right now:', nodes[0].gossip.ranked()[0][0][:8])
    for n in nodes:
        await n.stop()


asyncio.run(main())
