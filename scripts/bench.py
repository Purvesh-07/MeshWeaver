import asyncio
import time

from meshweaver import create_node, Config


async def main():
    nodes = []
    start = time.perf_counter()
    for i in range(10):
        boots = [('127.0.0.1', 9400)] if i else []
        node = create_node(Config(port=9400 + i, bootstrap=boots, gossip_interval=1.0))
        await node.start()
        nodes.append(node)
    while not all(len(n.gossip.view) == 10 for n in nodes):
        await asyncio.sleep(0.2)
    print('10 nodes discovered and gossip converged in %.1f s' % (time.perf_counter() - start))
    t0 = time.perf_counter()
    await asyncio.gather(*(nodes[0].submit(lambda: 1) for _ in range(100)))
    print('100 tiny tasks: %.1f tasks per second' % (100 / (time.perf_counter() - t0)))
    for n in nodes:
        await n.stop()


if __name__ == '__main__':
    asyncio.run(main())
