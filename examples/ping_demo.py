import asyncio
from meshweaver.config import Config
from meshweaver.node import Node


async def main():
    a = Node(Config(port=9000))
    b = Node(Config(port=9001))
    await a.start()
    await b.start()
    rtt = await a.ping(('127.0.0.1', 9001))
    print('ping rtt: %.2f ms' % rtt)
    print('ping to a dead port:', await a.ping(('127.0.0.1', 9999)))   # None after 2 s
    await a.stop()
    await b.stop()


asyncio.run(main())
