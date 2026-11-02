import asyncio
import sys

from meshweaver import create_node, Config


async def main(port, boot):
    boots = [('127.0.0.1', boot)] if boot else []
    node = create_node(Config(port=port, bootstrap=boots))
    await node.start()
    print('node', node.hex_id[:8], 'listening on', node.addr, flush=True)
    await asyncio.Event().wait()


if __name__ == '__main__':      # this guard is REQUIRED because of the process pool
    port = int(sys.argv[1])
    boot = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    asyncio.run(main(port, boot))
