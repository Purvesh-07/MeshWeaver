import asyncio
import subprocess
import sys

from meshweaver import create_node, Config


def slow(x):
    import time
    time.sleep(4)
    return x * 2


def start_node(port, boot=0):
    cmd = [sys.executable, 'examples/run_node.py', str(port)] + ([str(boot)] if boot else [])
    return subprocess.Popen(cmd)


async def main():
    procs = [start_node(9300)]
    await asyncio.sleep(1)
    procs += [start_node(p, 9300) for p in (9301, 9302, 9303)]
    me = create_node(Config(port=9310, bootstrap=[('127.0.0.1', 9300)]))
    await me.start()
    await asyncio.sleep(8)                         # let gossip spread
    jobs = [
        asyncio.create_task(
            me.submit(
                slow,
                i,
                retries=3,
                timeout=20
            )
        )
        for i in range(20)
    ]    
    await asyncio.sleep(2)
    procs[2].kill()
    procs[3].kill()
    print('killed two workers mid-task')
    results = await asyncio.gather(*jobs)
    print('all 20 correct:', sorted(results) == [i * 2 for i in range(20)])
    moved = sum(1 for r in me.tasks.records.values() if r['attempt'] > 1)
    print('tasks that had to be reassigned:', moved)
    for p in procs:
        p.kill()
    await me.stop()


if __name__ == '__main__':
    asyncio.run(main())
