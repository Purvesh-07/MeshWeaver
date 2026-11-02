import asyncio

from meshweaver import create_node, Config


async def main():
    a = create_node(Config(port=9000))
    b = create_node(Config(port=9001, bootstrap=[('127.0.0.1', 9000)]))
    await a.start()
    await b.start()

    def slow_square(x):                  # defined right here, never installed on B
        import time
        time.sleep(1)
        return x * x

    print('result from B:', await a.tasks.run_remote(b.addr, slow_square, (12,), {}, 10))

    def broken():
        return 1 / 0

    try:
        await a.tasks.run_remote(b.addr, broken, (), {}, 10)
    except Exception as exc:
        print('remote error was reported:', str(exc).splitlines()[-1])

    def make_job(scale):
        def job(n):
            import math
            return round(scale * sum(math.sin(i) ** 2 for i in range(n)), 3)
        return job

    job = make_job(2.5)

    remote = await a.tasks.run_remote(
        b.addr,
        job,
        (200000,),
        {},
        30
    )

    assert remote == job(200000)

    print('closure result matches local execution:', remote)

    await a.stop()
    await b.stop()


if __name__ == '__main__':
    asyncio.run(main())
