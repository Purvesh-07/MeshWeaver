import asyncio
import math

from meshweaver import create_node, Config


def heavy(n):
    return sum(math.sqrt(i) for i in range(10_000_000))

async def main():
    node = create_node(
        Config(
            port=9100,
            bootstrap=[
                ('127.0.0.1', 9000)
            ]
        )
    )

    await node.start()

    await asyncio.sleep(7)

    results = await asyncio.gather(
        *(
            node.submit(
                heavy,
                2_000_000
            )
            for _ in range(12)
        )
    )

    print(
        'got',
        len(results),
        'results'
    )

    print(dict(node.tasks.inflight))

    await node.stop()


if __name__ == "__main__":
    asyncio.run(main())