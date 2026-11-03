import asyncio

from meshweaver import create_node, Config


def tls(name):
    return {'ca': 'certs/ca.crt', 'cert': f'certs/{name}.crt', 'key': f'certs/{name}.key'}


async def main():
    a = create_node(Config(port=9000, tls=tls('node1')))
    b = create_node(Config(port=9001, bootstrap=[('127.0.0.1', 9000)], tls=tls('node2')))
    await a.start()
    await b.start()
    print('over mutual TLS:', await a.tasks.run_remote(b.addr, lambda: 'secure hello', (), {}, 10))

    rogue = create_node(Config(port=9002))            # no certificate
    await rogue.start()
    try:
        await rogue.tasks.run_remote(b.addr, lambda: 'evil', (), {}, 5)
        print('PROBLEM: rogue node got through')
    except Exception as exc:
        print('rogue node rejected:', type(exc).__name__)
    for n in (a, b, rogue):
        await n.stop()


if __name__ == '__main__':
    asyncio.run(main())
