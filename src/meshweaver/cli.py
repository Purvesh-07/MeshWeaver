# CLI entrypoint for MeshWeaver

import argparse
import asyncio
import importlib.util
import sys

import cloudpickle

from . import create_node, Config
from .dashboard import run_dashboard


def parse_addr(text):
    host, port = text.rsplit(':', 1)
    return (host, int(port))


def build_config(a):
    tls = None
    if a.ca and a.cert and a.tls_key:
        tls = {'ca': a.ca, 'cert': a.cert, 'key': a.tls_key}
    return Config(host=a.host, port=a.port, bootstrap=[parse_addr(b) for b in a.bootstrap],
                  key=a.key.encode(), tls=tls)


async def cmd_start(a):
    node = create_node(build_config(a))
    await node.start()
    try:
        if a.headless:
            print('node', node.hex_id[:8], 'listening on', node.addr, flush=True)
            await asyncio.Event().wait()
        else:
            await run_dashboard(node)
    finally:
        await node.stop()


async def cmd_submit(a):
    path, func_name = a.target.rsplit(':', 1)
    spec = importlib.util.spec_from_file_location('user_job', path)
    module = importlib.util.module_from_spec(spec)
    sys.modules['user_job'] = module
    spec.loader.exec_module(module)
    cloudpickle.register_pickle_by_value(module)      # ship the code itself, not just its name
    func = getattr(module, func_name)

    cfg = build_config(a)
    cfg.port = 0                                      # let the OS choose a free port
    node = create_node(cfg)
    await node.start()
    await asyncio.sleep(a.wait)                       # wait for gossip to learn the mesh
    args = [int(x) if x.lstrip('-').isdigit() else x for x in a.args]
    print('result:', await node.submit(func, *args))
    await node.stop()


def main():
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument('--host', default='127.0.0.1')
    common.add_argument('--port', type=int, default=9000)
    common.add_argument('--bootstrap', action='append', default=[], help='host:port (repeatable)')
    common.add_argument('--key', default='dev-cluster-key-change-me')
    common.add_argument('--ca')
    common.add_argument('--cert')
    common.add_argument('--tls-key')

    parser = argparse.ArgumentParser(prog='meshweaver')
    sub = parser.add_subparsers(dest='cmd', required=True)
    start = sub.add_parser('start', parents=[common], help='run a node')
    start.add_argument('--headless', action='store_true', help='no dashboard (for servers)')
    submit = sub.add_parser('submit', parents=[common], help='run file.py:function on the mesh')
    submit.add_argument('target')
    submit.add_argument('args', nargs='*')
    submit.add_argument('--wait', type=float, default=7.0)

    a = parser.parse_args()
    try:
        asyncio.run(cmd_start(a) if a.cmd == 'start' else cmd_submit(a))
    except KeyboardInterrupt:
        pass
