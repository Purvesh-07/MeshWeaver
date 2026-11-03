import asyncio
import time

from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

STATE_COLORS = {'PENDING': 'yellow', 'RUNNING': 'cyan', 'DONE': 'green',
                'FAILED': 'red', 'REASSIGNED': 'magenta'}


def bar(pct, width=10):
    filled = int(max(0.0, min(100.0, pct)) / 100 * width)
    return '#' * filled + '.' * (width - filled)


def render(node):
    peers = Table(expand=True)
    for col in ('Node', 'Address', 'CPU', 'RAM %', 'Tasks', 'Score'):
        peers.add_column(col)
    for nid, e in sorted(node.gossip.view.items(), key=lambda kv: kv[1]['score']):
        name = nid[:8] + (' (me)' if nid == node.hex_id else '')
        where = '%s:%s' % (e['addr'][0], e['addr'][1])
        cpu, mem = e['cpu'], e['mem']
        peers.add_row(name, where, '%s %5.1f%%' % (bar(cpu), cpu), '%.0f' % mem,
                      str(e['tasks']), '%.1f' % e['score'])

    tasks = Table(expand=True)
    for col in ('Task', 'Function', 'Node', 'State', 'Try', 'Age'):
        tasks.add_column(col)
    for rec in list(node.tasks.records.values())[-10:]:
        color = STATE_COLORS.get(rec['state'], 'white')
        state = '[%s]%s[/%s]' % (color, rec['state'], color)
        tasks.add_row(rec['id'][:6], rec['name'], (rec['assignee'] or '-')[:8], state,
                      str(rec['attempt']), '%.0fs' % (time.time() - rec['started']))

    events = chr(10).join(list(node.events)[-8:]) or 'no events yet'
    title = 'MeshWeaver node %s - %d nodes in mesh' % (node.hex_id[:8], len(node.gossip.view))
    return Group(Panel(peers, title=title),
                 Panel(tasks, title='Tasks submitted from this node'),
                 Panel(events, title='Events'))


async def run_dashboard(node):
    with Live(render(node), refresh_per_second=4, screen=True) as live:
        while True:
            live.update(render(node))
            await asyncio.sleep(0.5)
