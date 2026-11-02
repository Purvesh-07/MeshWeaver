from .config import Config
from .node import Node
from .dht import DHT
from .gossip import Gossip
from .tasks import Tasks

__all__ = ['Config', 'Node', 'create_node']


def create_node(config=None):
    # builds a complete MeshWeaver node: networking + DHT + gossip + tasks
    node = Node(config)
    node.dht = node.attach(DHT(node))
    node.gossip = node.attach(Gossip(node, node.dht))
    node.tasks = node.attach(Tasks(node, node.gossip, node.dht))
    node.submit = lambda *a, **k: node.tasks.submit(*a, **k)    # defined in Stage 6
    return node
