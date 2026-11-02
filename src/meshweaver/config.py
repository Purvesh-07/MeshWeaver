from dataclasses import dataclass, field


@dataclass
class Config:
    host: str = '127.0.0.1'          # address to bind AND advertise to peers
    port: int = 9000                 # UDP and TCP use the same number
    bootstrap: list = field(default_factory=list)   # [('127.0.0.1', 9000)]
    key: bytes = b'dev-cluster-key-change-me'       # shared secret for signatures
    k: int = 20                      # bucket size and lookup width
    alpha: int = 3                   # parallel queries during a lookup
    gossip_interval: float = 5.0     # seconds
    gossip_fanout: int = 3           # peers contacted per gossip round
    heartbeat_interval: float = 2.0  # worker -> submitter
    heartbeat_timeout: float = 6.0   # submitter gives up after this silence
    task_slots: int = 2              # tasks one node runs at the same time
    max_frame: int = 16 * 1024 * 1024    # biggest TCP message accepted
    tls: dict = None                 # {'ca': path, 'cert': path, 'key': path}
