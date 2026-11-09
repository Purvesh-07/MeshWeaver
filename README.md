\# MeshWeaver



Zero-infrastructure decentralized Python task queue.



\## Features



\- Peer-to-peer architecture

\- Kademlia DHT

\- Gossip load balancing

\- Remote Python execution

\- Fault tolerance

\- Mutual TLS security



\## Installation



pip install meshweaver



\## Quick Start



meshweaver start --port 9000



meshweaver start --port 9001 --bootstrap 127.0.0.1:9000



meshweaver submit jobs.py:heavy 3000000 --bootstrap 127.0.0.1:9000

## Fault Tolerance



MeshWeaver supports automatic node failure detection and task reassignment.

When workers become unavailable, pending work is redistributed to active peers.

This behavior was validated through chaos testing where worker nodes were terminated while tasks were executing.



Results:

\- 20/20 tasks completed successfully

\- 9 tasks automatically reassigned

\- No task loss observed



