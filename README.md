# 🎵 Chordify — Distributed Hash Table for P2P Song Lookup

Chordify is a **peer-to-peer lookup system** based on the **Chord Distributed Hash Table (DHT)** protocol.  
The project was developed as part of the **Distributed Systems course at NTUA**.

Chordify simulates a decentralized network where nodes collaborate to **store and locate songs efficiently** without relying on a central server.

Each node participates in a logical **Chord ring** and is responsible for a portion of the key space using **consistent hashing**.

---

# 🧱 Technologies Used

### Core Technologies
- Python
- TCP Sockets
- SHA-1 Hashing
- Multithreading

### Distributed Systems Concepts
- Distributed Hash Tables (DHT)
- Peer-to-peer networking
- Consistent hashing
- Data replication
- Consistency models

---

# ⚙️ System Functionality

### Core DHT Operations
- `insert(key, value)`
- `query(key)`
- `delete(key)`
- `query *` (retrieve all stored data)

### Network Management
- Node **join**
- **Graceful departure** (`depart`)
- **Ring topology visualization** (`overlay`)

---

# 🔁 Data Replication

Chordify supports **replication factor `k`**.

Each key-value pair is stored:
- on the **primary node** responsible for the key
- on the **next `k-1` successors** in the Chord ring.

This improves **data availability and fault tolerance**.

---

# 🔄 Consistency Models

The system supports two consistency modes.

### Linearizability
Implemented using **chain replication**.

- Writes propagate through a chain of replicas
- Reads always occur from the **tail replica**
- Guarantees that queries return the **latest committed value**

### Eventual Consistency

- Writes are applied on the **primary node first**
- Replicas are updated **asynchronously**
- Reads may temporarily return **stale values**

---

# 🚀 Running the System

### Start Bootstrap Node

```bash
python node.py --port 5000 --bootstrap
```

### Start Additional Nodes

```bash
python node.py --port 5001 --bootstrap-ip 127.0.0.1 --bootstrap-port 5000
```

### Start the CLI Client

```bash
python client.py --ip 127.0.0.1 --port 5000
```

### Frontend

```bash
python frontend.py
```

### CLI Commands

insert <key> <value>
delete <key>
query <key>
query *
overlay
depart
help

## Experiments

The system was evaluated using different configurations: 
- 10 nodes
- Replication factors:
-- k = 1
-- k = 3
-- k = 5

- Consistency modes:
-- Linearizability
-- Eventual Consistency

Metrics measured:
- Write throughput
- Read throughput

The experiments highlight the trade-offs between replication, consistency, and performance.

👨‍💻 Team
- Fany Kalogianni https://github.com/fanykl
- Alexandra Moraitaki https://github.com/alexandramoraitaki
- Thanasis Tsiatouras https://github.com/Thanasis-Tsiatouras
