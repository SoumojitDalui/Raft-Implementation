# Raft Consensus Algorithm Implementation (C++)

A robust C++ implementation of the Raft distributed consensus algorithm, designed to manage replicated logs and ensure strong consistency across a distributed cluster.

## Key Implemented Features

- **Core Consensus Module**: Implemented the fundamental replicated state machine logic, establishing a reliable foundation for distributed coordination.
- **Leader Election**: Implemented randomized election timeouts to minimize split votes and ensure stable leadership transitions.
- **Log Replication**: Developed the core logic for replicating log entries across the cluster to strictly enforce strong consistency.
- **Heartbeat Mechanism**: Engineered periodic heartbeat signals to maintain leader authority and efficiently detect follower failures.
- **State Machine Safety**: Connected the consensus module to a state machine to ensure commands are executed in a deterministic order across all replicas.

## Development Environment

A modern linux environment (e.g., Ubuntu 22.04) is recommended for building the project and running the reproduction testing scripts.

## Build Instructions

### Get source code
```bash
git clone --recursive [repo_address]
```

### Install dependencies
```bash
sudo apt-get update
sudo apt-get install -y \
    git \
    pkg-config \
    build-essential \
    clang \
    libapr1-dev libaprutil1-dev \
    libboost-all-dev \
    libyaml-cpp-dev \
    libjemalloc-dev \
    python2 \
    python3-dev \
    python3-pip \
    python3-wheel \
    python3-setuptools \
    libgoogle-perftools-dev
sudo pip3 install -r requirements.txt
```

## References
Implementation architecture based on the Raft consensus protocol and MIT 6.824 distributed systems design patterns.
