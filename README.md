# Distributed-RainStorm ⛈️

A distributed, fault-tolerant stream processing engine implemented in Python, featuring a custom distributed file system (HyDFS), exactly-once semantics, and dynamic autoscaling.

## 📖 Overview

Distributed-RainStorm is a full-stack distributed system built from scratch, inspired by frameworks like Apache Storm and Spark Streaming. It is designed to process unbounded streams of data in real-time across a cluster of commodity machines.

Unlike simple prototypes, this system implements a complete distributed stack:

- **Membership Protocol**: A SWIM-style failure detector to maintain cluster membership.
- **HyDFS (Hydra Distributed File System)**: A replicated distributed file system for reliable storage.
- **RainStorm Engine**: A stream processing framework with Leader-Worker architecture.

## 🌟 Key Features

### 1. Robust Stream Processing Engine
**Leader-Worker Architecture**: A centralized Leader manages job scheduling, while Workers execute tasks in parallel processes.

**Flexible Topology**: Supports multi-stage pipelines (Source → Filter → Transform/Aggregate).

**Supported Operators**:
- **Filter**: Pattern matching (Regex).
- **Transform**: String manipulation (e.g., custom cut operations).
- **Aggregate**: Stateful key-value counting.
- **Identity**: Passthrough for load testing.

### 2. Strong Consistency (Exactly-Once Semantics)
We guarantee that every tuple is processed exactly once, even in the presence of node failures.

- **Unique Identification**: Every tuple is assigned a unique SHA1 hash ID.
- **State Logging**: Task states and processed IDs are periodically synced to HyDFS (Distributed Write-Ahead Log).
- **Deduplication**: Workers check incoming tuple hashes against their restored state to prevent duplicate processing.
- **Zero Data Loss**: Upstream tasks utilize an infinite retry mechanism (while not sent) to handle downstream failures, ensuring reliable delivery.

### 3. Elasticity (Autoscaling)
The system adapts to workload changes in real-time.

- **Throughput Monitoring**: The Leader aggregates processing rates from all tasks every second.
- **Dynamic Scaling**:
  - **Scale Up**: Adds new tasks/workers when the average rate exceeds the High Watermark (HW).
  - **Scale Down**: Merges tasks when the average rate drops below the Low Watermark (LW).

### 4. Distributed Storage (HyDFS)
A custom DFS that underpins the state management.

- **Sharding & Replication**: Files are split into blocks and replicated across multiple nodes (Ring-based hashing) for high availability.
- **Consistency**: Supports concurrent appends and atomic reads.

## 🏗️ Architecture

```mermaid
graph TD
    User[Client] -->|Submit Job| Leader[RainStorm Leader]
    Leader -->|Schedule Tasks| W1[Worker Node 1]
    Leader -->|Schedule Tasks| W2[Worker Node 2]
    Leader -->|Monitor Rate| Autoscaler[Autoscaling Module]
    
    subgraph "Worker Node"
        W1 -->|Spawn| T1[Task Process A]
        W1 -->|Spawn| T2[Task Process B]
        T1 -->|Sync Log| HyDFS[HyDFS Storage]
    end
