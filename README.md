# Distributed Storage Engine: A High-Availability HDFS Simulation

A robust, distributed file system architecture modeled after the **Hadoop Distributed File System (HDFS)**. This project implements a scalable storage engine featuring automated file sharding, multi-node replication, and a centralized orchestration layer for metadata management and health monitoring.

---

## 🏗️ System Architecture
The system utilizes a **Leader-Follower (Master-Slave)** architecture designed for high throughput and fault tolerance:



* **NameNode (Control Plane):** Acts as the centralized metadata repository and orchestration engine. It manages file-to-chunk mapping, handles the replication protocol, and monitors cluster health via real-time heartbeat analysis.
* **DataNodes (Data Plane):** High-performance storage agents responsible for persisting data shards and serving retrieval requests. Each node maintains a persistent connection with the NameNode to signal availability.
* **Client Gateway:** A sophisticated interface providing end-to-end file processing, including client-side reconstruction and system-wide telemetry visualization.

---

## 🚀 Key Engineering Features
* **Intelligent Sharding:** Automatically decomposes large datasets into immutable **2MB chunks** to optimize parallel processing and distribution.
* **Resilient Replication Protocol:** Implements a redundancy strategy where each data shard is replicated across all available DataNodes, ensuring **zero data loss** even during physical node failure.
* **Fault Detection & Recovery:** The NameNode utilizes an asynchronous heartbeat listener thread to detect DataNode outages within **15 seconds**, triggering immediate system-wide metadata updates.
* **Real-time Cluster Telemetry:** A centralized dashboard provides deep visibility into node status (Heartbeat Monitoring), chunk distribution, and file integrity.
* **Binary-Safe Integrity:** Utilizes **SHA-256 checksums** and specialized encoding (**Base64/Latin-1**) to ensure bit-perfect file reconstruction during download.

---

## 🛠️ Technology Stack
* **Language:** Python (Multi-threaded execution for concurrent I/O).
* **Networking:** Low-level TCP/IP Sockets for high-speed inter-node communication.
* **Coordination:** Custom UDP Heartbeat protocol for cluster synchronization.
* **Interface:** Streamlit for real-time infrastructure visualization and management.

---

## 📂 Repository Structure
```text
├── namenode.py         # Metadata orchestration and cluster management
├── datanode1.py        # Primary storage agent (Host A)
├── datanode2.py        # Secondary storage agent (Host B)
├── client.py           # Streamlit-based management dashboard
├── metadata.json       # Persistent metadata storage (FSImage equivalent)
└── data_blocks/        # Local persistent storage for distributed shards
```
## ⚡ Quickstart

### 1. Initialize Cluster Control
Run the NameNode to start the metadata and heartbeat listener:
```bash
python namenode.py
```
### 2. Launch Storage Nodes
Run the DataNodes in separate terminals to join the cluster and begin sending real-time heartbeats to the NameNode:

```bash
# Terminal A
python datanode1.py

# Terminal B
python datanode2.py
```
### 3. Access Management Console
Launch the Streamlit dashboard to visualize node health, monitor chunk distribution, and manage file operations:
```bash
streamlit run client.py
```
## 📋 Implementation Details
Communication: Inter-node messaging is handled via JSON-serialized payloads over TCP.

Reliability: The NameNode tracks "last heartbeat age" to identify and exclude unhealthy nodes from the active write pool.

Storage: Chunks are persisted locally on DataNodes within the data_blocks/ directory, mirroring physical block storage in a production HDFS environment.
