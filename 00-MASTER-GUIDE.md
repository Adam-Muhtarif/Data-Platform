# 🚀 Modern Data Platform Master Learning & Setup Handbook
### *Building a Production-Grade Lakehouse on Ubuntu 24.04 LTS (Mac M4) with Kubernetes & Docker*

Welcome! As your mentor and guide, this series of guides is structured to take you from basic Docker knowledge to mastering enterprise data platform engineering. 

We will build everything step-by-step together on your **Mac M4 (16GB Unified RAM, 500GB SSD)**. By the end of this journey, you will not only have 11 running services, but you will also understand the **why**, the **architecture**, the **networking**, and the **data flows** connecting them.

---

## 🗺️ Curriculum & Learning Path

The guides are divided into sequential, self-contained modules located right inside this workspace:

| Module | Guide File | Core Concepts Covered | Output / Result |
| :--- | :--- | :--- | :--- |
| **Module 01** | `01-virtualization-ubuntu-setup.md` | Multipass, ARM64 Hypervisor, SSH keys, bridged Wi-Fi networking, memory limits | A running Ubuntu 24.04 LTS Server accessible from macOS terminal and your local Wi-Fi |
| **Module 02** | `02-docker-and-k3s-foundations.md` | Containerd, Docker Engine, k3s vs vanilla k8s, Pods, Services, Ingress, Helm | Certified Kubernetes cluster running inside Ubuntu with Traefik reverse proxy |
| **Module 03** | `03-storage-postgres-and-minio.md` | Object storage (S3) vs block storage, MinIO buckets, PostgreSQL metastore tuning | S3 storage with `warehouse` and `raw-data` buckets + central database |
| **Module 04** | `04-polaris-iceberg-catalog.md` | Apache Iceberg open table format, ACID on S3, REST Catalog specification | Polaris catalog orchestrating Iceberg tables on top of MinIO S3 |
| **Module 05** | `05-streaming-kafka-schema-registry.md` | Kafka KRaft (Zookeeper-less), topics, partitions, consumer groups, Avro Schema Registry | Real-time event streaming cluster + web UI for topic monitoring |
| **Module 06** | `06-ingestion-apache-nifi.md` | FlowFiles, Processors, Controller Services, JVM tuning, visual pipeline design | NiFi UI canvas fetching data and streaming into Kafka & MinIO |
| **Module 07** | `07-compute-spark-and-trino.md` | PySpark on K8s, distributed MPP SQL (Trino), Iceberg connectors, Parquet compaction | Interactive SQL engine querying Iceberg tables with millisecond latency |
| **Module 08** | `08-orchestration-apache-airflow.md` | DAGs, Operators, Sensors, Task scheduling, Postgres backend | Automated scheduled pipelines orchestrating end-to-end data pipelines |
| **Module 09** | `09-observability-prometheus-grafana.md` | Prometheus TSDB, node-exporter, JVM metrics scraping, Grafana dashboards | Real-time monitoring of CPU, RAM, throughput, and error rates |
| **Module 10** | `10-end-to-end-practice-pipeline.md` | Real-world streaming mock data -> NiFi -> Kafka -> Spark -> Iceberg -> Trino -> Grafana | Complete functional data platform operating across your local network |

---

## 🏛️ The Complete Architecture at a Glance

```
                         [Local Wi-Fi Network: 192.168.3.x]
                                       │
                      (Any Laptop, Phone, or Tablet)
                                       │
                                       ▼
                       [Mac M4 Host: 192.168.3.30]
                                       │ (Bridge / Ingress)
                                       ▼
        ┌─────────────────────────────────────────────────────────────┐
        │            Ubuntu Server 24.04 LTS (11GB RAM)               │
        │                                                             │
        │  ┌────────────────── k3s Kubernetes ─────────────────────┐  │
        │  │                                                       │  │
        │  │   [Ingress: Traefik Router]                           │  │
        │  │       │                                               │  │
        │  │       ├─► NiFi (8443) ────────► Raw Data ──┐          │  │
        │  │       ├─► Kafka (9092) / UI (8082)         │          │  │
        │  │       │      ▲                             ▼          │  │
        │  │       │      └─ Schema Registry (8081)  [MinIO S3]    │  │
        │  │       │                                    ▲          │  │
        │  │       ├─► Polaris Iceberg Catalog (8181) ──┤          │  │
        │  │       │          ▲                         │          │  │
        │  │       ├─► Trino SQL Engine (8085) ─────────┤          │  │
        │  │       ├─► Spark Processing Jobs ───────────┘          │  │
        │  │       ├─► Airflow Orchestrator (8088)                 │  │
        │  │       └─► Grafana (3000) ◄─── Prometheus (9090)       │  │
        │  │                                                       │  │
        │  │       [PostgreSQL 16 Metastore] (Airflow, Polaris)    │  │
        │  └───────────────────────────────────────────────────────┘  │
        └─────────────────────────────────────────────────────────────┘
```

---

## 💡 Memory Sizing on 16GB Mac M4
* **Host macOS**: 5 GB reserved (system stays smooth and responsive).
* **Ubuntu Server 24.04**: 11 GB allocated.
* **Component-level memory limits** inside Kubernetes ensure no OOM crashes:
  - k3s + OS: ~800MB
  - Postgres: ~350MB
  - MinIO: ~350MB
  - Kafka KRaft: ~750MB
  - Schema Registry: ~350MB
  - Polaris: ~500MB
  - NiFi: ~1.5GB
  - Airflow: ~750MB
  - Spark on K8s: ~1.2GB (transient jobs)
  - Trino: ~2.0GB
  - Prometheus + Grafana: ~550MB
  - Free Headroom: ~1.3GB

