# Enterprise Data Platform Master Reference Manual
## Complete Technical Architecture & Operations Guide: NiFi, Kafka, Schema Registry, MinIO, Iceberg, Polaris, Trino & Airflow
### Telesom & Dahabshiil Group Data Platform Engineering

---

## Executive Table of Contents
1. **The Modern Data Platform Architecture**
   - 1.1 The Decoupled Lakehouse Paradigm
   - 1.2 End-to-End Data Flow Topology
   - 1.3 Tool Responsibilities & Boundary Matrix
2. **Apache NiFi: Enterprise Data Ingestion & Transformation**
   - 2.1 Core Architectural Model (FlowFile, Repositories, Process Groups)
   - 2.2 Record-Oriented Architecture (Readers & Writers)
   - 2.3 Master Controller Services Encyclopedia
   - 2.4 Master Processors Catalog (Labs 1–4)
   - 2.5 Syntax Engines: Parameters (`#{...}`), Attributes (`${...}`), RecordPaths (`/...`)
   - 2.6 Parameter Sensitivity Rules & Cryptographic Isolation
3. **Apache Kafka & Confluent Schema Registry: Event Streaming & Governance**
   - 3.1 Distributed Log Architecture (Topics, Partitions, Offsets, Replicas)
   - 3.2 Producer & Consumer Dynamics (Acks, Consumer Groups, Lag)
   - 3.3 Confluent Schema Registry Architecture & Wire Format
   - 3.4 Schema Evolution & Compatibility Modes (`BACKWARD`, `FORWARD`, `FULL`, `NONE`)
   - 3.5 AKHQ / Kafka UI Operations & CLI Administration
4. **MinIO: High-Performance S3 Object Storage Layer**
   - 4.1 Object Storage Concepts vs. POSIX Filesystems
   - 4.2 Erasure Coding, Bitrot Protection & High Availability
   - 4.3 S3 Addressing Protocols: Path-Style Access vs. Virtual-Host Style
   - 4.4 Multi-Tenancy, IAM Policies & Bucket Management
   - 4.5 MinIO Client (`mc` CLI) & Console Operations
5. **Apache Iceberg: Modern Open Table Format**
   - 5.1 Why Traditional Hive Tables Fail at Scale
   - 5.2 The 3-Tier Metadata Tree (`metadata.json`, Manifest Lists, Manifests, Parquet Data)
   - 5.3 Hidden Partitioning & Partition Evolution
   - 5.4 Schema Evolution (Immutable Field IDs)
   - 5.5 ACID Transactions, Snapshot Isolation & Optimistic Concurrency
   - 5.6 Time Travel & Table History
6. **Apache Polaris: REST Iceberg Catalog & Governance**
   - 6.1 Role of the Central REST Catalog
   - 6.2 Polaris Hierarchy: Warehouses, Namespaces, Tables
   - 6.3 Security, OAuth2 Authentication & Role-Based Access Control (RBAC)
   - 6.4 Storage Credential Delegation vs. Direct FileIO Provider
   - 6.5 REST Catalog API Operations
7. **Trino: Distributed Massively Parallel Processing (MPP) SQL Engine**
   - 7.1 Distributed Architecture (Coordinator, Workers, Connectors, Splits)
   - 7.2 Configuring the Iceberg Connector (`lakehouse.properties`)
   - 7.3 Auditing Iceberg Metadata via Trino (`$snapshots`, `$files`, `$partitions`)
   - 7.4 Query Performance Tuning (CBO, Pruning, Dynamic Filtering)
   - 7.5 Trino Web UI Telemetry & CLI Administration
8. **Apache Airflow: Enterprise Orchestration & Workflow Scheduling**
   - 8.1 Core Architecture (Webserver, Scheduler, Database, Workers, Executors)
   - 8.2 DAG Authoring Fundamentals & Task Lifecycle
   - 8.3 Operators, Sensors, Hooks & Connections
   - 8.4 Production Banking DAG Patterns:
     - 8.4.1 Scheduled Iceberg Compaction & Maintenance DAG
     - 8.4.2 Daily End-of-Day (EOD) Financial Reconciliation DAG
     - 8.4.3 Automated Pipeline Health & SLA Alerting DAG
   - 8.5 Airflow Web UI Operations & Troubleshooting
9. **Cross-Platform Interoperability & Master Troubleshooting Matrix**
   - 9.1 End-to-End Enterprise Data Path
   - 9.2 The Top 20 Cross-Platform Failure Scenarios & Exact Fixes

---

# Part 1: The Modern Data Platform Architecture

### 1.1 The Decoupled Lakehouse Paradigm

Legacy big data architectures suffered from strict **compute-storage coupling** (e.g. legacy Hadoop HDFS) and **vendor lock-in** (proprietary cloud warehouses like Snowflake or BigQuery). 

The modern lakehouse architecture implemented in this platform completely separates every layer of the data lifecycle into open, specialized technologies:

```text
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                       ORCHESTRATION & MONITORING                                       │
│                         Apache Airflow (DAG Workflows, Maintenance, Health Audits)                     │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
                                                    │ (Triggers & Governs)
┌───────────────────────────────────────────────────▼────────────────────────────────────────────────────┐
│                                       ANALYTICS & QUERY ENGINE                                         │
│                Trino Distributed SQL (Sub-Second Ad-Hoc Analytics, Reporting, BI Dashboards)           │
└───────────────────────────────────────────────────▲────────────────────────────────────────────────────┘
                                                    │ (Reads / Writes Metadata)
┌───────────────────────────────────────────────────▼────────────────────────────────────────────────────┐
│                                       CATALOG & METASTORE GOVERNANCE                                   │
│                        Apache Polaris REST Catalog (OAuth2, Multi-Engine Synchronization)              │
└───────────────────────────────────────────────────▲────────────────────────────────────────────────────┘
                                                    │ (Defines Table Schema & Snapshots)
┌───────────────────────────────────────────────────▼────────────────────────────────────────────────────┐
│                                       OPEN TABLE STORAGE FORMAT                                        │
│                 Apache Iceberg (ACID Transactions, Time-Travel, Hidden Partitioning, Parquet)          │
└───────────────────────────────────────────────────▲────────────────────────────────────────────────────┘
                                                    │ (Stores Data & Metadata Objects)
┌───────────────────────────────────────────────────▼────────────────────────────────────────────────────┐
│                                       OBJECT STORAGE FOUNDATION                                        │
│                           MinIO Enterprise S3 Storage (NVMe, Multi-Bucket, Multi-Tenant)                │
└───────────────────────────────────────────────────▲────────────────────────────────────────────────────┘
                                                    │ (Bulk Load / Micro-Batch Sink)
┌───────────────────────────────────────────────────▼────────────────────────────────────────────────────┐
│                                      DATA INGESTION & TRANSFORMATION                                   │
│            Apache NiFi (Visual ELT, In-Memory Record Streaming, Bin-Packing, Stream Tapping)          │
└───────────────────────────────────────────────────▲────────────────────────────────────────────────────┘
                                                    │ (Event Stream Pub/Sub)
┌───────────────────────────────────────────────────▼────────────────────────────────────────────────────┐
│                                    STREAMING BACKBONE & SCHEMA GOVERNANCE                              │
│             Apache Kafka (Topic Partitioning) + Confluent Schema Registry (Avro Validation)            │
└───────────────────────────────────────────────────▲────────────────────────────────────────────────────┘
                                                    │ (Raw Ingestion)
                                         [Edge Systems & Data Sources]
                           SFTP Feeds • ISO 8583 Core Banking • REST APIs • Databases
```

### 1.2 Tool Responsibilities & Boundary Matrix

| Platform Tool | Core Responsibility | Input Format | Output Format | Primary Users |
| :--- | :--- | :--- | :--- | :--- |
| **Apache NiFi** | Ingestion, ETL/ELT, Protocol conversion, PII Masking, Micro-batching. | SFTP, CSV, JSON, Kafka, SQL | Binary Avro, S3 Parquet, Alerts | Data Engineers, Integration Specialists |
| **Apache Kafka** | Real-time decoupled event backbone, durable messaging buffer. | Events, Transactions, Logs | Partitioned Topic Streams | Stream Developers, Microservices |
| **Confluent Schema Registry** | Central source of truth for schemas, evolution enforcement. | Avro Schemas (JSON) | Schema IDs, Validated Streams | Data Architects, Governance Officers |
| **MinIO** | S3-compatible, high-performance object persistence. | S3 API PUT requests | Immutable Byte Objects | Storage Admins, Infrastructure |
| **Apache Iceberg** | ACID table semantics, snapshot management, metadata trees. | Parquet files, Manifests | Virtual SQL Tables | Data Lakehouse Architects |
| **Apache Polaris** | Centralized REST Catalog governing table locations and permissions. | Catalog REST API calls | Metadata Pointers, Auth Tokens | Platform Admins, Security Officers |
| **Trino** | Fast, distributed SQL execution across billions of rows. | SQL Queries | Result Sets, Aggregations | Data Analysts, BI Developers, Data Scientists |
| **Apache Airflow** | Workflow orchestration, scheduled compaction, EOD pipelines. | Python DAG Definitions | Scheduled Task Executions | Data Engineers, MLOps Engineers |

---

# Part 2: Apache NiFi: Enterprise Ingestion & Transformation

### 2.1 Core Architectural Model
- **FlowFile**: The basic unit of data inside NiFi. It consists of two parts:
  1. **Attributes**: Key-value string metadata (e.g. `filename`, `fileSize`, `el_record_date`, `uuid`). Stored in JVM memory and the FlowFile Repository.
  2. **Content**: The actual data payload (e.g. a 50 MB CSV or binary Avro stream). Stored on disk in the Content Repository.
- **Repositories**:
  - **FlowFile Repository**: Write-ahead log tracking FlowFile state and attributes across restarts.
  - **Content Repository**: Content-addressable storage where actual payload bytes reside. FlowFiles reference slices of these files using byte offsets.
  - **Provenance Repository**: Full audit trail recording every event (`CREATE`, `FETCH`, `MODIFY_CONTENT`, `JOIN`, `SEND`, `DROP`) for end-to-end data lineage.
- **Process Groups & Port Encapsulation**:
  - Encapsulation isolates business logic into self-contained modules (`EXTRACT`, `TRANSFORM`, `LOAD`, `MONITORING`, `ALERTING`).
  - **The Golden Rule**: Process groups communicate **exclusively via Input Ports and Output Ports**. No connection is ever permitted to cross a process group wall directly.

---

### 2.2 Record-Oriented Architecture (Readers & Writers)
In legacy NiFi flows, multi-megabyte files were split into individual FlowFiles (`SplitText`), causing massive JVM garbage collection pauses and disk thrashing.

The **Record-Oriented Engine** treats a FlowFile as a container of records:
- **`RecordReader`**: Parses incoming byte streams into internal Java `Record` objects based on a schema.
- **Processor Logic**: Mutates or filters records in memory without writing intermediate files to disk.
- **`RecordSetWriter`**: Serializes records back into the desired output format (e.g. Avro or Parquet).
- **Throughput Benefit**: A single FlowFile carrying 250,000 records passes through memory at line-rate speed with minimal disk I/O.

---

### 2.3 Master Controller Services Encyclopedia

#### 1. `ConfluentSchemaRegistry`
- **Bundle**: `org.apache.nifi - nifi-confluent-platform-nar`
- **What It Does**: Connects NiFi to an external Confluent / Kafka Schema Registry (`http://schemaregistry.confluent.svc:8081`). Fetches, caches, and validates Avro schemas across the enterprise.
- **Key Properties**:
  - `Schema Registry URLs`: `#{schema.registry.url}`
  - `Cache Expiration`: `1 hour` (avoids continuous HTTP lookups)
  - `Cache Size`: `1000`

#### 2. `AvroSchemaRegistry`
- **Bundle**: `org.apache.nifi - nifi-standard-nar`
- **What It Does**: Internal standalone Schema Registry inside NiFi. Schemas are defined as dynamic properties where Property Name is the subject name and Property Value is the Avro JSON schema. Ideal for isolated environments lacking Kafka.

#### 3. `CSVReader`
- **Bundle**: `org.apache.nifi - nifi-record-serialization-services-nar`
- **What It Does**: Parses delimited text (CSV, pipe `|`, TSV) into Record objects.
- **Key Properties**:
  - `Schema Access Strategy`: `Use 'Schema Name' Property`
  - `Schema Registry`: Point to `ConfluentSchemaRegistry` or `AvroSchemaRegistry`
  - `Schema Name`: `voice-cdr` (or `${schema.name}`)
  - `Value Separator`: `|` (pipe) or `,` (comma)
  - `Treat First Line as Header`: `false` (for raw telecom/banking streams)

#### 4. `AvroReader`
- **Bundle**: `org.apache.nifi - nifi-record-serialization-services-nar`
- **What It Does**: Reads binary Avro object containers.
- **Key Property**:
  - `Schema Access Strategy`: **`Embedded Avro Schema`** (reads schema directly from the Avro container header; requires zero remote network calls).

#### 5. `AvroRecordSetWriter`
- **Bundle**: `org.apache.nifi - nifi-record-serialization-services-nar`
- **What It Does**: Serializes in-flight records into compact binary Avro containers.
- **Key Properties**:
  - `Schema Write Strategy`: `Embed Avro Schema`
  - `Schema Access Strategy`: `Use 'Schema Name' Property`
  - `Compression Format`: `SNAPPY` or `NONE`

#### 6. `S3IcebergFileIOProvider`
- **Bundle**: `org.apache.nifi - nifi-iceberg-nar`
- **What It Does**: Implements Apache Iceberg's `FileIO` interface for S3/MinIO. Handles file-level write, read, and delete operations for Parquet files and Iceberg metadata.
- **Key Properties**:
  - `Endpoint URL`: `http://minio.minio.svc.cluster.local:9000`
  - `Access Key ID` / `Secret Access Key`: MinIO credentials
  - `Client Region`: `us-east-1`
  - `Path Style Access`: **`true`** *(CRITICAL: required for MinIO to disable virtual-host addressing)*

#### 7. `RESTIcebergCatalog`
- **Bundle**: `org.apache.nifi - nifi-iceberg-nar`
- **What It Does**: Communicates with the Apache Polaris REST Catalog to coordinate table schemas, commit snapshot metadata, and verify concurrency.
- **Key Properties**:
  - `Catalog URI`: `http://polaris.polaris.svc.cluster.local:8181/api/catalog`
  - `File IO Provider`: Select `s3-iceberg-file-io`
  - `Access Delegation Strategy`: **`disabled`** (direct I/O routes through MinIO)
  - `Authentication Strategy`: `OAuth 2.0` (Client ID & Client Secret)
  - `Warehouse Location`: `bigdata_catalog`

#### 8. `ParquetIcebergWriter`
- **Bundle**: `org.apache.nifi - nifi-iceberg-nar`
- **What It Does**: Serializes records into native Apache Parquet columnar files adhering to Iceberg field-ID specifications.

---

### 2.4 Master Processors Catalog (Labs 1–4)

| Processor | Primary Function | Scheduling | Key Relationships |
| :--- | :--- | :--- | :--- |
| **`ListSFTP`** | Scans remote SFTP directories and creates 0-byte metadata FlowFiles. | **`On Primary Node`** *(prevents duplicate listings)* | `success` |
| **`FetchSFTP`** | Downloads actual file payload from SFTP server and moves file to backup. | `Timer Driven` (with **`Round Robin`** connection balance) | `success`, `comms.failure`, `not.found` |
| **`ConvertRecord`** | Streams records through memory, converting from CSV to Avro. | `Timer Driven` | `success`, `failure` |
| **`UpdateRecord`** | Mutates individual record fields (e.g. deriving `el_record_date` from `/create_date`). | `Timer Driven` (**`Record Path Value`** strategy) | `success`, `failure` |
| **`MergeRecord`** | Bin-packs small FlowFiles into 50k–250k record batches correlated on partition keys. | `Timer Driven` (Correlate: `el_record_date`) | `merged`, `original` (auto-terminate), `failure` |
| **`PutIcebergRecord`** | Commits Parquet data to MinIO and registers snapshots with Polaris. | `Timer Driven` (`Unmatched Column: FAIL`) | `success` (to `load-out`), `failure` |
| **`PutS3Object`** | Direct raw binary object upload to MinIO/S3 bucket. | `Timer Driven` | `success`, `failure` |
| **`MonitorActivity`** | Watches data streams; fires alerts upon inactivity or traffic restoration. | **`On Primary Node`** (On tapped clone) | `success` (terminate), `inactive`, `activity.restored` |
| **`UpdateAttribute`** | Injects metadata tags (e.g. `alert.type`) for downstream alerting. | `Timer Driven` | `success` |
| **`PutEmail`** | Dispatches templated operational emails via SMTP. | `Timer Driven` | `success`, `failure` |

---

### 2.5 Syntax Engines: Parameters vs. Attributes vs. RecordPaths

```text
Are you configuring a value in NiFi?
  │
  ├── Is it a property of a Controller Service (URL, Catalog, Password, Host)?
  │     └── YES ➔ MUST use Parameter Context: #{parameter.name}
  │
  ├── Is it a FlowFile metadata attribute (filename, execution node, alert type)?
  │     └── YES ➔ MUST use Expression Language: ${attribute.name}
  │
  └── Is it a data field INSIDE the stream records (create_date, callingpartynumber)?
        └── YES ➔ MUST use RecordPath: /field_name
```

---

### 2.6 Parameter Sensitivity Rules & Cryptographic Isolation
- **Sensitive Properties**: Properties marked with a lock icon 🔒 (passwords, private keys, client secrets, access tokens) are encrypted on disk.
- **Rule**:
  - Sensitive Properties **CAN ONLY** reference Parameters created with **`Sensitive = Yes` (Checked)**.
  - Non-Sensitive Properties **CAN ONLY** reference Parameters created with **`Sensitive = No` (Unchecked)**.
  - NiFi throws a validation error if sensitivity does not match. You cannot toggle the sensitivity checkbox on an existing parameter; you must delete and recreate it.

---

# Part 3: Apache Kafka & Confluent Schema Registry

### 3.1 Distributed Log Architecture
- **Topic**: A logical stream of records (e.g. `voice-cdrs`, `banking.transactions`).
- **Partition**: The physical unit of parallelism. Each partition is an ordered, immutable sequence of messages stored on disk in append-only segment files.
- **Offset**: An immutable, sequential integer assigned to each message within a partition.
- **Replication**:
  - Each partition has one **Leader** and zero or more **Followers**.
  - All produce and consume requests go to the Leader.
  - **In-Sync Replicas (ISR)**: The set of replicas that are actively caught up with the Leader.

---

### 3.2 Producer & Consumer Dynamics
- **Producer Acks**:
  - `acks=0`: Producer does not wait for any acknowledgment. High throughput, risk of data loss.
  - `acks=1`: Leader writes record to local log and acknowledges. Minimal risk if Leader crashes before replication.
  - `acks=all` (`-1`): Leader waits for full ISR set to commit. **Mandatory for banking/financial transactions**.
- **Consumer Groups & Lag**:
  - Consumers sharing the same `group.id` divide partitions among themselves.
  - **Consumer Lag**: The delta between the latest offset produced to a partition and the current offset processed by the consumer. Monitored via AKHQ / Kafka UI.

---

### 3.3 Confluent Schema Registry Architecture & Wire Format
When producing Avro records to Kafka, sending the entire JSON schema with every 100-byte message creates 90% network overhead.

**The Confluent Wire Format (Magic Byte + Schema ID)**:
Instead of the full schema, the producer registers the schema once with Schema Registry, gets an integer **Schema ID**, and prepends a **5-byte header** to each message payload:
```text
Byte 0: Magic Byte (0x00)
Bytes 1–4: 4-Byte Integer Schema ID (e.g. ID = 42)
Bytes 5+: Raw Binary Avro Encoded Payload
```
Downstream consumers inspect bytes 1–4, fetch the schema from Schema Registry once, cache it in memory, and deserialize the binary payload instantly.

---

### 3.4 Schema Evolution & Compatibility Modes

| Compatibility Mode | Changes Allowed | Consumer / Producer Upgrade Order |
| :--- | :--- | :--- |
| **`BACKWARD`** (Default) | Delete fields, Add **optional** fields (with default values). | Upgrade consumers first, then producers. |
| **`FORWARD`** | Add new fields, Delete optional fields. | Upgrade producers first, then consumers. |
| **`FULL`** | Add/delete optional fields with defaults. | Upgrade in any order. |
| **`NONE`** | All changes allowed (no compatibility checks). | Used when bootstrapping new schemas. |

---

### 3.5 AKHQ / Kafka UI Operations & Useful CLI Commands

```bash
# List topics
kubectl exec -n confluent kafka-0 -- kafka-topics --bootstrap-server localhost:9092 --list

# Describe topic partitions and ISR
kubectl exec -n confluent kafka-0 -- kafka-topics --bootstrap-server localhost:9092 --describe --topic voice-cdrs

# Check Confluent Schema Registry registered subjects
curl -s http://schemaregistry.confluent.svc:8081/subjects

# Fetch latest schema version for a subject
curl -s http://schemaregistry.confluent.svc:8081/subjects/voice-cdr-partitioned/versions/latest | jq .
```

---

# Part 4: MinIO: S3 Object Storage Layer

### 4.1 Object Storage vs. POSIX Filesystems
Unlike hierarchical Linux file systems, MinIO stores data in a **flat namespace**:
- There are no real physical "directories".
- A key named `warehouse/cdrs/voice_lab/data/file.parquet` is simply an object name containing slashes.
- Objects are **immutable**: to change a byte, the entire object must be rewritten. This is why table formats like Iceberg write new Parquet files rather than modifying existing ones.

---

### 4.2 Erasure Coding & Bitrot Protection
MinIO uses **Reed-Solomon Erasure Coding** to protect data against hardware drive failures:
- In an 8-drive setup with `EC:4`, 4 parity blocks are generated for every 4 data blocks. The cluster can lose up to 4 entire hard drives simultaneously without losing a single byte of data.
- **Bitrot Detection**: MinIO continuously verifies SHA-256 / HighwayHash checksums during read operations to catch silent hardware data degradation.

---

### 4.3 S3 Addressing Protocols: Path-Style vs. Virtual-Host Style

```text
Path-Style Access (MANDATORY for MinIO):
http://169.58.218.71:9000 / big-data / warehouse / data.parquet
└──────── Endpoint ─────┘ └── Bucket ┘ └────── Key ────────┘

Virtual-Host Style (Default on AWS S3, FAILS on MinIO):
http://big-data.169.58.218.71:9000 / warehouse / data.parquet
```
> [!IMPORTANT]
> Because internal Kubernetes and on-premise DNS servers do not dynamically resolve subdomains like `big-data.minio.svc...`, you **MUST** configure `Path Style Access = true` across all NiFi controller services, Trino catalogs, and Airflow hooks!

---

### 4.4 MinIO Client (`mc` CLI) Essential Reference

```bash
# Configure alias for MinIO cluster
mc alias set samo-minio http://169.58.218.71:30900 BnMtc7hhYG705rlXFcdj VZP8Xcv7RHPUIRmiPZH1tCNOwqJUtmuJ8tvqv2ok

# List buckets
mc ls samo-minio

# List Iceberg table Parquet files recursively with human-readable sizes
mc ls -r --human samo-minio/big-data/warehouse/cdrs/voice_lab/data/

# Check storage cluster health and capacity
mc admin info samo-minio
```

---

# Part 5: Apache Iceberg: Modern Open Table Format

### 5.1 Why Traditional Hive Tables Fail at Scale
In the legacy Hive table format, a table was defined merely as a directory on storage (`/warehouse/table/`).
- **Partition Discovery**: To query a partition, Hive had to recursively scan all directories on S3 (`LIST` operations). On S3, scanning 100,000 files takes several minutes before query execution even begins!
- **Zero ACID Guarantees**: If a writer failed mid-way, partial files remained in the folder, corrupting analytical queries.
- **Concurrent Writes**: Two writers inserting data simultaneously corrupted directory state.

---

### 5.2 The 3-Tier Metadata Tree Architecture

Apache Iceberg replaces folder-based storage with an explicit, immutable **Metadata Tree**:

```text
                                       Polaris REST Catalog
                                                │
                                                ▼ (Current Metadata Pointer)
                                      v2.metadata.json
                             ┌──────────────────┴──────────────────┐
                             ▼                                     ▼
                    Snapshot 1 (ID: 1001)                 Snapshot 2 (ID: 1002)
                             │                                     │
                             ▼                                     ▼
                     snap-1001.avro (Manifest List)        snap-1002.avro (Manifest List)
                    ┌────────┴────────┐                   ┌────────┴────────┐
                    ▼                 ▼                   ▼                 ▼
             manifest-A.avro   manifest-B.avro     manifest-A.avro   manifest-C.avro
                    │                 │                                     │
                    ▼                 ▼                                     ▼
             data-1.parquet    data-2.parquet                        data-3.parquet
```

1. **Table Metadata (`v<N>.metadata.json`)**:
   - Contains table schema, Field IDs, partition specifications, snapshot history, and points to the current active snapshot.
2. **Manifest List (`snap-<ID>.avro`)**:
   - Represents a single snapshot commit. Contains a list of manifest files and summarizes the partition boundaries for each manifest.
3. **Manifest File (`*-m0.avro`)**:
   - Tracks individual Parquet data files. Stores per-column **lower and upper bounds**, null counts, and record counts.
   - **Pruning Power**: Trino checks column min/max bounds in the manifest file and skips reading 95% of Parquet files from S3 without ever opening them!
4. **Data Files (`*.parquet`)**:
   - Columnar Snappy-compressed data files containing actual records.

---

### 5.3 Hidden Partitioning
In legacy systems, users had to create artificial partition columns (e.g. `event_year`, `event_month`, `event_day`) and query them explicitly (`WHERE event_year = 2026 AND event_month = 9`). If an analyst queried `WHERE call_time >= '2026-09-14'`, the engine scanned the entire table!

**Iceberg Hidden Partitioning**:
- Iceberg partitions on **transformations of existing source columns**:
  - `day(el_record_date)`
  - `hour(call_timestamp)`
  - `bucket[16](callingpartynumber)`
  - `truncate[4](country_code)`
- **Advantage**: The user queries source columns directly (`WHERE el_record_date = DATE '2026-09-14'`), and Iceberg automatically prunes unneeded partitions behind the scenes!

---

### 5.4 Schema Evolution (Immutable Field IDs)
Every column in an Iceberg table is assigned a unique, immutable integer **Field ID** (e.g. `cdr_id = 1`, `create_date = 4`).
- **Renaming a column**: Renames the alias in `metadata.json`. Existing Parquet files are untouched.
- **Adding a column**: Assigns a new Field ID. When historical Parquet files lacking that ID are read, Iceberg automatically projects `NULL`.
- **Reordering columns**: Changes array order in metadata.
- **Zero Full-Table Rewrites**: Schema updates take 5 milliseconds regardless of whether the table has 100 rows or 100 billion rows.

---

# Part 6: Apache Polaris: REST Iceberg Catalog

### 6.1 Role of the Central REST Catalog
Apache Polaris implements the standardized **Apache Iceberg REST Catalog Specification**:
- Decouples table discovery from physical storage.
- Ensures multiple concurrent engines (Trino, Apache Spark, Apache Flink, Apache NiFi) share the exact same view of table state with **optimistic concurrency control (OCC)**.

---

### 6.2 Polaris Hierarchy & Entities
```text
Polaris Service
  └── Catalog (Warehouse: bigdata_catalog)
        ├── Namespace (cdrs)
        │     └── Table (voice_lab)
        └── Namespace (banking)
              ├── Table (transactions_raw)
              └── Table (customer_accounts)
```

---

### 6.3 Security, OAuth2 & Roles
- **Client ID & Client Secret**: Used to mint short-lived JWT Bearer tokens via `POST /api/catalog/v1/oauth/tokens`.
- **RBAC Grants**:
  - `CATALOG_MANAGE_METADATA`: Create/delete namespaces and tables.
  - `TABLE_WRITE_DATA`: Commit new snapshots and append data.
  - `TABLE_READ_DATA`: Query table metadata and data files.

---

# Part 7: Trino: Distributed Massively Parallel Processing SQL

### 7.1 Distributed Architecture
```text
                      [Client: DataGrip / DBeaver / CLI]
                                     │
                                     ▼ (SQL Query)
                     ┌───────────────────────────────┐
                     │       TRINO COORDINATOR       │
                     │  • Parser & Query Planner     │
                     │  • Cost-Based Optimizer (CBO) │
                     │  • Scheduler & Task Assigner  │
                     └───────────────┬───────────────┘
                                     │
            ┌────────────────────────┼────────────────────────┐
            ▼ (Split Assignment)     ▼                        ▼
     ┌──────────────┐         ┌──────────────┐         ┌──────────────┐
     │ TRINO WORKER │         │ TRINO WORKER │         │ TRINO WORKER │
     │  Read Splits │         │  Read Splits │         │  Read Splits │
     │  Filter/Hash │         │  Filter/Hash │         │  Filter/Hash │
     └──────┬───────┘         └──────┬───────┘         └──────┬───────┘
            └────────────────────────┼────────────────────────┘
                                     ▼
                           [MinIO S3 Parquet Data]
```

---

### 7.2 Iceberg Catalog Configuration (`lakehouse.properties`)
Configured inside `/etc/trino/catalog/lakehouse.properties`:
```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://polaris.polaris.svc.cluster.local:8181/api/catalog
iceberg.rest-catalog.warehouse=bigdata_catalog
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.credential=d5f9cdea48a6fb7f:ae88731a268f5c0e33e593508fad6fcf
iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL
hive.s3.endpoint=http://minio.minio.svc.cluster.local:9000
hive.s3.aws-access-key=BnMtc7hhYG705rlXFcdj
hive.s3.aws-secret-key=VZP8Xcv7RHPUIRmiPZH1tCNOwqJUtmuJ8tvqv2ok
hive.s3.path-style-access=true
hive.s3.ssl.enabled=false
```

---

### 7.3 Interrogating Iceberg Metadata via Trino

```sql
-- 1. Snapshot History Audit (Commit tracking, added rows, added files)
SELECT 
    snapshot_id, 
    committed_at, 
    operation, 
    summary['total-records'] AS total_rows,
    summary['added-records'] AS rows_added,
    summary['added-data-files'] AS files_added
FROM lakehouse.cdrs."voice_lab$snapshots"
ORDER BY committed_at DESC;

-- 2. Physical Data File Inspection (File sizes, record counts, partition values)
SELECT 
    file_path, 
    file_format, 
    record_count, 
    round(file_size_in_bytes / 1024.0 / 1024.0, 2) AS size_mb,
    partition
FROM lakehouse.cdrs."voice_lab$files";

-- 3. Partition Pruning Verification
EXPLAIN ANALYZE 
SELECT COUNT(*) 
FROM lakehouse.cdrs.voice_lab 
WHERE el_record_date = DATE '2026-09-14';
```

---

# Part 8: Apache Airflow: Enterprise Orchestration

### 8.1 Core Architecture & Roles
- **Webserver**: Flask-based UI displaying DAG runs, task status, Gantt charts, logs, and connection secrets.
- **Scheduler**: High-performance process scanning the `dags/` folder, resolving task dependencies (`>>`), and queuing tasks whose schedules have arrived.
- **Metadata Database**: PostgreSQL instance storing task execution state, XComs, Variables, and user permissions.
- **Workers / Executor**: Executes individual task tasks (`KubernetesExecutor` runs each task in a temporary Kubernetes pod).

---

### 8.2 Production Banking DAG Patterns

#### Pattern 1: Scheduled Iceberg Compaction & Maintenance DAG
*Purpose*: Over time, continuous streaming writes produce small files and historical snapshots that consume storage. This Airflow DAG runs nightly at 02:00 AM to compact Parquet files and purge snapshots older than 30 days.

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator

default_args = {
    'owner': 'data-platform',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email': ['ops-alerts@telesom.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'lakehouse_iceberg_maintenance_nightly',
    default_args=default_args,
    schedule_interval='0 2 * * *', # Nightly at 2:00 AM
    catchup=False,
    max_active_runs=1,
    tags=['lakehouse', 'maintenance', 'iceberg'],
) as dag:

    # Task 1: Compact small files into optimal 128MB Parquet files
    compact_voice_cdrs = TrinoOperator(
        task_id='compact_voice_cdrs',
        trino_conn_id='trino_lakehouse_conn',
        sql="""
        ALTER TABLE lakehouse.cdrs.voice_lab 
        EXECUTE optimize(file_size_threshold => '32MB');
        """,
    )

    # Task 2: Expire snapshots older than 30 days to free S3 storage
    expire_snapshots = TrinoOperator(
        task_id='expire_old_snapshots',
        trino_conn_id='trino_lakehouse_conn',
        sql="""
        ALTER TABLE lakehouse.cdrs.voice_lab 
        EXECUTE expire_snapshots(retention_threshold => '30d');
        """,
    )

    # Task 3: Remove orphaned files from MinIO storage
    remove_orphan_files = TrinoOperator(
        task_id='remove_orphan_files',
        trino_conn_id='trino_lakehouse_conn',
        sql="""
        ALTER TABLE lakehouse.cdrs.voice_lab 
        EXECUTE remove_orphan_files(retention_threshold => '7d');
        """,
    )

    compact_voice_cdrs >> expire_snapshots >> remove_orphan_files
```

---

#### Pattern 2: Daily Financial Reconciliation DAG
*Purpose*: Executes at 00:30 AM every morning. Reconciles e-Dahab mobile money transactions against Core Banking balances and validates zero data loss.

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.operators.email import EmailOperator

default_args = {
    'owner': 'finance-data',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
}

with DAG(
    'edahab_eod_reconciliation',
    default_args=default_args,
    schedule_interval='30 0 * * *', # Daily at 00:30 AM
    catchup=False,
    tags=['banking', 'edahab', 'reconciliation'],
) as dag:

    # Run audit query comparing ledger total against bank vault balance
    reconcile_balances = TrinoOperator(
        task_id='reconcile_daily_balances',
        trino_conn_id='trino_lakehouse_conn',
        sql="""
        INSERT INTO lakehouse.banking.daily_reconciliation_audit
        SELECT 
            CURRENT_DATE - INTERVAL '1' DAY AS audit_date,
            SUM(CASE WHEN txn_type = 'DEPOSIT' THEN amount_usd ELSE 0 END) AS total_deposits,
            SUM(CASE WHEN txn_type = 'WITHDRAWAL' THEN amount_usd ELSE 0 END) AS total_withdrawals,
            COUNT(*) AS total_transactions,
            NOW() AS audit_generated_at
        FROM lakehouse.banking.transactions_raw
        WHERE el_record_date = CURRENT_DATE - INTERVAL '1' DAY;
        """,
    )

    send_reconciliation_email = EmailOperator(
        task_id='send_reconciliation_report',
        to='finance-audit@dahabshiil.com',
        subject='Dahabshiil Bank: EOD Reconciliation Audit Complete - {{ ds }}',
        html_content="""
        <h3>Daily e-Dahab Reconciliation Audit Completed Successfully</h3>
        <p>The automated Lakehouse audit has reconciled all transactions for date: <b>{{ ds }}</b>.</p>
        <p>Please inspect the Trino table <code>lakehouse.banking.daily_reconciliation_audit</code> for details.</p>
        """,
    )

    reconcile_balances >> send_reconciliation_email
```

---

# Part 9: Cross-Platform Interoperability & Master Troubleshooting Matrix

### 9.1 End-to-End Enterprise Data Path

```text
SFTP Server (.p files)
  │ (ListSFTP Primary Node ➔ FetchSFTP Round Robin)
  ▼
NiFi [EXTRACT Group]
  │ (In-Flight Byte Stream)
  ▼
NiFi [TRANSFORM Group] ──(Validation)──▶ Confluent Schema Registry (:8081)
  │ (ConvertRecord CSV ➔ Avro)
  │ (UpdateRecord: RecordPath extracts /create_date ➔ el_record_date)
  ▼
NiFi [LOAD Group]
  │ (MergeRecord: Bin-packing 50k-250k rows correlated on el_record_date)
  ▼
PutIcebergRecord
  │ (File IO multipart write)
  ├──▶ MinIO S3 (s3://big-data/warehouse/cdrs/voice_lab/)
  │ (Metadata snapshot commit)
  └──▶ Polaris REST Catalog (:8181/api/catalog)
         │
         ▼
Trino Distributed SQL (:8080)
  ▲
  │ (Nightly Compaction & EOD Reconciliation)
Airflow Orchestrator (:8080)
```

---

### 9.2 The Top 20 Cross-Platform Failure Scenarios & Exact Fixes

| # | Error / Symptom | Root Cause | Exact Solution |
| :--- | :--- | :--- | :--- |
| **1** | `NiFi: Sensitivity of the parameter does not match property` | Sensitive parameter applied to non-sensitive property (or vice versa). | Delete parameter from context and re-add matching the exact sensitivity checkbox of the property. |
| **2** | `PutIcebergRecord: Unmatched Column Behavior FAIL` | Avro schema has fields missing from Iceberg table or mismatched names (`record_date` vs `el_record_date`). | Register matching Avro schema in Confluent Schema Registry (`voice-cdr-partitioned`) with identical column names and types. |
| **3** | `ClassCastException: String cannot be cast to LocalDate` | Date column declared as plain `string` in Avro schema. | Declare date column with Avro logical type: `{"type": "int", "logicalType": "date"}`. |
| **4** | `MinIO: 400 Bad Request / 403 Forbidden` | Client attempting virtual-host addressing (`bucket.minio:9000`). | In `S3IcebergFileIOProvider`, set **`Path Style Access: true`**. |
| **5** | `Polaris: Credential vending requested but disabled` | Polaris configured to vend AWS STS tokens without cloud IAM provider. | In `polaris-iceberg-catalog`, set **`Access Delegation Strategy: disabled`**. |
| **6** | `NiFi: RecordPath formula written as literal string` | `Replacement Value Strategy` left on `Literal Value`. | Switch `Replacement Value Strategy` to **`Record Path Value`** in `UpdateRecord`. |
| **7** | `NiFi: Duplicate files listed on SFTP` | `ListSFTP` scheduled on `All Nodes`. | Set `Execution: On Primary Node` in `ListSFTP` Scheduling tab. |
| **8** | `NiFi: All downloads execute on single worker node` | Missing load balancing after Primary Node listing. | On connection `ListSFTP ➔ FetchSFTP`, set **`Load Balance Strategy: Round Robin`**. |
| **9** | `NiFi: load-out port accumulating queued FlowFiles` | `load-out` port left unconnected on the parent canvas. | Connect `LOAD.load-out` to a **Success Funnel** on the top-level canvas to act as the completion sink. |
| **10**| `Kafka: UnknownTopicOrPartitionException` | Producer publishing to a topic that does not exist and auto-creation is disabled. | Create topic explicitly via AKHQ or `kafka-topics --create --topic <name> --partitions 3`. |
| **11**| `Kafka: SerializationException (Magic Byte Mismatch)` | Consumer expecting Confluent 5-byte header, but raw payload was published. | Ensure writer uses `Confluent Schema Registry Payload` strategy or reader uses `Embedded Avro`. |
| **12**| `Trino: Table 'cdrs.voice_lab' not found in lakehouse` | Table registered under a different Polaris namespace or warehouse name. | Run `SHOW SCHEMAS FROM lakehouse;` and `SHOW TABLES FROM lakehouse.<namespace>;` to verify exact catalog paths. |
| **13**| `Trino: Query exceeds memory limit per node` | Large unpartitioned table scan or Cartesian join without broadcast limit. | Ensure queries filter on partition columns (`el_record_date`) to leverage Iceberg manifest pruning. |
| **14**| `Trino: Iceberg REST catalog authentication failed (401)` | OAuth2 client secret expired or mismatched scope. | Verify client credentials in `lakehouse.properties` with `iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL`. |
| **15**| `Airflow: TrinoOperator fails with Connection refused` | Airflow connection pointing to localhost instead of Trino coordinator pod/service. | Set Connection Host to `trino-coordinator.trino.svc.cluster.local` and port `8080`. |
| **16**| `Airflow: DAG not showing in Web UI` | Python syntax error in DAG file or missing `DAG` import. | Run `python3 dags/<dag_file>.py` in terminal to check for Python compilation errors. |
| **17**| `Airflow: Task stuck in queued state indefinitely` | Worker pool exhausted or Celery/Kubernetes executor cannot reach Redis/DB. | Check Airflow scheduler logs: `kubectl logs -n airflow airflow-scheduler-...`. |
| **18**| `Iceberg: CommitFailedException (Conflict)` | Two concurrent writers attempting to commit conflicting changes to the same snapshot. | Implement retry loop in writer or serialize commits through a single queue/NiFi writer. |
| **19**| `MonitorActivity: Generating continuous false quiet alerts` | Processor reporting node not set to Primary, or threshold shorter than batch generation interval. | Set `Reporting Node: Primary Node`. Adjust `Threshold Duration` to be 2x the generator interval. |
| **20**| `MinIO: NoSuchBucket (404) during Iceberg commit` | Polaris warehouse location points to a bucket that was not created in MinIO. | Create bucket via MinIO Console (`mc mb samo-minio/big-data`) before creating Polaris warehouse. |

---
*Authored for Telesom & Dahabshiil Group — Big Data Platform Training Programme.*
