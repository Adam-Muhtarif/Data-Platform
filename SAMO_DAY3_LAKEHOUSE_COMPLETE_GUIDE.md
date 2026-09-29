# Telesom & Samo Data Platform — Day 3 Lakehouse Master Guide
## Apache Iceberg, Apache Polaris, Apache NiFi, MinIO & Trino Architecture Blueprint
### (Complete Dashboard-Driven & Visual Web UI Implementation Edition)

---

## Executive Table of Contents
1. **Comprehensive Theoretical Summary of Day 3 Material**
   - 1.1 The Lakehouse Paradigm: Storage vs. Table Format vs. Catalog
   - 1.2 Schema Governance & External Confluent Schema Registry
   - 1.3 In-Flight vs. At-Rest Serialization (Avro vs. Parquet)
   - 1.4 Apache Parquet Internal Anatomy & Query Acceleration
   - 1.5 Apache Iceberg Architecture, Snapshot Tree & ACID Guarantees
   - 1.6 REST Catalog Standard & Apache Polaris Architecture
   - 1.7 Stream Routing & Small-Files Mitigation in NiFi
   - 1.8 End-to-End Schema Evolution Dynamics (`roaming_flag`)
   - 1.9 Operational Table Maintenance & Multi-Engine Consumption
2. **Cluster Topology & Dashboard Access Directory (`samo` Namespace)**
3. **Step-by-Step Dashboard Implementation Guide**
   - **Step 1: SFTP Source Layer Configuration & CDR Generator (Server Edge)**
   - **Step 2: Object Storage Bucket Provisioning (MinIO Web Console Dashboard)**
   - **Step 3: Schema Registry Subject Creation (AKHQ / Kafka Web Dashboard)**
   - **Step 4: Inspecting Polaris Catalog Metastore (DataGrip / DBeaver DB GUI)**
   - **Step 5: Iceberg Schema & Table Creation (DataGrip / Trino SQL GUI)**
   - **Step 6: End-to-End Visual Pipeline Construction (Apache NiFi Canvas UI)**
     - *6.1 Configuring & Enabling Controller Services in NiFi UI*
     - *6.2 Drag-and-Drop Canvas Assembly: ListSFTP ➔ FetchSFTP ➔ Routing*
     - *6.3 Dual-Branch Visual Routing: Raw Archive vs. Lakehouse*
     - *6.4 Record Mutation & Date Partitioning (UpdateRecord UI)*
     - *6.5 Small-Files Mitigation (MergeRecord Bin-Packing UI)*
     - *6.6 Writing & Committing to Iceberg (PutIceberg UI)*
     - *6.7 Visual Queues, Backpressure & Pipeline Activation*
   - **Step 7: Real-Time Analytics & Snapshot Auditing (DataGrip & Trino UI)**
   - **Step 8: Verifying Physical Parquet & Manifests (MinIO Console UI)**
   - **Step 9: Automated Table Maintenance Procedures (SQL Execution)**
   - **Step 10: Complete Troubleshooting & Error Reference Matrix**
4. **Day 3 Knowledge Verification & Assessment Answers**

---

# Part 1: Comprehensive Summary of Day 3 Training Material

### 1.1 From Files to Tables — The Lakehouse Architecture
In traditional big data deployments (Day 1 & Day 2), ingestion pipelines land raw telecommunication Call Detail Records (CDRs) directly onto object storage (such as MinIO or AWS S3) as raw flat files (e.g., pipe-delimited `.p` files) partitioned strictly by directory paths:
```text
voice-cdr-raw/voice-cdr/record_date=2026-09-08/load_date=2026-09-08/cdr_voice_20260908131158_690.p
```
While this pattern serves as an immutable, low-cost audit archive, it fails as an enterprise analytics dataset due to four major deficiencies:
1. **No Schema Enforcement**: Files are unvalidated delimited text where schemas reside only in external NiFi registries rather than bound to the physical storage.
2. **No Table Semantics**: Query engines must recursively list storage prefixes, parse raw strings, and assume every file adheres to identical columns.
3. **No ACID Transactions**: Concurrent writes or mid-flight job failures expose dirty, partial, or torn reads to analytical consumers.
4. **No Historical Versioning**: Query engines cannot inspect how data looked before a batch load or roll back corrupted ingests.

The **Lakehouse Model** solves this by cleanly decoupling storage, metadata formats, and catalogs into three independent, pluggable layers:
- **Storage Layer (MinIO / S3)**: Manages pure byte blocks containing Parquet columnar data files and Iceberg metadata files side by side.
- **Table Format Layer (Apache Iceberg)**: A specification representing table state via an immutable hierarchical metadata tree (`metadata.json`, manifest lists, manifest files, data files), enabling full ACID guarantees without a locking server.
- **Catalog Layer (Apache Polaris / REST Catalog)**: An active, lightweight service that maps table names (`catalog.namespace.table`) to current top-level metadata pointers and performs atomic compare-and-swap (CAS) commits.

```text
┌─────────────────────────────────────────────────────────────┐
│ CATALOG: Apache Polaris (REST Spec)                         │
│ "Where is table X, and what is its current metadata file?"  │
│ - Namespaces, table pointers, atomic commits, RBAC          │
└───────────────────────────┬─────────────────────────────────┘
                            │ points to current metadata.json
┌───────────────────────────▼─────────────────────────────────┐
│ TABLE FORMAT: Apache Iceberg                                │
│ "What files compose table X right now, with what schema?"   │
│ - metadata.json, manifest lists, manifest files, snapshots  │
└───────────────────────────┬─────────────────────────────────┘
                            │ references
┌───────────────────────────▼─────────────────────────────────┐
│ STORAGE: MinIO S3 Object Storage                            │
│ "The physical bytes at rest"                                │
│ - Parquet data files + Iceberg metadata/manifest files      │
└─────────────────────────────────────────────────────────────┘
```

---

### 1.2 Schema Governance & External Confluent Schema Registry
While NiFi's internal `AvroSchemaRegistry` governs schemas within a single cluster, external consumers across Kafka, microservices, and billing engines require a single centralized source of truth.

The **Confluent Schema Registry** provides a decoupled REST API using the following core primitives:
- **Subject**: Scope for schema evolution, typically `<topic>-value` (e.g., `cdr.raw-value`).
- **Schema ID**: A globally unique integer assigned to each immutable schema version.
- **Wire Format**: Eliminates sending verbose schemas in every message payload. Producers prefix records with **`0x00` (1 magic byte)** + **4-byte big-endian Schema ID**, followed by the Avro binary payload. Consumers read the 4-byte ID, fetch the schema from cache/registry once, and decode payloads with near-zero overhead.

#### Compatibility Modes
| Mode | New Schema Must Be Able To... | Safe Changes Permitted | Rollout Sequence |
| :--- | :--- | :--- | :--- |
| **BACKWARD** *(Default)* | Read data written with previous schema | Delete optional fields; Add optional field with default | Update **Consumers** first, then Producers |
| **FORWARD** | Allow old schema to read new data | Add fields; Delete optional field | Update **Producers** first, then Consumers |
| **FULL** | Satisfy both Backward and Forward compatibility | Add or remove optional fields with defaults only | Any order |
| **NONE** | No compatibility validation enforced | Any change (High risk) | Manual coordination |

> **Golden Rule for Telecommunication CDRs**: Default to `BACKWARD` or `FULL`. When telecom switch vendors add fields, add them as nullable with defaults (`["null", "string"], "default": null`).

---

### 1.3 In-Flight vs. At-Rest Serialization: Avro vs. Parquet vs. JSON
| Format | Storage Orientation | Primary Target | Strengths |
| :--- | :--- | :--- | :--- |
| **Apache Avro** | Row-oriented | Data in motion (Kafka, NiFi) | Compact, schema-by-ID framing, low-latency single-row serialization. |
| **Apache Parquet** | Columnar | Data at rest (Lakehouse tables) | Heavy compression (Snappy/ZSTD), column projection, predicate pushdown. |
| **JSON** | Row / Document | Alerts, REST payloads, human debug | Flexible, nested, universally human-readable, high CPU overhead. |
| **Raw Bytes** | Original format | Compliance & Audit archive | 100% audit fidelity; raw source replay. |

---

### 1.4 Apache Parquet Internal Anatomy & Read Optimizations
Parquet stores data by column rather than by row. A file is divided horizontally into **Row Groups** (~128 MB default), which are subdivided into per-column **Column Chunks**, which are further split into **Pages** (~1 MB).
```text
┌────────────────────────────── Parquet File ──────────────────────────────┐
│ Magic Bytes: "PAR1"                                                      │
│ ┌─ Row Group 0 (~128MB) ───────────────────────────────────────────────┐ │
│ │ Column Chunk: cdr_id       ──▶ [Dictionary Page] [Data Pages...]     │ │
│ │ Column Chunk: status       ──▶ [Dictionary Page] [Data Pages...]     │ │
│ │ Column Chunk: waitduration ──▶ [Data Pages...]                       │ │
│ └──────────────────────────────────────────────────────────────────────┘ │
│ Footer: FileMetaData                                                     │
│ - Complete Schema & Field IDs                                            │
│ - Per-Row-Group Statistics: min_value, max_value, null_count, encodings   │
│ - Footer Length (4 bytes) + Magic Bytes: "PAR1"                          │
└──────────────────────────────────────────────────────────────────────────┘
```
- **Column Projection**: A query like `SELECT callingpartynumber, waitduration FROM cdr_raw` reads only the byte ranges of those two columns. The other 19 column chunks are completely bypassed on storage.
- **Predicate Pushdown & Row-Group Pruning**: For `WHERE status = '2'`, the engine inspects the Parquet footer statistics before decoding pages. If a row group's `status` has `min='0'` and `max='1'`, the entire 128 MB row group is skipped instantly.
- **Dictionary Encoding**: Repeated low-cardinality values (such as `cdr_type`, `status`, `mscaddress`) are stored as small integer dictionary indexes, shrinking file footprint by up to 80%.

---

### 1.5 Apache Iceberg Architecture, Metadata Tree & ACID Transactions
Apache Iceberg replaces folder conventions with an explicit, versioned metadata tree rooted at the catalog:
```text
Catalog Pointer (Polaris)
       │
       ▼
Table Metadata File (00004-<uuid>.metadata.json)
       │ Current Snapshot ID, Schema, Partition Specs
       ▼
Manifest List (snap-<snapshot_id>-<uuid>.avro)
       │ Partition boundaries & summary for each manifest
       ├─────────────────────────────────┐
       ▼                                 ▼
Manifest File (<uuid>-m0.avro)     Manifest File (<uuid>-m1.avro)
  - Data File 1 (Parquet)            - Data File 2 (Parquet)
  - Data File Stats (Min/Max/Null)   - Data File Stats (Min/Max/Null)
```
- **Atomic Commits via Compare-And-Swap (CAS)**:
  1. The writer writes new Parquet data files to object storage.
  2. The writer creates new manifest files and a new `metadata.json`.
  3. The writer requests the catalog to swap the pointer from version $v_n$ to $v_{n+1}$.
  4. If another writer committed concurrently, the swap fails, and the writer retries against the newly committed state. Readers never observe uncommitted or partial writes.
- **Hidden Partitioning**: Iceberg decouples logical partition definitions from physical folder paths. Partition transforms (e.g., `identity(record_date)`, `day(event_time)`, `bucket[16](callingpartynumber)`) allow users to query source columns directly (`WHERE event_time >= ...`), and Iceberg automatically prunes unneeded files without requiring artificial partition clauses.
- **Time Travel & Instant Rollbacks**: Historical snapshots are retained until explicitly expired. Queries can target past snapshots (`FOR SYSTEM_TIME AS OF ...`), and failed loads can be rolled back in milliseconds via metadata pointer rewrites without moving data bytes.

---

### 1.6 Catalog Standard & Apache Polaris
Apache Polaris (incubating) is an open-source, vendor-neutral implementation of the **Iceberg REST Catalog specification** originally created by Snowflake.
- **Stateless Application Tier**: Built on Java/Quarkus, exposing REST endpoints on port `8181` and admin metrics on `8182`.
- **Relational Metastore**: Persists catalog pointers, namespaces, table definitions, and security state in PostgreSQL without touching the underlying raw data files.
- **Credential Vending**: When an engine queries a table, Polaris validates permissions and vends short-lived, down-scoped storage credentials (or S3 access keys), preventing engines from holding permanent root keys to storage.
- **Role-Based Access Control (RBAC)**:
  $$\text{Principal} \longrightarrow \text{Principal Role} \longrightarrow \text{Catalog Role} \longrightarrow \text{Privilege (Grant on Table/Namespace)}$$

---

### 1.7 Stream Routing & Small-Files Mitigation in NiFi
1. **The Small-Files Problem**: In streaming pipelines, writing tiny batches directly creates thousands of small files and snapshots per hour, overwhelming catalog planning and degrading scan speeds.
2. **Mitigation (`MergeRecord`)**: Placing `MergeRecord` immediately prior to `PutIceberg` batches records using bin-packing (`min_records=50,000`, `max_records=250,000`, `max_bin_age=5 min`). Correlating on `${record_date}` guarantees that batches stay aligned with target partition boundaries.
3. **Multi-Stream Routing (`RouteOnAttribute`)**: Rather than running multiple SFTP pollers, a single ingestion pipeline ingests mixed files, derives `source` and `schema.name` from filename patterns via `UpdateAttribute`, and uses `RouteOnAttribute` to dispatch FlowFiles to their respective `voice_cdr`, `sms_cdr`, or `data_cdr` Iceberg tables. Non-matching files are routed to a dead-letter alert queue rather than auto-terminated.

---

### 1.8 End-to-End Schema Evolution (`roaming_flag` Example)
When a telecom provider introduces a new field (e.g., `roaming_flag`):
1. **Schema Registry**: Register version 2 with `"roaming_flag"` typed as `["null", "string"]`, default `null`.
2. **Iceberg Table DDL**: Run `ALTER TABLE lakehouse.voice.cdr_raw ADD COLUMN roaming_flag VARCHAR;` in Trino/Polaris. Iceberg assigns a unique integer field ID to the column without rewriting existing Parquet files.
3. **NiFi Ingestion**: PutIceberg receives records with the new field and safely maps them by column name.
4. **Historical Read Compatibility**: When Trino queries old Parquet files written prior to the evolution, the engine observes the missing field ID in the footer and automatically synthesizes `NULL` values on the fly.

---

# Part 2: Cluster Topology & Dashboard Access Directory

Before starting, ensure your local `/etc/hosts` (on Linux/macOS) or `C:\Windows\System32\drivers\etc\hosts` (on Windows) includes the ingress mapping for IP `169.58.218.71`:

```text
169.58.218.71   trino.transcode.com
169.58.218.71   minio-console.transcode.com
169.58.218.71   minio.transcode.com
169.58.218.71   polaris.transcode.com
169.58.218.71   kafkaui.transcode.com
169.58.218.71   schemaregistry.transcode.com
```

### Dashboard Directory Matrix:
| Dashboard / UI Tool | Web UI URL | Login Credentials | What You Use This Dashboard For |
| :--- | :--- | :--- | :--- |
| **Apache NiFi Canvas** | `https://169.58.218.71:31443/nifi` | User: `samo-admin`<br>Pass: `SamoNiFiPassword2026!` | Visual drag-and-drop pipeline design, controller service management, real-time FlowFile queue inspection. |
| **MinIO Web Console** | `http://169.58.218.71:30901` | User: `samo-admin`<br>Pass: `SamoSecureMinioPass2026!` | Visual bucket provisioning, browsing Iceberg metadata JSON trees, inspecting Parquet file parts. |
| **AKHQ (Kafka & Schema UI)** | `http://kafkaui.transcode.com` *(or port 8080)* | None (Anonymous) | Browsing Kafka clusters (`samo-cluster` & `mathew-cluster`), registering Avro schemas, inspecting topics (`telesom-cdrs-voice`, `telesom-cdrs-sms`, `telesom-cdrs-data`), consumer lag, and live message tracing. |
| **JetBrains DataGrip / DBeaver** | Host: `trino.transcode.com:80`<br>Catalog: `lakehouse` | User: `samo-admin`<br>Pass: *(Leave empty)* | Visual SQL database explorer, Iceberg table creation, analytical queries, snapshot table inspection. |
| **Trino Web UI Console** | `http://trino.transcode.com` | User: `samo-admin` *(or `admin`)* | Real-time query execution monitoring, stage graphs, worker memory pools, I/O throughput. |
| **PostgreSQL Metastore GUI** | DataGrip via SSH Tunnel: `localhost:5433` | User: `postgres`<br>Pass: `cHMKJDQMmQ4ZnIii`<br>DB: `polaris` | Visual inspection of Polaris internal entities, catalog registrations, and RBAC grants. |

> [!IMPORTANT]
> **Kafka Cluster Topology & Cleaned AKHQ State**:
> - **`suber-cluster` is completely removed** from all server configurations and AKHQ dashboards.
> - **`mathew-cluster` is 100% preserved** in namespace `mathew` with its original 3-broker configuration and `strmzi-connect`.
> - **`samo-cluster` is fully active** in namespace `samo` configured with **minimum resources** (1 replica dual-role controller/broker, `500m` CPU / `1Gi` RAM request, `10Gi` storage) and connected to `samo-connect` and Confluent Schema Registry.
> - When opening `http://kafkaui.transcode.com`, the cluster selector now cleanly toggles between **`samo-cluster`** and **`mathew-cluster`**.

---

# Part 3: Step-by-Step Dashboard Implementation Guide

---

### Step 1: SFTP Source Layer Configuration & CDR Generator (Server Edge)

#### WHAT WE ARE DOING:
Starting the synthetic telecom CDR generator on the server drop path.

#### WHY WE ARE DOING IT:
Simulates telecommunication MSC switches generating real-time pipe-delimited records.

#### GOAL:
Continuously produce files matching `cdr_voice_*.p` into `/home/samo/SFTP/VOICE/voice_cdrs/`.

#### Execution on Server:
```bash
ssh samo@169.58.218.161
# Password: 3682154Aa1!

cd ~/SFTP/VOICE
mkdir -p voice_cdrs voice_cdrs_backup
chmod 777 voice_cdrs voice_cdrs_backup
python3 cdr_file_generator.py &
```

---

### Step 2: Object Storage Bucket Provisioning (MinIO Web Console Dashboard)

#### WHAT WE ARE DOING:
Using the **MinIO Web Console UI** to visually create dedicated, isolated S3 buckets.

#### WHY WE ARE DOING IT:
Using the graphical console provides immediate visual confirmation of storage quotas, encryption policies, and bucket hierarchies without typing manual S3 CLI commands.

#### GOAL:
Create the following buckets on `samo-minio`:
- `voice-cdr-raw`: For immutable raw file archiving (Day 1 & Day 2 audit copy).
- `warehouse`: For Apache Iceberg Parquet data files and metadata trees.
- `samo-lakehouse`: For personal queries and staging.

#### Visual Steps in the MinIO Dashboard:
1. Open your browser and navigate to: **`http://169.58.218.71:30901`**.
2. Log in with credentials:
   - **Username**: `samo-admin`
   - **Password**: `SamoSecureMinioPass2026!`
3. On the left navigation sidebar under **Administrator**, click **Buckets**.
4. In the top-right corner, click the orange **Create Bucket** button.
5. Create the first bucket:
   - **Bucket Name**: `voice-cdr-raw`
   - Leave Versioning, Locking, and Quota at defaults.
   - Click **Create Bucket**.
6. Repeat the process to create the remaining buckets:
   - Click **Create Bucket** ➔ Name: `warehouse` ➔ Click **Create Bucket**.
   - Click **Create Bucket** ➔ Name: `samo-lakehouse` ➔ Click **Create Bucket**.
7. Confirm that all three buckets appear in the visual list with green checkmarks.

---

### Step 3: Schema Registry Subject Creation (AKHQ / Kafka Web Dashboard)

#### WHAT WE ARE DOING:
Using the **AKHQ Web UI** to register the official 19-column telecom CDR Avro schema.

#### WHY WE ARE DOING IT:
The web console lets you visually edit the Avro JSON schema, select compatibility rules (`BACKWARD`), and inspect schema versions with syntax validation.

#### GOAL:
Register the schema under subject `voice-cdr-value` so NiFi and Kafka consumers share a single schema contract.

#### Visual Steps in the AKHQ / Schema Dashboard:
1. Open your browser and navigate to: **`http://kafkaui.transcode.com`** *(or `http://169.58.218.71`)*.
2. In the left navigation menu, click **Schema Registry**.
3. In the top-right corner, click **Create a Subject** (or **+ Add Schema**).
4. Fill in the form:
   - **Subject Name**: `voice-cdr-value`
   - **Compatibility Level**: Select `BACKWARD` from the dropdown.
   - **Schema Type**: `AVRO`
5. In the **Schema Definition** code editor, paste the following Avro schema:
   ```json
   {
     "type": "record",
     "name": "VoiceCDR",
     "namespace": "com.telesom.cdr",
     "fields": [
       {"name": "cdr_id", "type": "string"},
       {"name": "cdr_type", "type": "string"},
       {"name": "status", "type": "string"},
       {"name": "create_date", "type": "string"},
       {"name": "start_date", "type": "string"},
       {"name": "end_date", "type": "string"},
       {"name": "pri_identity", "type": "string"},
       {"name": "actual_usage", "type": "string"},
       {"name": "callingpartynumber", "type": "string"},
       {"name": "calledpartynumber", "type": "string"},
       {"name": "callingpartyimsi", "type": "string"},
       {"name": "calledpartyimsi", "type": "string"},
       {"name": "dialednumber", "type": "string"},
       {"name": "callingcellid", "type": "string"},
       {"name": "chargingtime", "type": "string"},
       {"name": "waitduration", "type": "string"},
        {"name": "callreferencenumber", "type": "string"},
        {"name": "imei", "type": "string"},
        {"name": "mscaddress", "type": "string"},
        {"name": "record_date", "type": ["null", {"type": "int", "logicalType": "date"}], "default": null},
        {"name": "load_date", "type": ["null", {"type": "long", "logicalType": "timestamp-micros"}], "default": null}
      ]
    }
    ```
 6. Click **Save / Submit**.
 7. Confirm that `voice-cdr-value` (along with `voice-cdr` and `voice_cdr`) is registered.
> [!IMPORTANT]
> **Why all 21 fields and explicit logicalTypes are strictly required**:
> 1. **21-Field Table Alignment**: The Iceberg table `lakehouse.voice.cdr_raw` contains **21 columns** (the 19 raw CDR fields + `record_date` + `load_date`).
>    If the schema only contains 19 fields, `PutIcebergRecord` fails with:
>    `java.lang.ArrayIndexOutOfBoundsException: Index 19 out of bounds for length 19`
> 2. **Logical Types vs ClassCastException**: If `record_date` is typed as plain `"string"`, `PutIcebergRecord` throws:
>    `ClassCastException: class java.lang.String cannot be cast to class java.time.LocalDate`
>    Declaring `record_date` as Avro logical type `date` (int) and `load_date` as `timestamp-micros` (long) allows NiFi's `voice-csv-reader` to parse them directly into native `java.time.LocalDate` and `java.time.LocalDateTime` instances required by Iceberg's Parquet writer!
> 3. **Subject Aliases**: Both **`voice-cdr`**, **`voice_cdr`**, and **`voice-cdr-value`** must have this 21-field schema registered so that all NiFi reader and writer components resolve it immediately.

---

### Step 4: Inspecting Polaris Catalog Metastore (DataGrip / DBeaver DB GUI)

#### WHAT WE ARE DOING:
Connecting DataGrip or DBeaver to the underlying PostgreSQL database (`10.0.0.8:5432`) via an SSH tunnel to visually explore the Polaris catalog metastore.

#### WHY WE ARE DOING IT:
Polaris stores all catalog namespaces, tables, and RBAC grants in a relational database. Viewing this GUI proves how Iceberg decouples metadata storage from file storage.

#### Visual Steps in DataGrip / DBeaver:
1. Open **DataGrip** (or **DBeaver**).
2. Click **+ (New Data Source)** ➔ **PostgreSQL**.
3. In the **General** tab:
   - **Name**: `Polaris-Catalog-Postgres`
   - **Host**: `10.0.0.8`
   - **Port**: `5432`
   - **Database**: `polaris`
   - **User**: `postgres`
   - **Password**: `cHMKJDQMmQ4ZnIii`
4. Click the **SSH/SSL** tab:
   - Check **Use SSH tunnel**.
   - **Proxy Host**: `169.58.218.161`
   - **Port**: `22`
   - **User**: `samo`
   - **Authentication**: Password ➔ `3682154Aa1!`
5. Click **Test Connection** ➔ Once green, click **Apply** and **OK**.
6. In the Database Tree, expand `polaris` ➔ `schemas` ➔ `polaris_schema` ➔ `tables`:
   - Double-click `entities`: Visually see registered catalogs and namespaces.
   - Double-click `principal_authentication_data`: Inspect OAuth2 client IDs.

---

### Step 5: Iceberg Schema & Table Creation (DataGrip / Trino SQL GUI)

#### WHAT WE ARE DOING:
Using DataGrip's SQL editor to create the `voice` schema and `cdr_raw` Iceberg table in the `lakehouse` catalog.

#### WHY WE ARE DOING IT:
A GUI SQL editor provides auto-completion, schema tree visualization, and immediate syntax verification before starting data ingestion.

#### GOAL:
Establish the 21-column Iceberg table partitioned by `record_date` in MinIO storage via Polaris.

#### Visual Steps in DataGrip / Trino:
1. In DataGrip, click **+ (New Data Source)** ➔ **Trino**.
2. In the **General** tab:
   - **Name**: `Samo-Lakehouse-Trino`
   - **Host**: `trino.transcode.com` *(or `169.58.218.71`)*
   - **Port**: `80`
   - **User**: `samo-admin` (Password: empty)
   - **Catalog**: `lakehouse`
3. Click **Test Connection** ➔ Click **OK**.
4. Open a **New SQL Console** and execute:

```sql
-- 1. Create the voice domain namespace
CREATE SCHEMA IF NOT EXISTS lakehouse.voice;

-- 2. Create the 21-column Iceberg table partitioned by record_date
CREATE TABLE IF NOT EXISTS lakehouse.voice.cdr_raw (
    cdr_id VARCHAR,
    cdr_type VARCHAR,
    status VARCHAR,
    create_date VARCHAR,
    start_date VARCHAR,
    end_date VARCHAR,
    pri_identity VARCHAR,
    actual_usage VARCHAR,
    callingpartynumber VARCHAR,
    calledpartynumber VARCHAR,
    callingpartyimsi VARCHAR,
    calledpartyimsi VARCHAR,
    dialednumber VARCHAR,
    callingcellid VARCHAR,
    chargingtime VARCHAR,
    waitduration VARCHAR,
    callreferencenumber VARCHAR,
    imei VARCHAR,
    mscaddress VARCHAR,
    record_date DATE,
    load_date TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['record_date']
);
```
5. In the Database Tree, right-click `lakehouse` ➔ **Refresh**. Expand `voice` ➔ `Tables` to see `cdr_raw` visually appear with all 21 columns!

---

### Step 6: End-to-End Visual Pipeline Construction (Apache NiFi Canvas UI)

#### Canvas Architecture Overview:
```text
[SFTP: voice_cdrs]
       │
       ▼
   ListSFTP ──▶ FetchSFTP ──▶ UpdateAttribute ──▶ RouteOnAttribute
                                                        │ (voice)
                                                        ▼
                                                  UpdateRecord
                                                        │
                         ┌──────────────────────────────┴──────────────────────────────┐
                         ▼                                                             ▼
                    PutS3Object                                                   MergeRecord
               (Raw MinIO Archive)                                           (Bin-Packing 50k-250k)
                                                                                       │
                                                                                       ▼
                                                                                  PutIceberg
                                                                           (lakehouse.voice.cdr_raw)
```

---

#### 6.1 Configuring & Enabling Controller Services in NiFi UI

1. Open your browser to the NiFi Web Canvas: **`https://169.58.218.71:31443/nifi`**.
   - **Username**: `samo-admin`
   - **Password**: `SamoNiFiPassword2026!`
2. In the **Operate Palette** on the left side, click the **Gear icon (Configure)**.
3. Click the **Controller Services** tab.
4. Click the **`+` (Add Service)** button to add and configure each service:

##### Service A: `ConfluentSchemaRegistry`
- **Name**: `ConfluentSchemaRegistry`
- **Properties Tab**:
  - `Schema Registry URL`: `http://schemaregistry.confluent.svc:8081`
- Click **Apply**.

##### Service B: `CSVReader`
- **Name**: `voice-csv-reader`
- **Properties Tab**:
  - `Schema Access Strategy`: `Use 'Schema Name' Property`
  - `Schema Registry`: Select `ConfluentSchemaRegistry`
  - `Schema Name`: `voice-cdr`
  - `Value Separator`: `|`
  - `Treat First Line as Header`: `false`
  - `Date Format`: *(Leave blank / empty — allows automatic epoch/ISO resolution)*
  - `Timestamp Format`: `yyyy-MM-dd HH:mm:ss.SSS`
- Click **Apply**.

##### Service C: `CSVRecordSetWriter` *(or `AvroRecordSetWriter`)*
> [!NOTE]
> **Why `ParquetRecordSetWriter` is NOT used here**:
> In Day 3 PDF (Sections 3.4, 4.6 & Knowledge Check Question #7), the lab explicitly states:
> *"In the Day 3 flow you never configure a Parquet writer directly — PutIceberg writes Parquet for you internally."*
> For in-flight record processors (`UpdateRecord` and `MergeRecord`), use **`CSVRecordSetWriter`** (or **`AvroRecordSetWriter`**). `PutIcebergRecord` then reads these records and converts them to compressed Parquet automatically!
- **Service Name**: `csv-writer` (or `avro-writer`)
- **Properties Tab**:
  - `Schema Access Strategy`: `Inherit Record Schema`
  - `Value Separator`: `|` *(if CSV)*
  - `Include Header Line`: `false`
  - `Date Format`: *(Leave blank / empty)*
  - `Timestamp Format`: `yyyy-MM-dd HH:mm:ss.SSS`
- Click **Apply**.

##### Service D: `S3IcebergFileIOProvider` *(Required by RESTIcebergCatalog)*
> [!IMPORTANT]
> `RESTIcebergCatalog` requires a **File IO Provider** to read/write Parquet data files and metadata manifests on MinIO.
> **Critical Endpoint & Credentials**: Must point to the cluster MinIO instance hosting the Polaris `bigdata_catalog` warehouse (`s3://big-data/warehouse/`) so that both NiFi and Trino access the identical physical storage!
- **Service Name**: `s3-iceberg-file-io`
- **Controller Service Type**: `S3IcebergFileIOProvider` *(search `Iceberg` or `FileIO`)*
- **Properties Tab**:
  - `Access Key ID`: `BnMtc7hhYG705rlXFcdj`
  - `Secret Access Key`: `VZP8Xcv7RHPUIRmiPZH1tCNOwqJUtmuJ8tvqv2ok`
  - `Endpoint URL`: `http://minio.minio.svc.cluster.local:9000`
  - `Client Region`: `us-east-1`
  - `Path Style Access`: `true` *(Critical for MinIO!)*
- Click **Apply**.

##### Service E: `RESTIcebergCatalog` *(Exact NiFi 2.x Service Name)*
- **Service Name**: `polaris-iceberg-catalog`
- **Controller Service Type**: `RESTIcebergCatalog` *(search `Iceberg` or `REST`)*
- **Properties Tab**:
  - `Catalog URI`: `http://polaris.polaris.svc.cluster.local:8181/api/catalog`
  - `File IO Provider`: Select **`s3-iceberg-file-io`** *(resolves the File IO Provider required error!)*
  - `Access Delegation Strategy`: **`disabled`** *(Critical: Polaris on-prem does not vend MinIO STS credentials. Disabling delegates I/O to s3-iceberg-file-io!)*
  - `Authentication Strategy`: `OAuth 2.0`
  - `Authorization Server URI`: **`http://polaris.polaris.svc.cluster.local:8181/api/catalog/v1/oauth/tokens`** *(resolves the Authorization Server URI required error!)*
  - `Authorization Grant Type`: `Client Credentials`
  - `Client ID`: `d5f9cdea48a6fb7f`
  - `Client Secret`: `ae88731a268f5c0e33e593508fad6fcf`
  - `Access Token Scopes`: `PRINCIPAL_ROLE:ALL`
  - `Warehouse Location`: `bigdata_catalog`
- Click **Apply**.

##### Service F: `ParquetIcebergWriter` *(Required by PutIcebergRecord)*
> [!IMPORTANT]
> `PutIcebergRecord` requires an **Iceberg Writer** controller service to write Parquet files. Add this service!
- **Service Name**: `parquet-iceberg-writer`
- **Controller Service Type**: `ParquetIcebergWriter` *(search `Parquet` or `Iceberg`)*
- **Properties Tab**: No custom properties needed (uses defaults).
- Click **Apply**.

##### Service G: `AWSCredentialsProviderControllerService`
- **Name**: `samo-minio-s3-creds`
- **Properties Tab**:
  - `Access Key`: `samo-admin`
  - `Secret Key`: `SamoSecureMinioPass2026!`
- Click **Apply**.

##### Enabling the Services:
Click the **Lightning Bolt (⚡)** icon next to each service in the list, choose **Service and referencing components**, and click **Enable**. All services will turn to a green **Enabled** state.

---

#### 6.2 Adding and Wiring Processors on the NiFi Canvas

Drag processor icons from the top toolbar onto the canvas and configure each one:

##### 1. `ListSFTP`
- **Settings Tab**: Penalty Duration: `30 sec`.
- **Scheduling Tab**: Run Schedule: `10 sec`.
- **Properties Tab**:
  - `Hostname`: `169.58.218.161`
  - `Port`: `22`
  - `Username`: `samo`
  - `Password`: `3682154Aa1!`
  - `Remote Path`: `/home/samo/SFTP/VOICE/voice_cdrs`
  - `Search Recursively`: `false`
  - `File Filter Regex`: `cdr_.*\.p`
- Click **Apply**.

##### 2. `FetchSFTP`
- Connect `ListSFTP` ➔ `FetchSFTP` (relationship: `success`).
- **Properties Tab**:
  - `Hostname`: `${sftp.remote.host}`
  - `Port`: `22`
  - `Username`: `samo`
  - `Password`: `3682154Aa1!`
  - `Remote File`: `${path}/${filename}`
  - `Completion Strategy`: `Move File`
  - `Move Destination Directory`: `/home/samo/SFTP/VOICE/voice_cdrs_backup`
- **Settings Tab**: Auto-terminate `comms.failure`, `not.found`, `permission.denied`.
- Click **Apply**.

##### 3. `UpdateAttribute`
- Connect `FetchSFTP` ➔ `UpdateAttribute` (relationship: `success`).
- **Properties Tab**: Click `+` to add dynamic properties:
  - `source`: `${filename:substringAfter('cdr_'):substringBefore('_')}`
  - `schema.name`: `${source}_cdr`
  - `record_date`: `${filename:substringAfter('cdr_voice_'):substring(0,8):toDate('yyyyMMdd'):format('yyyy-MM-dd')}` *(extracts e.g. 2026-09-13 from filename for S3 archiving and MergeRecord bin-packing)*
- Click **Apply**.

##### 4. `RouteOnAttribute`
- Connect `UpdateAttribute` ➔ `RouteOnAttribute` (relationship: `success`).
- **Properties Tab**:
  - `Routing Strategy`: `Route to Property name`
  - Click `+` to add dynamic properties:
    - `voice`: `${source:equals('voice')}`
    - `sms`: `${source:equals('sms')}`
    - `data`: `${source:equals('data')}`
    - `ats`: `${schema.name:equals('ats_cdr')}`
- Route the `unmatched` relationship to a dead-letter funnel (do **not** auto-terminate!).
- Click **Apply**.

##### 5. `UpdateRecord`
- Connect `RouteOnAttribute` ➔ `UpdateRecord` (relationship: `voice`).
- **Properties Tab**:
  - `Record Reader`: Select `voice-csv-reader`.
  - `Record Writer`: Select `csv-writer` *(or `avro-writer`)*.
  - `Replacement Value Strategy`: `Literal Value`
  - Click `+` to add dynamic record path properties:
    - `/record_date`: `${record_date:replaceEmpty('${now():format("yyyy-MM-dd")}')}`
    - `/load_date`: `${now():toNumber()}`
- **Settings Tab**: Auto-terminate `failure`.
- Click **Apply**.

---

#### 6.3 Configuring Dual-Branch Sinks: Raw Archive vs. Lakehouse

From `UpdateRecord`'s `success` relationship, branch into two parallel targets:

##### Branch 1: `PutS3Object` (Raw Immutable Archive)
- Connect `UpdateRecord` ➔ `PutS3Object` (relationship: `success`).
- **Properties Tab**:
  - `Bucket`: `voice-cdr-raw`
  - `Object Key`: `voice-cdr/record_date=${record_date:replaceEmpty('${now():format("yyyy-MM-dd")}')}/load_date=${now():format('yyyy-MM-dd')}/${filename}`
  - `Endpoint Override URL`: `http://samo-minio.samo.svc.cluster.local:9000` *(or `http://169.58.218.71:30900`)*
  - `Region`: **`us-east-1`** *(Critical: MinIO's default region. If left as `us-west-2`, MinIO throws HTTP 400 Bad Request!)*
  - `Use Path Style Access`: `true`
  - `Use Chunked Encoding`: **`false`** *(Critical: MinIO rejects AWS SDK v2 chunked signature trailers!)*
  - `AWS Credentials Provider Service`: Select `samo-minio-s3-creds`
  - > [!CAUTION]
  - > **DO NOT add `Signer Override` as a dynamic property!** In NiFi 2.x, AWS SDK v2 uses SigV4 automatically. Adding `Signer Override` creates an illegal HTTP header with a space (`Signer Override: Signature V4`), causing MinIO to reject all PUT requests with `400 Bad Request: invalid header name`!
  - *Optional*: Clear out any default ACL properties (`FullControl User List`, `Canned ACL`, etc.) to prevent sending empty ACL headers to MinIO.
- **Settings Tab**: Auto-terminate `success`, `failure`.
- Click **Apply**.

##### Branch 2: `MergeRecord` (Small-Files Prevention)
- Connect `UpdateRecord` ➔ `MergeRecord` (relationship: `success`).
- **Properties Tab**:
  - `Record Reader`: Select `voice-csv-reader`.
  - `Record Writer`: Select `csv-writer` *(or `avro-writer`)*.
  - `Merge Strategy`: `Bin-Packing`
  - `Minimum Number of Records`: `50000`
  - `Maximum Number of Records`: `250000`
  - `Max Bin Age`: `5 mins`
  - `Correlation Attribute Name`: `record_date` *(Critical: keeps bins partition-aligned!)*
- **Settings Tab**: Auto-terminate `failure`, `original`.
- Click **Apply**.

##### Branch 2 Terminal Sink: `PutIcebergRecord` *(Exact NiFi 2.x Processor Name)*
> [!IMPORTANT]
> In NiFi 2.x / 2.9.0, `PutIcebergRecord` supports **ONLY 5 properties**.
> **DO NOT click `+` (Add Property)** to add extra properties like `File Format`, `Maximum File Size`, `Number of Commit Retries`, or `Unmatched Column Behavior`! If you added them, delete them by clicking the trash can icon.
- Connect `MergeRecord` ➔ `PutIcebergRecord` (relationship: `merged`).
- **Properties Tab** *(Fill in these 5 properties only)*:
  - `Iceberg Catalog`: Select **`polaris-iceberg-catalog`** *(the `RESTIcebergCatalog` service)*.
  - `Iceberg Writer`: Select **`parquet-iceberg-writer`** *(the `ParquetIcebergWriter` service)*.
  - `Record Reader`: Select **`voice-csv-reader`** *(matches the reader/writer used in MergeRecord)*.
  - `Namespace`: **`voice`**.
  - `Table Name`: **`cdr_raw`**.
- **Settings Tab**: Auto-terminate `success`, `failure`.
- Click **Apply**.

---

#### 6.4 Visual Activation & Queue Monitoring in NiFi UI
1. Select all processors on the canvas (or right-click blank canvas ➔ **Start**).
2. Watch the processor status boxes turn green with a running icon `▶`.
3. Notice the FlowFiles moving through the queues:
   - Queues show visual meters with file counts and byte volumes.
   - `MergeRecord` holds records until the bin threshold is reached or 5 minutes elapse, then releases a single merged file to `PutIcebergRecord`.
   - `PutIcebergRecord` commits an append snapshot to Polaris and writes a Parquet file to MinIO.

> [!TIP]
> **Troubleshooting: `SchemaNotFoundException: Could not retrieve schema with name 'voice-cdr'`**:
> If `UpdateRecord` routes to `failure` with `Could not retrieve schema with name 'voice-cdr'`, this means the Confluent Schema Registry only had `voice-cdr-value` instead of `voice-cdr`.
> - **Fix**: Both `voice-cdr` and `voice_cdr` have been registered under Schema ID `1` in the cluster's Schema Registry.
> - **In NiFi**: Right-click the `failure` queue coming out of `UpdateRecord` ➔ **Empty Queue** (or re-route them back to `UpdateRecord`), then start `UpdateRecord`. New and pending FlowFiles will parse successfully!

---

### Step 7: Real-Time Analytics & Snapshot Auditing (DataGrip & Trino UI)

#### 1. Running Analytical Queries in DataGrip:
Open your SQL console in DataGrip connected to `Samo-Lakehouse-Trino`:

```sql
-- 1. Inspect real-time total CDR count
SELECT COUNT(*) AS total_records FROM lakehouse.voice.cdr_raw;

-- 2. Partition-pruned analytical query
SELECT 
    callingpartynumber,
    calledpartynumber,
    actual_usage,
    waitduration
FROM lakehouse.voice.cdr_raw
WHERE record_date = CURRENT_DATE
  AND status = '0';

-- 3. Audit Iceberg Snapshots (Visual table of commits)
SELECT 
    snapshot_id,
    committed_at,
    operation,
    summary['total-records'] AS total_rows,
    summary['added-data-files'] AS files_added
FROM lakehouse.voice."cdr_raw$snapshots"
ORDER BY committed_at DESC;

-- 4. Inspect Data Files Registered in the Catalog
SELECT file_path, file_format, record_count, file_size_in_bytes
FROM lakehouse.voice."cdr_raw$files";

-- 5. Time-Travel Query (view table state 15 minutes ago)
SELECT COUNT(*) 
FROM lakehouse.voice.cdr_raw 
FOR SYSTEM_TIME AS OF (CURRENT_TIMESTAMP - INTERVAL '15' MINUTE);
```

#### 2. Monitoring Live Execution in Trino Web UI:
1. Open your browser to **`http://trino.transcode.com`** (User: `samo-admin` / No password).
2. The dashboard displays:
   - **Active Workers & Threads**: Visual node topology.
   - **Cluster Memory Usage**: Real-time heap consumption.
   - **Live Queries**: Click any query to see the **Visual Execution Stage Graph**, showing input rows, splits, and I/O rates.

---

### Step 8: Verifying Physical Parquet & Manifests (MinIO Console UI)

1. Return to the **MinIO Web Console** (`http://169.58.218.71:30901`).
2. Click **Buckets** on the left menu:
   - Click `voice-cdr-raw`: Expand `voice-cdr/` ➔ `record_date=2026-09-13/` to see the original raw `.p` files stored as an audit archive.
   - Click `warehouse`: Expand `voice/cdr_raw/`:
     - **`metadata/`**: Observe `00000-<uuid>.metadata.json`, `snap-<id>.avro` (manifest lists), and manifest `.avro` files generated by Iceberg commits!
     - **`data/record_date=2026-09-13/`**: Observe clean, compressed `.parquet` files written by NiFi!

---

### Step 9: Automated Table Maintenance Procedures (SQL Execution)

Execute these maintenance commands periodically in DataGrip to keep the lakehouse performant:

```sql
-- 1. Hourly Compaction: Merge small Parquet files into ~128MB chunks
ALTER TABLE lakehouse.voice.cdr_raw EXECUTE optimize(file_size_threshold => '100MB');

-- 2. Daily Snapshot Expiration: Purge snapshots older than 7 days
ALTER TABLE lakehouse.voice.cdr_raw EXECUTE expire_snapshots(retention_threshold => '7d');

-- 3. Weekly Manifest Clustering: Re-cluster manifest lists
ALTER TABLE lakehouse.voice.cdr_raw EXECUTE rewrite_manifests;

-- 4. Remove Orphan Files: Delete uncommitted remnants
ALTER TABLE lakehouse.voice.cdr_raw EXECUTE remove_orphan_files(retention_threshold => '1d');
```

---

### Step 10: Complete Troubleshooting & Error Reference Matrix

Below is the definitive reference of all real-world issues encountered during Day 3 implementation, their underlying architectural root causes, and their exact verified solutions:

| # | Error / Symptom | Root Cause | Exact Solution |
| :--- | :--- | :--- | :--- |
| **1** | `Component is invalid: 'File IO Provider' is invalid because File IO Provider is required` | In NiFi 2.x, `RESTIcebergCatalog` does not perform direct S3 I/O itself. It requires a dedicated `File IO Provider` service to manage Parquet and manifest byte streams on object storage. | Create a `S3IcebergFileIOProvider` service named `s3-iceberg-file-io`. Enable it, then select it in `polaris-iceberg-catalog` under property `File IO Provider`. |
| **2** | `Component is invalid: 'Authorization Server URI' is invalid because Authorization Server URI is required` | `RESTIcebergCatalog` configured with `OAuth 2.0` requires an explicit token endpoint URL to request bearer tokens from Polaris. | Set `Authorization Server URI` to: `http://polaris.polaris.svc.cluster.local:8181/api/catalog/v1/oauth/tokens`. |
| **3** | `PutIcebergRecord: Credential vending was requested for table voice.cdr_raw, but no credentials are available` | `Access Delegation Strategy` in `polaris-iceberg-catalog` was set to `vended-credentials`. On-premise Polaris does not vend MinIO STS credentials. | Set `Access Delegation Strategy: disabled`. This tells NiFi to bypass catalog credential vending and use the direct credentials configured in `s3-iceberg-file-io`. |
| **4** | `PutS3Object: software.amazon.awssdk.services.s3.model.S3Exception: (Service: S3, Status Code: 400)` / `invalid header name` | 1. `Signer Override: Signature V4` was added as a dynamic property using `+`, which created an illegal HTTP header with a space.<br>2. Region was left as `us-west-2` instead of MinIO's `us-east-1`.<br>3. `Use Chunked Encoding` was `true`. | 1. Delete dynamic property `Signer Override`.<br>2. Set Region to `us-east-1` (`US East (N. Virginia)`).<br>3. Set `Use Chunked Encoding: false`.<br>4. Set `Use Path Style Access: true`. |
| **5** | `UpdateRecord: SchemaNotFoundException: Could not retrieve schema with name 'voice-cdr'` | Schema Registry only contained subject `voice-cdr-value`, but `voice-csv-reader` looked up `voice-cdr`. | Register the schema under both `voice-cdr` and `voice_cdr` in Schema Registry. |
| **6** | `PutIcebergRecord: java.lang.ArrayIndexOutOfBoundsException: Index 19 out of bounds for length 19` | The Trino Iceberg table `lakehouse.voice.cdr_raw` has **21 columns** (partition key `record_date` is column index `19`, `load_date` is `20`). Schema Registry only declared 19 fields, so records only contained indices `0..18`. | Register the full 21-column schema in Schema Registry, declaring `record_date` and `load_date` with `"default": null`. |
| **7** | `PutIcebergRecord: ClassCastException: class java.lang.String cannot be cast to class java.time.LocalDate` | `record_date` was registered as a plain Avro `"string"`. Iceberg's Parquet writer expects a native `java.time.LocalDate` object. | In Schema Registry, register `record_date` as Avro logical type `date`: `{"type": "int", "logicalType": "date"}` and `load_date` as `{"type": "long", "logicalType": "timestamp-micros"}`. |
| **8** | `UpdateRecord: FieldConversionException: Conversion failed for [...] named [load_date] to [java.time.LocalDateTime] [java.lang.NumberFormatException]` | `load_date` is typed as Avro `timestamp-micros` (primitive `long`). `UpdateRecord` does not pass a timestamp format pattern when evaluating literal values, causing NiFi to parse strings with `Long.parseLong()`. | In `UpdateRecord`, set `/load_date` to `${now():toNumber()}`. This evaluates to an epoch number, which NiFi converts directly to `LocalDateTime` / `Instant` without string parsing errors. |
| **9** | `MergeRecord: MalformedRecordException: Conversion failed for [1789344000000] named [record_date] to [java.time.LocalDate] [DateTimeParseException]` | `CSVRecordSetWriter` serializes dates as numeric epoch days/millis. When `voice-csv-reader` had `Date Format: yyyy-MM-dd` hardcoded, it failed to parse numeric representations. | Leave `Date Format` blank (empty) in both `voice-csv-reader` and `csv-writer`. When empty, NiFi natively handles both ISO strings and epoch numeric values. In `UpdateRecord`, set `/record_date` to `${record_date:replaceEmpty('${now():format("yyyy-MM-dd")}')}`. |
| **10** | `PutIcebergRecord` unable to access table metadata / NoSuchBucket error | Polaris warehouse `bigdata_catalog` is stored at `s3://big-data/warehouse/` on the cluster MinIO (`minio.minio.svc.cluster.local:9000`). If `s3-iceberg-file-io` pointed to `samo-minio`, it could not access the bucket or files created by Trino. | Configure `S3IcebergFileIOProvider` to point to `http://minio.minio.svc.cluster.local:9000` with credentials `BnMtc7hhYG705rlXFcdj` / `VZP8Xcv7RHPUIRmiPZH1tCNOwqJUtmuJ8tvqv2ok`. |

---

# Part 4: Day 3 Knowledge Verification & Assessment Answers

1. **Four things raw `.p` archive in MinIO cannot do that an Iceberg table can:**
   - Enforce schema validation on write (raw text accepts any malformed string).
   - Provide atomic multi-file ACID transactions (concurrent queries see torn reads).
   - Perform partition pruning without physical directory path dependencies (no hidden partitioning).
   - Execute time-travel queries or instant zero-copy rollbacks.

2. **The three layers of the lakehouse and their independence:**
   - **Storage Layer (MinIO/S3)**: Holds bytes (Parquet data + metadata files).
   - **Table Format Layer (Apache Iceberg)**: Defines the table metadata tree, schema, and snapshots.
   - **Catalog Layer (Apache Polaris)**: Maps table names to metadata pointers and coordinates commits.
   - *Independence*: Any layer can be swapped (e.g., MinIO to S3, or Polaris to Unity Catalog) without migrating data files.

3. **Confluent Schema Registry concepts and wire format:**
   - **Subject**: Named scope for schema evolution (e.g., `cdr.raw-value`).
   - **Version**: Incrementing integer assigned to schema updates.
   - **Schema ID**: Globally unique integer across all schemas.
   - **Wire Format**: `0x00` (1 magic byte) + 4-byte Schema ID + Avro binary payload.

4. **BACKWARD compatibility guarantee & rollout sequence:**
   - Guarantees that new consumer schemas can read data written with older schemas.
   - Permits: Deleting a field, or adding an optional field with a default value.
   - Rollout: Update **Consumers first**, then Producers.

5. **Why Registry Schema and Iceberg Table Schema must evolve together:**
   - Registry governs data *in flight* (NiFi/Kafka); Iceberg governs data *at rest*. If a field is ingested without adding it to the table, `PutIceberg` halts to prevent drift.
   - NiFi safety catch: `Unmatched Column Behavior = FAIL`.

6. **AvroRecordSetWriter embedded vs. reference schema:**
   - **Embedded**: Includes full schema JSON in the file header (ideal for standalone files).
   - **Reference**: Emits only the schema ID/name; requires a registry to decode (ideal for low-latency streaming).

7. **Why ParquetRecordSetWriter is NOT attached to PutIceberg:**
   - `PutIceberg` has its own built-in Iceberg Parquet writer that automatically controls file formats, row groups, and field-ID metadata.

8. **Columnar storage and query efficiency:**
   - Row stores read every column of every row sequentially. Parquet reads only the specific column chunks requested (`waitduration`), completely bypassing all other columns from disk I/O.

9. **Parquet Anatomy & Footer contents:**
   - $\text{File} \longrightarrow \text{Row Groups} \longrightarrow \text{Column Chunks} \longrightarrow \text{Pages}$.
   - The **Footer** contains the file schema, row-group byte offsets, and column statistics (`min`, `max`, `null_count`, `distinct_count`).

10. **Dictionary Encoding:**
    - Replaces repeated string/code values with small integer indexes pointing to a page dictionary. Low-cardinality telecom fields benefiting most: `status`, `cdr_type`, `mscaddress`, `callingcellid`.

11. **Predicate pushdown using `WHERE status = '2'`:**
    - The engine inspects the Parquet footer statistics before reading data pages. If a row group lists `min='0'` and `max='1'`, the engine immediately skips that entire row group.

12. **Iceberg metadata layers from top to bottom:**
    - Catalog Pointer $\longrightarrow$ Table Metadata JSON (`metadata.json`) $\longrightarrow$ Manifest List (`.avro`) $\longrightarrow$ Manifest Files (`.avro`) $\longrightarrow$ Data Files (`.parquet`).
    - Manifests use **Avro** because they are read row-by-row in full; Data files use **Parquet** because they are scanned column-by-column for analytics.

13. **Iceberg commit process and conflict handling:**
    - Writer outputs Parquet files $\rightarrow$ writes manifests $\rightarrow$ writes new `metadata.json` $\rightarrow$ attempts atomic CAS swap on catalog pointer. If another writer committed first, the swap fails; the writer re-reads the new base, re-applies changes, and retries.

14. **ACID without a lock server:**
    - Coordination occurs strictly at the catalog's atomic compare-and-swap pointer swap. A write is entirely invisible until the pointer changes.

15. **Hidden Partitioning:**
    - Iceberg derives partition values from existing columns using transforms (`day(ts)`). Queries filter on the timestamp without knowing physical partition paths, and pruning occurs automatically.

16. **Why `rollback_to_snapshot` is cheap:**
    - It merely re-points the catalog's current snapshot pointer to an older snapshot ID in the metadata tree. Zero data files are copied or moved.

17. **Maintenance procedures and execution ownership:**
    - `rewrite_data_files` (Compaction), `expire_snapshots` (Purge history), `rewrite_manifests` (Re-cluster indexes), `remove_orphan_files` (Delete abandoned writes).
    - Executed by **Apache Spark or Trino scheduled jobs**, never NiFi.

18. **Why `One FlowFile = One Snapshot` is problematic and how `MergeRecord` solves it:**
    - Ingesting high-velocity files creates thousands of tiny Parquet files and commit snapshots, bloating metadata and destroying read performance.
    - `MergeRecord` batches records via bin-packing into clean, large files (50k–250k rows) aligned to `record_date` partition boundaries.

19. **Field ID tracking in Iceberg:**
    - Iceberg assigns a permanent, immutable integer Field ID to every column. Name renames, additions, drops, and reorders are pure metadata operations because engines resolve data by Field ID rather than column index or column name.

20. **Reading old Parquet files after adding a new column (`roaming_flag`):**
    - The manifest indicates the table has Field ID $N$. When reading older Parquet files, the engine detects Field ID $N$ is absent in the footer and automatically synthesizes a stream of `NULL`s with zero table migration required.
