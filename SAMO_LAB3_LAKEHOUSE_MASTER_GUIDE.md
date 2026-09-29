# Complete Master Guide: Apache Iceberg Lakehouse on K8s
## (UI Dashboards, DataGrip DB Integration & Architecture Blueprint)

---

## Table of Contents
1. [Architecture Overview: What Each Component Does & Why We Use It](#1-architecture-overview-what-each-component-does--why-we-use-it)
2. [Local Host Mapping (`/etc/hosts`)](#2-local-host-mapping-etchosts)
3. [Cluster Services, Credentials & Access Endpoints](#3-cluster-services-credentials--access-endpoints)
4. [Connecting to PostgreSQL from Local PC (The SSH Tunnel Solution)](#4-connecting-to-postgresql-from-local-pc-the-ssh-tunnel-solution)
   - [4.1 Why Direct Connection Fails (The Core Issue)](#41-why-direct-connection-fails-the-core-issue)
   - [4.2 Method 1: DataGrip / DBeaver Native SSH Tunnel (GUI)](#42-method-1-datagrip--dbeaver-native-ssh-tunnel-gui)
   - [4.3 Method 2: Terminal SSH Port Forwarding (CLI `psql`)](#43-method-2-terminal-ssh-port-forwarding-cli-psql)
   - [4.4 Common Pitfalls & Troubleshooting](#44-common-pitfalls--troubleshooting)
5. [Step-by-Step UI Dashboard Guide](#5-step-by-step-ui-dashboard-guide)
   - [5.1 Connect JetBrains DataGrip to Lakehouse (Trino)](#51-connect-jetbrains-datagrip-to-the-lakehouse-trino)
   - [5.2 Connect DataGrip to PostgreSQL (Polaris Catalog Metastore)](#52-connect-datagrip-to-postgresql-polaris-catalog-under-the-hood)
   - [5.3 Inspecting Storage in MinIO Web Console UI](#53-inspecting-storage-in-the-minio-web-console-ui)
   - [5.4 Trino Web UI Management Dashboard](#54-trino-web-ui-dashboard)
6. [Running Lakehouse Operations in DataGrip (SQL Blueprint)](#6-running-lakehouse-operations-in-datagrip)
   - [6.1 Create Schemas and Iceberg Tables](#61-create-schemas-and-iceberg-tables)
   - [6.2 Insert Data (Writes to MinIO via Polaris)](#62-insert-data-writes-to-minio-via-polaris)
   - [6.3 Query Analytics](#63-query-analytics)
   - [6.4 Iceberg Metadata & Time-Travel Queries](#64-iceberg-metadata--time-travel-queries)
7. [Verifying Storage in the MinIO Console](#7-verifying-in-the-minio-console)
8. [Mathew's Exact Working Helm Configurations (For Reference)](#8-mathews-exact-working-helm-configurations-for-reference)
9. [Summary Checklist](#9-summary-checklist)

---

## 1. Architecture Overview: What Each Component Does & Why We Use It

A modern Lakehouse architecture decouples **Storage**, **Metadata Catalog**, and **Compute Engine** into three distinct, specialized layers:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                             CONSUMPTION & TOOLS                             │
│       ┌───────────────────────┐             ┌───────────────────────┐       │
│       │  JetBrains DataGrip   │             │   MinIO Web Console   │       │
│       │  (SQL / Visual IDE)   │             │   (Bucket & Files UI) │       │
│       └───────────┬───────────┘             └───────────┬───────────┘       │
└───────────────────┼─────────────────────────────────────┼───────────────────┘
                    │                                     │
                    ▼ JDBC (Port 80 / 8080)               │
┌──────────────────────────────────────────────────┐      │
│ 1. COMPUTE / WRITE ENGINE: Trino                 │      │
│    • What: Distributed ANSI SQL execution engine │      │
│    • Why: Executes queries, partitions, & writes │      │
│    • UI Dashboard: Tracks query execution & RAM  │      │
└───────────┬──────────────────────────────────────┘      │
            │                                             │
            │ Iceberg REST Protocol (OAuth2)              │
            ▼                                             │
┌──────────────────────────────────────────────────┐      │
│ 2. METADATA CATALOG: Apache Polaris              │      │
│    • What: Iceberg REST metadata service         │      │
│    • Why: Manages schemas, commits, & snapshots. │      │
│           Prevents conflicts; stores NO raw data │      │
│    • Backed by: PostgreSQL (persistence)         │      │
└───────────┬──────────────────────────────────────┘      │
            │                                             │
            │ Resolves file pointers                      │
            ▼                                             ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 3. STORAGE LAYER: MinIO S3 Object Store                                     │
│    • What: High-performance, S3-compatible storage                         │
│    • Why: Stores actual Parquet files & Iceberg JSON manifest trees         │
│    • Bucket: s3://big-data/warehouse or s3://samo-lakehouse/warehouse       │
└─────────────────────────────────────────────────────────────────────────────┘
```

### The Three Distinct Layers Explained:

#### 1. MinIO (Object Storage Layer)
- **What it does**: Holds raw Parquet columnar data files and Iceberg manifest/metadata JSON/Avro files.
- **Why we use it**: Decouples compute from storage. Disk storage scales independently of CPU/RAM, and files can be read in parallel.
- **UI Dashboard**: MinIO Console lets you visually create buckets, browse Parquet parts, and download Iceberg metadata files.

#### 2. Apache Polaris (Metadata & REST Catalog Layer)
- **What it does**: Implements the open Apache Iceberg REST specification with OAuth2 authentication and Role-Based Access Control (RBAC).
- **Why we use it**: Instead of locking into proprietary catalogs or legacy Hive Metastore, Polaris acts as a neutral catalog. When Trino creates a table, Polaris records its schema, partition spec, and pointer to the current snapshot in PostgreSQL.
- **Backed by**: PostgreSQL (`10.0.0.8:5432`).

#### 3. Trino (Query & Execution Engine Layer)
- **What it does**: Distributed query engine that parses ANSI SQL, plans distributed tasks, and processes queries.
- **Why we use it**: Trino reads metadata from Polaris to discover which files to read, then streams Parquet data directly from MinIO into memory for high-performance analytics.
- **UI Dashboard**: Displays live query plans, data throughput, active workers, and memory usage.

---

## 2. Local Host Mapping (`/etc/hosts`)

To access all UI dashboards and connect DataGrip using human-readable domain names on standard port `80`, add the cluster ingress IP (`169.58.218.71`) to your local machine's `hosts` file.

### How to edit:
- **Windows**: Open Notepad as **Administrator** ➔ `C:\Windows\System32\drivers\etc\hosts`
- **Linux / macOS**: Open terminal ➔ `sudo nano /etc/hosts`

### Lines to Add:
```text
# ==========================================
# Data Platform & Lakehouse Cluster Hosts
# IP: 169.58.218.71
# ==========================================
169.58.218.71   trino.transcode.com
169.58.218.71   minio-console.transcode.com
169.58.218.71   minio.transcode.com
169.58.218.71   polaris.transcode.com
169.58.218.71   kafkaui.transcode.com
169.58.218.71   nifi.transcode.com
169.58.218.71   airflow.transcode.com
169.58.218.71   openmetadata.transcode.com
169.58.218.71   grafana.transcode.com
169.58.218.71   dashboard.transcode.com
169.58.218.71   opencost.transcode.com
169.58.218.71   keycloak.transcode.com
169.58.218.71   schemaregistry.transcode.com
```

---

## 3. Cluster Services, Credentials & Access Endpoints

Your server has two setups configured: the existing **Mathew setup** (shared baseline) and the **Samo Data Platform**:

### Active Endpoints on Your Server (`169.58.218.161` / `169.58.218.71`)

| Component | Ingress / URL | Internal Kubernetes Endpoint | Credentials | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **Trino Web UI / JDBC** | `http://trino.transcode.com` | `http://trino.trino.svc.cluster.local:8080` | User: `samo-admin` *(or `admin`)*<br>Password: *(leave empty)* | Query execution plans, worker threads, live queries |
| **MinIO Console (Mathew)**| `http://minio-console.transcode.com` | `http://minio-console.minio.svc:9001` | Root User / Access Key | Shared MinIO instance |
| **MinIO Console (Samo)** | `http://169.58.218.71:30901` | `http://samo-minio.samo.svc:9001` | User: `samo-admin`<br>Password: `SamoSecureMinioPass2026!` | Dedicated Samo MinIO storage UI |
| **MinIO S3 API (Samo)** | `http://169.58.218.71:30900` | `http://samo-minio.samo.svc:9000` | User: `samo-admin`<br>Password: `SamoSecureMinioPass2026!` | S3 API endpoint for SDKs & engines |
| **Polaris REST Catalog** | `http://polaris.transcode.com` | `http://polaris.polaris.svc:8181/api/catalog` | OAuth2 Bearer Token | Iceberg REST catalog endpoint |
| **PostgreSQL DB** | `10.0.0.8:5432` | Accessible from cluster pods | User: `postgres`<br>Password: `cHMKJDQMmQ4ZnIii` | Polaris persistence metadata |
| **AKHQ (Kafka UI)** | `http://kafkaui.transcode.com` | `http://akhq.samo-akhq.svc:8080` | *(None required)* | Kafka topic & message viewer |
| **Apache NiFi Canvas** | `https://169.58.218.71:31443/nifi` | `https://samo-nifi.samo.svc:8443` | User: `samo-admin`<br>Password: `SamoNiFiPassword2026!` | Visual ETL pipeline designer |

---

## 4. Connecting to PostgreSQL from Local PC (The SSH Tunnel Solution)

### 4.1 Why Direct Connection Fails (The Core Issue)

When attempting to connect to PostgreSQL from your local PC or DataGrip, typing `Host: 10.0.0.8` directly fails immediately with:
```text
Connection refused
Network is unreachable
Connection timed out
```

```
┌─────────────────────────┐                 ┌─────────────────────────┐                 ┌─────────────────────────┐
│     Your Local PC       │                 │   Public Jumphost       │                 │   Internal Network      │
│  (DataGrip / psql CLI)  │                 │   (169.58.218.161)      │                 │   PostgreSQL DB         │
│                         │                 │                         │                 │   (10.0.0.8:5432)       │
│  Cannot reach 10.0.0.8  │ ── ✗ FAILS ───► │                         │                 │                         │
│  (Private Subnet)       │                 │                         │                 │                         │
│                         │                 │                         │                 │                         │
│  SSH Tunnel Port 22     │ ─── SECURE ───► │  Forward Port to:       │ ─── INTERNAL ─► │  Listens on 5432        │
│  (Encrypted Session)    │     TUNNEL      │  10.0.0.8:5432          │      TRAFFIC    │  Accepts Connection!    │
└─────────────────────────┘                 └─────────────────────────┘                 └─────────────────────────┘
```

**Reason**: `10.0.0.8` is an **internal private IP address** inside the cloud/datacenter network. It does not have a public IP and cannot be routed across the open internet.

**Solution**: Use the server `169.58.218.161` (which has a public IP and SSH port 22 open) as an **SSH Bastion / Jump Host**. An SSH tunnel encrypts your connection and securely forwards your local traffic directly to `10.0.0.8:5432`.

---

### 4.2 Method 1: DataGrip / DBeaver Native SSH Tunnel (GUI)

DataGrip and DBeaver have built-in SSH tunneling so you don't need any terminal background commands.

#### Step 1: Open Data Source Window
In DataGrip, click **File** ➔ **New** ➔ **Data Source** ➔ **PostgreSQL**.

#### Step 2: Configure the SSH Tunnel First
1. Click on the **SSH/SSL** tab at the top.
2. Check the box **Use SSH tunnel**.
3. Next to the tunnel configuration dropdown, click the **`...`** (or **Manage**) button to add the SSH host:
   - **Host**: `169.58.218.161`
   - **Port**: `22`
   - **User**: `samo`
   - **Authentication type**: `Password`
   - **Password**: `3682154Aa1!`
4. Click **Test Connection** for the SSH tunnel. Once verified, click **OK**.

#### Step 3: Configure the Database Settings (General Tab)
Switch back to the **General** tab:
- **Host**: `10.0.0.8` *(DataGrip will route this through the tunnel!)*
- **Port**: `5432`
- **Database**: `polaris` *(or `samo_polaris`)*
- **User**: `postgres`
- **Password**: `cHMKJDQMmQ4ZnIii`

#### Step 4: Test & Connect
Click **Test Connection**. You will see:
```text
DBMS: PostgreSQL (ver. 16.x)
Case sensitivity: plain=lower, delimited=exact
Driver: PostgreSQL JDBC Driver
```
Click **Apply** and **OK**.

---

### 4.3 Method 2: Terminal SSH Port Forwarding (CLI `psql`)

If you want to use the command-line `psql` client from your local PC or any local application:

#### Step 1: Open the SSH Tunnel in Your Local Terminal
Run this single command in your local terminal:

```bash
# Forwards local port 5433 -> Jumphost -> 10.0.0.8:5432
ssh -L 5433:10.0.0.8:5432 -N -o ServerAliveInterval=60 samo@169.58.218.161
```
*Enter password: `3682154Aa1!`*

> [!NOTE]
> We use local port **`5433`** instead of `5432` to avoid conflicts in case you already have a local PostgreSQL instance running on your PC.
> The `-N` flag tells SSH not to open a remote shell, just keep the tunnel open.
> The `-o ServerAliveInterval=60` ensures the tunnel does not disconnect if idle.

#### Step 2: Connect via `psql` on Your Local PC
Open a second terminal window on your PC:

```bash
PGPASSWORD='cHMKJDQMmQ4ZnIii' psql -h localhost -p 5433 -U postgres -d polaris
```

You are now connected to the remote database! Run:
```sql
-- List Polaris catalog tables
SELECT table_name FROM information_schema.tables WHERE table_schema = 'polaris_schema';

-- View registered Iceberg entities (catalogs, tables)
SELECT * FROM polaris_schema.entities;
```

---

### 4.4 Common Pitfalls & Troubleshooting

1. **"Address already in use" error on local machine**:
   - Cause: Port 5432 or 5433 is already occupied by a local process.
   - Fix: Change the local port on the left side of the colon:
     `ssh -L 5434:10.0.0.8:5432 samo@169.58.218.161` (then connect to `localhost:5434`).
2. **"Connection reset by peer" or timeout during queries**:
   - Cause: Idle SSH connection dropped by firewalls.
   - Fix: Add `-o ServerAliveInterval=30 -o ServerAliveCountMax=5` to the SSH tunnel command.
3. **DataGrip says "Unknown host: 10.0.0.8"**:
   - Cause: SSH Tunnel box is unchecked in the **SSH/SSL** tab, so DataGrip is trying to resolve `10.0.0.8` on your local Wi-Fi.
   - Fix: Ensure the **Use SSH tunnel** checkbox is checked in the **SSH/SSL** tab.

---

## 5. Step-by-Step UI Dashboard Guide

### 5.1 Connect JetBrains DataGrip to the Lakehouse (Trino)

DataGrip gives you a full GUI to view tables, write SQL, inspect partitions, and run analytics.

#### 1. Open DataGrip:
Click **File** ➔ **New** ➔ **Data Source** ➔ **Trino**.

#### 2. Configure General Connection Settings:
- **Name**: `Samo-Lakehouse-Trino`
- **Driver**: Select **Trino** (DataGrip will auto-download the driver if missing).
- **Host**: `trino.transcode.com` *(or `169.58.218.71`)*
- **Port**: `80` *(or `8080` if using port-forwarding)*
- **User**: `samo-admin` *(or `admin` — any non-empty string)*
- **Password**: *(Leave empty — plain HTTP authentication)*
- **Database / Catalog**: `lakehouse`
- **Schema**: *(Leave blank)*

> [!TIP]
> **If connecting via SSH Tunnel in DataGrip**:
> Click the **SSH/SSL** tab in DataGrip:
> - Check **Use SSH tunnel**.
> - **Proxy Host**: `169.58.218.161`
> - **Port**: `22`
> - **User**: `samo`
> - **Password**: `3682154Aa1!`
> - In the **General** tab, set Host to `trino.trino.svc.cluster.local` and Port to `8080`.

#### 3. Test Connection:
Click **Test Connection**. You should see:
```text
DBMS: Trino (ver. 480)
Case sensitivity: plain=mixed, delimited=exact
Driver: Trino JDBC Driver
```
Click **Apply** and **OK**.

---

### 5.2 Connect DataGrip to PostgreSQL (Polaris Catalog Under the Hood)

Follow the instructions in [Section 4.2](#42-method-1-datagrip--dbeaver-native-ssh-tunnel-gui) to inspect the catalog schema.

In the database tree, expand `polaris` ➔ `schemas` ➔ `polaris_schema`. You will see the internal catalog tables:
- `principal_authentication_data`: Stores service credentials and root hashes.
- `entities`: Stores registered catalogs, namespaces, and table entities.
- `grant_records`: Stores RBAC privilege grants.

---

### 5.3 Inspecting Storage in the MinIO Web Console UI

1. Open your browser and navigate to:
   - **Samo MinIO**: `http://169.58.218.71:30901`  
     *(User: `samo-admin` / Password: `SamoSecureMinioPass2026!`)*
   - **Shared MinIO**: `http://minio-console.transcode.com`
2. In the left navigation bar, click **Buckets**:
   - You will see the bucket `big-data` (or `samo-lakehouse`).
3. Click on the bucket to browse the object tree:
   - `warehouse/` ➔ schemas ➔ tables.
   - When Trino writes data, you will see real-time `.parquet` files and `.metadata.json` manifests created here!

---

### 5.4 Trino Web UI Dashboard

Trino includes a built-in web management console to monitor query performance, resource groups, and execution traces.

1. Open in your browser: `http://trino.transcode.com`  
   *(or port-forward: `kubectl port-forward -n trino svc/trino 8080:8080` ➔ `http://localhost:8080`)*.
2. Login with username: `admin` (no password).
3. The dashboard shows:
   - **Active Workers & Threads**
   - **Memory Pool Utilization**
   - **Live Queries & Completed Queries list**
   - **Query Execution Graph (Visual Stages, Splits & IO throughput)**

---

## 6. Running Lakehouse Operations in DataGrip

Open a new SQL Console in DataGrip connected to `Samo-Lakehouse-Trino` (`lakehouse` catalog):

### 6.1 Create Schemas and Iceberg Tables
```sql
-- 1. Create a schema for telecom CDRs
CREATE SCHEMA lakehouse.cdrs;

-- 2. Create an Iceberg table partitioned by day with ZSTD Parquet compression
CREATE TABLE lakehouse.cdrs.voice (
    cdr_id VARCHAR,
    subscriber_id VARCHAR,
    call_duration_seconds INTEGER,
    call_status VARCHAR,
    call_cost DECIMAL(10, 2),
    event_time TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    format_version = 2,
    partitioning = ARRAY['day(event_time)']
);
```

### 6.2 Insert Data (Writes to MinIO via Polaris)
```sql
INSERT INTO lakehouse.cdrs.voice VALUES
    ('CDR-001', '252615000001', 120, 'COMPLETED', 0.50, TIMESTAMP '2026-09-13 10:15:00'),
    ('CDR-002', '252615000002', 45,  'COMPLETED', 0.20, TIMESTAMP '2026-09-13 10:18:30'),
    ('CDR-003', '252615000003', 310, 'COMPLETED', 1.25, TIMESTAMP '2026-09-13 11:00:15'),
    ('CDR-004', '252615000004', 15,  'FAILED',    0.00, TIMESTAMP '2026-09-13 11:30:00');
```

### 6.3 Query Analytics
```sql
-- Standard aggregation query
SELECT 
    call_status,
    COUNT(*) AS total_calls,
    SUM(call_duration_seconds) AS total_duration,
    SUM(call_cost) AS total_revenue
FROM lakehouse.cdrs.voice
GROUP BY call_status;
```

### 6.4 Iceberg Metadata & Time-Travel Queries
Because Iceberg is an open table format, Polaris tracks table snapshots. You can query table history directly in DataGrip:

```sql
-- 1. Inspect all snapshot commits made to the table
SELECT snapshot_id, committed_at, operation, summary['total-records'] AS records
FROM lakehouse.cdrs."voice$snapshots";

-- 2. Inspect individual Parquet files on MinIO
SELECT file_path, file_format, record_count, file_size_in_bytes
FROM lakehouse.cdrs."voice$files";

-- 3. Time travel: query how the table looked at a specific snapshot
-- (Replace <snapshot_id> with an ID from the query above)
-- SELECT * FROM lakehouse.cdrs.voice FOR VERSION AS OF <snapshot_id>;
```

---

## 7. Verifying in the MinIO Console

Once you execute the queries in DataGrip:
1. Open the **MinIO Web Console** (`http://minio-console.transcode.com` or `http://169.58.218.71:30901`).
2. Navigate to bucket: `big-data` ➔ `warehouse/cdrs/voice/`.
3. Notice the two folders:
   - `data/event_time_day=2026-09-13/*.parquet`: The partitioned Parquet columnar file written by Trino.
   - `metadata/*.metadata.json`: The Iceberg metadata tree created and registered in Polaris!

---

## 8. Mathew's Exact Working Helm Configurations (For Reference)

If you need to review the exact production values that Mathew deployed on this server:

### Polaris Values (`polaris` namespace):
```yaml
advancedConfig:
  quarkus.datasource.jdbc.acquisition-timeout: 30S
  quarkus.datasource.jdbc.idle-removal-interval: 5M
  quarkus.datasource.jdbc.max-size: "20"
  quarkus.datasource.jdbc.min-size: "5"

persistence:
  type: relational-jdbc
  relationalJdbc:
    secret:
      name: polaris-persistence
      jdbcUrl: jdbcUrl
      username: username
      password: password

storage:
  secret:
    name: polaris-storage
    awsAccessKeyId: access-key
    awsSecretAccessKey: secret-key

resources:
  requests:
    cpu: 200m
    memory: 500Mi
  limits:
    cpu: "2"
    memory: 2Gi

ingress:
  enabled: true
  className: nginx
  hosts:
    - host: polaris.transcode.com
      paths:
        - path: /
          pathType: ImplementationSpecific
```

### Trino Values (`trino` namespace):
```yaml
additionalConfigProperties:
  - http-server.process-forwarded=true

catalogs:
  lakehouse: |-
    connector.name=iceberg
    iceberg.catalog.type=rest
    iceberg.rest-catalog.uri=http://polaris.polaris.svc.cluster.local:8181/api/catalog
    iceberg.rest-catalog.warehouse=bigdata_catalog
    iceberg.rest-catalog.security=OAUTH2
    iceberg.rest-catalog.oauth2.credential=d5f9cdea48a6fb7f:ae88731a268f5c0e33e593508fad6fcf
    iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL
    iceberg.rest-catalog.view-endpoints-enabled=false

ingress:
  enabled: true
  className: nginx
  hosts:
    - host: trino.transcode.com
      paths:
        - path: /
          pathType: ImplementationSpecific
```

---

## 9. Summary Checklist

- [x] Host entries added to local `/etc/hosts` pointing to `169.58.218.71`.
- [x] SSH Tunnel configured in DataGrip or Terminal for PostgreSQL (`10.0.0.8:5432` via `169.58.218.161:22`).
- [x] MinIO UI open to view buckets and Parquet files (`http://minio-console.transcode.com` / `http://169.58.218.71:30901`).
- [x] DataGrip connected to Trino (`Host: trino.transcode.com`, `Port: 80`, `Catalog: lakehouse`, `User: samo-admin`).
- [x] DataGrip connected to PostgreSQL (`Host: 10.0.0.8`, `Port: 5432`, `Database: polaris`, `Schema: polaris_schema`).
- [x] Trino Web UI accessible at `http://trino.transcode.com` (user `admin`).
- [x] Tables created, queried, and verified with zero terminal friction.
