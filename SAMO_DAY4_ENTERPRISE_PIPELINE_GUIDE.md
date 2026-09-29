# Telesom & Samo Data Platform — Day 4 Enterprise Lakehouse Master Guide
## Modular Multi-Group Architecture, RecordPath Transformations, Stream Tapping & Automated Alerting
### (Complete Dashboard-Driven & Visual Web UI Implementation Edition)

---

## Executive Table of Contents
1. **Comprehensive Theoretical Summary of Day 4 Training Material**
   - 1.1 Enterprise Pipeline Modularization & Encapsulation via Port-Connected Process Groups
   - 1.2 In-Flight Format Conversion: Delimited Text (CSV) to Native Binary Avro
   - 1.3 Record-Level Mutation via NiFi RecordPath vs. Filename Metadata Derivation
   - 1.4 Partition-Aligned Bin-Packing (`el_record_date`) & Small-Files Mitigation
   - 1.5 Non-Intrusive Stream Tapping vs. Inline Pipeline Monitoring
   - 1.6 Cluster-Wide Workload Distribution: Primary Node Listing with Round-Robin Fetch Balancing
   - 1.7 Enterprise Alerting Architecture: Merging Processing Failures and Health Heartbeats
2. **Cluster Topology & Dashboard Access Directory (`samo` Namespace)**
3. **Step-by-Step Implementation Guide**
   - **Step 0: Continuous Data Generator Setup (`cdr_file_generator.py`)**
   - **Step 1: Top-Level Process Group, Parameters & Schema Registry Setup**
     - *1.1 Confluent Schema Registry Setup via Kafka UI / AKHQ Dashboard*
     - *1.2 Creating `CDR_FULL_PIPELINE` on the Canvas*
     - *1.3 Defining the Pipeline Parameter Context*
     - *1.4 Configuring Top-Level Controller Services (ConfluentSchemaRegistry, Readers, Writers, Catalog)*
   - **Step 2: Child Process Group 1 — `EXTRACT` (SFTP Ingestion & Cluster Load Balancing)**
     - *2.1 Ports Configuration (`records-out`, `monitor-tap-out`, `failures-out`)*
     - *2.2 `ListSFTP` Configuration (Anchored Regex & Primary Node)*
     - *2.3 Round-Robin Load Balancing on `ListSFTP ➔ FetchSFTP` Connection*
     - *2.4 `FetchSFTP` & Dual-Connection Output Port Tapping*
   - **Step 3: Child Process Group 2 — `TRANSFORM` (RecordPath Mutation & In-Flight Serialization)**
     - *3.1 Ports Configuration (`transform-in`, `transform-out`, `failures-out`)*
     - *3.2 `ConvertRecord` (CSV to Avro Raw Conversion)*
     - *3.3 `UpdateRecord` (RecordPath Value: Deriving `el_record_date` from `/create_date`)*
     - *3.4 `UpdateRecord` (Literal Value: Deriving `load_date`)*
   - **Step 4: Child Process Group 3 — `LOAD` (Partitioned Bin-Packing & Iceberg Ingestion)**
     - *4.1 Ports Configuration (`load-in`, `load-out`, `failures-out`)*
     - *4.2 `MergeRecord` (Bin-Packing 50k–250k Rows Aligned on `el_record_date`)*
     - *4.3 `PutIcebergRecord` (Target Table `lakehouse.cdrs.voice_lab`)*
   - **Step 5: Child Process Group 4 — `MONITORING` (Non-Intrusive Stream Inactivity Tapping)**
     - *5.1 Ports Configuration (`monitor-in`, `monitor-alerts-out`)*
     - *5.2 `MonitorActivity` (Threshold Duration, Continually Send Messages, Primary Node)*
   - **Step 6: Child Process Group 5 — `ALERTING` (Unified Failure & Heartbeat Email Notification)**
     - *6.1 Ports Configuration (`failures-in`, `monitor-in`)*
     - *6.2 `UpdateAttribute` (Tagging Dynamic Alert Context & Failure Categorization)*
     - *6.3 `PutEmail` (Dynamic Templated Alert Dispatch)*
   - **Step 7: Canvas Top-Level Assembly & Inter-Group Port Wiring**
   - **Step 8: End-to-End Operational Verification Across Dashboards**
     - *8.1 Visual Storage Inspection via MinIO Console Dashboard*
     - *8.2 Analytics & Snapshot Auditing via Trino Web UI / SQL Console*
     - *8.3 Real-Time Pipeline Inspection via NiFi Queues, Provenance & Bulletins*
   - **Step 9: Testing & Exercising the Alerting Subsystem (Failure, Stall, Recovery)**
   - **Step 10: Complete Troubleshooting & Error Reference Matrix**
4. **Day 4 Knowledge Verification & Assessment Review**

---

# Part 1: Comprehensive Summary of Day 4 Training Material

### 1.1 Enterprise Pipeline Modularization & Encapsulation
In Day 3, ingestion was implemented as a single, linear visual flow. While functional for development, enterprise production environments demand strict **separation of concerns**, **fault containment**, and **reusability**.

Day 4 introduces **Process Group Encapsulation**:
- The pipeline is partitioned into five distinct, specialized functional domains: `EXTRACT`, `TRANSFORM`, `LOAD`, `MONITORING`, and `ALERTING`.
- **The Golden Encapsulation Rule**: Process groups communicate **exclusively through Input and Output Ports**. No connection is ever permitted to cross a process group boundary directly.
- **Inheritance Hierarchy**: Parameter Contexts and Controller Services are declared once on the parent process group (`CDR_FULL_PIPELINE`) and automatically inherited by all child groups.

```text
┌────────────────────────────────────── CDR_FULL_PIPELINE ──────────────────────────────────────┐
│                                                                                               │
│  ┌───────────────┐           ┌─────────────────┐           ┌──────────────┐                   │
│  │    EXTRACT    │records-out│    TRANSFORM    │transform  │     LOAD     │load-out           │
│  │   ListSFTP    ├──────────▶│  ConvertRecord  ├──────────▶│ MergeRecord  ├─────────▶ [Done]  │
│  │   FetchSFTP   │           │  UpdateRecords  │    -out   │  PutIceberg  │                   │
│  └───────┬───────┘           └────────┬────────┘           └──────┬───────┘                   │
│          │monitor-tap-out             │failures-out               │failures-out               │
│          │                            │                           │                           │
│          ▼                            ▼                           ▼                           │
│  ┌───────────────┐               ┌─────────────────────────────────────┐                      │
│  │  MONITORING   │               │              Funnel                 │                      │
│  │MonitorActivity│               └──────────────────┬──────────────────┘                      │
│  └───────┬───────┘                                  │failures-in                              │
│          │monitor-alerts-out                        ▼                                         │
│          └───────────────────────────────────▶ ┌──────────────┐                               │
│                                                │   ALERTING   │                               │
│                                                │   PutEmail   │                               │
│                                                └──────────────┘                               │
└───────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

### 1.2 In-Flight Format Conversion: CSV to Native Binary Avro
Raw telecommunication CDRs arrive from network switches as flat, pipe-delimited text (`.p` files) lacking schema framing.
- **Drawback of Delimited Text in Streaming**: Every downstream processor (`UpdateRecord`, `MergeRecord`) must repeatedly split strings and re-parse column delimiters.
- **The Solution (`ConvertRecord`)**: Converting incoming CSV records immediately to **Apache Avro** upon entry into `TRANSFORM`:
  1. Serializes records into compact binary payloads governed by a formal schema.
  2. Embeds or binds the schema via Schema Registry, preventing downstream type ambiguities.
  3. Enables high-speed record manipulation via native object trees rather than expensive string parsing.

---

### 1.3 Record-Level Mutation: RecordPath vs. Filename Derivation
In Day 3, partition dates were derived from the *filename* (`cdr_voice_20260914...`). In real-world telecom networks, this is an anti-pattern:
- **Arrival Time vs. Event Time**: The filename reflects when the mediation file was generated or transferred by SFTP (arrival time), **not when the subscriber made the call** (event time). Delayed or re-transmitted files cause CDRs to land in incorrect date partitions.
- **True Event-Time Partitioning**: Field index 4 (`create_date`) in the CDR payload contains the actual call timestamp formatted as `yyyyMMddHHmm`.
- **RecordPath Evaluation**:
  - In `UpdateRecord`, setting `Replacement Value Strategy` to **`Record Path Value`** allows executing RecordPath functions directly on internal fields:
    ```text
    /el_record_date = format(toDate(/create_date, 'yyyyMMddHHmm'), 'yyyy-MM-dd')
    ```
  - > [!IMPORTANT]
    > RecordPath functions (such as `format()` and `toDate()`) evaluate **only** when `Replacement Value Strategy` is set to **`Record Path Value`**. If set to `Literal Value`, NiFi treats them as literal strings and fails!

---

### 1.4 Partition-Aligned Bin-Packing (`el_record_date`) & Small-Files Mitigation
Streaming ingestion without consolidation creates the **Small-Files Problem**: writing millions of tiny 20-row Parquet files that degrade query scan times and overwhelm the Iceberg catalog.
- **`MergeRecord` with Bin-Packing**: Consolidates streaming records into massive, query-optimized files (50,000 to 250,000 records).
- **Partition Alignment**: `MergeRecord` groups bins by `Correlation Attribute Name = el_record_date`. This guarantees that records with different event dates are never packed into the same bin, strictly aligning with Iceberg's `day(el_record_date)` partition spec.

---

### 1.5 Non-Intrusive Stream Tapping vs. Inline Pipeline Monitoring
Detecting silent failures (e.g., when the upstream SFTP switch stops generating files) requires monitoring data flow heartbeat.
- **The Anti-Pattern (Inline Monitoring)**: Placing a monitoring processor inline in the primary data path creates a single point of failure. If the monitor experiences thread lock, backpressure, or failure, the entire production ingestion pipeline halts.
- **The Solution (Stream Tapping)**:
  1. In `EXTRACT`, `FetchSFTP`'s `success` relationship is wired to **two distinct connections**:
     - Connection A ➔ `records-out` (Primary ingestion pipeline)
     - Connection B ➔ `monitor-tap-out` (Non-intrusive monitoring clone)
  2. In NiFi, multiple connections originating from the same relationship **clone the FlowFile** (zero byte duplication via copy-on-write content repository pointers).
  3. `MonitorActivity` inspects the tapped clone. In `MONITORING`, normal records that pass `MonitorActivity`'s `success` relationship are **auto-terminated**.
  4. Only when no traffic arrives within `Threshold Duration` (e.g., 5 minutes), `MonitorActivity` generates an `inactive` marker FlowFile routed to `ALERTING`. When traffic resumes, an `activity.restored` marker is generated.

---

### 1.6 Cluster-Wide Workload Distribution: Round-Robin Balancing
In multi-node Apache NiFi clusters:
- `ListSFTP` is designed to run **strictly on the Primary Node** to prevent duplicate listings of remote files across cluster instances.
- **The Ingestion Bottleneck**: If the connection between `ListSFTP` and `FetchSFTP` uses default routing, all file downloads occur on the Primary Node, leaving secondary cluster nodes idle while the Primary Node exhausts memory and network bandwidth.
- **The Fix (`Round Robin Load Balancing`)**: Configuring the connection between `ListSFTP` and `FetchSFTP` with:
  - **Load Balance Strategy**: **`Round Robin`**
  - FlowFiles representing listed remote files are distributed evenly across all cluster worker nodes. Each worker connects to SFTP independently, fetching file contents in parallel and balancing network I/O across the entire cluster.

---

### 1.7 Enterprise Alerting Architecture: Unified Multi-Signal Dispatch
Enterprise operations teams require a single alerting sink capable of handling heterogeneous event types:
1. **Processing Failures**: Route failures from `EXTRACT`, `TRANSFORM`, and `LOAD` to an aggregation Funnel.
2. **Health Heartbeats / Silence Alerts**: Route `inactive` and `activity.restored` signals from `MONITORING`.
3. **Contextual Tagging (`UpdateAttribute`)**: Tags each FlowFile with `alert.type` (`pipeline-failure`, `pipeline-inactive`, `pipeline-restored`).
4. **Unified Notification (`PutEmail`)**: Formats an email containing server hostname, affected filename, partition date, and outage duration.

---

# Part 2: Cluster Topology & Dashboard Access Directory

Before starting, ensure your local `/etc/hosts` includes the cluster ingress entries for IP `169.58.218.71`:
```text
169.58.218.71   trino.transcode.com
169.58.218.71   minio-console.transcode.com
169.58.218.71   minio.transcode.com
169.58.218.71   polaris.transcode.com
169.58.218.71   kafkaui.transcode.com
169.58.218.71   schemaregistry.transcode.com
169.58.218.71   airflow.transcode.com
```

### Dashboard Access Matrix (`samo` Namespace):
| Dashboard / UI Tool | Web UI URL | Login Credentials | Functional Role in Lab 4 |
| :--- | :--- | :--- | :--- |
| **Apache NiFi Web Canvas** | `https://169.58.218.71:31443/nifi` | User: `samo-admin`<br>Pass: `SamoNiFiPassword2026!` | Visual construction of 5-group modular pipeline, port connections, monitoring tap, queue inspection & provenance. |
| **Kafka UI / AKHQ Dashboard** | `http://kafkaui.transcode.com` *(or `http://169.58.218.71:30080`)* | (None required) | Managing Schema Registry subjects (`voice-cdr`, `voice-cdr-partitioned`), inspecting schema versions and Kafka topics. |
| **MinIO Console (S3 Storage)** | `http://minio-console.transcode.com` *(or `http://169.58.218.71:30901`)* | User: `samo-admin`<br>Pass: `SamoSecureMinioPass2026!` | Visual Object Browser verifying physical Parquet files in `warehouse/cdrs/voice_lab` and raw archive in `voice-cdr-raw`. |
| **Trino Web UI & Query Console** | `http://trino.transcode.com` *(or `http://169.58.218.71:8080` / DataGrip)* | User: `samo-admin`<br>Catalog: `lakehouse` | Visual query execution, active worker monitoring, validating `lakehouse.cdrs.voice_lab`, snapshots, and data files. |
| **Polaris Catalog Metastore** | `http://polaris.transcode.com` | Client ID: `d5f9cdea48a6fb7f`<br>Secret: `ae88731a268f5c0e33e593508fad6fcf` | REST Iceberg Catalog governing table metadata and snapshot commits. |
| **Apache Airflow Dashboard** | `http://airflow.transcode.com` *(or `http://169.58.218.71:30080`)* | User: `samo`<br>Pass: `3682154Aa1!` | Monitoring workflow orchestration, DAG runs, and pipeline schedule triggers. |
| **SFTP Server Edge** | Host: `169.58.218.161`<br>Port: `22` | User: `samo`<br>Pass: `3682154Aa1!` | SFTP ingestion directory `/home/samo/SFTP/VOICE/voice_cdrs/` and CDR generator. |

---

# Part 3: Step-by-Step Implementation Guide

---

### Step 0: Keep the CDR File Generator Running

The synthetic CDR generator continuously outputs pipe-delimited raw files matching `^cdr_voice_\d{14}_\d{3}\.p$` into `/home/samo/SFTP/VOICE/voice_cdrs/`.

1. SSH into the jumphost:
   ```bash
   ssh samo@169.58.218.161
   # Password: SamoSecureSSHPassword2026! (or your server pass 3682154Aa1!)
   ```
2. Verify if the generator is active:
   ```bash
   ps aux | grep cdr_file_generator.py | grep -v grep
   ```
3. If not running, start it in the background:
   ```bash
   cd /home/samo/SFTP/VOICE
   nohup python3 cdr_file_generator.py > /dev/null 2>&1 &
   ```
4. Verify files are being generated:
   ```bash
   ls -la /home/samo/SFTP/VOICE/voice_cdrs/
   ```

---

### Step 1: Top-Level Process Group, Parameters & Schema Registry Setup

#### 1.1 Confluent Schema Registry Setup via Kafka UI / AKHQ Dashboard
Before configuring NiFi, verify and register the required Avro schemas directly in the central Confluent Schema Registry using the **Kafka UI / AKHQ Dashboard**:

1. Open your browser and navigate to the Kafka UI:
   - **URL**: `http://kafkaui.transcode.com` *(or `http://169.58.218.71:30080`)*
2. In the left navigation sidebar, click **Schema Registry**.
3. **Verify Existing Raw Schema (`voice-cdr`)**:
   - Locate the subject **`voice-cdr`** (or `voice-cdr-value`) created in Lab 2.
   - Click it to verify it contains the 19 raw telecom CDR fields (`cdr_id` through `mscaddress`).
4. **Register Partitioned Schema (`voice-cdr-partitioned`) via the UI**:
   - In the top-right corner of the Schema Registry page, click **Create Subject** (or **Add Schema**).
   - In the creation form:
     - **Subject Name**: `voice-cdr-partitioned`
     - **Compatibility Level**: Select **`NONE`** *(ensures schema is created without conflicting with global compatibility settings)*
     - **Schema Type**: Select **`AVRO`**
     - **Schema Definition**: Paste the complete 21-field Avro schema JSON below:
       ```json
       {
         "type": "record",
         "name": "VoiceCDRPartitioned",
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
           {"name": "el_record_date", "type": ["null", {"type": "int", "logicalType": "date"}], "default": null},
           {"name": "load_date", "type": ["null", "string"], "default": null}
         ]
       }
       ```
   - Click **Save** / **Submit**.
   - Verify that `voice-cdr-partitioned` now appears in the Schema Registry table with **Version 1** and **21 fields**.

> [!TIP]
> **Companion CLI Method (Alternative to UI)**:
> If you prefer using terminal commands or need to automate schema registration, run these commands from the jumphost:
> ```bash
> # 1. Set compatibility to NONE
> curl -s -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
>   --data '{"compatibility": "NONE"}' \
>   http://schemaregistry.confluent.svc:8081/config/voice-cdr-partitioned
>
> # 2. Register the 21-column schema
> curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
>   --data '{"schema": "{\"type\":\"record\",\"name\":\"VoiceCDRPartitioned\",\"namespace\":\"com.telesom.cdr\",\"fields\":[{\"name\":\"cdr_id\",\"type\":\"string\"},{\"name\":\"cdr_type\",\"type\":\"string\"},{\"name\":\"status\",\"type\":\"string\"},{\"name\":\"create_date\",\"type\":\"string\"},{\"name\":\"start_date\",\"type\":\"string\"},{\"name\":\"end_date\",\"type\":\"string\"},{\"name\":\"pri_identity\",\"type\":\"string\"},{\"name\":\"actual_usage\",\"type\":\"string\"},{\"name\":\"callingpartynumber\",\"type\":\"string\"},{\"name\":\"calledpartynumber\",\"type\":\"string\"},{\"name\":\"callingpartyimsi\",\"type\":\"string\"},{\"name\":\"calledpartyimsi\",\"type\":\"string\"},{\"name\":\"dialednumber\",\"type\":\"string\"},{\"name\":\"callingcellid\",\"type\":\"string\"},{\"name\":\"chargingtime\",\"type\":\"string\"},{\"name\":\"waitduration\",\"type\":\"string\"},{\"name\":\"callreferencenumber\",\"type\":\"string\"},{\"name\":\"imei\",\"type\":\"string\"},{\"name\":\"mscaddress\",\"type\":\"string\"},{\"name\":\"el_record_date\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}],\"default\":null},{\"name\":\"load_date\",\"type\":[\"null\",\"string\"],\"default\":null}]}"}' \
>   http://schemaregistry.confluent.svc:8081/subjects/voice-cdr-partitioned/versions
> ```

---

#### 1.2 Creating `CDR_FULL_PIPELINE` on the Canvas
1. Open the NiFi canvas at **`https://169.58.218.71:31443/nifi`**.
2. Log in using `samo-admin` / `SamoNiFiPassword2026!`.
3. Drag the **Process Group** icon from the top toolbar onto the canvas.
4. In the dialog, set:
   - **Process Group Name**: **`CDR_FULL_PIPELINE`**
5. Double-click `CDR_FULL_PIPELINE` to enter its empty canvas.

---

#### 1.3 Defining the Pipeline Parameter Context
Parameters allow central management of environment-specific values across all child groups:
1. In the top-right hamburger menu (☰) of NiFi, click **Parameter Contexts**.
2. Click the **`+` (Add Parameter Context)** button:
   - **Name**: `samo-cdr-pipeline-params`
3. In the **Parameters** tab, click `+` to add the following parameters:

| Parameter Name | Value | Sensitive? | Purpose |
| :--- | :--- | :--- | :--- |
| `cdr.remote.host` | `169.58.218.161` | No | SFTP server hostname. |
| `cdr.remote.port` | `22` | No | SFTP server SSH port. |
| `cdr.remote.user` | `samo` | No | SFTP username. |
| `cdr.remote.password` | `3682154Aa1!` | **Yes** | SFTP password. |
| `cdr.remote.path` | `/home/samo/SFTP/VOICE/voice_cdrs` | No | Remote directory monitored by ListSFTP. |
| `cdr.backup.path` | `/home/samo/SFTP/VOICE/voice_cdrs_backup` | No | Destination directory for fetched files. |
| `polaris.catalog.uri` | `http://polaris.polaris.svc.cluster.local:8181/api/catalog` | No | REST Iceberg catalog URI. |
| `polaris.oauth.uri` | `http://polaris.polaris.svc.cluster.local:8181/api/catalog/v1/oauth/tokens` | No | OAuth2 token server endpoint. |
| `polaris.client.id` | `d5f9cdea48a6fb7f` | No | OAuth2 client identifier. |
| `polaris.client.secret` | `ae88731a268f5c0e33e593508fad6fcf` | **Yes** | OAuth2 client secret. |
| `polaris.warehouse` | `bigdata_catalog` | No | Target Polaris warehouse. |
| `s3.minio.endpoint` | `http://minio.minio.svc.cluster.local:9000` | No | Object storage endpoint for Iceberg Parquet files. |
| `s3.minio.access.key`| `BnMtc7hhYG705rlXFcdj` | No | S3 access key for cluster MinIO. |
| `s3.minio.secret.key`| `VZP8Xcv7RHPUIRmiPZH1tCNOwqJUtmuJ8tvqv2ok` | **Yes** | S3 secret key for cluster MinIO. |
| `schema.registry.url` | `http://schemaregistry.confluent.svc:8081` | No | Central Confluent Schema Registry (Kafka) endpoint inside K8s cluster. |
| `smtp.host` | `169.58.218.161` | No | SMTP mail relay host. |
| `smtp.port` | `25` | No | SMTP port (standard 25 or 587). |
| `smtp.from` | `nifi-pipeline@telesom.com` | No | From alert email address. |
| `smtp.to` | `ops-alerts@telesom.com` | No | Operations team alert distribution list. |

4. Click **Apply**.
5. Inside `CDR_FULL_PIPELINE`, open the **Operate Palette** (left sidebar), click **Gear (Configure)**:
   - In the **General** tab under **Parameter Context**, select **`samo-cdr-pipeline-params`**.
   - Click **Apply**. Now all 5 child process groups will inherit these parameters!

---

#### 1.4 Configuring Top-Level Controller Services
In the same Configuration modal on `CDR_FULL_PIPELINE`, click the **Controller Services** tab. Add and configure the following services:

##### Service A: `ConfluentSchemaRegistry` (Central Kafka Schema Registry)
- **Controller Service Type**: `ConfluentSchemaRegistry`
- **Bundle**: `org.apache.nifi - nifi-confluent-platform-nar`
- **Name**: `confluent-schema-registry`
- **Properties Tab**:
  - `Schema Registry URLs`: `#{schema.registry.url}` *(resolves to `http://schemaregistry.confluent.svc:8081`)*
  - `SSL Context Service`: *(Leave blank / No SSL for internal cluster communication)*
  - `Timeout`: `30 secs`
  - `Cache Expiration`: `1 hour`
  - `Cache Size`: `1000`
- Click **Apply**.

##### Service B: `CSVReader`
- **Controller Service Type**: `CSVReader`
- **Name**: `voice-csv-reader`
- **Properties Tab**:
  - `Schema Access Strategy`: `Use 'Schema Name' Property`
  - `Schema Registry`: Select **`confluent-schema-registry`**
  - `Schema Name`: `voice-cdr`
  - `Value Separator`: `|`
  - `Treat First Line as Header`: `false`
  - `Date Format`: *(Leave blank / empty)*
  - `Timestamp Format`: `yyyy-MM-dd HH:mm:ss.SSS`
- Click **Apply**.

##### Service C: `AvroRecordSetWriter` (Raw 19-Column Writer)
- **Controller Service Type**: `AvroRecordSetWriter`
- **Name**: `voice-avro-writer-raw`
- **Properties Tab**:
  - `Schema Write Strategy`: `Embed Avro Schema`
  - `Schema Access Strategy`: `Use 'Schema Name' Property`
  - `Schema Registry`: Select **`confluent-schema-registry`**
  - `Schema Name`: `voice-cdr`
- Click **Apply**.

##### Service D: `AvroRecordSetWriter` (Partitioned 21-Column Writer)
- **Controller Service Type**: `AvroRecordSetWriter`
- **Name**: `voice-avro-writer-partitioned`
- **Properties Tab**:
  - `Schema Write Strategy`: `Embed Avro Schema`
  - `Schema Access Strategy`: `Use 'Schema Name' Property`
  - `Schema Registry`: Select **`confluent-schema-registry`**
  - `Schema Name`: `voice-cdr-partitioned`
- Click **Apply**.

##### Service E: `AvroReader`
- **Controller Service Type**: `AvroReader`
- **Name**: `voice-avro-reader`
- **Properties Tab**:
  - `Schema Access Strategy`: `Embedded Avro Schema`
- Click **Apply**.

##### Service F: `S3IcebergFileIOProvider`
- **Controller Service Type**: `S3IcebergFileIOProvider`
- **Name**: `s3-iceberg-file-io`
- **Properties Tab**:
  - `Access Key ID`: `#{s3.minio.access.key}`
  - `Secret Access Key`: `#{s3.minio.secret.key}`
  - `Endpoint URL`: `#{s3.minio.endpoint}`
  - `Client Region`: `us-east-1`
  - `Path Style Access`: `true`
- Click **Apply**.

##### Service G: `RESTIcebergCatalog`
- **Controller Service Type**: `RESTIcebergCatalog`
- **Name**: `polaris-iceberg-catalog`
- **Properties Tab**:
  - `Catalog URI`: `#{polaris.catalog.uri}`
  - `File IO Provider`: Select **`s3-iceberg-file-io`**
  - `Access Delegation Strategy`: **`disabled`**
  - `Authentication Strategy`: `OAuth 2.0`
  - `Authorization Server URI`: `#{polaris.oauth.uri}`
  - `Authorization Grant Type`: `Client Credentials`
  - `Client ID`: `#{polaris.client.id}`
  - `Client Secret`: `#{polaris.client.secret}`
  - `Access Token Scopes`: `PRINCIPAL_ROLE:ALL`
  - `Warehouse Location`: `#{polaris.warehouse}`
- Click **Apply**.

##### Service H: `ParquetIcebergWriter`
- **Controller Service Type**: `ParquetIcebergWriter`
- **Name**: `parquet-iceberg-writer`
- **Properties Tab**: (Leave defaults)
- Click **Apply**.

##### Enabling Controller Services:
Click the **Lightning Bolt (⚡)** icon next to each controller service, select **Service and referencing components**, and click **Enable**. Verify that all services display a green **Enabled** status.

---

### Step 2: Child Process Group 1 — `EXTRACT`

1. Inside `CDR_FULL_PIPELINE`, drag a new Process Group onto the canvas named **`EXTRACT`**.
2. Double-click to enter `EXTRACT`.

#### 2.1 Adding Output Ports:
Drag the **Output Port** icon onto the canvas three times:
- Port 1: `records-out` (Transfers fetched files to `TRANSFORM`)
- Port 2: `monitor-tap-out` (Clones fetched files to `MONITORING`)
- Port 3: `failures-out` (Transfers SFTP fetch errors to `ALERTING`)

#### 2.2 Configuring `ListSFTP`:
- **Processor**: `ListSFTP`
- **Scheduling Tab**:
  - `Execution`: **`On Primary Node`** *(Critical: prevents multi-node listing collisions!)*
  - `Run Schedule`: `10 sec`
- **Properties Tab**:
  - `Hostname`: `#{cdr.remote.host}`
  - `Port`: `#{cdr.remote.port}`
  - `Username`: `#{cdr.remote.user}`
  - `Password`: `#{cdr.remote.password}`
  - `Remote Path`: `#{cdr.remote.path}`
  - `Search Recursively`: `false`
  - `File Filter Regex`: `^cdr_voice_\d{14}_\d{3}\.p$` *(Anchored regex)*
- Click **Apply**.

#### 2.3 Round-Robin Load Balancing on `ListSFTP ➔ FetchSFTP`:
- Drag a `FetchSFTP` processor onto the canvas.
- Connect `ListSFTP` ➔ `FetchSFTP` (relationship: `success`).
- In the Connection Configuration dialog:
  - Click the **Settings** tab.
  - In **Load Balance Strategy**, select **`Round Robin`**.
  - Leave **Load Balance Compression** at `Do not compress`.
  - Click **Apply**.
  - > [!TIP]
    > **Why Round Robin is Essential**: `ListSFTP` runs exclusively on the Primary Node. Without `Round Robin` balancing on this connection, every file download would occur on that single node. Round Robin redistributes listed FlowFiles evenly across all cluster worker nodes for parallel fetching.

#### 2.4 Configuring `FetchSFTP` & Dual-Output Tapping:
- **Processor**: `FetchSFTP`
- **Properties Tab**:
  - `Hostname`: `${sftp.remote.host}`
  - `Port`: `${sftp.remote.port}`
  - `Username`: `#{cdr.remote.user}`
  - `Password`: `#{cdr.remote.password}`
  - `Remote File`: `${path}/${filename}`
  - `Completion Strategy`: `Move File`
  - `Move Destination Directory`: `#{cdr.backup.path}`
- **Settings Tab**:
  - Auto-terminate: `comms.failure`, `not.found`, `permission.denied`.
- Click **Apply**.

#### Wiring Outputs in `EXTRACT`:
1. Connect `FetchSFTP` ➔ `records-out` (relationship: `success`).
2. Connect `FetchSFTP` ➔ `monitor-tap-out` (relationship: `success`).
   *(Notice that `FetchSFTP` now has TWO connections from `success`. This creates the non-intrusive stream tap clone!)*
3. Connect `FetchSFTP` ➔ `failures-out` (relationship: (select any failure/error relationships not auto-terminated, or route failure to `failures-out`)).

---

### Step 3: Child Process Group 2 — `TRANSFORM`

1. Return to `CDR_FULL_PIPELINE`, drag a new Process Group onto the canvas named **`TRANSFORM`**.
2. Double-click to enter `TRANSFORM`.

#### 3.1 Adding Ports:
- **Input Port**: `transform-in`
- **Output Port 1**: `transform-out`
- **Output Port 2**: `failures-out`

#### 3.2 Configuring `ConvertRecord` (CSV ➔ Avro):
- **Processor**: `ConvertRecord`
- Connect `transform-in` ➔ `ConvertRecord`.
- **Properties Tab**:
  - `Record Reader`: Select `voice-csv-reader`.
  - `Record Writer`: Select `voice-avro-writer-raw`.
- **Settings Tab**: Route `failure` ➔ `failures-out` (or connect failure relationship to `failures-out`).
- Click **Apply**.

#### 3.3 Configuring First `UpdateRecord` (Deriving `el_record_date` from Record):
- **Processor**: `UpdateRecord`
- **Name**: `UpdateRecord-DerivePartition`
- Connect `ConvertRecord` ➔ `UpdateRecord-DerivePartition` (relationship: `success`).
- **Properties Tab**:
  - `Record Reader`: Select `voice-avro-reader`.
  - `Record Writer`: Select `voice-avro-writer-partitioned`.
  - `Replacement Value Strategy`: **`Record Path Value`** *(CRITICAL! Must be Record Path Value)*.
  - Click `+` to add dynamic property:
    - **`/el_record_date`**: `format(toDate(/create_date, 'yyyyMMddHHmm'), 'yyyy-MM-dd')`
- **Settings Tab**: Route `failure` ➔ `failures-out`.
- Click **Apply**.

#### 3.4 Configuring Second `UpdateRecord` (Deriving `load_date`):
- **Processor**: `UpdateRecord`
- **Name**: `UpdateRecord-AddLoadDate`
- Connect `UpdateRecord-DerivePartition` ➔ `UpdateRecord-AddLoadDate` (relationship: `success`).
- **Properties Tab**:
  - `Record Reader`: Select `voice-avro-reader`.
  - `Record Writer`: Select `voice-avro-writer-partitioned`.
  - `Replacement Value Strategy`: `Literal Value`.
  - Click `+` to add dynamic property:
    - **`/load_date`**: `${now():format('yyyy-MM-dd')}`
- **Settings Tab**: Route `failure` ➔ `failures-out`.
- Click **Apply**.
- Connect `UpdateRecord-AddLoadDate` ➔ `transform-out` (relationship: `success`).

---

### Step 4: Child Process Group 3 — `LOAD`

1. Return to `CDR_FULL_PIPELINE`, drag a new Process Group onto the canvas named **`LOAD`**.
2. Double-click to enter `LOAD`.

#### 4.1 Adding Ports:
- **Input Port**: `load-in`
- **Output Port 1**: `load-out`
- **Output Port 2**: `failures-out`

#### 4.2 Configuring `MergeRecord` (Bin-Packing):
- **Processor**: `MergeRecord`
- Connect `load-in` ➔ `MergeRecord`.
- **Properties Tab**:
  - `Record Reader`: Select `voice-avro-reader`.
  - `Record Writer`: Select `voice-avro-writer-partitioned`.
  - `Merge Strategy`: `Bin-Packing Algorithm`.
  - `Attribute Strategy`: `Keep Only Common Attributes`.
  - `Minimum Number of Records`: `50000` *(for rapid testing, set to `1` temporarily, then revert to 50k)*.
  - `Maximum Number of Records`: `250000`.
  - `Max Bin Age`: `5 min` *(for rapid testing, set to `10 sec` temporarily, then revert to 5 min)*.
  - `Correlation Attribute Name`: **`el_record_date`** *(CRITICAL: prevents cross-partition binning!)*.
- **Settings Tab**: Auto-terminate `original`, route `failure` ➔ `failures-out`.
- Click **Apply**.

#### 4.3 Configuring `PutIcebergRecord` (Terminal Lakehouse Sink):
- **Processor**: `PutIcebergRecord`
- Connect `MergeRecord` ➔ `PutIcebergRecord` (relationship: `merged`).
- **Properties Tab** *(Fill in these 5 properties only)*:
  - `Iceberg Catalog`: Select `polaris-iceberg-catalog`.
  - `Iceberg Writer`: Select `parquet-iceberg-writer`.
  - `Record Reader`: Select `voice-avro-reader`.
  - `Namespace`: **`cdrs`**.
  - `Table Name`: **`voice_lab`**.
- **Settings Tab**:
  - Connect `success` ➔ `load-out`.
  - Route `failure` ➔ `failures-out`.
- Click **Apply**.

---

### Step 5: Child Process Group 4 — `MONITORING`

1. Return to `CDR_FULL_PIPELINE`, drag a new Process Group onto the canvas named **`MONITORING`**.
2. Double-click to enter `MONITORING`.

#### 5.1 Adding Ports:
- **Input Port**: `monitor-in`
- **Output Port**: `monitor-alerts-out`

#### 5.2 Configuring `MonitorActivity`:
- **Processor**: `MonitorActivity`
- Connect `monitor-in` ➔ `MonitorActivity`.
- **Properties Tab**:
  - `Threshold Duration`: `5 min` *(or `1 min` for lab alert testing)*.
  - `Continually Send Messages`: `true` *(repeats alert if ingestion remains quiet)*.
  - `Reporting Node`: **`Primary Node`**.
  - `Inactivity Message`: `LAKEHOUSE WARNING: Ingestion pipeline has been quiet for 5 minutes. No CDR files received.`
  - `Activity Restored Message`: `LAKEHOUSE RECOVERY: CDR ingestion traffic resumed. Processing active.`
- **Settings Tab**:
  - Auto-terminate: `success` *(the tapped FlowFile clone has fulfilled its purpose and must be discarded)*.
- Connect relationships `inactive` and `activity.restored` ➔ `monitor-alerts-out`.
- Click **Apply**.

---

### Step 6: Child Process Group 5 — `ALERTING`

1. Return to `CDR_FULL_PIPELINE`, drag a new Process Group onto the canvas named **`ALERTING`**.
2. Double-click to enter `ALERTING`.

#### 6.1 Adding Input Ports:
- **Input Port 1**: `failures-in`
- **Input Port 2**: `monitor-in`

#### 6.2 Adding `UpdateAttribute` for Alert Classification:
- **Processor**: `UpdateAttribute`
- **Name**: `UpdateAttribute-TagAlertContext`
- Connect `failures-in` ➔ `UpdateAttribute-TagAlertContext`.
- Connect `monitor-in` ➔ `UpdateAttribute-TagAlertContext`.
- **Properties Tab**: Click `+` to add dynamic properties:
  - `alert.type`: `${inactivityDurationMillis:isEmpty():ifElse('pipeline-failure', ${inactivityDurationMillis:gt(0):ifElse('pipeline-quiet', 'pipeline-restored')})}`
  - `alert.timestamp`: `${now():format('yyyy-MM-dd HH:mm:ss')}`
- Click **Apply**.

#### 6.3 Configuring `PutEmail`:
- **Processor**: `PutEmail`
- Connect `UpdateAttribute-TagAlertContext` ➔ `PutEmail` (relationship: `success`).
- **Properties Tab**:
  - `SMTP Hostname`: `#{smtp.host}`
  - `SMTP Port`: `#{smtp.port}`
  - `From`: `#{smtp.from}`
  - `To`: `#{smtp.to}`
  - `Subject`: `CDR pipeline alert — ${alert.type}`
  - `Message`:
    ```text
    =======================================================
    TELESOM LAKEHOUSE AUTOMATED PIPELINE ALERT
    =======================================================
    Alert Type       : ${alert.type}
    Event Timestamp  : ${alert.timestamp}
    Cluster Hostname : ${hostname()}
    Source File      : ${filename:replaceEmpty('N/A (Heartbeat Monitor)')}
    Record Partition : ${el_record_date:replaceEmpty('N/A')}
    Inactivity (ms)  : ${inactivityDurationMillis:replaceEmpty('0')}

    Please inspect Apache NiFi canvas and Polaris Iceberg commit status.
    =======================================================
    ```
  - `Include All Attributes`: `true`.
- **Settings Tab**: Auto-terminate `success`, `failure`.
- Click **Apply**.

---

### Step 7: Canvas Top-Level Assembly & Inter-Group Port Wiring

Return to the top-level canvas inside `CDR_FULL_PIPELINE`. Arrange the five process groups visually and connect their ports:

```text
┌─────────────────┐  records-out ──▶  ┌───────────────────┐  transform-out ──▶  ┌──────────────┐  load-out ──▶  ┌────────────────┐
│     EXTRACT     ├───────────────────┤     TRANSFORM     ├─────────────────────┤     LOAD     ├──────────────▶│ Success Funnel │
└────────┬───┬────┘                   └─────────┬─────────┘                     └───────┬──────┘               └────────────────┘
         │   │failures-out                      │failures-out                           │failures-out
         │   └────────────────────────┬─────────┴───────────────────────────────────────┘
         │monitor-tap-out             │
         ▼                            ▼
┌─────────────────┐             ┌───────────┐
│   MONITORING    │             │  Funnel   │
└────────┬────────┘             └─────┬─────┘
         │monitor-alerts-out          │
         ▼                            │failures-in
┌─────────────────────────────────────▼┐
│               ALERTING               │
└──────────────────────────────────────┘
```

#### Exact Top-Level Wiring Steps:
1. **Ingestion Trunk**:
   - Drag from `EXTRACT` ➔ `TRANSFORM`: Select `records-out` ➔ `transform-in`.
   - Drag from `TRANSFORM` ➔ `LOAD`: Select `transform-out` ➔ `load-in`.
2. **Monitoring Tap**:
   - Drag from `EXTRACT` ➔ `MONITORING`: Select `monitor-tap-out` ➔ `monitor-in`.
3. **Terminal Completion Sink (`LOAD.load-out`)**:
   - Drag a **Funnel** onto the canvas to the right of `LOAD` (serving as the `Pipeline-Success-Sink`).
   - Drag a connection from `LOAD` ➔ this Funnel: Select **`load-out`**.
   - > [!TIP]
   - > **Why `load-out` Must Be Connected**: In Step 4, `PutIcebergRecord` routes successfully committed FlowFiles to the `load-out` output port. If `load-out` is left unconnected on the parent canvas, successfully processed FlowFiles remain queued indefinitely inside the `LOAD` group. Connecting `load-out` to a Funnel on the parent canvas clears the queue cleanly and provides a live visual counter of completed lakehouse batches!
4. **Failure Aggregation Funnel**:
   - Drag another **Funnel** onto the canvas between `TRANSFORM` and `ALERTING`.
   - Connect `EXTRACT` ➔ Funnel: Select `failures-out`.
   - Connect `TRANSFORM` ➔ Funnel: Select `failures-out`.
   - Connect `LOAD` ➔ Funnel: Select `failures-out`.
   - Connect Funnel ➔ `ALERTING`: Select `failures-in`.
5. **Monitoring Alert Wire**:
   - Drag from `MONITORING` ➔ `ALERTING`: Select `monitor-alerts-out` ➔ `monitor-in`.
6. **Start Pipeline**:
   - Right-click the blank canvas inside `CDR_FULL_PIPELINE` ➔ Click **Start**.
   - All processors in all 5 child groups will turn green `▶`.

---

### Step 8: End-to-End Operational Verification Across Dashboards

Execute the verification steps across all web dashboards (MinIO Console, Trino Web UI, and Apache NiFi Canvas) to confirm data integrity, storage layout, partition alignment, and operational health.

---

#### 8.1 Visual Storage Inspection via MinIO Console Dashboard
Verify that Iceberg Parquet files and metadata are physically written into the object store:

1. Open your browser and navigate to **MinIO Console**:
   - **URL**: `http://minio-console.transcode.com` *(or `http://169.58.218.71:30901`)*
   - **Username**: `samo-admin`
   - **Password**: `SamoSecureMinioPass2026!`
2. In the left navigation menu, click **Object Browser**.
3. **Inspect the Iceberg Table Storage**:
   - Click the bucket **`big-data`**.
   - Navigate into: **`warehouse` ➔ `cdrs` ➔ `voice_lab`**.
   - You will see two primary subdirectories: `metadata/` and `data/`.
4. **Inspect `metadata/` Folder**:
   - Click into **`metadata/`**.
   - Verify the presence of:
     - `v1.metadata.json`, `v2.metadata.json`, etc.: Contains table schema, Field IDs, and partition specifications.
     - `snap-*.avro`: Manifest lists detailing table snapshot states.
     - `*-m0.avro`: Manifest files cataloging individual Parquet data files.
   - Click on any metadata file to view its exact size, creation timestamp, and object details.
5. **Inspect `data/` Folder (Partition Directories & Parquet Files)**:
   - Navigate back to `voice_lab/` and click into **`data/`**.
   - Verify that partitions appear as:
     - **`el_record_date=2026-09-14/`** *(or the corresponding event date from `/create_date`)*.
   - Click inside the partition folder:
     - Observe the clean, Snappy-compressed `.parquet` files written by `PutIcebergRecord`.
     - Notice that because of `MergeRecord` bin-packing, files are properly sized (several megabytes each) instead of thousands of tiny 2 KB fragments!
6. **Inspect Raw File Archive**:
   - In Object Browser, click into bucket **`voice-cdr-raw`** to verify that raw SFTP files are also safely preserved for auditing and replay.

---

#### 8.2 Analytics & Snapshot Auditing via Trino Web UI & SQL Console
Validate record counts, event-time partitioning, snapshot commits, and file registration using the **Trino Web UI / SQL Console** (or DataGrip / DBeaver connected to `http://trino.transcode.com`):

1. Open the **Trino Web UI**:
   - **URL**: `http://trino.transcode.com` *(or `http://169.58.218.71:8080`)*
   - **User**: `samo-admin`
   - Here you can monitor live cluster metrics, active workers, queries per second, and execution splits.
2. In DataGrip, DBeaver, or Trino CLI, execute the following audit queries:

```sql
-- Query 1: Inspect real-time total row count
SELECT COUNT(*) AS total_rows FROM lakehouse.cdrs.voice_lab;

-- Query 2: Validate partition grouping by el_record_date (verifies RecordPath derived event dates)
SELECT 
    el_record_date,
    COUNT(*) AS records_per_partition,
    MIN(create_date) AS min_call_time,
    MAX(create_date) AS max_call_time
FROM lakehouse.cdrs.voice_lab
GROUP BY el_record_date
ORDER BY el_record_date DESC;

-- Query 3: Audit Iceberg Snapshots & commits via metadata table
SELECT 
    snapshot_id,
    committed_at,
    operation,
    summary['total-records'] AS total_rows,
    summary['added-data-files'] AS files_added,
    summary['added-records'] AS rows_added
FROM lakehouse.cdrs."voice_lab$snapshots"
ORDER BY committed_at DESC;

-- Query 4: Audit physical Parquet data files registered in Polaris catalog
SELECT 
    file_path, 
    file_format, 
    record_count, 
    file_size_in_bytes,
    partition
FROM lakehouse.cdrs."voice_lab$files";
```

3. **Verify Expected Output**:
   - `total_rows` continuously increments as new batches arrive.
   - `el_record_date` groups strictly align with the `create_date` values in the records.
   - Each snapshot record in `voice_lab$snapshots` shows `operation: append` with accurate `added-records` and `added-data-files`.

---

#### 8.3 Real-Time Pipeline Inspection via NiFi Dashboard (Queues, Provenance & Bulletins)
Use NiFi's built-in visual diagnostic tools to monitor execution and inspect in-flight FlowFiles:

1. **Global Process Group Summary**:
   - In the top-right toolbar of NiFi, click the **Summary** icon (📊 table icon).
   - In the **Process Groups** tab, inspect all 5 child groups (`EXTRACT`, `TRANSFORM`, `LOAD`, `MONITORING`, `ALERTING`).
   - Check the **In**, **Out**, and **Queued** columns to verify continuous data flow with zero backpressure.
2. **Visually Inspecting FlowFiles in Queues**:
   - Right-click any connection (e.g., between `UpdateRecord-DerivePartition` and `UpdateRecord-AddLoadDate` or between `MergeRecord` and `PutIcebergRecord`).
   - Select **List Queue**.
   - A queue listing modal appears showing all FlowFiles waiting in the buffer.
   - Click the **View (Eye 👁️)** icon on any FlowFile:
     - In the **Details** tab: View FlowFile UUID, size, and creation timestamp.
     - In the **Attributes** tab: Verify attributes such as `el_record_date`, `filename`, and `path`.
     - In the **Content** tab: Click **View** (or **Download**) to view formatted record content.
3. **Data Provenance & Lineage Graph**:
   - Right-click `PutIcebergRecord` (or `ConvertRecord`).
   - Select **View data provenance**.
   - Click the **Lineage** icon (branch icon) on the right side of any event.
   - NiFi displays the complete visual DAG of that specific FlowFile: tracing its journey from `FETCH` (SFTP) ➔ `CONVERT` (CSV to Avro) ➔ `MODIFY_CONTENT` (RecordPath date extraction) ➔ `JOIN` (MergeRecord) ➔ `SEND` (Iceberg Parquet commit).
4. **Bulletin Board**:
   - Click the **Bulletin Board** icon (top right 🔔 bell / newspaper icon).
   - Verify that there are no red error bulletins. Any schema mismatches, network timeouts, or invalid configurations will be prominently highlighted here with exact stack traces.

---

### Step 9: Testing & Exercising the Alerting Subsystem

To verify that the monitoring and alerting system functions under all operating conditions, execute the following three tests:

#### Test Scenario A: Pipeline Processing Failure Alert
1. In `CDR_FULL_PIPELINE` ➔ `LOAD`, temporarily stop `PutIcebergRecord`.
2. Configure `Table Name` in `PutIcebergRecord` to a non-existent table: `invalid_table_test`.
3. Start `PutIcebergRecord` and let one merged FlowFile enter.
4. `PutIcebergRecord` will reject the write and route to `failures-out`.
5. Verify in your email client that an email arrives:
   - **Subject**: `CDR pipeline alert — pipeline-failure`
   - **Message**: Contains hostname, affected file, and failure context.
6. Revert `Table Name` back to `voice_lab` and restart.

#### Test Scenario B: Ingestion Pipeline Stall (Inactivity) Alert
1. Stop the CDR generator on the jumphost:
   ```bash
   pkill -f cdr_file_generator.py
   ```
2. Wait past the configured `Threshold Duration` (e.g., 5 minutes, or 1 minute if set for testing).
3. `MonitorActivity` detects zero FlowFiles arriving on `monitor-in`.
4. Verify an alert arrives:
   - **Subject**: `CDR pipeline alert — pipeline-quiet`
   - **Message**: Displays inactivity duration and warning message.

#### Test Scenario C: Ingestion Recovery (Activity Restored) Alert
1. Restart the CDR generator on the jumphost:
   ```bash
   cd /home/samo/SFTP/VOICE && python3 cdr_file_generator.py &
   ```
2. As soon as `FetchSFTP` fetches the next new file and passes the tapped clone to `MonitorActivity`, `activity.restored` fires.
3. Verify the recovery alert arrives:
   - **Subject**: `CDR pipeline alert — pipeline-restored`
   - **Message**: Indicates traffic has resumed and pipeline is healthy.

---

### Step 10: Complete Troubleshooting & Error Reference Matrix

| # | Symptom / Error | Root Cause | Exact Solution |
| :--- | :--- | :--- | :--- |
| **1** | `ListSFTP` lists nothing | Filename does not match anchored regex `^cdr_voice_\d{14}_\d{3}\.p$`. | Ensure files in `/home/samo/SFTP/VOICE/voice_cdrs/` strictly match the 14-digit timestamp and 3-digit suffix. |
| **2** | All fetches stay pinned to Primary Node | Round Robin was set on the wrong connection or `ListSFTP` is not set to `On Primary Node`. | Set `Execution: On Primary Node` on `ListSFTP`. Set `Load Balance Strategy: Round Robin` on the connection between `ListSFTP` and `FetchSFTP`. |
| **3** | `el_record_date` is `null` in Iceberg table | `UpdateRecord` had `Replacement Value Strategy` set to `Literal Value` instead of `Record Path Value`. | Change `Replacement Value Strategy` to **`Record Path Value`** on `UpdateRecord-DerivePartition`. RecordPath functions only evaluate under this strategy. |
| **4** | `el_record_date = today` instead of actual call date | `UpdateRecord` was referencing the arrival `filename` or `${now()}` instead of the record's `/create_date` field. | Use RecordPath expression: `format(toDate(/create_date, 'yyyyMMddHHmm'), 'yyyy-MM-dd')`. |
| **5** | `PutIcebergRecord` throws unmatched column error | Attached `voice-avro-writer-raw` instead of `voice-avro-writer-partitioned` after the derive steps. | Ensure `UpdateRecord` and `MergeRecord` output records using `voice-avro-writer-partitioned` containing all 21 schema fields. |
| **6** | No `inactive` email generated | Threshold Duration too long for test window, or `MonitorActivity` reporting node is not set to Primary Node. | Set `Reporting Node: Primary Node`. Reduce `Threshold Duration` to `1 min` during testing. |
| **7** | `MONITORING` group stalling halts entire data flow | `MonitorActivity` was placed inline in the main ingestion path instead of on the tapped clone. | Ensure `MonitorActivity` sits strictly inside the `MONITORING` group connected via `monitor-tap-out`. Auto-terminate its `success` relationship. |
| **8** | `PutIcebergRecord: java.lang.ArrayIndexOutOfBoundsException` | The schema registry has fewer columns than the target Iceberg table. | Register the full 21-column schema (`voice-cdr-partitioned`) with `default: null` for `el_record_date` and `load_date`. |
| **9** | `PutIcebergRecord: Credential vending requested...` | Polaris catalog has `Access Delegation Strategy` enabled. | In `polaris-iceberg-catalog`, set `Access Delegation Strategy: disabled`. Direct storage calls are delegated to `s3-iceberg-file-io`. |
| **10**| `S3IcebergFileIOProvider: NoSuchBucket / 404` | FileIO points to a local isolated MinIO instance instead of the cluster MinIO hosting `s3://big-data/warehouse/`. | Set `Endpoint URL: http://minio.minio.svc.cluster.local:9000` with cluster MinIO credentials (`BnMtc7hhYG705rlXFcdj` / `VZP8Xcv7RHPUIRmiPZH1tCNOwqJUtmuJ8tvqv2ok`). |

---

# Part 4: Day 4 Knowledge Verification & Assessment Review

1. **Why does `ListSFTP` run strictly on the Primary Node?**
   - If multiple nodes execute `ListSFTP`, each node independently polls the remote directory, creating duplicate FlowFiles for identical remote files. Running on the Primary Node guarantees a single, coordinated listing.

2. **Why must the `ListSFTP ➔ FetchSFTP` connection use Round Robin load balancing?**
   - Because `ListSFTP` runs on the Primary Node, all downstream FlowFiles originate there. Without Round Robin balancing, `FetchSFTP` would execute only on that single node. Round Robin redistributes files across all cluster worker nodes to parallelize file downloads across network interfaces.

3. **Why must `el_record_date` be derived from `/create_date` and not the filename?**
   - The filename records the time the mediation batch file was written or transferred (arrival time). Field 4 (`create_date`) records the exact moment the phone call occurred (event time). Event-time partitioning ensures data resides in accurate historical partitions regardless of transmission delays.

4. **Why does `UpdateRecord` require `Replacement Value Strategy = Record Path Value` for `format(toDate(...))`?**
   - Under `Literal Value`, NiFi treats input text literally or as NiFi Expression Language (`${...}`). RecordPath functions operating on payload fields (like `/create_date`) are only parsed and executed when the processor is explicitly told to evaluate expressions as RecordPath values.

5. **Why must `MonitorActivity` sit on a tapped clone rather than inline?**
   - If placed inline, any delay, lock, or failure in the monitoring subsystem causes backpressure that directly stalls raw CDR ingestion. Placing `MonitorActivity` on an asynchronous tapped clone decouples monitoring from data transport.

6. **Why does `MergeRecord` correlate on `el_record_date`?**
   - Iceberg partitions `lakehouse.cdrs.voice_lab` by `day(el_record_date)`. If `MergeRecord` did not correlate on `el_record_date`, a single bin could contain records spanning multiple days. When written to Iceberg, this would force the writer to open multiple partition files simultaneously, degrading commit performance and causing partition fragmentation.

7. **How does Iceberg handle schema evolution when fields are added?**
   - Iceberg uses immutable, integer-based Field IDs. Adding a field merely assigns a new Field ID in `metadata.json`. Existing Parquet files are untouched; when engines query historical files lacking the new column, the engine synthesizes `NULL` values automatically without full-table rewrites.
