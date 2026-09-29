# Telesom & Dahabshiil Data Platform — Apache Airflow Lakehouse Orchestration Master Guide
## Automated Lakehouse Maintenance, Daily Aggregations, Trino-to-Postgres Analytics Sync & SLA Alerting
### (Complete Dashboard-Driven & Production Web UI Implementation Edition)

---

## Executive Table of Contents
1. **Airflow in the Modern Lakehouse Architecture**
   - 1.1 What Problem Airflow Actually Solves
   - 1.2 Core Architecture: Webserver, Scheduler, Metadata Database & Executors
   - 1.3 Timezones & Scheduling (`Africa/Mogadishu`, UTC+3, `start_date`, `catchup=False`)
   - 1.4 Connections & Security (`conn_id` vs Hardcoded Secrets)
   - 1.5 GitLab GitOps & GitSync: Continuous DAG Deployment in Production
2. **Cluster Topology & Dashboard Access Directory**
3. **Step-by-Step Implementation Guide**
   - **Step 1: Airflow Environment & Connection Setup via Web UI Dashboard**
     - *1.1 Accessing the Airflow Web UI*
     - *1.2 Configuring `trino_lakehouse` Connection (UI & CLI)*
     - *1.3 Configuring `rdbms_postgres` Analytics Connection (UI & CLI)*
     - *1.4 Configuring `minio_voice_landing` Object Storage Connection*
     - *1.5 Configuring `smtp_transcode` Alerting Connection*
   - **Step 2: Destination Table Creation in Postgres & Trino**
     - *2.1 Creating the Analytics Table in Postgres (`rdbms.public.voice_lab`)*
     - *2.2 Creating the Summary Rollup Table in Trino (`lakehouse.cdrs.voice_lab_daily_summary`)*
   - **Step 3: DAG 1 — Daily CDR Aggregation & Rollup (`daily_voice_cdr_aggregation`)**
     - *3.1 Business Purpose & Architectural Flow*
     - *3.2 Complete Python DAG Code (`TrinoHook` Implementation)*
     - *3.3 Production Deployment via GitLab CI/CD & GitSync Sidecar*
     - *3.4 Triggering & Verification in the Airflow Grid & Graph Views*
   - **Step 4: DAG 2 — Lakehouse-to-Postgres Analytics Sync (`voice_cdr_lakehouse_to_rdbms_analytics`)**
     - *4.1 The Idempotent "Delete-Then-Insert" Partition Pattern*
     - *4.2 Complete Python DAG Code (`TrinoOperator` Implementation)*
     - *4.3 Jinja Templating & Macro Expansion (`{{ macros.ds_add(ds, -1) }}`)*
     - *4.4 Verifying Data Arrival in Postgres via DataGrip / DBeaver*
   - **Step 5: DAG 3 — Automated Iceberg Lakehouse Maintenance & Compaction (`iceberg_lakehouse_maintenance`)**
     - *5.1 The Small Files & Snapshot Accumulation Problem*
     - *5.2 Static Compaction DAG (`optimize`, `expire_snapshots`, `remove_orphan_files`)*
     - *5.3 Advanced Dynamic Partition Compaction via TaskFlow (`@task` & `.expand()`)*
   - **Step 6: DAG 4 — Automated Cross-System Reconciliation & Alerting (`cdr_reconciliation_and_sla_monitor`)**
     - *6.1 Cross-System Reconciliation Logic (Lakehouse Count vs. Postgres Count)*
     - *6.2 Complete Python DAG Code (`TrinoHook` + `PostgresHook`)*
     - *6.3 Exercising Email Failure Notifications on Data Discrepancy*
   - **Step 7: Testing, Operational Execution & Visual Dashboard Auditing**
     - *7.1 Manual DAG Triggering & Parameterized Runs*
     - *7.2 Reading Task Execution Logs in the Airflow UI*
     - *7.3 Inspecting XComs and Task Instance Details*
   - **Step 8: Complete Troubleshooting & Error Reference Matrix**
4. **Airflow Knowledge Verification & Assessment Review**

---

# Part 1: Airflow in the Modern Lakehouse Architecture

### 1.1 What Problem Airflow Actually Solves

Every other component in our big data platform does work **only when explicitly told to**:
- An Apache NiFi processor runs when a file lands or a timer fires.
- A Trino query runs when an analyst clicks "Execute" in DataGrip.
- MinIO and Apache Iceberg passively store bytes and metadata.

**None of those tools decide:**
- When something should happen on its own (e.g. *"Run this every night at 02:00 EAT"*).
- What should happen **after** something else finishes (e.g. *"Run Step B only after Step A succeeds; run C and D in parallel"*).
- What to do when a step fails (e.g. *"Retry 3 times with exponential backoff, then page the on-call engineer"*).
- How to track operational audits (e.g. *"Did the daily financial reconciliation succeed yesterday at 00:30, who triggered it, and what were the exact execution logs?"*).

**That is the entire purpose of Apache Airflow.**

> [!NOTE]
> Concretely on this platform, Airflow is the orchestrator that:
> 1. Runs aggregation queries in Trino that summarize raw CDRs into hourly and daily rollups.
> 2. Loads clean data from the Iceberg lakehouse (`lakehouse.cdrs.voice_lab`) into the analytics Postgres database (`rdbms.public.voice_lab`) on schedule.
> 3. Automates Iceberg table maintenance (`optimize`, `expire_snapshots`, `remove_orphan_files`) so storage doesn't degrade over time.
> 4. Validates cross-system data reconciliation and alerts via SMTP if data is missing.

---

### 1.2 Core Architecture

Airflow consists of four distinct components running in Kubernetes:

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                               METADATA DATABASE (PostgreSQL)                           │
│       Stores DAG/task state, connections, variables, XComs, users, and execution logs  │
└──────────────┬────────────────────────┬────────────────────────┬───────────────────────┘
               │                        │                        │
┌──────────────▼──────┐  ┌──────────────▼──────┐  ┌──────────────▼───────────────────────┐
│      WEBSERVER      │  │      SCHEDULER      │  │          EXECUTOR + WORKERS          │
│   Flask UI & REST   │  │   Parses DAG files, │  │   Where task code actually executes  │
│   Monitors state,   │  │   evaluates cron,   │  │   • LocalExecutor                    │
│   manual triggers   │  │   queues due tasks  │  │   • KubernetesExecutor (pod per task)│
└─────────────────────┘  └─────────────────────┘  └──────────────────────────────────────┘
```

1. **Metadata Database (PostgreSQL)**: The single source of truth. Every task state, connection credential, and execution history record lives here.
2. **Scheduler**: Continuously monitors the `dags/` folder, parses Python DAG files every few seconds, evaluates schedules, and queues tasks whose time has arrived.
3. **Webserver**: The browser UI (Flask) allowing engineers to inspect DAG graphs, trigger runs, clear failed tasks, read logs, and configure connections.
4. **Executor & Workers**:
   - `LocalExecutor`: Runs tasks as subprocesses on the scheduler node.
   - `KubernetesExecutor` (Enterprise Standard): Spins up a dedicated, isolated Kubernetes worker pod for each task instance and destroys it upon completion.

---

### 1.3 Timezones & Scheduling (`Africa/Mogadishu`, UTC+3)

> [!IMPORTANT]
> **Airflow's Internal Timezone Rule**:
> - Airflow stores and evaluates all timestamps internally in **UTC**.
> - If you use naive Python `datetime.datetime(2026, 9, 1)`, Airflow interprets it as UTC, causing your runs to trigger **3 hours late** compared to East Africa Time (EAT)!
> - Always declare a timezone-aware pendulum instance:
>   ```python
>   import pendulum
>   LOCAL_TZ = pendulum.timezone("Africa/Mogadishu") # UTC+3
>   start_date = pendulum.datetime(2026, 9, 1, tz=LOCAL_TZ)
>   ```
> - A cron string like `schedule="0 2 * * *"` with `start_date` set to `LOCAL_TZ` will execute exactly at **02:00 AM EAT**.
> - **`catchup=False`**: Always set `catchup=False` unless you intentionally want Airflow to execute every missed historical schedule interval since `start_date`!

---

### 1.4 Connections & Security (`conn_id` vs. Hardcoded Secrets)

A **Connection** is a named, encrypted credential record stored in Airflow's metadata database and referenced by its `conn_id`.
- Credentials, hostnames, passwords, and tokens **NEVER** appear in Python DAG code.
- Rotating a password or changing a Trino hostname requires updating **one row** in the Airflow UI (`Admin -> Connections`), with zero code changes or redeployments.

---

#### 1.5 GitLab GitOps & GitSync: Continuous DAG Deployment in Production

In our enterprise lakehouse platform, DAG files are **never manually copied via SSH or SFTP** into production nodes. Manual file copying introduces high operational risks: broken Python syntax can crash the Airflow scheduler, unreviewed SQL changes can corrupt data, and there is zero audit trail of who modified what.

Instead, we enforce a strict **GitOps Continuous Deployment model** using our enterprise GitLab server (`gitlab.transcode.com`):

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                GITLAB REPOSITORY (airflow-dags)                        │
│                                                                                        │
│   Developer creates         GitLab Merge Request          Automated GitLab CI/CD       │
│   feature branch        ──▶ with Peer Review          ──▶ Pipeline Runs:               │
│   (git push)                (Protected 'main' branch)     • flake8 / ruff linting      │
│                                                           • pytest DAG integrity tests │
│                                                                        │               │
│                                                                        │ (CI Passes &  │
│                                                                        ▼  MR Merged)   │
│                                                           ┌────────────────────────┐   │
│                                                           │ Protected 'main' Branch│   │
│                                                           └───────────┬────────────┘   │
└───────────────────────────────────────────────────────────────────────┼────────────────┘
                                                                        │ Polls GitLab
                                                                        │ every 60s via
                                                                        │ Deploy Token
                                                                        ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        KUBERNETES AIRFLOW PODS (samo namespace)                        │
│                                                                                        │
│   ┌─────────────────────────────────┐       ┌──────────────────────────────────────┐   │
│   │   git-sync Sidecar Container    │       │        Airflow Scheduler Pod         │   │
│   │   (Clones/pulls repo via HTTPS) │ ────▶ │        (Reads /opt/airflow/dags)     │   │
│   └────────────────┬────────────────┘       └──────────────────┬───────────────────┘   │
│                    │                                           │                       │
│                    ▼ Mounts                                    ▼ Dynamically           │
│        Shared Kubernetes EmptyDir Volume ◀─────────────────────┘ Refreshes DAGs        │
│                (/opt/airflow/dags/)                              Without Restarts!     │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

#### Core Benefits of the GitLab GitOps Pattern:
1. **Automated CI DAG Validation**: The GitLab CI pipeline executes a headless Python test suite verifying that all DAGs compile, have unique `dag_id`s, and contain no syntax errors before merging to `main`.
2. **Zero-Downtime Deployment**: The Kubernetes `git-sync` sidecar synchronizes the repository to a shared volume (`/opt/airflow/dags/`). The scheduler and webserver detect new and updated DAGs automatically within 60 seconds without needing pod restarts.
3. **Auditability & Compliance**: Every DAG modification, schedule adjustment, and query change is version-controlled, signed, and attributed to a specific data engineer in GitLab.
4. **Instant Rollback**: If a bad pipeline is introduced, reverting the merge commit in GitLab instantly pulls the previous stable commit across all Airflow pods.

---

# Part 2: Cluster Topology & Dashboard Access Directory

Ensure your local `/etc/hosts` contains the platform ingress routes for `169.58.218.71`:
```text
169.58.218.71   airflow.transcode.com
169.58.218.71   gitlab.transcode.com
169.58.218.71   trino.transcode.com
169.58.218.71   minio-console.transcode.com
169.58.218.71   polaris.transcode.com
169.58.218.71   kafkaui.transcode.com
```

### Dashboard Access Matrix (`samo` Namespace):
| Dashboard / UI Tool | Web UI URL | Credentials | Role in Airflow Lab |
| :--- | :--- | :--- | :--- |
| **Enterprise GitLab** | `http://gitlab.transcode.com` | User: `samo-admin`<br>Pass: `SamoSecureGitLab2026!` *(or FreeIPA LDAP)* | **Central GitOps repository for Airflow DAGs** (`telecom-lakehouse/airflow-dags.git`). Runs CI/CD integrity testing & triggers GitSync deployment. |
| **Apache Airflow Webserver** | `http://airflow.transcode.com`<br>*(or `http://169.58.218.71:30080`)* | User: `samo-admin`<br>Pass: `SamoAirflowPass2026!` *(or `admin`)* | Triggering DAGs, inspecting task execution grids, Gantt charts, task logs, and configuring connections. |
| **Trino Query UI / CLI** | `http://trino.transcode.com`<br>*(or DataGrip)* | User: `samo-admin`<br>Catalog: `lakehouse` | Validating source data in `lakehouse.cdrs.voice_lab` and rollup output in `voice_lab_daily_summary`. |
| **MinIO Console (S3 Storage)** | `http://minio-console.transcode.com`<br>*(or `http://169.58.218.71:30901`)* | User: `samo-admin`<br>Pass: `SamoSecureMinioPass2026!` | Verifying physical Parquet files and checking Airflow remote log storage in bucket `big-data`. |
| **PostgreSQL Analytics DB** | Host: `169.58.218.161`<br>Port: `5432`<br>DB: `transcode` | User: `samo`<br>Pass: `3682154Aa1!` | Destination analytical database for the BI serving layer (`rdbms.public.voice_lab`). |
| **SFTP Server Edge (Legacy/Jump)** | Host: `169.58.218.161`<br>Port: `22` | User: `samo`<br>Pass: `3682154Aa1!` | Jumphost / edge landing zone only. *Note: DAG deployments are strictly handled via GitLab GitSync.* |

---

# Part 3: Step-by-Step Implementation Guide

---

### Step 1: Airflow Environment & Connection Setup via Web UI Dashboard

Before running any DAGs, create the reusable connection endpoints inside the Airflow metadata store.

#### 1.1 Accessing the Airflow Web UI
1. Open your browser and navigate to: **`http://airflow.transcode.com`** *(or `http://169.58.218.71:30080`)*.
2. Log in with your admin credentials.
3. In the top navigation bar, click **Admin** ➔ **Connections**.

---

#### 1.2 Configuring `trino_lakehouse` Connection
Allows Airflow tasks (`TrinoOperator`, `TrinoHook`) to submit SQL queries to Trino.

1. On the Connections page, click the **`+` (Add a new record)** button.
2. Enter the following parameters:
   - **Connection Id**: `trino_lakehouse`
   - **Connection Type**: Select **`Trino`**
   - **Host**: `trino-coordinator.trino.svc.cluster.local` *(or `169.58.218.71`)*
   - **Port**: `8080`
   - **Login**: `samo-admin` *(or `svc_airflow`)*
   - **Password**: *(Leave empty or enter service pass)*
   - **Extra**:
     ```json
     {
       "catalog": "lakehouse",
       "schema": "cdrs"
     }
     ```
3. Click **Test** to verify network connectivity.
4. Click **Save**.

---

#### 1.3 Configuring `rdbms_postgres` Analytics Connection
Allows Airflow tasks to query and load data into the downstream analytics PostgreSQL database.

1. Click **`+` (Add a new record)**.
2. Enter:
   - **Connection Id**: `rdbms_postgres`
   - **Connection Type**: Select **`Postgres`**
   - **Host**: `169.58.218.161` *(or `postgres.transcode.internal`)*
   - **Database**: `transcode`
   - **Login**: `samo`
   - **Password**: `3682154Aa1!`
   - **Port**: `5432`
   - **Extra**:
     ```json
     {
       "sslmode": "disable"
     }
     ```
3. Click **Test** ➔ Click **Save**.

---

#### 1.4 Configuring `minio_voice_landing` Object Storage Connection
Allows S3 sensors (`S3KeySensor`) and file operators to inspect MinIO buckets.

1. Click **`+` (Add a new record)**.
2. Enter:
   - **Connection Id**: `minio_voice_landing`
   - **Connection Type**: Select **`Amazon Web Services`**
   - **Login (AWS Access Key ID)**: `BnMtc7hhYG705rlXFcdj`
   - **Password (AWS Secret Access Key)**: `VZP8Xcv7RHPUIRmiPZH1tCNOwqJUtmuJ8tvqv2ok`
   - **Extra**:
     ```json
     {
       "endpoint_url": "http://minio.minio.svc.cluster.local:9000"
     }
     ```
3. Click **Save**.

---

#### 1.5 Configuring `smtp_transcode` Alerting Connection
Allows `EmailOperator` to dispatch operational alerts upon failure.

1. Click **`+` (Add a new record)**.
2. Enter:
   - **Connection Id**: `smtp_transcode`
   - **Connection Type**: Select **`Email`**
   - **Host**: `169.58.218.161`
   - **Port**: `25` *(or `587`)*
   - **Login**: `nifi-pipeline@telesom.com`
   - **Extra**:
     ```json
     {
       "from_email": "ops-alerts@telesom.com"
     }
     ```
3. Click **Save**.

> [!TIP]
> **Companion CLI Method (Alternative to UI)**:
> You can also create all connections instantly via the Airflow CLI on the server:
> ```bash
> airflow connections add trino_lakehouse \
>   --conn-type trino \
>   --conn-host trino-coordinator.trino.svc.cluster.local \
>   --conn-port 8080 \
>   --conn-login samo-admin \
>   --conn-extra '{"catalog": "lakehouse", "schema": "cdrs"}'
>
> airflow connections add rdbms_postgres \
>   --conn-type postgres \
>   --conn-host 169.58.218.161 \
>   --conn-port 5432 \
>   --conn-schema transcode \
>   --conn-login samo \
>   --conn-password '3682154Aa1!'
> ```

---

### Step 2: Destination Table Creation in Postgres & Trino

Before running our DAGs, prepare the target tables in PostgreSQL and Trino.

#### 2.1 Creating the Analytics Table in Postgres (`rdbms.public.voice_lab`)
Connect to PostgreSQL on `169.58.218.161:5432/transcode` (via DataGrip or `psql -U samo -d transcode`) and execute:

```sql
-- Create destination analytical serving table in PostgreSQL
CREATE TABLE IF NOT EXISTS public.voice_lab (
    cdr_id VARCHAR(64) PRIMARY KEY,
    cdr_type VARCHAR(16),
    status VARCHAR(16),
    create_date VARCHAR(32),
    start_date VARCHAR(32),
    end_date VARCHAR(32),
    pri_identity VARCHAR(32),
    actual_usage VARCHAR(16),
    callingpartynumber VARCHAR(32),
    calledpartynumber VARCHAR(32),
    callingpartyimsi VARCHAR(32),
    calledpartyimsi VARCHAR(32),
    dialednumber VARCHAR(32),
    callingcellid VARCHAR(32),
    chargingtime VARCHAR(16),
    waitduration VARCHAR(16),
    callreferencenumber VARCHAR(64),
    imei VARCHAR(32),
    mscaddress VARCHAR(32),
    el_record_date DATE NOT NULL,
    load_date VARCHAR(32)
);

-- Index partition date for fast BI queries
CREATE INDEX IF NOT EXISTS idx_voice_lab_el_record_date ON public.voice_lab(el_record_date);
```

#### 2.2 Creating the Summary Rollup Table in Trino (`lakehouse.cdrs.voice_lab_daily_summary`)
Connect to Trino (`lakehouse` catalog) and execute:

```sql
CREATE TABLE IF NOT EXISTS lakehouse.cdrs.voice_lab_daily_summary (
    el_record_date DATE,
    status VARCHAR,
    call_count BIGINT,
    total_wait BIGINT,
    summary_generated_at TIMESTAMP(6)
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(el_record_date)']
);
```

---

### Step 3: DAG 1 — Daily CDR Aggregation & Rollup (`daily_voice_cdr_aggregation`)

#### 3.1 Business Purpose & Architectural Flow
Raw CDR tables contain millions of individual call rows. Business Intelligence dashboards (Tableau, PowerBI, Superset) should not scan millions of rows to answer "How many calls failed yesterday?".

**The Solution**: This DAG runs every night at 01:00 AM, calculates the daily aggregates in Trino, and inserts them into `voice_lab_daily_summary`.

```text
Scheduled: 01:00 AM EAT
      │
      ▼
TrinoHook: Query lakehouse.cdrs.voice_lab for logical date {{ ds }}
      │ (Calculates call_count, sum(waitduration) grouped by status)
      ▼
INSERT INTO lakehouse.cdrs.voice_lab_daily_summary
      │
      ▼
BI Dashboards query summarized table with sub-millisecond response!
```

#### 3.2 Complete Python DAG Code
GitLab Repository Path: `dags/daily_voice_cdr_aggregation.py` (committed to `telecom-lakehouse/airflow-dags.git`):

```python
"""
DAG: daily_voice_cdr_aggregation
Purpose: Daily rollup of raw telecommunication CDRs into daily summary table in Trino Iceberg.
Schedule: Daily at 01:00 AM (Africa/Mogadishu EAT)
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.trino.hooks.trino import TrinoHook

# Set EAT local timezone
LOCAL_TZ = pendulum.timezone("Africa/Mogadishu")

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "email_on_failure": True,
    "email": ["ops-alerts@telesom.com"],
}

with DAG(
    dag_id="daily_voice_cdr_aggregation",
    description="Summarize raw CDRs from voice_lab into daily aggregation table",
    default_args=default_args,
    schedule="0 1 * * *",  # Nightly at 01:00 AM EAT
    start_date=pendulum.datetime(2026, 9, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "cdrs", "aggregation"],
) as dag:

    def build_daily_summary(**context) -> None:
        """
        Executes aggregation query in Trino for the logical date (ds).
        """
        trino = TrinoHook(trino_conn_id="trino_lakehouse")
        ds = context["ds"]  # Logical execution date formatted as YYYY-MM-DD
        
        sql = f"""
        INSERT INTO lakehouse.cdrs.voice_lab_daily_summary
        SELECT 
            el_record_date, 
            status, 
            COUNT(*) AS call_count,
            SUM(CAST(waitduration AS BIGINT)) AS total_wait,
            CURRENT_TIMESTAMP AS summary_generated_at
        FROM lakehouse.cdrs.voice_lab
        WHERE el_record_date = DATE '{ds}'
        GROUP BY el_record_date, status;
        """
        print(f"Executing aggregation query for date: {ds}")
        trino.run(sql)
        print(f"Aggregation complete for date: {ds}")

    summarize_task = PythonOperator(
        task_id="build_daily_summary",
        python_callable=build_daily_summary,
    )
```

---

#### 3.3 Production Deployment via GitLab CI/CD & GitSync Sidecar

DAG files are managed exclusively in our internal GitLab repository and synchronized to Airflow automatically using GitOps.

##### 3.3.1 GitLab Repository Structure (`airflow-dags`)
Our centralized DAG repository is structured with automated testing and version control:

```text
airflow-dags/
├── .gitlab-ci.yml                     # Automated GitLab CI/CD validation pipeline
├── requirements-dev.txt               # Testing dependencies (pytest, ruff, apache-airflow)
├── tests/
│   └── test_dag_integrity.py         # Pytest suite verifying no parse errors or cycles
└── dags/
    ├── daily_voice_cdr_aggregation.py
    ├── voice_cdr_lakehouse_to_rdbms_analytics.py
    ├── iceberg_lakehouse_maintenance.py
    └── cdr_reconciliation_and_sla_monitor.py
```

##### 3.3.2 Local Git Workspace Setup
Clone the centralized repository from internal GitLab to your local workstation:

```bash
# 1. Clone the repository via HTTPS (or SSH: git@gitlab.transcode.com:telecom-lakehouse/airflow-dags.git)
git clone http://gitlab.transcode.com/telecom-lakehouse/airflow-dags.git
cd airflow-dags

# 2. Configure Git identity
git config user.name "Samo Data Engineer"
git config user.email "samo@telesom.com"

# 3. Create a feature branch for the new DAG
git checkout -b feature/daily-cdr-aggregation
```

##### 3.3.3 Enterprise `.gitlab-ci.yml` Pipeline Configuration
Create `.gitlab-ci.yml` in the root of the repository. This guarantees that **no broken Python code or syntax error is ever merged to `main`**:

```yaml
stages:
  - lint
  - test

variables:
  AIRFLOW__CORE__LOAD_EXAMPLES: "False"
  AIRFLOW__CORE__UNIT_TEST_MODE: "True"

# Stage 1: Fast Python linting and code hygiene
lint-job:
  stage: lint
  image: python:3.10-slim
  before_script:
    - pip install --upgrade pip ruff flake8
  script:
    - echo "Running ruff and flake8 linting checks..."
    - ruff check dags/
    - flake8 dags/ --max-line-length=120 --ignore=E501,W503

# Stage 2: Headless DAG Integrity & Cycle Testing
dag-integrity-test:
  stage: test
  image: apache/airflow:2.8.1-python3.10
  before_script:
    - pip install -r requirements-dev.txt
  script:
    - echo "Validating DAG imports and dependency graphs..."
    - pytest tests/test_dag_integrity.py -v
```

##### 3.3.4 DAG Integrity Test Suite (`tests/test_dag_integrity.py`)
Create `tests/test_dag_integrity.py` to assert that every DAG parses cleanly and has no cyclical dependencies:

```python
"""
Automated DAG Integrity Test Suite executed by GitLab CI/CD.
Fails the build if any DAG fails to parse or contains cycle dependencies.
"""
from __future__ import annotations

import os
import pytest
from airflow.models import DagBag

DAGS_DIR = os.path.join(os.path.dirname(__file__), "..", "dags")

@pytest.fixture(scope="session")
def dagbag():
    """Instantiate DagBag to parse all Python files in dags/ folder."""
    return DagBag(dag_folder=DAGS_DIR, include_examples=False)

def test_no_import_errors(dagbag):
    """Verify that zero DAG files threw Python exceptions on compilation."""
    assert len(dagbag.import_errors) == 0, (
        f"DAG Import Failures Detected:\n{dagbag.import_errors}"
    )

def test_dag_cycle_checks(dagbag):
    """Verify that no DAG contains dependency cycles."""
    for dag_id, dag in dagbag.dags.items():
        dag.test_cycle()

def test_mandatory_tags(dagbag):
    """Ensure every DAG has organizational tags for UI filtering."""
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"DAG '{dag_id}' must have at least one tag configured."
```

##### 3.3.5 GitLab Deploy Token & Kubernetes Secret
The Kubernetes `git-sync` sidecar container needs read-only credentials to pull from GitLab without human interaction:

1. In GitLab: Go to **Project Settings** ➔ **Repository** ➔ **Deploy Tokens**.
2. Create a token:
   - **Name**: `airflow-gitsync-token`
   - **Scopes**: Check `read_repository`
3. In Kubernetes: Store the deploy token credentials as a Kubernetes Secret in the `samo` namespace:
   ```bash
   kubectl create secret generic airflow-gitlab-credentials \
     --namespace samo \
     --from-literal=GIT_SYNC_USERNAME='airflow-gitsync-token' \
     --from-literal=GIT_SYNC_PASSWORD='<generated-deploy-token-password>'
   ```

##### 3.3.6 Kubernetes / Helm `git-sync` Configuration
In the Airflow Helm chart values (`values.yaml`), `gitSync` is enabled to automatically poll GitLab:

```yaml
dags:
  persistence:
    enabled: false # No shared NFS/EFS required!
  gitSync:
    enabled: true
    repo: "http://gitlab.transcode.com/telecom-lakehouse/airflow-dags.git"
    branch: "main"
    rev: "HEAD"
    subPath: "dags"
    syncWait: 60 # Sync every 60 seconds
    credentialsSecret: "airflow-gitlab-credentials"
```

##### 3.3.7 Developer Workflow: From Push to Production
1. Add the new DAG file to the `dags/` folder:
   ```bash
   # Add, commit, and push feature branch to GitLab
   git add dags/daily_voice_cdr_aggregation.py
   git commit -m "feat(cdrs): add daily voice cdr aggregation DAG"
   git push origin feature/daily-cdr-aggregation
   ```
2. In GitLab: Open a **Merge Request (MR)** from `feature/daily-cdr-aggregation` into `main`.
3. The GitLab CI pipeline automatically runs `lint-job` and `dag-integrity-test`.
4. Once the pipeline passes and code review is complete, click **Merge**.
5. Within **60 seconds**, the `git-sync` sidecar container inside the Airflow scheduler and webserver pods pulls the commit:
   ```text
   INFO: git-sync: pulling "http://gitlab.transcode.com/telecom-lakehouse/airflow-dags.git" [branch: main]
   INFO: git-sync: synced commit 7d3f9a2e to /opt/airflow/dags/
   ```

---

#### 3.4 Triggering & Verification in the Airflow Grid & Graph Views
1. Open the Airflow Web UI at **`http://airflow.transcode.com`**.
2. Filter DAGs by the tag **`lakehouse`**.
3. Confirm that **`daily_voice_cdr_aggregation`** appears with green status (it loads automatically within 60 seconds of git-sync completing without pod restarts).
4. Click the toggle switch to unpause the DAG.
5. Click **Trigger DAG (▶)** to run an ad-hoc test execution, then inspect the **Grid View** and **Graph View** to monitor task progress.

---

### Step 4: DAG 2 — Lakehouse-to-Postgres Analytics Sync (`voice_cdr_lakehouse_to_rdbms_analytics`)

#### 4.1 The Idempotent "Delete-Then-Insert" Partition Pattern
Downstream applications and web applications (like the customer portal or billing dashboard) need high-concurrency access that PostgreSQL provides better than an OLAP engine like Trino.

> [!IMPORTANT]
> **Why Plain INSERT Duplicates Data**:
> If a pipeline fails midway or is manually re-run, a naive `INSERT` creates duplicate rows.
> **The Idempotent Solution**:
> 1. First task: `DELETE FROM rdbms.public.voice_lab WHERE el_record_date = DATE '...'`
> 2. Second task: `INSERT INTO rdbms.public.voice_lab SELECT * FROM lakehouse.cdrs.voice_lab WHERE el_record_date = DATE '...'`
> This makes the DAG **idempotent**: you can run it 100 times for the same day, and the result will always be identical!

#### 4.2 Complete Python DAG Code
GitLab Repository Path: `dags/voice_cdr_lakehouse_to_rdbms_analytics.py` (committed to `telecom-lakehouse/airflow-dags.git`):

```python
"""
DAG: voice_cdr_lakehouse_to_rdbms_analytics
Purpose: Idempotent transfer of yesterday's CDR partition from Iceberg Lakehouse to Postgres serving DB.
Schedule: Daily at 06:30 AM (Africa/Mogadishu EAT)
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator

LOCAL_TZ = pendulum.timezone("Africa/Mogadishu")

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "email_on_failure": True,
    "email": ["ops-alerts@telesom.com"],
}

with DAG(
    dag_id="voice_cdr_lakehouse_to_rdbms_analytics",
    description="Sync yesterday's Iceberg CDR partition to Postgres RDBMS",
    default_args=default_args,
    schedule="30 6 * * *",  # 06:30 AM EAT (ready before business day starts)
    start_date=pendulum.datetime(2026, 9, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "postgres", "sync"],
) as dag:

    # Task 1: Delete existing partition in target RDBMS (prevents duplicate data on rerun)
    delete_existing_partition = TrinoOperator(
        task_id="delete_existing_partition_in_rdbms",
        trino_conn_id="trino_lakehouse",
        sql="""
        DELETE FROM rdbms.public.voice_lab 
        WHERE el_record_date = DATE '{{ macros.ds_add(ds, -1) }}';
        """,
    )

    # Task 2: Insert yesterday's complete partition from Lakehouse into RDBMS
    load_analytics_partition = TrinoOperator(
        task_id="load_analytics_partition",
        trino_conn_id="trino_lakehouse",
        sql="""
        INSERT INTO rdbms.public.voice_lab
        SELECT * FROM lakehouse.cdrs.voice_lab
        WHERE el_record_date = DATE '{{ macros.ds_add(ds, -1) }}';
        """,
    )

    # Define task dependency
    delete_existing_partition >> load_analytics_partition
```

---

### Step 5: DAG 3 — Automated Iceberg Maintenance & Compaction

#### 5.1 The Small Files & Snapshot Accumulation Problem
Continuous streaming ingestion from NiFi appends Parquet files every 5 minutes. Over 30 days, this creates thousands of files and snapshot metadata objects that slow down Trino query planning.

#### 5.2 Static Compaction DAG (`optimize`, `expire_snapshots`, `remove_orphan_files`)
GitLab Repository Path: `dags/iceberg_lakehouse_maintenance.py` (committed to `telecom-lakehouse/airflow-dags.git`):

```python
"""
DAG: iceberg_lakehouse_maintenance
Purpose: Optimize Iceberg storage, compact small files, expire snapshots, remove orphan files.
Schedule: Weekly on Sunday at 02:00 AM EAT
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.trino.operators.trino import TrinoOperator

LOCAL_TZ = pendulum.timezone("Africa/Mogadishu")

default_args = {
    "owner": "platform-ops",
    "retries": 1,
    "email_on_failure": True,
    "email": ["ops-alerts@telesom.com"],
}

with DAG(
    dag_id="iceberg_lakehouse_maintenance",
    description="Nightly Iceberg table optimization and metadata compaction",
    default_args=default_args,
    schedule="0 2 * * 0",  # Every Sunday at 02:00 AM
    start_date=pendulum.datetime(2026, 9, 1, tz=LOCAL_TZ),
    catchup=False,
    tags=["iceberg", "maintenance", "storage"],
) as dag:

    # 1. Compact small files into optimal 128MB files
    compact_files = TrinoOperator(
        task_id="compact_small_parquet_files",
        trino_conn_id="trino_lakehouse",
        sql="""
        ALTER TABLE lakehouse.cdrs.voice_lab 
        EXECUTE optimize(file_size_threshold => '128MB');
        """,
    )

    # 2. Expire old snapshots older than 30 days
    expire_snapshots = TrinoOperator(
        task_id="expire_old_snapshots",
        trino_conn_id="trino_lakehouse",
        sql="""
        ALTER TABLE lakehouse.cdrs.voice_lab 
        EXECUTE expire_snapshots(retention_threshold => '30d');
        """,
    )

    # 3. Clean up unreferenced orphan files from MinIO S3
    remove_orphans = TrinoOperator(
        task_id="remove_orphan_files",
        trino_conn_id="trino_lakehouse",
        sql="""
        ALTER TABLE lakehouse.cdrs.voice_lab 
        EXECUTE remove_orphan_files(retention_threshold => '7d');
        """,
    )

    compact_files >> expire_snapshots >> remove_orphans
```

---

#### 5.3 Advanced Dynamic Task Mapping (`.expand()`)
If you have multiple partitions, you don't want to blindly optimize every partition every night. You can use Airflow's **Dynamic Task Mapping** (`.expand()`) to query Trino for only those partitions with `file_count > 500`:

```python
from airflow.decorators import task
from airflow.providers.trino.hooks.trino import TrinoHook

@task
def list_partitions_needing_compaction() -> list[str]:
    """Dynamically queries Iceberg metadata for partitions with too many files."""
    hook = TrinoHook(trino_conn_id="trino_lakehouse")
    rows = hook.get_records("""
        SELECT DISTINCT el_record_date
        FROM lakehouse.cdrs."voice_lab$partitions"
        WHERE file_count > 500
    """)
    return [str(r[0]) for r in rows]

@task
def compact_partition(partition_date: str) -> None:
    """Compacts one specific partition."""
    hook = TrinoHook(trino_conn_id="trino_lakehouse")
    hook.run(f"""
        ALTER TABLE lakehouse.cdrs.voice_lab
        EXECUTE optimize(file_size_threshold => '128MB')
        WHERE el_record_date = DATE '{partition_date}';
    """)

# Airflow dynamically spawns one mapped task instance per partition!
compact_partition.expand(partition_date=list_partitions_needing_compaction())
```

---

### Step 6: DAG 4 — Automated Cross-System Reconciliation & SLA Alerting

#### 6.1 Cross-System Reconciliation Logic
In financial and telecommunications platforms, proving that **zero records were lost during transit** between the Lakehouse and the serving RDBMS is mandatory for compliance.

GitLab Repository Path: `dags/cdr_reconciliation_and_sla_monitor.py` (committed to `telecom-lakehouse/airflow-dags.git`):

```python
"""
DAG: cdr_reconciliation_and_sla_monitor
Purpose: Audits row counts between Lakehouse and Postgres; alerts if mismatch occurs.
Schedule: Daily at 07:30 AM (after sync DAG completes)
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.trino.hooks.trino import TrinoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

LOCAL_TZ = pendulum.timezone("Africa/Mogadishu")

default_args = {
    "owner": "audit-team",
    "email_on_failure": True,
    "email": ["audit-alerts@telesom.com"],
}

with DAG(
    dag_id="cdr_reconciliation_and_sla_monitor",
    description="Cross-check record counts between Lakehouse and Postgres RDBMS",
    default_args=default_args,
    schedule="30 7 * * *",
    start_date=pendulum.datetime(2026, 9, 1, tz=LOCAL_TZ),
    catchup=False,
    tags=["reconciliation", "audit", "compliance"],
) as dag:

    def verify_reconciliation(**context) -> None:
        target_date = context["macros"].ds_add(context["ds"], -1)
        print(f"Auditing reconciliation for date: {target_date}")

        trino = TrinoHook(trino_conn_id="trino_lakehouse")
        pg = PostgresHook(postgres_conn_id="rdbms_postgres")

        # 1. Fetch count from Iceberg Lakehouse
        lakehouse_cnt = trino.get_first(f"""
            SELECT COUNT(*) 
            FROM lakehouse.cdrs.voice_lab 
            WHERE el_record_date = DATE '{target_date}';
        """)[0]

        # 2. Fetch count from PostgreSQL Serving DB
        rdbms_cnt = pg.get_first(f"""
            SELECT COUNT(*) 
            FROM public.voice_lab 
            WHERE el_record_date = DATE '{target_date}';
        """)[0]

        print(f"Lakehouse Count: {lakehouse_cnt} | RDBMS Count: {rdbms_cnt}")

        # 3. Assert zero data loss
        if lakehouse_cnt != rdbms_cnt:
            raise ValueError(
                f"RECONCILIATION AUDIT FAILURE for {target_date}: "
                f"Lakehouse has {lakehouse_cnt} records, but Postgres has {rdbms_cnt} records! "
                f"Discrepancy: {abs(lakehouse_cnt - rdbms_cnt)} records."
            )
        
        print(f"RECONCILIATION SUCCESS: Both stores hold exactly {lakehouse_cnt} records.")

    reconcile_task = PythonOperator(
        task_id="reconcile_row_counts",
        python_callable=verify_reconciliation,
    )
```

---

### Step 7: Testing, Operational Execution & Visual Dashboard Auditing

#### 7.1 Manual DAG Triggering & Verification
1. Open the Airflow Web UI: **`http://airflow.transcode.com`**.
2. Find **`daily_voice_cdr_aggregation`** in the DAGs list.
3. Toggle the blue **Pause/Unpause** switch to activate the DAG.
4. On the far right, click the **Trigger DAG (▶ Play icon)** button ➔ click **Trigger DAG**.
5. Click on the DAG name to enter the **Grid View**:
   - Observe the task box transition from light blue (`scheduled`) ➔ dark blue (`queued`) ➔ green (`running`) ➔ dark green (`success`).
6. Click the **Graph** tab:
   - Visually observe task dependencies and execution timing.

#### 7.2 Inspecting Task Execution Logs
1. In the Grid View, click on any task square (e.g. `build_daily_summary`).
2. In the slide-out panel on the right, click **Log**.
3. Inspect the live stdout/stderr stream:
   - Notice the exact Trino query executed.
   - Notice the logical date (`ds`) resolution.
   - Verify connection resolution through `trino_lakehouse`.

#### 7.3 Validating Output in Trino and Postgres
Open DataGrip or Trino CLI and verify data was generated:

```sql
-- 1. Check rollup output in Trino
SELECT * FROM lakehouse.cdrs.voice_lab_daily_summary ORDER BY el_record_date DESC;

-- 2. Check sync output in PostgreSQL
SELECT el_record_date, COUNT(*) FROM public.voice_lab GROUP BY el_record_date;
```

---

# Part 8: Complete Troubleshooting & Error Reference Matrix

| # | Symptom / Error | Root Cause | Exact Solution |
| :--- | :--- | :--- | :--- |
| **1** | `ModuleNotFoundError: No module named 'airflow.providers.trino'` | The Trino provider package is missing from the Airflow environment. | Run `pip install apache-airflow-providers-trino` on the worker/scheduler or add it to `requirements.txt`. |
| **2** | `airflow dags list-import-errors` shows syntax error | Python syntax error, unclosed parenthesis, or bad indentation in DAG file. | Run `python3 -m py_compile dags/<file>.py` in terminal to get the exact file line number. |
| **3** | `DuplicateTaskIdFound: ...` | Two tasks in the same DAG have identical `task_id`. | Ensure every `task_id` within a single DAG is unique. |
| **4** | `DAG never appears in Web UI; no error shown` | Two separate DAG files define the exact same `dag_id`. | Search the `dags/` folder for duplicate `dag_id` strings and give each file a unique ID. |
| **5** | `jinja2.exceptions.TemplateSyntaxError` | Typo in Jinja macro expression (e.g. mismatched `{{` or invalid macro name). | Verify syntax against Airflow macros reference: `{{ ds }}` or `{{ macros.ds_add(ds, -1) }}`. |
| **6** | `Task is stuck in queued or scheduled forever` | No worker slots available, or KubernetesExecutor worker pod failed to launch. | Run `kubectl get pods -n airflow` to inspect worker pod status and check scheduler logs. |
| **7** | `DAG executes 3 hours earlier/later than expected` | DAG `start_date` was defined using a naive datetime instead of timezone-aware pendulum. | Use `LOCAL_TZ = pendulum.timezone("Africa/Mogadishu")` and set `tz=LOCAL_TZ` on `start_date`. |
| **8** | `Airflow immediately executes 50 historical runs on unpause` | `catchup` was left on its default value of `True`. | Set `catchup=False` in the `DAG(...)` constructor. |
| **9** | `Import works in terminal, but scheduler parse loop is slow` | Top-level DAG code makes active network/database calls. | Move all database and network calls **inside task callables** (`def do_work(...)`). Top-level code runs on every scheduler loop! |
| **10**| `TrinoHook: could not connect to server (Connection refused)` | `trino_lakehouse` connection host is pointing to localhost instead of K8s cluster service. | Set Host to `trino-coordinator.trino.svc.cluster.local` and Port to `8080`. |
| **11**| `PostgresHook: password authentication failed for user` | Wrong credentials in `rdbms_postgres` connection. | Go to `Admin -> Connections -> rdbms_postgres` and re-enter password `3682154Aa1!`. |
| **12**| `EmailOperator: ConnectionRefusedError` | SMTP host or port is unreachable from worker pod. | Verify SMTP host `169.58.218.161` on port `25` or `587`. |
| **13**| `GitLab CI/CD: test stage fails with DAG import error` | Python syntax error, unhandled exception, or missing library in new DAG file. | Check GitLab CI job execution log; run `pytest tests/test_dag_integrity.py` locally to inspect the trace. |
| **14**| `git-sync: Authentication failed (HTTP 401 Unauthorized)` | GitLab Deploy Token has expired or username/password in Kubernetes Secret is wrong. | Generate a new Deploy Token in GitLab (`Settings -> Repository -> Deploy Tokens`) with `read_repository` scope and update secret `airflow-gitlab-credentials`. |
| **15**| `New DAG merged to GitLab main but does not appear in Airflow UI` | GitSync sidecar sync latency, or DAG has top-level exception preventing scheduler registration. | Check git-sync sidecar logs: `kubectl logs -n airflow -l component=scheduler -c git-sync`. Inspect scheduler errors: `airflow dags list-import-errors`. |
| **16**| `git-sync: Could not resolve host: gitlab.transcode.com` | Internal Kubernetes CoreDNS cannot resolve domain `gitlab.transcode.com`. | Add `hostAliases` (`169.58.218.71 gitlab.transcode.com`) to the Airflow Helm values or update cluster CoreDNS ConfigMap. |

---
*Authored for Telesom & Dahabshiil Group — Big Data Platform Training Programme.*
