# 🏦 The Bank Data Platform & Automated Watchdog: Visual Workflow Guide
### Complete Step-by-Step Mermaid Workflows for the Banking Pipeline, Airflow Orchestration & 24/7 Monitoring
*(Designed for anyone to understand—explained with clean visual workflows and plain English!)*

---

## 🗺️ Master Workflow 1: End-to-End Banking Data Lifecycle

This is the primary workflow diagram showing the complete journey of a financial transaction: from the moment a customer touches an ATM, through real-time streaming, lakehouse storage, nightly audit, and emergency alerting.

```mermaid
flowchart TD
    %% Global Class Definitions
    classDef startEnd fill:#E8EAF6,stroke:#3F51B5,stroke-width:2px,color:#1A237E;
    classDef process fill:#E3F2FD,stroke:#1565C0,stroke-width:2px,color:#0D47A1;
    classDef decision fill:#FFF9C4,stroke:#FBC02D,stroke-width:2px,color:#F57F17;
    classDef storage fill:#E8F5E9,stroke:#2E7D32,stroke-width:2px,color:#1B5E20;
    classDef airflow fill:#F3E5F5,stroke:#7B1FA2,stroke-width:2px,color:#4A148C;
    classDef alert fill:#FFEBEE,stroke:#C62828,stroke-width:2px,color:#B71C1C;

    START([🏁 Step 1: Customer Makes a Transaction]):::startEnd
    
    SUB_SOURCES[/ATM Cash Swipes, Mobile Money e-Dahab, or UK/US Remittances/]:::process
    START --> SUB_SOURCES

    %% Real-time Ingestion Workflow
    subgraph WF_NIFI["⚡ 2. Real-Time Ingestion Workflow (Apache NiFi)"]
        direction TB
        N1["Step 2.1: List & Fetch Settlement Files<br/>(Evenly distributed across cluster nodes)"]:::process
        
        N2["Step 2.2: Convert Text to Avro & Validate Schema<br/>(Reads event timestamp /create_date via RecordPath)"]:::process
        
        N3["Step 2.3: Pack 50,000 to 250,000 Records into a Bin<br/>(Correlated by date to prevent small files)"]:::process
        
        N1 --> N2 --> N3
    end

    SUB_SOURCES --> N1

    %% Storage Workflow
    subgraph WF_STORAGE["💾 3. Secure Storage Workflow (The Lakehouse Vault)"]
        direction TB
        S1[("Apache Polaris REST Catalog<br/>Atomic Snapshot Metadata")]:::storage
        S2[("MinIO S3 Object Storage<br/>Immutable Compressed Parquet Files")]:::storage
        S1 <--> S2
    end

    N3 ==>|Commit Data & Metadata| WF_STORAGE

    %% Parallel Stream Tap Workflow
    subgraph WF_TAP["⏱️ Parallel Speedometer Workflow (Non-Intrusive Stream Tap)"]
        direction TB
        T1["Clone Stream at FetchSFTP<br/>(Zero disk duplication)"]:::process
        T2{"Did new transactions arrive<br/>within 3 minutes?"}:::decision
        T3["Quietly discard clone<br/>(Normal traffic flowing)"]:::process
        T4["🚨 Trigger P1 Emergency Alarm:<br/>'Payment Switch Stalled!'"]:::alert
        
        T1 --> T2
        T2 -->|YES: Traffic Normal| T3
        T2 -->|NO: Switch Silent| T4
    end

    N1 -.->|Mirror Copy| T1

    %% Nightly Orchestration Workflow
    subgraph WF_AIRFLOW["🌙 4. Nightly Audit Workflow (Apache Airflow)"]
        direction TB
        A1["00:30 AM: DAG 1 — Daily Branch Rollup<br/>(Calculate daily revenues & volumes)"]:::airflow
        A2["01:00 AM: DAG 2 — Sync to Postgres Mart<br/>(Powers executive dashboards)"]:::airflow
        A3["02:00 AM: DAG 4 — Balance Check Audit<br/>(Compare Core Bank DB vs. Lakehouse Vault)"]:::airflow
        
        A1 --> A2 --> A3
    end

    WF_STORAGE <==>|Query Data via Trino| WF_AIRFLOW

    A_DEC{"Do Core Bank & Lakehouse<br/>Balances Match 100%?"}:::decision
    A3 --> A_DEC

    A_PASS["✅ Audit Passed:<br/>Send daily financial sign-off to CFO"]:::storage
    A_FAIL["🚨 Audit Breach:<br/>Freeze reporting & page Chief Risk Officer"]:::alert

    A_DEC -->|YES: Variance = $0.00| A_PASS
    A_DEC -->|NO: Variance > $0.00| A_FAIL

    %% Final Alert Sink
    ONCALL([📱 On-Call DataOps & SecOps Engineers]):::startEnd
    T4 ==>|Instant Page / SMS| ONCALL
    A_FAIL ==>|Instant Page / Email| ONCALL
```

> ### 🗣️ What to Say When Showing Master Workflow 1:
> *"This workflow shows the entire lifecycle of bank data:  
> 1. A customer swipes their card or sends money on the top left.  
> 2. The transaction flows into our NiFi ingestion workflow, which checks dates and packs records into large, efficient bins.  
> 3. Records are committed to our Iceberg Lakehouse vault.  
> 4. In parallel, our stream-tap sensor watches traffic like a speedometer—if transactions stop for 3 minutes, it alerts the on-call team.  
> 5. At night, Apache Airflow wakes up to audit yesterday's totals. If a single cent does not match the core banking ledger, it immediately halts reporting and pages leadership."*

---

## 🧩 Lab 4 Workflow 2: The 5-Stage Modular NiFi Pipeline

In **Lab 4**, we split our pipeline into **5 isolated stages (Process Groups)** that communicate **only through Input and Output Ports**. This prevents one broken processor from crashing the entire bank.

```mermaid
flowchart TD
    %% Node Styles
    classDef port fill:#CFD8DC,stroke:#37474F,stroke-width:2px,color:#263238;
    classDef proc fill:#E1F5FE,stroke:#0288D1,stroke-width:2px,color:#01579B;
    classDef pack fill:#E8F5E9,stroke:#2E7D32,stroke-width:2px,color:#1B5E20;
    classDef dec fill:#FFF9C4,stroke:#FBC02D,stroke-width:2px,color:#F57F17;
    classDef alert fill:#FFEBEE,stroke:#C62828,stroke-width:2px,color:#B71C1C;
    classDef done fill:#F1F8E9,stroke:#558B2F,stroke-width:2px,color:#33691E;

    START_FILES[/📁 Raw Files Drop on SFTP: 169.58.218.161/] --> P_IN1

    %% Stage 1: Extract
    subgraph STAGE1["📦 STAGE 1: EXTRACT (Download & Load-Balance)"]
        direction TB
        P_IN1(("records-in")):::port
        E1["ListSFTP: Scheduled on Primary Node Only<br/>(Prevents duplicate file downloads)"]:::proc
        E2["Queue: Round-Robin Load Balancing<br/>(Deals files evenly across all cluster worker nodes)"]:::proc
        E3["FetchSFTP: Distributed parallel download"]:::proc
        
        P_IN1 --> E1 --> E2 --> E3
    end

    P_OUT1(("records-out")):::port
    P_TAP(("monitor-tap-out")):::port
    P_ERR1(("failures-out")):::port

    E3 --> P_OUT1
    E3 -.->|Mirror Clone| P_TAP
    E3 -.->|Network / Auth Error| P_ERR1

    %% Stage 2: Transform
    subgraph STAGE2["⚙️ STAGE 2: TRANSFORM (Clean, Translate & Validate Dates)"]
        direction TB
        P_IN2(("transform-in")):::port
        T1["ConvertRecord: Delimited CSV ➔ Binary Avro<br/>(Governed by Confluent Schema Registry :8081)"]:::proc
        T2["UpdateRecord (RecordPath):<br/>Extract REAL customer payment timestamp /create_date<br/>Derive partition key: /el_record_date"]:::proc
        T3["UpdateRecord (Literal):<br/>Stamp pipeline ingestion timestamp: /load_date"]:::proc
        
        P_IN2 --> T1 --> T2 --> T3
    end

    P_OUT1 --> P_IN2
    P_OUT2(("transform-out")):::port
    P_ERR2(("failures-out")):::port

    T3 --> P_OUT2
    T1 -.->|Schema Mismatch| P_ERR2
    T2 -.->|Corrupt Date Format| P_ERR2

    %% Stage 3: Load
    subgraph STAGE3["💾 STAGE 3: LOAD (Bin-Packing & Lakehouse Ingestion)"]
        direction TB
        P_IN3(("load-in")):::port
        L1["MergeRecord: Bin-Packing Engine<br/>Group 50,000 to 250,000 rows by el_record_date<br/>(Eliminates the Small-Files Problem)"]:::pack
        L2["PutIcebergRecord: Atomic Snapshot Write<br/>• Writes Parquet blocks to MinIO S3 (:30900)<br/>• Commits metadata to Polaris REST Catalog (:8181)"]:::pack
        
        P_IN3 --> L1 --> L2
    end

    P_OUT2 --> P_IN3
    P_OUT3([🏁 Done Funnel: Ingestion Complete]):::done
    P_ERR3(("failures-out")):::port

    L2 --> P_OUT3
    L1 -.->|Merge Timeout / Corrupt Record| P_ERR3
    L2 -.->|MinIO S3 / Polaris Commit Error| P_ERR3

    %% Stage 4: Monitoring Stream Tap
    subgraph STAGE4["⏱️ STAGE 4: MONITORING (The Non-Intrusive Speedometer)"]
        direction TB
        P_IN4(("monitor-in")):::port
        M1["MonitorActivity: Inactivity Threshold = 3 Minutes<br/>Continually Send Messages = True"]:::proc
        M_DEC{"Did traffic arrive<br/>within 3 mins?"}:::dec
        M_OK["Normal Traffic:<br/>Auto-terminate clone (0 disk cost)"]:::proc
        M_STALL["🚨 Silence Detected:<br/>Generate 'inactive' marker FlowFile"]:::alert
        M_RESUME["🟢 Traffic Resumed:<br/>Generate 'activity.restored' marker FlowFile"]:::proc
        
        P_IN4 --> M1 --> M_DEC
        M_DEC -->|Files Flowing| M_OK
        M_DEC -->|3 Min Silence| M_STALL
        M_DEC -->|Recovery| M_RESUME
    end

    P_TAP --> P_IN4
    P_ALERTS(("monitor-alerts-out")):::port
    M_STALL --> P_ALERTS
    M_RESUME --> P_ALERTS

    %% Stage 5: Alerting
    subgraph STAGE5["🚨 STAGE 5: ALERTING (Unified Alarm Dispatcher)"]
        direction TB
        P_IN5(("failures-in")):::port
        A1["UpdateAttribute: Tag context, server host, severity, and timestamp"]:::alert
        A2["PutEmail / PagerDuty Webhook:<br/>Dispatch urgent email & phone alert to On-Call Ops"]:::alert
        
        P_IN5 --> A1 --> A2
    end

    P_ERR1 & P_ERR2 & P_ERR3 --> P_IN5
    P_ALERTS --> P_IN5
    A2 --> PAGER_SINK([📱 On-Call Pager / SMS / Email]):::alert
```

> ### 🗣️ What to Say When Showing Lab 4 Workflow 2:
> *"Notice the modularity:  
> - **Stage 1 (Extract)**: `ListSFTP` runs on the Primary Node only to prevent duplicate downloads, and deals work out via Round-Robin balancing.  
> - **Stage 2 (Transform)**: Converts raw text to Avro and uses **RecordPath** to read the customer's actual transaction time (`/create_date`) rather than file arrival time.  
> - **Stage 3 (Load)**: Bins 50k to 250k transactions together before writing to Iceberg and MinIO S3, solving the Small-Files Problem.  
> - **Stage 4 (Monitoring)**: Taps a mirror copy to check if the ATM switch stalls for > 3 minutes.  
> - **Stage 5 (Alerting)**: Catches any failure across all stages and pages the engineer."*

---

## ⏱️ Workflow 3: The Stream Tap & Inactivity Detection Engine

This workflow explains how our **Stream Tap** detects a dead payment switch without slowing down or risking real customer payments.

```mermaid
flowchart TD
    classDef start fill:#E8EAF6,stroke:#3F51B5,stroke-width:2px;
    classDef normal fill:#E8F5E9,stroke:#2E7D32,stroke-width:2px;
    classDef sensor fill:#F3E5F5,stroke:#7B1FA2,stroke-width:2px;
    classDef dec fill:#FFF9C4,stroke:#FBC02D,stroke-width:2px;
    classDef alert fill:#FFEBEE,stroke:#C62828,stroke-width:2px;

    IN["📥 Step 1: Banking Transaction Arrives at FetchSFTP"]:::start
    
    FORK{"Step 2: Dual-Port Fork<br/>(NiFi Copy-on-Write Pointer)"}:::dec
    IN --> FORK

    %% Main Branch
    FORK ==>|Path A: Primary Production Flow| MAIN["Step 3A: Convert, Pack & Save into Iceberg Vault"]:::normal
    MAIN ==> SUCCESS([💰 Money Safely Credited in Bank Vault]):::normal

    %% Sensor Branch
    FORK -.->|Path B: Non-Intrusive Mirror Clone| SENSOR["Step 3B: Enter MonitorActivity Sensor<br/>(3-Minute Inactivity Timer)"]:::sensor
    
    CHECK{"Step 4: Check Inactivity Timer"}:::dec
    SENSOR --> CHECK

    CHECK -->|File arrived within 180s| C_RESET["Traffic Active:<br/>Reset timer to 0s & auto-terminate clone"]:::normal
    
    CHECK -->|Zero files for > 180s!| C_INACT["🚨 INACTIVITY DETECTED:<br/>Synthesize 'inactive' marker FlowFile"]:::alert
    
    C_INACT --> PAGE["Step 5: Send P1 Alert to Ops:<br/>'Burao ATM Switch Stalled for 3 Minutes!'"]:::alert
    
    CHECK -->|First file arrives after outage| C_RESTORE["🟢 RECOVERY DETECTED:<br/>Synthesize 'activity.restored' marker"]:::normal
    
    C_RESTORE --> RESOLVE["Step 6: Send Recovery Notice:<br/>'Switch back online. Outage lasted 12 mins.'"]:::normal
```

> ### 🗣️ What to Say When Showing Workflow 3:
> *"Why is this design brilliant?  
> Look at the fork at Step 2: Path A is the real bank money, heading straight into storage. Path B is a zero-cost mirror copy that feeds our 3-minute timer.  
> Because the sensor is on a separate branch, it can NEVER block or delay customer payments. If files are flowing, it quietly drops the clone. But if 3 minutes pass with zero files, it synthesizes an emergency alert and pages on-call staff!"*

---

## 🌙 Workflow 4: Apache Airflow's Nightly Audit & Reconciliation

This workflow details what happens every night between **00:00 and 02:30 AM**, when Apache Airflow acts as the automated bank auditor.

```mermaid
flowchart TD
    classDef cron fill:#E8EAF6,stroke:#3F51B5,stroke-width:2px;
    classDef task fill:#E1F5FE,stroke:#0288D1,stroke-width:2px;
    classDef dec fill:#FFF9C4,stroke:#FBC02D,stroke-width:2px;
    classDef pass fill:#E8F5E9,stroke:#2E7D32,stroke-width:2px;
    classDef fail fill:#FFEBEE,stroke:#C62828,stroke-width:2px;

    CRON([⏰ 00:00 AM: East Africa Time Midnight Trigger]):::cron
    
    %% DAG 1
    subgraph DAG1["1️⃣ DAG 1: Daily Transaction Rollup (00:30 AM)"]
        direction TB
        D1_SQL["TrinoOperator: Merge raw transactions into<br/>lakehouse.banking.daily_branch_summary"]:::task
        D1_SUM["Calculate: Total Disbursed $, Total Fees $, Count per Branch"]:::task
        D1_SQL --> D1_SUM
    end

    CRON --> DAG1

    %% DAG 2
    subgraph DAG2["2️⃣ DAG 2: Lakehouse-to-PostgreSQL Sync (01:00 AM)"]
        direction TB
        D2_DEL["PostgresOperator: Idempotently DELETE yesterday's partition<br/>(Guarantees zero duplicate rows if rerun)"]:::task
        D2_INS["PythonOperator: Transfer clean summarized rows from Trino<br/>into public.daily_bank_mart"]:::task
        D2_DASH["Executive Tableau & Grafana Dashboards Auto-Update"]:::task
        D2_DEL --> D2_INS --> D2_DASH
    end

    DAG1 --> DAG2

    %% DAG 4
    subgraph DAG4["3️⃣ DAG 4: Cross-System Financial Reconciliation Audit (02:00 AM)"]
        direction TB
        A_Q1["PostgresHook: Query Core Banking Database<br/>SELECT COUNT(*), SUM(payout_amount) FROM core_tx"]:::task
        A_Q2["TrinoHook: Query Iceberg Lakehouse<br/>SELECT COUNT(*), SUM(payout_amount) FROM lakehouse_tx"]:::task
        A_DIFF{"Compare Totals:<br/>Is Count Diff == 0 AND Amount Diff == $0.00?"}:::dec
        
        A_Q1 & A_Q2 --> A_DIFF
    end

    DAG2 --> DAG4

    %% Audit Outcomes
    A_PASS["✅ AUDIT PASSED (100% Match):<br/>• Log status = 'VERIFIED' in audit_log<br/>• Email CFO: 'Daily Books Balanced'"]:::pass
    
    A_FAIL["🚨 AUDIT BREACH DETECTED:<br/>• Raise AirflowException<br/>• Freeze automated financial feeds<br/>• P1 Page to Chief Risk Officer & SecOps"]:::fail

    A_DIFF -->|YES: 100% Balanced| A_PASS
    A_DIFF -->|NO: Even $0.01 Missing!| A_FAIL
```

> ### 🗣️ What to Say When Showing Workflow 4:
> *"Here is how Airflow enforces financial integrity:  
> - At **00:30 AM**, DAG 1 calculates yesterday's total branch disbursements and fee revenues.  
> - At **01:00 AM**, DAG 2 pushes these clean numbers into PostgreSQL for executive dashboards using an idempotent delete-then-insert pattern.  
> - At **02:00 AM**, DAG 4 runs our **Cross-System Audit**: it fetches the official bank vault balance from Core Banking PostgreSQL and compares it against our Iceberg Lakehouse.  
> - If they match 100%, it logs a clean audit and emails the CFO. If even 1 penny is off, it halts the system and pages leadership!"*

---

## 🛡️ Workflow 5: The 3-Tier Bank Watchdog & Escalation Matrix

This workflow details how the platform monitors physical servers, big-data software daemons, and banking transaction flows around the clock.

```mermaid
flowchart TD
    classDef t1 fill:#ECEFF1,stroke:#37474F,stroke-width:2px;
    classDef t2 fill:#E1F5FE,stroke:#0277BD,stroke-width:2px;
    classDef t3 fill:#FFF3E0,stroke:#E65100,stroke-width:2px;
    classDef router fill:#FFF9C4,stroke:#FBC02D,stroke-width:2px;
    classDef p1 fill:#FFEBEE,stroke:#B71C1C,stroke-width:2px,color:#B71C1C;
    classDef p2 fill:#FFF3E0,stroke:#E65100,stroke-width:2px,color:#E65100;
    classDef p3 fill:#E8F5E9,stroke:#1B5E20,stroke-width:2px,color:#1B5E20;

    %% 3 Tiers of Telemetry
    subgraph T1["🖥️ TIER 1: PHYSICAL HARDWARE SERVERS"]
        H1["• Bare-Metal Server (169.58.218.71) CPU > 85%?<br/>• Node RAM Memory > 90%?<br/>• MinIO Hard Drive Capacity > 80%?"]:::t1
    end

    subgraph T2["⚙️ TIER 2: BIG DATA SOFTWARE SERVICES"]
        H2["• Apache NiFi Queue Backpressure (>10,000 FlowFiles)?<br/>• Apache Airflow Scheduler Missed Heartbeat (>120s)?<br/>• Polaris REST Catalog Latency > 500ms?<br/>• Trino Worker Memory Limit Exceeded?"]:::t2
    end

    subgraph T3["💰 TIER 3: FINANCIAL BUSINESS FLOW"]
        H3["• Payment Switch Inactivity (> 3 Minutes Silence)?<br/>• Schema Registry Mismatch / Field Drift?<br/>• Daily Financial Reconciliation Variance (> $0.00)?"]:::t3
    end

    %% Router
    ROUTER{"Incident Severity Evaluator"}:::router
    T1 & T2 & T3 --> ROUTER

    %% Escalations
    ROUTER -->|P1: Critical Outage / Money Discrepancy| P1_ESC["🚨 P1 CRITICAL EMERGENCY<br/>• Automated Voice Phone Call & SMS<br/>• PagerDuty alert to SecOps & FinOps<br/>• SLA: 5-minute response time required"]:::p1

    ROUTER -->|P2: System Degraded / Queue Backpressure| P2_ESC["⚠️ P2 HIGH WARNING<br/>• Team Slack notification (#alerts-platform)<br/>• Operations ticket created in Jira<br/>• SLA: 30-minute response time"]:::p2

    ROUTER -->|P3: Normal Event / Traffic Restored| P3_ESC["🟢 P3 INFORMATIONAL<br/>• Resolution note: 'ATM Switch Back Online'<br/>• Nightly audit success digest<br/>• SLA: Logged for weekly review"]:::p3
```

> ### 🗣️ What to Say When Showing Workflow 5:
> *"Our Bank Watchdog monitors three distinct layers:  
> 1. **Hardware**: Are any servers overheating or running out of disk space?  
> 2. **Software**: Are NiFi, Airflow, and the database processing without thread lock?  
> 3. **The Business**: Is money flowing properly?  
> Any issue is immediately routed by severity: P1 incidents trigger an automated phone call to on-call engineers, P2 warnings alert our Slack channel, and P3 notices confirm when the system has successfully recovered."*

---

---

## 🕵️ Workflow 6: Real-World Example 1 — Real-Time Bank Fraud & AML Detection System

In a major bank like **Dahabshiil Bank**, thousands of international remittances and mobile money transfers arrive every minute. If stolen credit cards, money laundering, or sanctioned individuals move money through the bank, **the bank can face millions in regulatory fines or lose its banking license**.

```mermaid
flowchart TD
    %% Workflow Styles
    classDef start fill:#E8EAF6,stroke:#3F51B5,stroke-width:2px;
    classDef check fill:#E1F5FE,stroke:#0288D1,stroke-width:2px;
    classDef dec fill:#FFF9C4,stroke:#FBC02D,stroke-width:2px;
    classDef block fill:#FFEBEE,stroke:#C62828,stroke-width:2px,color:#B71C1C;
    classDef allow fill:#E8F5E9,stroke:#2E7D32,stroke-width:2px,color:#1B5E20;
    classDef storage fill:#EDE7F6,stroke:#512DA8,stroke-width:2px,color:#311B92;

    START_TX([🏁 Step 1: Customer Submits $9,800 Wire Transfer]):::start

    subgraph WF_FRAUD["⚡ Step 2: In-Flight Real-Time Fraud & AML Engine (Sub-Second)"]
        direction TB
        F1["Check 1: Sanction Watchlist Lookup<br/>(Fuzzy lookup against UN, OFAC & Central Bank PEP lists)"]:::check
        F2["Check 2: Velocity & Impossible Travel Check<br/>(Did this account transfer money from London 10 mins ago?)"]:::check
        F3["Check 3: Smurfing & Structuring Check<br/>(Multiple transfers just below $10,000 threshold?)"]:::check
        
        F1 --> F2 --> F3
    end
    START_TX --> WF_FRAUD

    DEC_FRAUD{"Step 3: Fraud Evaluator<br/>Is transaction clean?"}:::dec
    WF_FRAUD --> DEC_FRAUD

    %% Path A: Fraud Flagged
    subgraph WF_BLOCKED["🚨 FRAUD DETECTED: Automatic Freeze"]
        direction TB
        B1["Route to Kafka topic: 'dahabshiil.compliance.alerts'"]:::block
        B2["Freeze Branch Payout & Lock Customer Account"]:::block
        B3["Send P1 Emergency Alert to Compliance & Anti-Fraud Team"]:::block
        B1 --> B2 --> B3
    end
    DEC_FRAUD ==>|SUSPICIOUS / WATCHLIST MATCH| WF_BLOCKED

    %% Path B: Clean Transaction
    subgraph WF_CLEAN["✅ TRANSACTION VERIFIED: Normal Payout"]
        direction TB
        C1["Route to Kafka topic: 'dahabshiil.remittance.inbound'"]:::allow
        C2["Instant Cash Disbursement at Burao Branch or e-Dahab Wallet"]:::allow
        C3[("Store Immutable Record in Iceberg Lakehouse Vault")]:::storage
        C1 --> C2 --> C3
    end
    DEC_FRAUD ==>|CLEAN / VERIFIED| WF_CLEAN

    %% Nightly Deep Learning Scan
    subgraph WF_NIGHT_ML["🌙 Step 4: Airflow Nightly Graph Analytics (02:30 AM)"]
        direction TB
        M1["Airflow DAG runs Trino Graph & Cluster Analysis"]:::check
        M2["Identifies Hidden Fraud Networks & Suspicious Multi-Account Rings"]:::check
        M1 --> M2
    end
    C3 -.->|Nightly Audit| WF_NIGHT_ML
```

> ### 🗣️ What to Say When Showing Workflow 6 (Fraud Detection):
> *"Here is how our real-time fraud system works:  
> 1. When a \$9,800 transfer arrives from London, it goes through 3 instant checks in less than a second: a sanctions watchlist check (UN/OFAC), an impossible travel check (could the sender be in two cities at once?), and a structuring check (trying to stay below \$10,000).  
> 2. If it fails ANY check, the money is instantly frozen BEFORE it can be paid out at the branch, and our compliance officer gets an immediate alert.  
> 3. If clean, the family receives their money immediately, and the transaction is permanently stored in our Iceberg vault.  
> 4. Finally, every night at 2:30 AM, Apache Airflow analyzes 90 days of transaction graphs to uncover hidden money-laundering rings!"*

---

## 🖥️ Workflow 7: Real-World Example 2 — 24/7 Server & Service Sentinel (Self-Healing & Alerting)

Computer hardware and data services can run out of memory, fill up disks, or freeze. In a bank, **downtime costs thousands of dollars per minute**. Here is how our automated Server Sentinel detects problems, heals itself, and alerts engineers.

```mermaid
flowchart TD
    %% Workflow Styles
    classDef sensor fill:#ECEFF1,stroke:#37474F,stroke-width:2px;
    classDef check fill:#E1F5FE,stroke:#0288D1,stroke-width:2px;
    classDef dec fill:#FFF9C4,stroke:#FBC02D,stroke-width:2px;
    classDef heal fill:#E8F5E9,stroke:#2E7D32,stroke-width:2px,color:#1B5E20;
    classDef alarm fill:#FFEBEE,stroke:#C62828,stroke-width:2px,color:#B71C1C;
    classDef done fill:#F1F8E9,stroke:#558B2F,stroke-width:2px,color:#33691E;

    START_SENTINEL([📡 Step 1: Continuous 15-Second Telemetry Probe]):::sensor

    subgraph WF_PROBE["🔍 Step 1.1: Health Sensors Probe Every Machine & Container"]
        direction TB
        P1["Sensor 1: Server Disk Space (MinIO S3 Disk Capacity > 88%?)"]:::sensor
        P2["Sensor 2: Software Memory (NiFi JVM Heap > 90%?)"]:::sensor
        P3["Sensor 3: Queue Congestion (NiFi Ingestion Queue > 10,000 files?)"]:::sensor
        P4["Sensor 4: Airflow Heartbeat (Scheduler unresponsive > 60s?)"]:::sensor
        P1 & P2 & P3 & P4
    end
    START_SENTINEL --> WF_PROBE

    DEC_HEALTH{"Step 2: Are all servers<br/>and services healthy?"}:::dec
    WF_PROBE --> DEC_HEALTH

    DEC_HEALTH -->|YES: All Green| HEALTHY([🟢 System 100% Operational]):::done

    %% Automated Self-Healing Flow
    subgraph WF_HEAL["🛠️ Step 3: Automated Self-Healing (Tries to fix without waking humans)"]
        direction TB
        H1["1. Disk Full? Automatically trigger Trino to prune old metadata snapshots"]:::heal
        H2["2. Memory Spiked? Automatically flush temporary file caches and scale worker threads"]:::heal
        H3["3. Scheduler Frozen? Automatically restart Airflow container in Kubernetes"]:::heal
        H1 & H2 & H3
    end
    DEC_HEALTH -->|NO: Failure / Spike Detected| WF_HEAL

    DEC_RECOVER{"Step 4: Did automated<br/>self-healing fix the problem<br/>within 60 seconds?"}:::dec
    WF_HEAL --> DEC_RECOVER

    DEC_RECOVER -->|YES: Problem Solved!| RECOVERED([🟢 Auto-Recovery Logged to Slack]):::heal

    %% Emergency Human Escalation Flow
    subgraph WF_ESCALATE["🚨 Step 5: Emergency Incident War-Room Paging"]
        direction TB
        E1["Phone Call & SMS: Rings On-Call Infrastructure Architect's phone"]:::alarm
        E2["Blinking Red Alert on Grafana Live Wall Dashboard"]:::alarm
        E3["Automatic Incident Ticket Created in Jira with error logs attached"]:::alarm
        E1 --> E2 --> E3
    end
    DEC_RECOVER -->|NO: Hard Failure Persists!| WF_ESCALATE
    WF_ESCALATE ==> WAR_ROOM([👨‍💻 Emergency War-Room Activated: 5-min SLA]):::alarm
```

> ### 🗣️ What to Say When Showing Workflow 7 (Server Sentinel):
> *"Here is how our platform stays online 24 hours a day without crashes:  
> 1. Every 15 seconds, our Sentinel probes hardware disks, computer memory, queue depths, and software heartbeats.  
> 2. If a server starts running out of disk or memory, the system doesn't immediately wake up engineers at 3 AM. It first runs **Automated Self-Healing**: it clears old temporary files, compacts storage, or reboots the frozen container.  
> 3. If self-healing resolves the issue, it logs a quiet green note to Slack.  
> 4. But if the problem persists for more than 60 seconds, it sounds the alarms: an automated voice call wakes up the lead engineer, an incident ticket is generated with exact error logs, and the emergency team resolves it before customers ever experience an outage!"*

---

## 🎤 The 60-Second Presentation Cheat Sheet

When someone asks you to explain the architecture during an interview, meeting, or demo, follow this simple 5-step script:

| Step | What to Point at: | Exactly What to Say: |
| :--- | :--- | :--- |
| **1. The Problem** | Master Workflow 1 (Sources) | *"We process millions of dollars daily from ATMs, mobile phones, and remittances across Dahabshiil Bank. If an ATM goes offline or a transaction is dropped, the bank loses money."* |
| **2. The Ingestion Engine** | Lab 4 Workflow 2 (5 Rooms) | *"In Lab 4, we built an Apache NiFi pipeline split into 5 isolated rooms. It cleans the text, dates transactions by when the customer paid rather than file arrival, and packs 100,000 records together into our Iceberg Lakehouse vault."* |
| **3. Stream Tap & Inactivity** | Workflow 3 (Stream Tap) | *"We tap a mirror copy of the stream into a 3-minute silence detector. If a payment switch dies, it fires a P1 alarm instantly without ever touching or risking real customer payments."* |
| **4. Real-Time Fraud & AML** | Workflow 6 (Fraud System) | *"Every payment undergoes sub-second AML and impossible-travel screening. If flagged, payouts are frozen before cash leaves the branch, and Airflow runs deep graph scans nightly."* |
| **5. Self-Healing Server Sentinel** | Workflow 7 (Server Sentinel) | *"Every 15 seconds, our Sentinel checks server disks and RAM. It self-heals by pruning old snapshots, and calls engineers by phone if a hard failure persists."* |
| **6. Airflow Nightly Audit** | Workflow 4 (Airflow) | *"Every night at 2:00 AM, Apache Airflow compares the Core Banking database against the Lakehouse vault. If there is even a single penny missing, it halts reporting and pages the Chief Risk Officer."* |

---

*Authored for Telesom & Dahabshiil Group — Big Data Platform Engineering.*  
*Visual Workflow Master Guide — Lab 4 Modular Pipeline, Apache Airflow & 24/7 Bank Watch.*

