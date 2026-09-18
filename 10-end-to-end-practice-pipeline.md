# 🏆 Module 10: The Grand Capstone Project (Full Lakehouse Flow)

## 🎯 What You Will Accomplish in this Capstone
In this final module, you will tie all 11 services together into one seamless, production-grade automated pipeline:

1. **Ingestion**: Apache NiFi generates realistic streaming user activity and order transactions.
2. **Buffering & Governance**: Events pass through Apache Kafka; Schema Registry validates schemas.
3. **Storage & Landing**: Raw events land as JSON in MinIO bucket `raw-data`.
4. **Lakehouse ETL**: Apache Spark reads raw JSON, performs schema enforcement, deduplication, and appends to an **Apache Iceberg** table registered in **Apache Polaris**.
5. **Interactive Analytics**: Trino runs high-speed analytical queries on the Iceberg table in MinIO.
6. **Orchestration**: Apache Airflow schedules and triggers daily compactions and validation tasks.
7. **Observability**: Prometheus captures throughput while Grafana graphs real-time pipeline activity.
8. **Multi-Device Wi-Fi Verification**: Access everything from your phone or secondary laptop!

---

## 🗺️ Complete Data Flow Pipeline

```
[Simulated Web Activity]
           │
           ▼
     [Apache NiFi] 
           │
           ├────────────────────────────┐
           ▼                            ▼
  [Kafka Topic: orders]         [MinIO S3: raw-data/]
           ▲                            │
           │ (Schema Check)             │
  [Schema Registry]                     ▼
                                [Apache Spark Job]
                                        │
                                        ▼ (Register Snapshot)
                           [Apache Polaris Iceberg Catalog]
                                        │
                                        ▼ (Write Parquet Data)
                                [MinIO S3: warehouse/]
                                        ▲
                                        │ (SQL Queries)
                                   [Trino Engine]
                                        ▲
                                        │ (Nightly Compaction)
                                 [Apache Airflow]
                                        │
  [Prometheus Metrics] ◄────────────────┴──────────────────► [Grafana Dashboards]
```

---

## 📋 Comprehensive Port & Service Access Reference

Here is your master cheat-sheet for accessing your platform across your local Wi-Fi (`192.168.3.30`):

| Service | Wi-Fi URL | Credentials | Purpose |
| :--- | :--- | :--- | :--- |
| **Apache NiFi Canvas** | `http://192.168.3.30:30880/nifi` | `admin` / `DataPlatform2026!` | Visual ETL flow management |
| **Kafka UI** | `http://192.168.3.30:30082` | *(None)* | Topic & message inspection |
| **Schema Registry** | `http://192.168.3.30:30081` | *(None)* | Schema contract REST API |
| **MinIO Console** | `http://192.168.3.30:30901` | `admin` / `password123` | S3 bucket & object browser |
| **MinIO S3 API** | `http://192.168.3.30:30900` | `admin` / `password123` | S3 API endpoint for SDKs |
| **Apache Polaris** | `http://192.168.3.30:30181/api/v1`| *(None)* | Iceberg REST Catalog |
| **Trino Query UI** | `http://192.168.3.30:30085` | `admin` | SQL query engine & stages |
| **Airflow Web UI** | `http://192.168.3.30:30088` | `admin` / `admin` | Pipeline orchestrator & DAGs |
| **Prometheus UI** | `http://192.168.3.30:9090` | *(None)* | PromQL query & TSDB |
| **Grafana Dashboards**| `http://192.168.3.30:30300` | `admin` / `admin` | Cluster monitoring dashboards |

---

## 🛠️ Step-by-Step Capstone Execution

### Step 1: Turn on the Streaming Pipeline in NiFi
1. Open **NiFi Canvas** (`http://192.168.3.30:30880/nifi`).
2. Start your `GenerateFlowFile`, `PublishKafka`, and `PutS3Object` processors.
3. Open **Kafka UI** (`http://192.168.3.30:30082`) and confirm that messages are incrementing in topic `orders_stream`.
4. Open **MinIO Console** (`http://192.168.3.30:30901`) and verify files arriving under `s3://raw-data/orders/`.

### Step 2: Run the PySpark Batch Ingestion
Execute the Spark processing script (from Module 07) to ingest raw S3 files into the Iceberg table:
```bash
kubectl exec -n data-platform -it deploy/trino -- \
  trino --execute "
    INSERT INTO iceberg.lakehouse_db.customer_orders
    VALUES ('ord_auto_1', 'cust_10', 89.99, 'USD', CURRENT_DATE);
  "
```

### Step 3: Run Interactive SQL Analytics in Trino
Open your Trino terminal or web UI and run analytical aggregations:
```sql
SELECT 
    currency, 
    COUNT(*) AS total_transactions, 
    ROUND(AVG(amount), 2) AS avg_transaction_value,
    ROUND(SUM(amount), 2) AS total_revenue
FROM iceberg.lakehouse_db.customer_orders
GROUP BY currency;
```

### Step 4: Run Table Maintenance via Airflow
1. Open **Airflow** (`http://192.168.3.30:30088`).
2. Trigger the `lakehouse_daily_pipeline` DAG.
3. Observe the task progression from Green to Success!

### Step 5: Monitor Platform Telemetry in Grafana
1. Open **Grafana** (`http://192.168.3.30:30300`).
2. Observe the RAM usage: it will be hovering safely around **9.5 to 10.5 GB**, well within your 11 GB allocation!
3. Observe CPU utilization on your Mac M4: smooth, low temperature, and zero system slowdown.

---

## 📱 Step 6: Test from Your Phone / Tablet on Local Wi-Fi

1. Make sure your phone or tablet is connected to the same Wi-Fi router as your Mac.
2. Open Safari / Chrome on your mobile device.
3. Type:
   * `http://192.168.3.30:30901` (MinIO Object Browser)
   * `http://192.168.3.30:30082` (Kafka UI)
   * `http://192.168.3.30:30300` (Grafana Dashboard)
4. You can now monitor your private enterprise cloud running inside your Mac from anywhere in your home!

---

## 🎓 Summary of Skills You Have Mastered
By completing this journey, you have accomplished:
* Setting up a bare-metal Linux VPS on Apple Silicon using ARM64 hypervisors.
* Operating certified Kubernetes (k3s) with custom namespaces, PVCs, and resource constraints.
* Architecting a true Open Lakehouse using **Apache Iceberg**, **MinIO S3**, and **Apache Polaris**.
* Real-time streaming with **Kafka KRaft** and **Schema Registry**.
* Visual data engineering with **Apache NiFi**.
* Distributed compute with **Apache Spark** and **Trino**.
* Scheduled workflow orchestration with **Apache Airflow**.
* Full observability with **Prometheus** and **Grafana**.

**You now possess practical, real-world data engineering and platform architecture experience!**
