# ⚡ Module 07: Compute & Query Engines (Trino & Apache Spark)

## 🎯 What You Will Learn in this Module
1. The difference between **Batch/Streaming ETL (Apache Spark)** and **Interactive MPP SQL (Trino)**.
2. How Trino queries Iceberg tables without needing a running database server.
3. How to connect Trino to **Apache Polaris Catalog** and **MinIO S3**.
4. How to run PySpark jobs in Kubernetes that read raw files and write clean Iceberg tables.
5. Tuning Trino memory limits to 2.0 GB so it never crashes your Mac.

---

## 🧠 Core Concept: Spark vs. Trino in the Lakehouse

* **Apache Spark**:
  * Designed for heavy-lifting batch transformations, machine learning, and streaming ETL.
  * Writes Parquet files, compacts small files, and updates Iceberg table snapshots.
  * In Kubernetes, Spark jobs can run as **ephemeral pods** (they start, do work, and terminate to free up RAM).
* **Trino** (formerly PrestoSQL):
  * Designed for fast, sub-second interactive SQL queries across petabytes of data.
  * Uses Massively Parallel Processing (MPP).
  * Does NOT store data; it queries the files directly in MinIO using the Polaris catalog metadata!

---

## 🚀 Step 1: Deploy Trino with Iceberg Connector

We will configure Trino to connect directly to our Polaris REST catalog (`http://polaris:8181/api/v1`) and MinIO S3 (`http://minio:9000`).

### Manifest: `trino.yaml`
Create `trino.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: trino-config
  namespace: data-platform
data:
  node.properties: |
    node.environment=production
    node.data-dir=/data/trino
  jvm.config: |
    -server
    -Xmx1800M
    -XX:+UseG1GC
    -XX:G1HeapRegionSize=32M
    -XX:+ExplicitGCInvokesConcurrent
    -XX:+ExitOnOutOfMemoryError
  config.properties: |
    coordinator=true
    node-scheduler.include-coordinator=true
    http-server.http.port=8080
    query.max-memory=1GB
    query.max-memory-per-node=1GB
    discovery.uri=http://localhost:8080
  iceberg.properties: |
    connector.name=iceberg
    iceberg.catalog.type=rest
    iceberg.rest-catalog.uri=http://polaris:8181/api/v1
    iceberg.rest-catalog.warehouse=warehouse
    hive.s3.endpoint=http://minio:9000
    hive.s3.aws-access-key=admin
    hive.s3.aws-secret-key=password123
    hive.s3.path-style-access=true
    hive.s3.ssl.enabled=false
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: trino
  namespace: data-platform
spec:
  replicas: 1
  selector:
    matchLabels:
      app: trino
  template:
    metadata:
      labels:
        app: trino
    spec:
      containers:
      - name: trino
        image: trinodb/trino:445
        ports:
        - containerPort: 8080
          name: http
        resources:
          requests:
            memory: "1200Mi"
            cpu: "500m"
          limits:
            memory: "2000Mi"
            cpu: "2000m"
        volumeMounts:
        - name: config-volume
          mountPath: /etc/trino
        - name: catalog-volume
          mountPath: /etc/trino/catalog
      volumes:
      - name: config-volume
        configMap:
          name: trino-config
          items:
          - key: node.properties
            path: node.properties
          - key: jvm.config
            path: jvm.config
          - key: config.properties
            path: config.properties
      - name: catalog-volume
        configMap:
          name: trino-config
          items:
          - key: iceberg.properties
            path: iceberg.properties
---
apiVersion: v1
kind: Service
metadata:
  name: trino
  namespace: data-platform
spec:
  type: NodePort
  ports:
  - port: 8080
    targetPort: 8080
    name: http
    nodePort: 30085
  selector:
    app: trino
```

Apply this manifest:
```bash
kubectl apply -f trino.yaml
```

Check the pod status:
```bash
kubectl get pods -n data-platform -l app=trino
```

---

## 🌐 Step 2: Open Trino Web Console

Open your browser from your Mac or Wi-Fi device:
👉 **`http://192.168.3.30:30085`**

* **User**: `admin` (no password needed for development)

You will see Trino's live cluster overview:
* Active Workers: 1
* Running Queries: 0
* Active Memory Pool

---

## 🛠️ Step 3: Run Interactive SQL on Iceberg Tables

Let's use the Trino CLI to create an Iceberg table, insert rows, and query them.

Connect via Trino CLI inside the pod:
```bash
kubectl exec -n data-platform -it deploy/trino -- trino --catalog iceberg
```

Run these SQL commands in the Trino prompt:

```sql
-- 1. Create a schema (namespace in Polaris)
CREATE SCHEMA IF NOT EXISTS iceberg.lakehouse_db
WITH (location = 's3://warehouse/lakehouse_db');

-- 2. Create an Apache Iceberg table
CREATE TABLE iceberg.lakehouse_db.customer_orders (
    order_id VARCHAR,
    customer_id VARCHAR,
    amount DOUBLE,
    currency VARCHAR,
    order_date DATE
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['order_date']
);

-- 3. Insert sample rows into Iceberg table
INSERT INTO iceberg.lakehouse_db.customer_orders VALUES
    ('ord_101', 'cust_1', 150.00, 'USD', DATE '2026-09-18'),
    ('ord_102', 'cust_2', 320.50, 'USD', DATE '2026-09-18'),
    ('ord_103', 'cust_1', 45.00, 'USD', DATE '2026-09-18');

-- 4. Query the table
SELECT customer_id, count(*) as total_orders, sum(amount) as total_spent
FROM iceberg.lakehouse_db.customer_orders
GROUP BY customer_id;
```

Type `quit;` to exit.

---

## 🔍 Step 4: Verify the Underlying Data in MinIO

Open your **MinIO Console** (`http://192.168.3.30:30901`):
1. Navigate to bucket **`warehouse`**.
2. Open folder **`lakehouse_db/customer_orders/`**.
3. You will see:
   * **`data/`**: The raw columnar **`.parquet`** data files written by Trino.
   * **`metadata/`**: The Iceberg snapshot manifests (`.metadata.json`, `.avro` manifest lists) tracked by Polaris!

*You now have a 100% functional, open-format Lakehouse!*

---

## 🐍 Step 5: Running PySpark on Kubernetes

Spark can be run interactively or as scheduled batch jobs. 

Here is a ready-to-run PySpark script (`etl_orders.py`) that reads raw JSON files dumped by NiFi from `s3://raw-data/orders/` and appends them to our Iceberg table:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder \
    .appName("LakehouseETL") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "rest") \
    .config("spark.sql.catalog.iceberg.uri", "http://polaris:8181/api/v1") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Read raw JSON files from MinIO
df = spark.read.json("s3a://raw-data/orders/*/*/*.json")

# Clean & Cast
clean_df = df.withColumn("order_date", to_date(col("created_at")))

# Write directly to Iceberg
clean_df.writeTo("iceberg.lakehouse_db.customer_orders").append()
print("Successfully wrote batch to Iceberg table!")
```

---

## ✅ Checkpoint 7 Checklist
- [ ] Trino is running within the 2.0 GB memory envelope.
- [ ] You created an Iceberg table registered in Polaris.
- [ ] Trino executed queries and wrote Parquet data files into MinIO.
- [ ] You verified the snapshot files in the MinIO web console.

➡️ **Next Step**: Proceed to `08-orchestration-apache-airflow.md` to automate pipelines with Airflow!
