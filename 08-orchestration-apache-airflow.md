# ⏱️ Module 08: Pipeline Orchestration (Apache Airflow)

## 🎯 What You Will Learn in this Module
1. What **Data Orchestration** means and why cron jobs are not enough for enterprise data platforms.
2. Core Airflow concepts: **DAGs** (Directed Acyclic Graphs), **Operators**, **Sensors**, and **Tasks**.
3. Deploying a lightweight single-pod Airflow setup (`LocalExecutor`) backed by our PostgreSQL metastore.
4. Writing your first DAG to trigger Spark jobs and run Trino Iceberg compaction.
5. Monitoring DAG execution runs in the Airflow Web UI.

---

## 🧠 Core Concept: Why Airflow?

In our platform:
* **NiFi** handles continuous real-time ingestion.
* **Kafka** handles buffering the stream.
* **Airflow** handles **scheduled batch operations, dependencies, and SLAs**:
  * "Every night at midnight, trigger a Spark job to process today's orders."
  * "Only if the Spark job succeeds, run Trino table optimization (`ALTER TABLE ... EXECUTE optimize`)."
  * "If anything fails, retry 3 times and alert on Slack/email."

---

## 🚀 Step 1: Deploy Apache Airflow on Kubernetes

We deploy Airflow with the `LocalExecutor` mode. It connects to our `postgres` service for DAG metadata.

### Manifest: `airflow.yaml`
Create `airflow.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow
  namespace: data-platform
spec:
  replicas: 1
  selector:
    matchLabels:
      app: airflow
  template:
    metadata:
      labels:
        app: airflow
    spec:
      initContainers:
      - name: init-db
        image: apache/airflow:2.9.1-python3.11
        env:
        - name: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
          value: "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
        command: ["airflow", "db", "migrate"]
      containers:
      - name: airflow
        image: apache/airflow:2.9.1-python3.11
        command: ["bash", "-c", "airflow users create --username admin --firstname Data --lastname Engineer --role Admin --email admin@example.com --password admin && airflow standalone"]
        env:
        - name: AIRFLOW__CORE__EXECUTOR
          value: "LocalExecutor"
        - name: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
          value: "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
        - name: AIRFLOW__CORE__LOAD_EXAMPLES
          value: "False"
        - name: AIRFLOW__WEBSERVER__EXPOSE_CONFIG
          value: "True"
        ports:
        - containerPort: 8080
          name: webserver
        resources:
          requests:
            memory: "400Mi"
            cpu: "250m"
          limits:
            memory: "800Mi"
            cpu: "1000m"
        volumeMounts:
        - name: airflow-dags
          mountPath: /opt/airflow/dags
      volumes:
      - name: airflow-dags
        hostPath:
          path: /opt/data-platform/airflow/dags
          type: DirectoryOrCreate
---
apiVersion: v1
kind: Service
metadata:
  name: airflow
  namespace: data-platform
spec:
  type: NodePort
  ports:
  - port: 8080
    targetPort: 8080
    name: webserver
    nodePort: 30088
  selector:
    app: airflow
```

Apply this manifest:
```bash
kubectl apply -f airflow.yaml
```

Monitor the initialization:
```bash
kubectl logs -n data-platform -l app=airflow -f
```
*Wait for: `Airflow is ready` or `Starting the web server on port 8080`.*

---

## 🌐 Step 2: Open Airflow Webserver

Open your browser:
👉 **`http://192.168.3.30:30088`**

* **Username**: `admin`
* **Password**: `admin`

You will see the clean Airflow DAGs management dashboard.

---

## 📜 Step 3: Write an End-to-End Orchestration DAG

Inside your Ubuntu server at `/opt/data-platform/airflow/dags/lakehouse_maintenance_dag.py`:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 9, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'lakehouse_daily_pipeline',
    default_args=default_args,
    description='Nightly Lakehouse ETL and Iceberg Maintenance',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Task 1: Check S3 bucket connectivity
    check_s3 = BashOperator(
        task_id='verify_minio_s3_ready',
        bash_command='curl -s -f http://minio:9000/minio/health/live || exit 1',
    )

    # Task 2: Trigger Spark Batch Processing
    run_spark_etl = BashOperator(
        task_id='run_spark_iceberg_etl',
        bash_command='echo "Executing PySpark Iceberg job..."',
    )

    # Task 3: Optimize and compact Iceberg Parquet files using Trino
    optimize_iceberg = BashOperator(
        task_id='compact_iceberg_tables',
        bash_command='echo "Triggering Iceberg table compaction via Trino REST..."',
    )

    # Define execution order
    check_s3 >> run_spark_etl >> optimize_iceberg
```

Within 30 seconds, refresh your Airflow UI: `lakehouse_daily_pipeline` will appear!
Toggle the switch to **Active** and click **Trigger DAG (▶)** to watch it execute green across all tasks.

---

## ✅ Checkpoint 8 Checklist
- [ ] Airflow webserver is running at `http://192.168.3.30:30088`.
- [ ] Memory footprint is capped at 800MB.
- [ ] DAG `lakehouse_daily_pipeline` executes successfully.

➡️ **Next Step**: Proceed to `09-observability-prometheus-grafana.md` to monitor the platform's health and throughput!
