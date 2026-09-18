# ❄️ Module 04: Apache Polaris (Iceberg REST Catalog)

## 🎯 What You Will Learn in this Module
1. What **Apache Iceberg** is and why it replaced legacy Hive tables.
2. What a **Catalog** does in an open Lakehouse architecture.
3. Why **Apache Polaris** (open-sourced by Snowflake) is the new standard Iceberg REST catalog.
4. How to deploy Polaris on Kubernetes connecting to PostgreSQL and MinIO.
5. How to test the REST API and create an Iceberg namespace.

---

## 🧠 Core Concept: What is an Iceberg Catalog?

In a traditional database (like MySQL), the engine itself knows where rows are stored. 
In a **Data Lakehouse**, your data lives as raw **Parquet files** in S3 (MinIO). Multiple distinct engines (Spark, Trino, Flink, DuckDB) need to read and write to those exact same files at the same time without corrupting them.

How do they know which Parquet files belong to which version of the table? **The Catalog!**
* The catalog holds the atomic pointer to the current snapshot of every table.
* **Apache Polaris** implements the **Apache Iceberg REST Catalog Specification**.
* When Trino or Spark wants to query table `sales`, it asks Polaris: *"Where is the metadata for sales?"*
* Polaris verifies security and returns the S3 URI (`s3://warehouse/sales/metadata/v1.metadata.json`).

---

## 🚀 Step 1: Deploy Apache Polaris on Kubernetes

Polaris runs as a lightweight service. We configure it to use our PostgreSQL instance as its persistence backend.

### Manifest: `polaris.yaml`
Create `polaris.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: polaris
  namespace: data-platform
spec:
  replicas: 1
  selector:
    matchLabels:
      app: polaris
  template:
    metadata:
      labels:
        app: polaris
    spec:
      containers:
      - name: polaris
        image: apache/polaris:latest
        env:
        - name: POLARIS_PERSISTENCE_TYPE
          value: "eclipse-link"
        - name: POLARIS_PERSISTENCE_URL
          value: "jdbc:postgresql://postgres:5432/polaris"
        - name: POLARIS_PERSISTENCE_USER
          value: "polaris"
        - name: POLARIS_PERSISTENCE_PASSWORD
          value: "polaris"
        - name: AWS_ACCESS_KEY_ID
          value: "admin"
        - name: AWS_SECRET_ACCESS_KEY
          value: "password123"
        - name: AWS_ENDPOINT_URL
          value: "http://minio:9000"
        - name: AWS_REGION
          value: "us-east-1"
        - name: JAVA_OPTS
          value: "-Xmx384m -Xms128m"
        ports:
        - containerPort: 8181
          name: api
        resources:
          requests:
            memory: "250Mi"
            cpu: "200m"
          limits:
            memory: "500Mi"
            cpu: "1000m"
---
apiVersion: v1
kind: Service
metadata:
  name: polaris
  namespace: data-platform
spec:
  type: NodePort
  ports:
  - port: 8181
    targetPort: 8181
    name: api
    nodePort: 30181
  selector:
    app: polaris
```

Apply the manifest:
```bash
kubectl apply -f polaris.yaml
```

Verify Polaris is running:
```bash
kubectl get pods -n data-platform -l app=polaris
```

---

## 🔍 Step 2: Verify Polaris Health & REST Catalog API

Polaris exposes standard Iceberg REST endpoints under `/api/v1`.

Test health from your Mac or inside Ubuntu:
```bash
curl -i http://192.168.3.30:30181/api/v1/config
```
You will receive an HTTP `200 OK` with JSON configuration details!

---

## 🛠️ Step 3: Create a Warehouse & Namespace in Polaris

We will create our first catalog namespace named `lakehouse_db` located in `s3://warehouse/`:

```bash
# Create namespace via REST API
curl -X POST http://192.168.3.30:30181/api/v1/namespaces \
  -H "Content-Type: application/json" \
  -d '{
    "namespace": ["lakehouse_db"],
    "properties": {
      "location": "s3://warehouse/lakehouse_db"
    }
  }'
```

List namespaces to confirm:
```bash
curl -s http://192.168.3.30:30181/api/v1/namespaces | grep lakehouse_db
```

*Congratulations! Your Iceberg REST Catalog is live and ready for Spark and Trino!*

---

## ✅ Checkpoint 4 Checklist
- [ ] Polaris pod is in `Running` state with memory capped at 500MB.
- [ ] REST API `/api/v1/config` returns HTTP 200.
- [ ] Namespace `lakehouse_db` is created in Polaris backed by MinIO S3.

➡️ **Next Step**: Proceed to `05-streaming-kafka-schema-registry.md` to set up our real-time streaming backbone!
