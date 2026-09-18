# 🗄️ Module 03: Storage & Metastore Layer (PostgreSQL & MinIO S3)

## 🎯 What You Will Learn in this Module
1. The difference between **Block Storage** (disks) and **Object Storage** (AWS S3 / MinIO).
2. Why modern Data Lakehouses store data in S3 instead of HDFS or relational databases.
3. How to deploy **MinIO** (high-performance, S3-compatible object storage).
4. How to deploy **PostgreSQL 16** with tuned memory limits to serve as the unified metadata store for Airflow, Polaris, and Superset.
5. How to write standard Kubernetes `Deployment`, `Service`, and `PersistentVolumeClaim` (PVC) manifests.

---

## 🧠 Architectural Understanding

In a cloud-native data platform:
* **PostgreSQL** is used for **metadata** (transaction logs, Airflow DAG state, user logins, catalog references). It stores small, relational rows.
* **MinIO (S3)** is used for the **actual data lake** (Parquet files, raw JSON, CSV, image blobs). MinIO presents the exact same API as Amazon Web Services (AWS S3), meaning any code you write here works identically in AWS, Azure, or GCP!

---

## 🐘 Step 1: Deploy PostgreSQL 16 (Tuned Metastore)

We will deploy PostgreSQL with 3 distinct databases:
1. `airflow`
2. `polaris`
3. `metastore`

### Manifest: `postgres.yaml`
Inside your Ubuntu server (or on Mac in your workspace), create `postgres.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: postgres-init-scripts
  namespace: data-platform
data:
  init.sql: |
    CREATE DATABASE airflow;
    CREATE DATABASE polaris;
    CREATE DATABASE metastore;
    CREATE USER airflow WITH ENCRYPTED PASSWORD 'airflow';
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
    CREATE USER polaris WITH ENCRYPTED PASSWORD 'polaris';
    GRANT ALL PRIVILEGES ON DATABASE polaris TO polaris;
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
  namespace: data-platform
spec:
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
      - name: postgres
        image: postgres:16-alpine
        env:
        - name: POSTGRES_USER
          value: "postgres"
        - name: POSTGRES_PASSWORD
          value: "postgres123"
        # Tuned memory flags for 350MB footprint
        - name: POSTGRES_INITDB_ARGS
          value: "-c shared_buffers=128MB -c max_connections=100 -c work_mem=4MB"
        ports:
        - containerPort: 5432
          name: postgres
        resources:
          requests:
            memory: "150Mi"
            cpu: "100m"
          limits:
            memory: "350Mi"
            cpu: "500m"
        volumeMounts:
        - name: postgres-data
          mountPath: /var/lib/postgresql/data
        - name: init-scripts
          mountPath: /docker-entrypoint-initdb.d
      volumes:
      - name: postgres-data
        hostPath:
          path: /opt/data-platform/postgres
          type: DirectoryOrCreate
      - name: init-scripts
        configMap:
          name: postgres-init-scripts
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
  namespace: data-platform
spec:
  type: NodePort
  ports:
  - port: 5432
    targetPort: 5432
    nodePort: 30432
  selector:
    app: postgres
```

Apply this manifest:
```bash
kubectl apply -f postgres.yaml
```

Verify it is running:
```bash
kubectl get pods -n data-platform -l app=postgres
```

---

## 🪣 Step 2: Deploy MinIO S3 Object Storage

MinIO provides an S3 API endpoint (port `9000`) and a rich Web Console (port `9001`).

### Manifest: `minio.yaml`
Create `minio.yaml`:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: minio-credentials
  namespace: data-platform
type: Opaque
stringData:
  MINIO_ROOT_USER: "admin"
  MINIO_ROOT_PASSWORD: "117470908Aa1!"

---

apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
  namespace: data-platform
spec:
  replicas: 1
  selector:
    matchLabels:
      app: minio
  template:
    metadata:
      labels:
        app: minio
    spec:
      containers:
        - name: minio
          image: quay.io/minio/minio:latest

          command:
            - /bin/sh
            - -c
            - minio server /data --console-address :9001

          env:
            - name: MINIO_ROOT_USER
              valueFrom:
                secretKeyRef:
                  name: minio-credentials
                  key: MINIO_ROOT_USER

            - name: MINIO_ROOT_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: minio-credentials
                  key: MINIO_ROOT_PASSWORD

          ports:
            - name: s3-api
              containerPort: 9000

            - name: web-console
              containerPort: 9001

          resources:
            requests:
              memory: "64Mi"
              cpu: "25m"

            limits:
              memory: "128Mi"
              cpu: "250m"

          volumeMounts:
            - name: minio-data
              mountPath: /data

          readinessProbe:
            httpGet:
              path: /minio/health/ready
              port: 9000
            initialDelaySeconds: 10
            periodSeconds: 10
            timeoutSeconds: 5
            failureThreshold: 6

          livenessProbe:
            httpGet:
              path: /minio/health/live
              port: 9000
            initialDelaySeconds: 30
            periodSeconds: 20
            timeoutSeconds: 5
            failureThreshold: 3

      volumes:
        - name: minio-data
          hostPath:
            path: /opt/data-platform/minio
            type: DirectoryOrCreate

---

apiVersion: v1
kind: Service
metadata:
  name: minio
  namespace: data-platform
spec:
  type: NodePort

  ports:
    - name: api
      port: 9000
      targetPort: 9000
      nodePort: 30900

    - name: console
      port: 9001
      targetPort: 9001
      nodePort: 30901

  selector:
    app: minio
```

Apply the manifest:
```bash
kubectl apply -f minio.yaml
```

Check the pod status:
```bash
kubectl get pods -n data-platform -l app=minio
```

---

## 🛠️ Step 3: Initialize S3 Buckets

We will create the standard buckets required for our Lakehouse:
* `warehouse` - Stores Apache Iceberg metadata and Parquet data files.
* `raw-data` - Ingestion landing zone for NiFi / Kafka file dumps.
* `checkpoints` - Spark streaming checkpoint state.

Run a quick one-off container with the MinIO client (`mc`):

```bash
kubectl run minio-mc -n data-platform --rm -i --restart='Never' --image=minio/mc --command -- /bin/sh -c "
  mc alias set myminio http://minio:9000 admin password123 &&
  mc mb myminio/warehouse || true &&
  mc mb myminio/raw-data || true &&
  mc mb myminio/checkpoints || true &&
  mc ls myminio
"
```
You will see all three buckets listed!

---

## 🌐 Step 4: Access MinIO Console from Your Wi-Fi Network

Open your browser from **your Mac or any device on your Wi-Fi**:
👉 **`http://192.168.3.30:30901`** *(or `http://<VM-IP>:9001`)*

* **Username**: `admin`
* **Password**: `password123`

You will see the MinIO Object Browser with your `warehouse`, `raw-data`, and `checkpoints` buckets ready!

---

## ✅ Checkpoint 3 Checklist
- [ ] PostgreSQL is running and seeded with databases (`airflow`, `polaris`).
- [ ] MinIO is running and accessible via browser.
- [ ] Buckets `warehouse`, `raw-data`, and `checkpoints` are visible in the MinIO UI.

➡️ **Next Step**: Proceed to `04-polaris-iceberg-catalog.md` to deploy **Apache Polaris**, our open Lakehouse Catalog!
