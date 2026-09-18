# 🔄 Module 06: Data Ingestion & Flow Automation (Apache NiFi)

## 🎯 What You Will Learn in this Module
1. What **Apache NiFi** is and why it's the industry's leading visual ETL/ELT tool.
2. Core concepts: **FlowFiles**, **Processors**, **Connections**, and **Controller Services**.
3. Tuning NiFi's Java Virtual Machine (JVM) heap so it doesn't starve your Mac's 16GB RAM.
4. Deploying NiFi on Kubernetes with persistent flow configuration.
5. Building your first visual flow: fetching real-time data from an API, parsing JSON, and pushing it to Kafka and MinIO S3.

---

## 🧠 Core Concept: How NiFi Works

Apache NiFi is based on **Flow-Based Programming (FBP)**:
* **FlowFile**: A packet of data passing through the system. It consists of:
  1. *Content* (the actual payload, e.g. JSON, CSV, binary image).
  2. *Attributes* (key-value metadata, e.g. `filename`, `mime.type`, `timestamp`).
* **Processor**: A node that transforms, routes, or ingests data (e.g. `InvokeHTTP`, `SplitJson`, `PublishKafka_2_6`, `PutS3Object`).
* **Queues**: Buffers between processors with backpressure limits to prevent memory overflow.

---

## ⚠️ Memory Optimization for NiFi on 16GB Mac

NiFi out of the box will attempt to reserve 4 to 8 GB of RAM. 
To run reliably alongside our other 10 services on an 11GB VM:
* We cap NiFi's JVM heap at **`-Xmx1024m -Xms512m`**.
* We set K8s memory request to `800Mi` and limit to `1500Mi`.

---

## 🚀 Step 1: Deploy Apache NiFi on Kubernetes

### Manifest: `nifi.yaml`
Create `nifi.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nifi
  namespace: data-platform
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nifi
  template:
    metadata:
      labels:
        app: nifi
    spec:
      containers:
      - name: nifi
        image: apache/nifi:1.25.0
        env:
        - name: NIFI_WEB_HTTP_PORT
          value: "8080"
        - name: NIFI_JVM_HEAP_INIT
          value: "512m"
        - name: NIFI_JVM_HEAP_MAX
          value: "1024m"
        - name: SINGLE_USER_CREDENTIALS_USERNAME
          value: "admin"
        - name: SINGLE_USER_CREDENTIALS_PASSWORD
          value: "DataPlatform2026!"
        ports:
        - containerPort: 8080
          name: http
        resources:
          requests:
            memory: "800Mi"
            cpu: "250m"
          limits:
            memory: "1500Mi"
            cpu: "1500m"
        volumeMounts:
        - name: nifi-conf
          mountPath: /opt/nifi/nifi-current/conf
        - name: nifi-flow
          mountPath: /opt/nifi/nifi-current/flow_storage
      volumes:
      - name: nifi-conf
        hostPath:
          path: /opt/data-platform/nifi/conf
          type: DirectoryOrCreate
      - name: nifi-flow
        hostPath:
          path: /opt/data-platform/nifi/flow
          type: DirectoryOrCreate
---
apiVersion: v1
kind: Service
metadata:
  name: nifi
  namespace: data-platform
spec:
  type: NodePort
  ports:
  - port: 8080
    targetPort: 8080
    name: http
    nodePort: 30880
  selector:
    app: nifi
```

Apply this manifest:
```bash
kubectl apply -f nifi.yaml
```

Monitor the startup logs (NiFi takes ~60-90 seconds to unpack Java bundles on first boot):
```bash
kubectl logs -n data-platform -l app=nifi -f
```

*Wait until you see: `NiFi has started. The UI is available at...`*

Fix Issues

```bash
kubectl patch deployment nifi -n data-platform --type='json' -p='[{"op":"remove","path":"/spec/template/spec/containers/0/volumeMounts/0"},{"op":"remove","path":"/spec/template/spec/volumes/0"}]'
```

```bash
sudo chown -R 1000:1000 /opt/data-platform/nifi
```

```bash
kubectl set env deployment/nifi -n data-platform \
  NIFI_WEB_HTTPS_HOST=nifi.local \
  NIFI_WEB_PROXY_HOST=nifi.local:30880
```

```bash
kubectl set env deployment/nifi -n data-platform NIFI_WEB_HTTPS_HOST=0.0.0.0
```

```bash
kubectl rollout restart deployment nifi -n data-platform
```

```bash
kubectl get pods -n data-platform -w
```
---

## 🌐 Step 2: Access the NiFi Web Canvas

From any browser on your Wi-Fi:
👉 **`http://192.168.3.30:30880/nifi`**

* **Username**: `admin`
* **Password**: `DataPlatform2026!`

You will be greeted by the blank NiFi canvas grid.

---

## 🎨 Step 3: Build Your First Visual Ingestion Pipeline

Let's build a real pipeline that generates fake user purchase transactions every 5 seconds, parses them, and publishes them into Kafka and MinIO!

### 1. Add `GenerateFlowFile` (Data Generator)
1. Drag the **Processor** icon from the top toolbar onto the canvas.
2. Search for `GenerateFlowFile` and click **Add**.
3. Right-click the processor -> **Configure**:
   * **Scheduling** tab:
     * *Run Schedule*: `5 sec`
   * **Properties** tab:
     * *Custom Text*:
       ```json
       {
         "order_id": "${UUID()}",
         "customer_id": "CUST_${random():mod(100)}",
         "amount": ${random():mod(500):plus(10)},
         "currency": "USD",
         "created_at": "${now():format('yyyy-MM-dd HH:mm:ss')}"
       }
       ```
   * Click **Apply**.

### 2. Add `PublishKafka_2_6` (Push to Streaming Topic)
1. Drag another **Processor** onto the canvas.
2. Search for `PublishKafka_2_6` (or modern `PublishKafka`).
3. Configure it:
   * **Properties**:
     * *Kafka Brokers*: `kafka:9092`
     * *Topic Name*: `orders_stream`
     * *Delivery Guarantee*: `1` (Best effort / at least once)
4. Connect `GenerateFlowFile` to `PublishKafka`:
   * Hover over `GenerateFlowFile` until the arrow appears.
   * Drag arrow to `PublishKafka`.
   * Check relationship: `success`.

### 3. Add `PutS3Object` (Archive Raw Events in MinIO S3)
1. Add processor `PutS3Object`.
2. Configure:
   * *Bucket*: `raw-data`
   * *Object Key*: `orders/${now():format('yyyy/MM/dd')}/${filename}.json`
   * *Access Key*: `admin`
   * *Secret Key*: `password123`
   * *Endpoint Override URL*: `http://minio:9000`
   * *Signer Override*: `AWSS3V4SignerType`
3. Connect `GenerateFlowFile` to `PutS3Object` as well!

### 4. Start the Flow!
1. Press `Ctrl + A` (or `Cmd + A`) on the canvas to select all processors.
2. Click the green **Start (▶)** button in the top toolbar.

### 5. Verify the Flow
* Open **Kafka UI** (`http://192.168.3.30:30082`): Look at topic `orders_stream`. You will see purchase orders streaming in every 5 seconds!
* Open **MinIO Console** (`http://192.168.3.30:30901`): Look inside bucket `raw-data/orders/`. You will see JSON order archives being created!

---

## ✅ Checkpoint 6 Checklist
- [ ] NiFi UI is running at `http://192.168.3.30:30880/nifi`.
- [ ] Memory footprint is verified under 1.5 GB via `kubectl top pod -n data-platform`.
- [ ] Simulated data streams into both Kafka topic `orders_stream` and MinIO bucket `raw-data`.

➡️ **Next Step**: Proceed to `07-compute-spark-and-trino.md` to deploy our SQL engine (Trino) and ETL processor (Spark)!
