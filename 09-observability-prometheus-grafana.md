git branch -M main# 📊 Module 09: Observability & Monitoring (Prometheus & Grafana)

## 🎯 What You Will Learn in this Module
1. Why observability is non-negotiable for enterprise data platforms.
2. The 3 pillars of observability (Metrics, Logs, Traces).
3. How **Prometheus** scrapes metrics using pull-based HTTP endpoints.
4. How to deploy a lightweight Prometheus + Grafana stack with Helm.
5. Building dashboards to monitor Kafka message throughput, NiFi queues, and JVM RAM usage on your Mac M4.

---

## 🧠 Core Concept: Scraping Metrics in Kubernetes

Every component in our platform exposes metrics:
* **k3s / Ubuntu**: Exposes CPU, RAM, and disk I/O metrics.
* **Kafka**: Exposes incoming/outgoing messages per second and consumer lag.
* **MinIO**: Exposes S3 request rates and storage gigabytes used under `http://minio:9000/minio/v2/metrics/cluster`.
* **Trino**: Exposes query execution latency and memory pool usage.

**Prometheus** queries these HTTP endpoints every 15 seconds and stores them as time-series data.
**Grafana** queries Prometheus and paints dashboards.

---

## 🚀 Step 1: Deploy Prometheus & Grafana via Helm

Using Helm makes deploying Prometheus and Grafana take just two commands.

Inside your Ubuntu terminal (or Mac with kubectl):

```bash
# 1. Add Prometheus and Grafana Helm repositories
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update

# 2. Deploy lightweight Prometheus (retention 3 days to save disk space)
helm install prometheus prometheus-community/prometheus \
  --namespace data-platform \
  --set server.retention=3d \
  --set server.resources.limits.memory=350Mi \
  --set alertmanager.enabled=false \
  --set prometheus-node-exporter.resources.limits.memory=50Mi

# 3. Deploy Grafana
helm install grafana grafana/grafana \
  --namespace data-platform \
  --set adminPassword='admin' \
  --set resources.limits.memory=200Mi \
  --set service.type=NodePort \
  --set service.nodePort=30300
```

Verify the pods:
```bash
kubectl get pods -n data-platform -l 'app.kubernetes.io/name in (prometheus, grafana)'
```

---

## 🌐 Step 2: Open Grafana Dashboard

Open your browser from your Mac or any device on your Wi-Fi:
👉 **`http://192.168.3.30:30300`**

* **Username**: `admin`
* **Password**: `admin`

---

## 📈 Step 3: Add Prometheus as a Data Source in Grafana

1. In Grafana, click the **Gear icon (Connections / Data sources)**.
2. Click **Add data source** -> Select **Prometheus**.
3. In the **Prometheus server URL** field, enter the internal K8s cluster DNS:
   ```text
   http://prometheus-server.data-platform.svc.cluster.local
   ```
4. Scroll to the bottom and click **Save & Test**.
   *You will see a green checkmark: "Successfully queried the Prometheus server."*

---

## 🎨 Step 4: Import Ready-Made Kubernetes & JVM Dashboards

Grafana has thousands of community dashboards ready to import:

1. Click the **+ (Plus icon)** in top right -> **Import dashboard**.
2. Enter Dashboard ID **`315`** (Kubernetes Cluster Monitoring) or **`1860`** (Node Exporter Full) and click **Load**.
3. Select your Prometheus datasource and click **Import**.

### What You Can Now See Live:
* Exact RAM usage across each of your 11 pods.
* CPU core utilization on your Mac M4.
* MinIO S3 I/O read/write speeds.
* Alert thresholds if memory approaches the 11 GB ceiling!

---

## ✅ Checkpoint 9 Checklist
- [ ] Prometheus is scraping cluster metrics with a 350MB footprint.
- [ ] Grafana is accessible at `http://192.168.3.30:30300`.
- [ ] You have a live dashboard visualizing your cluster's CPU & memory consumption.

➡️ **Next Step**: Proceed to `10-end-to-end-practice-pipeline.md` for our grand capstone project!
