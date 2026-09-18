# ⚡ Module 05: Real-Time Event Streaming (Kafka KRaft & Schema Registry)

## 🎯 What You Will Learn in this Module
1. How Apache Kafka works (Topics, Partitions, Offsets, Consumers).
2. Why we use **KRaft mode** (Kafka Raft metadata mode) instead of ZooKeeper.
3. What a **Schema Registry** is and why it prevents data corruption in streaming pipelines.
4. How to deploy Kafka, Schema Registry, and **Kafka UI** on Kubernetes.
5. How to produce and consume messages using both the CLI and web interface.

---

## 🧠 Architectural Understanding: KRaft vs ZooKeeper

Historically, running Kafka required running a separate, memory-hungry cluster called **Apache ZooKeeper**. ZooKeeper held topic metadata, controller elections, and ACLs. This doubled the RAM required.

With modern Kafka (v3.3+), Kafka has integrated **KRaft (Kafka Raft)**:
* Kafka nodes elect their own quorum leader using the Raft consensus algorithm.
* ZooKeeper is completely eliminated!
* Saves ~500MB of RAM and boots in seconds.

---

## 📜 What is Schema Registry?

In an enterprise data platform, microservices and NiFi produce JSON or Avro events to Kafka. If a developer accidentally renames a field from `user_id` to `id`, downstream Spark jobs will crash!

**Confluent Schema Registry** acts as a contract enforcer:
* Producers register an Avro/JSON schema (e.g. `UserCreated.avsc`).
* The registry checks schema compatibility (backward/forward).
* If a producer tries to send malformed data, it is rejected immediately before corrupting your data lake!

---

## 🚀 Step 1: Deploy Kafka (KRaft Mode), Schema Registry & Kafka UI

### Manifest: `kafka-stack.yaml`
Create `kafka-stack.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka
  namespace: data-platform
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka
  template:
    metadata:
      labels:
        app: kafka
    spec:
      containers:
      - name: kafka
        image: apache/kafka:3.7.0
        env:
        - name: KAFKA_NODE_ID
          value: "1"
        - name: KAFKA_PROCESS_ROLES
          value: "broker,controller"
        - name: KAFKA_LISTENERS
          value: "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093"
        - name: KAFKA_ADVERTISED_LISTENERS
          value: "PLAINTEXT://kafka:9092"
        - name: KAFKA_CONTROLLER_LISTENER_NAMES
          value: "CONTROLLER"
        - name: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP
          value: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
        - name: KAFKA_CONTROLLER_QUORUM_VOTERS
          value: "1@kafka:9093"
        - name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR
          value: "1"
        - name: KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR
          value: "1"
        - name: KAFKA_TRANSACTION_STATE_LOG_MIN_ISR
          value: "1"
        - name: KAFKA_LOG_DIRS
          value: "/tmp/kraft-combined-logs"
        - name: KAFKA_CLUSTER_ID
          value: "MkU3OEVBNTcwNTJENDM2Qk"
        - name: KAFKA_JVM_PERFORMANCE_OPTS
          value: "-Xmx512m -Xms256m"
        ports:
        - containerPort: 9092
          name: broker
        resources:
          requests:
            memory: "350Mi"
            cpu: "200m"
          limits:
            memory: "750Mi"
            cpu: "1000m"
---
apiVersion: v1
kind: Service
metadata:
  name: kafka
  namespace: data-platform
spec:
  type: ClusterIP
  ports:
  - port: 9092
    targetPort: 9092
    name: broker
  selector:
    app: kafka
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: schema-registry
  namespace: data-platform
spec:
  replicas: 1
  selector:
    matchLabels:
      app: schema-registry
  template:
    metadata:
      labels:
        app: schema-registry
    spec:
      containers:
      - name: schema-registry
        image: confluentinc/cp-schema-registry:7.6.1
        env:
        - name: SCHEMA_REGISTRY_HOST_NAME
          value: "schema-registry"
        - name: SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS
          value: "PLAINTEXT://kafka:9092"
        - name: SCHEMA_REGISTRY_LISTENERS
          value: "http://0.0.0.0:8081"
        - name: SCHEMA_REGISTRY_HEAP_OPTS
          value: "-Xmx256m -Xms128m"
        ports:
        - containerPort: 8081
          name: api
        resources:
          requests:
            memory: "200Mi"
            cpu: "100m"
          limits:
            memory: "350Mi"
            cpu: "500m"
---
apiVersion: v1
kind: Service
metadata:
  name: schema-registry
  namespace: data-platform
spec:
  type: NodePort
  ports:
  - port: 8081
    targetPort: 8081
    name: api
    nodePort: 30081
  selector:
    app: schema-registry
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-ui
  namespace: data-platform
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka-ui
  template:
    metadata:
      labels:
        app: kafka-ui
    spec:
      containers:
      - name: kafka-ui
        image: provectuslabs/kafka-ui:latest
        env:
        - name: KAFKA_CLUSTERS_0_NAME
          value: "local-lakehouse"
        - name: KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS
          value: "kafka:9092"
        - name: KAFKA_CLUSTERS_0_SCHEMAREGISTRY
          value: "http://schema-registry:8081"
        - name: JAVA_OPTS
          value: "-Xmx256m -Xms128m"
        ports:
        - containerPort: 8080
          name: web
        resources:
          requests:
            memory: "150Mi"
            cpu: "100m"
          limits:
            memory: "350Mi"
            cpu: "500m"
---
apiVersion: v1
kind: Service
metadata:
  name: kafka-ui
  namespace: data-platform
spec:
  type: NodePort
  ports:
  - port: 8080
    targetPort: 8080
    name: web
    nodePort: 30082
  selector:
    app: kafka-ui
```

Apply this manifest:
```bash
kubectl apply -f kafka-stack.yaml
```

Check the pods status:
```bash
kubectl get pods -n data-platform -l 'app in (kafka, schema-registry, kafka-ui)'
```

---

## 🌐 Step 2: Open Kafka UI from Your Wi-Fi Network

Open your browser from any device on your Wi-Fi:
👉 **`http://192.168.3.30:30082`**

You will see the **Kafka UI dashboard**:
* Broker Health: 1 Online
* Topics: 0 (or internal consumer offset topics)
* Schema Registry: Connected

---

## 🧪 Step 3: Create a Topic & Produce Messages

Let's test Kafka by creating a topic `iot_sensor_events` with 3 partitions:

1. In Kafka UI, click **Topics** -> **Add a Topic**.
   * Name: `iot_sensor_events`
   * Partitions: `3`
   * Click **Create**.

2. Or run it directly via Kafka CLI inside the pod:
   ```bash
   kubectl exec -n data-platform -it deploy/kafka -- \
     /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
     --create --topic iot_sensor_events --partitions 3 --replication-factor 1
   ```

3. Send a test JSON message:
   ```bash
   kubectl exec -n data-platform -it deploy/kafka -- \
     /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic iot_sensor_events
   ```
   Type:
   `{"device_id": "sensor_01", "temperature": 23.5, "timestamp": "2026-09-18T17:00:00Z"}`
   Press **Enter**, then **Ctrl+C**.

4. Refresh **Kafka UI** -> **iot_sensor_events** -> **Messages**: you will see the message stored with offset 0 and partition timestamp!

---

## ✅ Checkpoint 5 Checklist
- [ ] Kafka broker is healthy in KRaft mode (no ZooKeeper).
- [ ] Schema Registry is accessible at port `30081`.
- [ ] Kafka UI is accessible at `http://192.168.3.30:30082`.
- [ ] You produced and viewed a message in `iot_sensor_events`.

➡️ **Next Step**: Proceed to `06-ingestion-apache-nifi.md` to visually build automated ingestion flows!
