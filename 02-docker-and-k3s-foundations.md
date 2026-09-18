# 📦 Module 02: Docker Engine & Kubernetes (k3s) Foundations

## 🎯 What You Will Learn in this Module
1. Why we install native **Docker CE** and **k3s** inside Ubuntu 24.04.
2. How Kubernetes differs from plain Docker Compose (Pods, Deployments, Services, ConfigMaps).
3. Why **k3s** is the gold-standard lightweight certified Kubernetes engine for ARM64 edge/development.
4. How to manage the cluster with `kubectl` and `helm` from both inside Ubuntu and your Mac terminal.
5. Setting up the built-in **Traefik Ingress Controller** to route traffic to all 11 services.

---

## 🧠 Core Concept: Docker vs. Kubernetes in a Data Platform

* **Docker / Docker Compose**:
  * Good for running single containers or simple multi-container apps on a single machine.
  * Lacks auto-healing: if a container runs out of memory (OOM), it often fails silently or crashes the host.
  * Harder to configure dynamic storage and service discovery.
* **Kubernetes (k8s)**:
  * The industry standard for enterprise data platforms.
  * **Declarative**: You write YAML files describing the state you want; Kubernetes makes it happen.
  * **Resource Quotas**: You can strictly cap NiFi at 1.5GB and Kafka at 750MB. If one spikes, K8s throttles or restarts only that pod, protecting your Mac!
  * **Service Discovery**: Pods talk to each other via internal DNS names (e.g. `http://kafka:9092` or `http://minio:9000`).

---

## 🛠️ Step 1: Install Docker CE inside Ubuntu 24.04

Log into your Ubuntu server:
```bash
ssh dataplat
```

Run the standard Docker CE installation for Ubuntu 24.04:

```bash
# 1. Update package index and install prerequisites
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg lsb-release

# 2. Add Docker's official GPG key
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# 3. Add the repository to Apt sources
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# 4. Install Docker Engine, CLI, and Containerd
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# 5. Allow ubuntu user to run docker without sudo
sudo usermod -aG docker $USER
```

Verify Docker works:
```bash
# Log out and back in to apply group changes
exit
ssh dataplat

docker run --rm hello-world
```
*You should see "Hello from Docker!"*

---

## ☸️ Step 2: Install k3s (Lightweight Certified Kubernetes)

Instead of bloated full Kubernetes (which eats 2.5 GB RAM just for its control plane), we install **k3s** by Rancher/CNCF. It is 100% compliant Kubernetes, packaged as a single ~60MB binary, consuming only ~400–500MB of RAM.

Inside Ubuntu, run:

```bash
# Install k3s with Traefik ingress enabled and Docker engine integration
curl -sfL https://get.k3s.io | sh -s - \
  --write-kubeconfig-mode 644 \
  --docker
```

Verify the node is ready:
```bash
kubectl get nodes -o wide
```
Output will show `dataplat-server` with status `Ready` (ARM64).

Check core system pods:
```bash
kubectl get pods -A
```
You will see CoreDNS, Traefik ingress controller, and local-path-provisioner running smoothly.

---

## 📦 Step 3: Install Helm (The Kubernetes Package Manager)

Helm is the "apt-get / brew" of Kubernetes. We will use it to easily install complex tools like Prometheus, Grafana, and Trino.

Inside Ubuntu:
```bash
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

helm version
```

---

## 💻 Step 4: Access `kubectl` Directly from Your Mac

You don't always need to SSH into Ubuntu to run `kubectl`. You can run `kubectl` on your Mac and manage the cluster remotely!

1. On your **Mac**, install `kubectl` (if not already installed):
   ```bash
   brew install kubectl
   ```

2. Copy the kubeconfig from Ubuntu to your Mac:
   ```bash
   VM_IP=$(multipass info dataplat-server | grep IPv4 | awk '{print $2}')
   mkdir -p ~/.kube
   scp dataplat:/etc/rancher/k3s/k3s.yaml ~/.kube/config-dataplat

   # Replace localhost/127.0.0.1 with the actual VM IP:
   sed -i '' "s/127.0.0.1/$VM_IP/g" ~/.kube/config-dataplat

   # Merge or set KUBECONFIG
   export KUBECONFIG=~/.kube/config-dataplat
   echo 'export KUBECONFIG=~/.kube/config-dataplat' >> ~/.zshrc
   ```

3. Test it on your Mac:
   ```bash
   kubectl get nodes
   ```
   *Boom! You are now managing your Linux Kubernetes cluster directly from your macOS terminal!*

---

## 🌐 Step 5: Understanding K8s Namespaces & Storage

We will create a dedicated namespace in Kubernetes called `data-platform` where all 11 services will live:

```bash
kubectl create namespace data-platform
```

Create a standard directory on the Ubuntu host for persistent storage:
```bash
# Inside Ubuntu:
sudo mkdir -p /opt/data-platform/{postgres,minio,kafka,nifi,airflow}
sudo chmod -R 777 /opt/data-platform
```

---

## ✅ Checkpoint 2 Checklist
- [ ] Docker runs without `sudo` on Ubuntu (`docker ps`).
- [ ] `kubectl get nodes` shows `Ready`.
- [ ] `helm version` outputs Helm v3.
- [ ] `kubectl get ns data-platform` exists.

➡️ **Next Step**: Proceed to `03-storage-postgres-and-minio.md` to deploy our central database (PostgreSQL) and object storage (MinIO S3).
