# Kubernetes & K3s — Simple Notes

## 1. What is Kubernetes?

**Kubernetes (K8s)** is a system for managing containers.

With Docker, you might run:

```bash
docker run nginx
```

With Kubernetes, you tell it:

> "I want 3 copies of my application running."

Kubernetes then manages those containers for you.

It can:

* Start containers
* Restart failed containers
* Scale applications
* Distribute traffic
* Update applications
* Manage multiple servers

---

## 2. K8s vs K3s

### K8s

**K8s** is simply a short name for **Kubernetes**.

```text
K + 8 letters + S = K8s
Kubernetes
```

### K3s

**K3s is a lightweight Kubernetes distribution.**

It is still Kubernetes, but it is designed to be:

* Smaller
* Easier to install
* Less resource-hungry

K3s is commonly used for:

* Learning
* Home labs
* Small servers
* Edge computing
* Small production environments

### Simple way to remember

```text
K8s = Kubernetes

K3s = Lightweight Kubernetes
```

---

# 3. Kubernetes Architecture

The basic structure is:

```text
Cluster
   |
   +-- Node
         |
         +-- Pod
               |
               +-- Container
```

## Cluster

The entire Kubernetes environment.

```text
Kubernetes Cluster
│
├── Node 1
├── Node 2
└── Node 3
```

---

## Node

A machine running Kubernetes.

A node can be:

* Physical server
* Virtual machine
* Cloud server
* Your computer

---

## Pod

A **Pod** is the basic unit Kubernetes manages.

Usually:

```text
Pod
└── Container
```

Sometimes:

```text
Pod
├── Application container
└── Sidecar container
```

For beginners, think:

> **Pod = wrapper around a container.**

---

# 4. Deployment

A Deployment manages Pods.

For example:

```text
Deployment
    |
    +-- Pod
    +-- Pod
    +-- Pod
```

If you say:

```text
replicas: 3
```

Kubernetes tries to keep **3 Pods running**.

If one crashes:

```text
Before:

Pod   Pod   Pod
 |     |     |
OK    OK    OK

One crashes:

Pod   X     Pod
 |           |
OK          OK
```

Kubernetes notices that only 2 are running and creates another:

```text
Pod   Pod   Pod
 |     |     |
OK    OK    OK
```

This is one of the most important ideas in Kubernetes:

> **You tell Kubernetes what you want, and Kubernetes works to keep that state.**

---

# 5. Service

Pods can be created and destroyed, so their IP addresses can change.

A **Service** provides a stable way to access Pods.

```text
             Service
                |
       +--------+--------+
       |        |        |
      Pod      Pod      Pod
```

It can also distribute traffic between the Pods.

---

# 6. Ingress

Ingress is commonly used to handle HTTP/HTTPS traffic.

```text
Internet
   |
   v
Ingress
   |
   v
Service
   |
   +---- Pod
   +---- Pod
   +---- Pod
```

For example:

```text
example.com
     |
     v
  Ingress
     |
     +---- /api --> API Service
     |
     +---- /    --> Web Service
```

---

# 7. Docker vs Kubernetes

| Docker         | Kubernetes                     |
| -------------- | ------------------------------ |
| Container      | Container inside a Pod         |
| `docker run`   | Deployment / Pod               |
| `docker ps`    | `kubectl get pods`             |
| `docker logs`  | `kubectl logs`                 |
| `docker exec`  | `kubectl exec`                 |
| Docker Compose | Kubernetes YAML / Helm         |
| Docker network | Kubernetes Services/networking |
| Docker Swarm   | Kubernetes                     |

The comparison isn't exact, but it is useful for understanding the concepts.

---

# 8. kubectl

`kubectl` is the main command-line tool for Kubernetes.

Think:

```text
Docker      → docker

Kubernetes  → kubectl
```

---

# 9. Most Useful Commands

## Check Nodes

```bash
kubectl get nodes
```

Example:

```text
NAME        STATUS   ROLES
server1     Ready    control-plane
```

---

## Check Pods

```bash
kubectl get pods
```

All namespaces:

```bash
kubectl get pods -A
```

---

## Check Deployments

```bash
kubectl get deployments
```

---

## Check Services

```bash
kubectl get services
```

---

## See Everything

```bash
kubectl get all
```

---

## Get More Pod Information

```bash
kubectl get pods -o wide
```

---

## Describe a Pod

Useful when something is broken:

```bash
kubectl describe pod POD_NAME
```

Example:

```bash
kubectl describe pod nginx-abc123
```

---

# 10. Logs

Docker:

```bash
docker logs CONTAINER
```

Kubernetes:

```bash
kubectl logs POD_NAME
```

Follow logs:

```bash
kubectl logs -f POD_NAME
```

`-f` means **follow**.

---

# 11. Enter a Container

Docker:

```bash
docker exec -it CONTAINER bash
```

Kubernetes:

```bash
kubectl exec -it POD_NAME -- bash
```

If Bash isn't installed:

```bash
kubectl exec -it POD_NAME -- sh
```

---

# 12. Apply a YAML File

Kubernetes applications are commonly described using YAML.

For example:

```yaml
apiVersion: apps/v1
kind: Deployment

metadata:
  name: nginx

spec:
  replicas: 3

  selector:
    matchLabels:
      app: nginx

  template:
    metadata:
      labels:
        app: nginx

    spec:
      containers:
        - name: nginx
          image: nginx:latest
```

Save it as:

```text
nginx.yaml
```

Then run:

```bash
kubectl apply -f nginx.yaml
```

Check the result:

```bash
kubectl get pods
```

You should see 3 Pods.

---

# 13. Scaling

Scale an application:

```bash
kubectl scale deployment nginx --replicas=5
```

Now Kubernetes tries to run:

```text
Pod
Pod
Pod
Pod
Pod
```

Scale back:

```bash
kubectl scale deployment nginx --replicas=2
```

---

# 14. Delete Resources

Delete a Deployment:

```bash
kubectl delete deployment nginx
```

Delete a Pod:

```bash
kubectl delete pod POD_NAME
```

Delete everything defined in a YAML file:

```bash
kubectl delete -f nginx.yaml
```

---

# 15. Namespaces

Namespaces separate resources inside a cluster.

For example:

```text
Cluster
│
├── development
├── staging
└── production
```

Create one:

```bash
kubectl create namespace development
```

View its Pods:

```bash
kubectl get pods -n development
```

---

# 16. Very Useful Command Cheat Sheet

```bash
# Nodes
kubectl get nodes

# Pods
kubectl get pods

# All Pods
kubectl get pods -A

# Deployments
kubectl get deployments

# Services
kubectl get services

# Everything
kubectl get all

# More Pod information
kubectl get pods -o wide

# Detailed information
kubectl describe pod POD_NAME

# Logs
kubectl logs POD_NAME

# Follow logs
kubectl logs -f POD_NAME

# Enter container
kubectl exec -it POD_NAME -- sh

# Create/update from YAML
kubectl apply -f file.yaml

# Delete from YAML
kubectl delete -f file.yaml

# Scale
kubectl scale deployment NAME --replicas=3
```

---

# 17. The Big Picture

Remember this diagram:

```text
                  INTERNET
                     |
                     v
                  Ingress
                     |
                     v
                  Service
                     |
          +----------+----------+
          |          |          |
          v          v          v
        Pod        Pod        Pod
         |          |          |
      Container  Container  Container
```

And:

```text
Deployment
     |
     +---- Pod
     +---- Pod
     +---- Pod
```

The Deployment makes sure the desired number of Pods exist.

The Service gives those Pods a stable way to communicate.

Ingress can expose HTTP/HTTPS applications to the outside world.

---

# 18. Learning Order

Since you already know Docker, learn Kubernetes in this order:

1. **Cluster**
2. **Node**
3. **Pod**
4. **Deployment**
5. **Service**
6. **Ingress**
7. **Namespaces**
8. **ConfigMaps**
9. **Secrets**
10. **Volumes**
11. **PersistentVolumes**
12. **Jobs / CronJobs**
13. **StatefulSets**
14. **Helm**
15. **RBAC**
16. **Monitoring**

Don't try to learn everything at once.

The most important concepts at the beginning are:

```text
Pod
 ↓
Deployment
 ↓
Service
 ↓
Ingress
```

---

# 19. Best First Experiment

After installing K3s, deploy Nginx.

Then practice:

```text
1. Create Deployment
        ↓
2. See Pods
        ↓
3. Create Service
        ↓
4. Access Nginx
        ↓
5. Scale to 3 Pods
        ↓
6. Delete one Pod
        ↓
7. Watch Kubernetes create it again
```

That last experiment is particularly important because it lets you **see Kubernetes' self-healing behavior in action**.

---

# 20. One Sentence to Remember

> **Docker runs containers; Kubernetes manages containers at scale and keeps the system in the state you declared.**

And:

> **K3s is a lightweight distribution of Kubernetes.**
