# 🖥️ Module 01: Creating the Ubuntu Server 24.04 LTS "VPS" on Mac M4

## 🎯 What You Will Learn in this Module
1. How hypervisors work on Apple Silicon (`Hypervisor.framework`).
2. Why we choose **Ubuntu Server 24.04 LTS ARM64** over standard containers.
3. How to install and configure **Multipass** (Canonical's native Ubuntu manager for macOS).
4. How to set up SSH keys so you can log into your server without passwords.
5. How networking works between macOS and the Ubuntu VM, and how to expose ports to your local Wi-Fi.

---

## 🧠 Architectural Understanding: VM vs. Docker on Mac

When you run Docker on a Mac, Docker doesn't run natively on macOS kernel (macOS is BSD/Darwin-based, not Linux). Docker on Mac secretly runs a small hidden Linux VM behind the scenes!

Instead of letting Docker hide that VM, we are going to create our **own dedicated Ubuntu Server 24.04 LTS VM**. 
* You will have **root** access (`sudo`).
* You will manage Linux systemd services, disk partitions, and firewall rules.
* Inside this Ubuntu server, you will run Docker and Kubernetes (k3s).
* This gives you 1:1 identical experience to having a real VPS in AWS (EC2), GCP, or Hetzner!

---

## 🛠️ Step 1: Install Multipass on macOS

We use **Multipass**, developed by Canonical (the creators of Ubuntu). It uses Apple's native hardware hypervisor and consumes virtually zero CPU when idle.

Open your **macOS Terminal** and run:

```bash
# Install Multipass using Homebrew
brew install --cask multipass
```

Verify the installation:
```bash
multipass version
```

---

## 🚀 Step 2: Launch the Ubuntu 24.04 LTS Instance

Now we will launch an Ubuntu 24.04 LTS instance named `dataplat-server`.

### Hardware Allocation Calculation:
* `--cpus 6`: Gives 6 virtual CPU cores to Linux (M4 handles this effortlessly with asymmetric efficiency cores).
* `--memory 11G`: Allocates 11 GB of RAM to the VM, keeping 5 GB for macOS.
* `--disk 80G`: Reserves up to 80 GB of dynamic storage on your SSD (it only uses what is written).

Run this command in your Mac terminal:

```bash
multipass launch 24.04 \
  --name dataplat-server \
  --cpus 6 \
  --memory 11G \
  --disk 80G
```

> ⏱️ *This will download the official Ubuntu 24.04 ARM64 cloud image (~600MB) and boot it up in ~20-30 seconds.*

---

## 🔍 Step 3: Verify the Running VM

Check the status of your new Linux server:

```bash
multipass list
```

You will see an output like:
```text
Name                    State             IPv4             Image
dataplat-server         Running           192.168.64.2     Ubuntu 24.04 LTS
```
*Note down the IPv4 address! (e.g., `192.168.64.2`). This is the VM's internal IP.*

Open an interactive shell inside your Ubuntu server:
```bash
multipass shell dataplat-server
```
You are now inside **Ubuntu Server 24.04 LTS**! Run:
```bash
uname -a
lsb_release -a
free -h
```
You will see `aarch64` (ARM64) and ~11 GB of available memory.

Type `exit` to return to your Mac terminal.

---

## 🔑 Step 4: Configure Seamless SSH Access from Mac

Rather than typing `multipass shell`, professional engineers use SSH with keys.

1. On your **Mac**, generate an SSH key if you don't already have one:
   ```bash
   # Check if id_ed25519 exists, if not generate it:
   [ -f ~/.ssh/id_ed25519.pub ] || ssh-keygen -t ed25519 -N "" -f ~/.ssh/id_ed25519
   ```

2. Copy your Mac's public key into the Ubuntu server:
   ```bash
   PUBKEY=$(cat ~/.ssh/id_ed25519.pub)
   multipass exec dataplat-server -- bash -c "mkdir -p ~/.ssh && echo '$PUBKEY' >> ~/.ssh/authorized_keys && chmod 700 ~/.ssh && chmod 600 ~/.ssh/authorized_keys"
   ```

3. Add an entry to your Mac's `~/.ssh/config`:
   Get your VM IP first:
   ```bash
   VM_IP=$(multipass info dataplat-server | grep IPv4 | awk '{print $2}')
   echo "Your VM IP is: $VM_IP"
   ```

   Add this block to your Mac's `~/.ssh/config`:
   ```bash
   cat << EOF >> ~/.ssh/config

   Host dataplat
       HostName $VM_IP
       User ubuntu
       IdentityFile ~/.ssh/id_ed25519
       StrictHostKeyChecking no
   EOF
   ```

4. Now test logging in directly:
   ```bash
   ssh dataplat
   ```
   You should instantly connect without a password!

---

## 🌐 Step 5: Network Bridge & LAN Access (Wi-Fi Sharing)

You want your phone, tablet, and other computers on Wi-Fi (e.g. `192.168.3.x`) to access the Web UIs running inside this VM.

There are two clean ways to do this:

### Option A: Reverse Port Forwarding via SSH / Socat (Simplest & Most Reliable)
Your Mac's Wi-Fi IP is `192.168.3.30`. We can map any inbound request on your Mac to the VM IP using a simple background port-forwarder or reverse proxy (`caddy` or `socat` or SSH tunnel).

Example: Exposing ports 8080, 8082, 8443, 9001, etc.
```bash
# On Mac: Forward Mac port 9001 (MinIO) to VM port 9001
ssh -N -f -L 0.0.0.0:9001:localhost:9001 dataplat
```

### Option B: Multipass Bridged Network (Direct Wi-Fi IP)
Multipass also supports direct bridging:
```bash
# List available network interfaces on Mac
multipass networks
```
*If bridged to `en0` (Wi-Fi), the Ubuntu VM gets its own IP directly from your home router (e.g., `192.168.3.45`).* We will configure this in Step 2 with our Kubernetes ingress controller so everything is automated!

---

## ✅ Checkpoint 1 Checklist
Before moving to Module 02, make sure:
- [ ] `multipass list` shows `dataplat-server` as **Running**.
- [ ] `ssh dataplat` logs you into Ubuntu 24.04 with zero prompts.
- [ ] `free -h` inside Ubuntu shows ~11 GB RAM.

➡️ **Next Step**: Proceed to `02-docker-and-k3s-foundations.md` to install Docker and our lightweight Kubernetes cluster (k3s).
