# EC2 build host: build images, push to ECR, deploy to EKS

A step-by-step setup for an Amazon Linux 2023 (x86_64) EC2 instance that clones this repository, builds the six images, pushes them to ECR and installs the chart on EKS with the `values-eks.yaml` profile (S3 instead of MinIO). [eks.md](eks.md) explains the profile itself; this page is the copy-paste path from a fresh instance.

> **Status:** the EKS profile **has not been run on AWS yet** (see [eks.md](eks.md#what-the-local-rehearsal-proved)). Treat the first install as the test run.

## 0. AWS setup (console or CloudShell, once)

**0a. Build host:** Amazon Linux 2023, x86_64, at least 16 GB RAM (`c6i.4xlarge` recommended; the query image compiles DataFusion) and a **100 GB gp3** root volume. If the volume was grown after launch:
```bash
sudo growpart /dev/nvme0n1 1 && sudo xfs_growfs -d /
df -h /
```

**0b. IAM role on the instance** (EC2 → Actions → Security → Modify IAM role):
- `AmazonEC2ContainerRegistryPowerUser`
- an inline policy allowing `eks:DescribeCluster` and `eks:ListClusters`

The one-time S3 and IAM user setup (step 9) needs admin rights: run it in **AWS CloudShell** rather than giving the build host those permissions.

**0c. Let the role into the cluster** (as a cluster admin):
```bash
aws eks create-access-entry --cluster-name <CLUSTER> \
  --principal-arn arn:aws:iam::<ACCOUNT>:role/<EC2_ROLE>
aws eks associate-access-policy --cluster-name <CLUSTER> \
  --principal-arn arn:aws:iam::<ACCOUNT>:role/<EC2_ROLE> \
  --policy-arn arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy \
  --access-scope type=cluster
```

**0d. The cluster needs:**
- **x86_64 managed nodes**, not Graviton or Fargate: the appender image is amd64-only, and the OTel Collector reads `/var/log/pods` from the host.
- **Capacity:** about **3× m6i.2xlarge**. The profile requests about 10 vCPU and 16 GiB with the load generator idle; 3× m6i.xlarge runs out once compaction or autoscaling starts.
- **Add-ons:** `aws-ebs-csi-driver`, `metrics-server`, `eks-pod-identity-agent`.
- **Node role:** `AmazonEC2ContainerRegistryReadOnly`, and `AmazonEBSCSIDriverPolicy` unless the EBS CSI add-on has its own Pod Identity role.
- **Outbound internet** (a NAT gateway on private subnets): Kafka, Nessie, Prometheus and the others come from Docker Hub and ghcr.io, and the Kafka JMX agent from Maven Central.

Without a cluster, step 7b creates one that meets all of these.

## 1. Settings (once, on the instance)

Edit the region and cluster name, then paste:
```bash
cat > ~/tl.env <<'EOF'
export AWS_REGION=us-west-2              # <-- your region
export CLUSTER=telemetry-lake            # <-- your EKS cluster name
export ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export REGISTRY=$ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com
export BUCKET=telemetry-lake-$ACCOUNT
export AWS_DEFAULT_REGION=$AWS_REGION
EOF
echo 'source ~/tl.env' >> ~/.bashrc
source ~/tl.env
echo "$ACCOUNT $REGISTRY $BUCKET"
```
An empty account ID means the role from 0b isn't attached.

## 2. Base packages

```bash
sudo dnf -y update
sudo dnf install -y git unzip jq make tar gzip openssl
aws --version    # AWS CLI v2 is preinstalled
```

## 3. Docker and buildx

AL2023's `docker` package doesn't include buildx, so the plugin is installed separately:
```bash
sudo dnf install -y docker
sudo systemctl enable --now docker
sudo usermod -aG docker $USER

BX=$(curl -fsSL https://api.github.com/repos/docker/buildx/releases/latest | jq -r .tag_name)
mkdir -p ~/.docker/cli-plugins
curl -fsSL -o ~/.docker/cli-plugins/docker-buildx \
  https://github.com/docker/buildx/releases/download/$BX/buildx-$BX.linux-amd64
chmod +x ~/.docker/cli-plugins/docker-buildx
```
**Log out and SSH back in** for the docker group, then:
```bash
docker run --rm hello-world && docker buildx version
```

## 4. Clone the repository

```bash
ssh-keygen -t ed25519 -C "ec2-build" -f ~/.ssh/id_ed25519 -N ""
cat ~/.ssh/id_ed25519.pub
```
Add the key on GitHub (repo → **Settings → Deploy keys → Add deploy key**, read-only), then:
```bash
ssh -o StrictHostKeyChecking=accept-new -T git@github.com
git clone git@github.com:vinaykumar1690/telemetry-lake.git ~/telemetry-lake
cd ~/telemetry-lake && git log --oneline -1
ls helm/telemetry-lake/values-eks.yaml
```

## 5. ECR repositories

MinIO isn't deployed on AWS, so its image isn't needed.
```bash
for img in ingester appender query ui compaction loadgen; do
  aws ecr describe-repositories --repository-names telemetry-lake/$img >/dev/null 2>&1 \
    || aws ecr create-repository --repository-name telemetry-lake/$img \
         --image-scanning-configuration scanOnPush=true >/dev/null
  echo "ok: telemetry-lake/$img"
done
```

## 6. Build and push

```bash
cat > ~/build-push.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
source ~/tl.env
cd ~/telemetry-lake
aws ecr get-login-password | docker login --username AWS --password-stdin $REGISTRY
TAG=$(git describe --tags --always --dirty)
for img in ingester appender query ui compaction loadgen; do
  echo "=== building $img:$TAG"
  docker buildx build --platform linux/amd64 \
    --build-arg TELEMETRY_LAKE_VERSION="$TAG" \
    -f docker/$img/Dockerfile \
    -t $REGISTRY/telemetry-lake/$img:$TAG \
    --push .
done
echo "$TAG" > ~/.tl-tag
echo "Pushed all images with tag $TAG"
EOF
chmod +x ~/build-push.sh
~/build-push.sh 2>&1 | tee ~/build.log
```
Every image gets the same tag, which is what `images.tag` expects. Don't push `latest`: with `pullPolicy: IfNotPresent`, nodes never pull a newer `latest`. The script logs in to ECR on each run, since the token expires after 12 hours.

Check:
```bash
aws ecr describe-images --repository-name telemetry-lake/appender --query 'imageDetails[].imageTags'
```

## 7. kubectl, Helm and the cluster

**7a. Install kubectl (matching the cluster's version) and Helm, and connect:**
```bash
KUBE_MINOR=$(aws eks describe-cluster --name $CLUSTER --query cluster.version --output text 2>/dev/null || echo 1.33)
curl -fsSLO "https://dl.k8s.io/release/$(curl -fsSL https://dl.k8s.io/release/stable-$KUBE_MINOR.txt)/bin/linux/amd64/kubectl"
sudo install -m 0755 kubectl /usr/local/bin/kubectl && rm kubectl

curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

aws eks update-kubeconfig --name $CLUSTER
kubectl get nodes -L kubernetes.io/arch,node.kubernetes.io/instance-type   # every node amd64
```

**7b. Only without a cluster:** create one with eksctl. This needs admin-level IAM (CloudShell, or temporary rights on the instance role).
```bash
curl -fsSL "https://github.com/eksctl-io/eksctl/releases/latest/download/eksctl_Linux_amd64.tar.gz" | tar xz -C /tmp
sudo install -m 0755 /tmp/eksctl /usr/local/bin/eksctl
cat > ~/eks.yaml <<EOF
apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig
metadata: { name: $CLUSTER, region: $AWS_REGION, version: "1.33" }
iam: { withOIDC: true }
addonsConfig: { autoApplyPodIdentityAssociations: true }
addons:
  - name: vpc-cni
  - name: coredns
  - name: kube-proxy
  - name: eks-pod-identity-agent
  - name: aws-ebs-csi-driver
  - name: metrics-server
managedNodeGroups:
  - name: default
    instanceType: m6i.2xlarge
    minSize: 3
    desiredCapacity: 3
    maxSize: 6
    volumeSize: 50
    privateNetworking: true
EOF
eksctl create cluster -f ~/eks.yaml
```

**7c. Existing cluster:** add any missing add-ons:
```bash
for a in aws-ebs-csi-driver metrics-server eks-pod-identity-agent; do
  aws eks describe-addon --cluster-name $CLUSTER --addon-name $a >/dev/null 2>&1 \
    || aws eks create-addon --cluster-name $CLUSTER --addon-name $a
done
kubectl get pods -n kube-system | grep -E 'ebs-csi|metrics-server'
```

## 8. gp3 StorageClass (once per cluster)

The profile sets `storageClass: gp3`, and EKS doesn't create that class:
```bash
kubectl apply -f - <<'EOF'
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: gp3
provisioner: ebs.csi.aws.com
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
parameters: {type: gp3}
EOF
kubectl get storageclass gp3
```

## 9. S3 bucket, IAM user and Secret (once)

**9a. In CloudShell** (admin rights), with the same variables:
```bash
export AWS_REGION=us-west-2
export ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export BUCKET=telemetry-lake-$ACCOUNT

aws s3api create-bucket --bucket $BUCKET --region $AWS_REGION \
  $( [ "$AWS_REGION" != us-east-1 ] && echo --create-bucket-configuration LocationConstraint=$AWS_REGION )
aws s3api put-public-access-block --bucket $BUCKET \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

aws iam create-user --user-name telemetry-lake
aws iam put-user-policy --user-name telemetry-lake --policy-name lake --policy-document "{
  \"Version\": \"2012-10-17\",
  \"Statement\": [
    {\"Effect\": \"Allow\", \"Action\": [\"s3:ListBucket\", \"s3:GetBucketLocation\"], \"Resource\": \"arn:aws:s3:::$BUCKET\"},
    {\"Effect\": \"Allow\", \"Action\": [\"s3:GetObject\", \"s3:PutObject\", \"s3:DeleteObject\", \"s3:AbortMultipartUpload\"], \"Resource\": \"arn:aws:s3:::$BUCKET/*\"}
  ]}"
aws iam create-access-key --user-name telemetry-lake
```
Copy `AccessKeyId` and `SecretAccessKey` from the output; the secret is shown only once.

**9b. On the instance:** the namespace and the Secret. The profile sets `createNamespace: false`, and expects a Secret named `telemetry-lake-s3` with the keys `access-key` and `secret-key`:
```bash
kubectl create namespace telemetry-lake
read -p  "AccessKeyId: " AK
read -sp "SecretAccessKey: " SK; echo
kubectl create secret generic telemetry-lake-s3 -n telemetry-lake \
  --from-literal=access-key="$AK" --from-literal=secret-key="$SK"
unset AK SK
kubectl get secret telemetry-lake-s3 -n telemetry-lake
```

**9c. Grafana admin password**, generated once so upgrades don't change it:
```bash
[ -f ~/.tl-grafana ] || (openssl rand -hex 16 > ~/.tl-grafana && chmod 600 ~/.tl-grafana)
echo "Grafana admin password: $(cat ~/.tl-grafana)"
```

## 10. Deploy

```bash
cat > ~/deploy.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
source ~/tl.env
TAG=$(cat ~/.tl-tag)
CHART=~/telemetry-lake/helm/telemetry-lake
helm upgrade --install telemetry-lake $CHART \
  -f $CHART/values-eks.yaml \
  --set images.registry=$REGISTRY \
  --set images.tag=$TAG \
  --set s3.bucket=$BUCKET \
  --set s3.region=$AWS_REGION \
  --set s3.endpoint=https://s3.$AWS_REGION.amazonaws.com \
  --set observability.grafana.adminPassword=$(cat ~/.tl-grafana)
echo "Deployed tag $TAG"
EOF
chmod +x ~/deploy.sh
~/deploy.sh
```
The chart refuses to render, with a clear message, when the registry, bucket, endpoint, S3 Secret or Grafana password is missing, or when a memory budget doesn't fit its container.

## 11. Check

```bash
kubectl get pods,pvc -n telemetry-lake -w   # PVCs Bound (gp3), pods Ready
kubectl get hpa -n telemetry-lake           # targets show %, not <unknown>
```

After sending some logs (step 12), data files appear in the bucket (from CloudShell, or with `s3:ListBucket` on the instance role):
```bash
aws s3 ls s3://$BUCKET/ --recursive | head
```

Nessie keeps the catalog on RocksDB, so it survives a restart:
```bash
kubectl delete pod -n telemetry-lake -l app=nessie
kubectl get pods -n telemetry-lake -l app=nessie -w
```
Once it is back, searches in the UI should return the same data.

## 12. UI and Grafana

On the instance:
```bash
kubectl port-forward -n telemetry-lake svc/ui 8080:80 &
kubectl port-forward -n telemetry-lake svc/grafana 3000:3000 &
```
From your laptop:
```bash
ssh -L 8080:localhost:8080 -L 3000:localhost:3000 ec2-user@<EC2_PUBLIC_IP>
```
- UI: http://localhost:8080. Send logs from the Generate tab or run a short load test, then search.
- Grafana: http://localhost:3000, user `admin`, password from `~/.tl-grafana`. Status → Targets should all be up.

Keep the UI behind port-forward or an authenticated ingress: it has no login, and it can start load tests.

**Agents outside the cluster** send to an internal NLB; add to `~/deploy.sh`:
```bash
  --set ingester.service.type=LoadBalancer \
  --set-string 'ingester.service.annotations.service\.beta\.kubernetes\.io/aws-load-balancer-scheme=internal'
```

**Load tests:** scale the workers for the run and back to 0 afterwards (idle workers hold their CPU requests):
```bash
helm upgrade telemetry-lake ~/telemetry-lake/helm/telemetry-lake --reuse-values --set loadgen.workers.replicas=4
```

## After each code change

```bash
cd ~/telemetry-lake && git pull
~/build-push.sh && ~/deploy.sh
```

## Optional: native toolchains

The image builds compile inside their own builder stages; these are only for building and testing on the host.

**Rust** (1.95 or newer; `aws-lc-sys` needs cmake and clang):
```bash
sudo dnf install -y gcc clang cmake
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y && source ~/.cargo/env
cd ~/telemetry-lake/query && cargo test --workspace
cd ~/telemetry-lake/loadgen && cargo test
```

**Node 22** (UI):
```bash
curl -fsSL https://rpm.nodesource.com/setup_22.x | sudo bash - && sudo dnf install -y nodejs
cd ~/telemetry-lake/ui && npm ci && npm test
```

**C++:** AL2023's GCC 11 can't build the C++23 code, and it doesn't package librdkafka, asio or gmock, so build in an Ubuntu 24.04 container (the same base as `docker/appender/Dockerfile`):
```bash
docker run --rm -it -v ~/telemetry-lake:/src -w /src ubuntu:24.04 bash -c '
apt-get update && apt-get install -y build-essential cmake ninja-build git pkg-config curl unzip \
  librdkafka-dev libboost-all-dev libasio-dev zlib1g-dev libssl-dev libgtest-dev libgmock-dev &&
curl -fsSL https://github.com/CrowCpp/Crow/releases/download/v1.3.0/Crow-1.3.0-Linux.tar.gz | tar xz -C /tmp &&
cp -r /tmp/Crow-1.3.0-Linux/include/* /usr/local/include/ && mkdir -p /usr/local/lib/cmake &&
cp -r /tmp/Crow-1.3.0-Linux/lib/cmake/* /usr/local/lib/cmake/ &&
curl -fsSL -o /tmp/d.zip https://github.com/duckdb/duckdb/releases/download/v1.5.6/libduckdb-linux-amd64.zip &&
unzip -q /tmp/d.zip -d /tmp/d && cp /tmp/d/duckdb.h* /usr/local/include/ && cp /tmp/d/libduckdb.so /usr/local/lib/ && ldconfig &&
cmake -B /tmp/build -GNinja -DCMAKE_POLICY_VERSION_MINIMUM=3.5 -DDUCKDB_ROOT=/usr/local &&
ninja -C /tmp/build && cd /tmp/build && ctest --output-on-failure'
```

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `permission denied ... docker.sock` | Log out and back in after step 3 |
| Build exits 137 / `Killed` | Out of memory: bigger instance, or swap: `sudo dd if=/dev/zero of=/swapfile bs=1M count=16384 && sudo chmod 600 /swapfile && sudo mkswap /swapfile && sudo swapon /swapfile` |
| `no space left on device` | Grow the disk (0a) or `docker builder prune -af` |
| `kubectl`: "You must be logged in" | The access entry from 0c is missing |
| `helm`: "images.registry is required" (or bucket, endpoint, password) | An empty `--set` value: check `source ~/tl.env` and `~/.tl-tag` |
| `helm`: secret not found | Step 9b missing, or the Secret is in another namespace |
| `helm`: `namespaces "telemetry-lake" not found` | `kubectl create namespace telemetry-lake`; the profile doesn't create it |
| PVCs `Pending` | The `gp3` StorageClass (step 8), or the EBS CSI driver or its IAM permissions |
| `ImagePullBackOff` on `telemetry-lake/*` | Wrong tag, or the node role lacks ECR read access |
| `ImagePullBackOff` on kafka, nessie, grafana | No internet egress from the nodes (NAT gateway) |
| Kafka init container fails | It can't reach Maven Central for the JMX agent (NAT) |
| S3 `403` in appender, query or Nessie logs | Wrong keys in the Secret, the policy doesn't match the bucket, or `s3.region` is wrong |
| HPA shows `<unknown>` | metrics-server isn't installed (7c) |
| Pods `Pending`: "Insufficient cpu/memory" | Nodes too small: 3× m6i.2xlarge |
| Pods crash with `exec format error` | arm64 nodes: use an amd64 node group |
