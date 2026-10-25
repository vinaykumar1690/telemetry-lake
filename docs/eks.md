# Deploying to AWS EKS

The chart has two profiles:
- `helm/telemetry-lake/values-local.yaml` for a laptop: MinIO, in-memory Nessie, small memory budgets.
- `helm/telemetry-lake/values-eks.yaml` for AWS: S3, gp3 volumes, Nessie on RocksDB, larger budgets.

This page sets up EKS for the second.

> **Status:** the EKS profile has been rendered and linted, and its pieces were exercised on kind: the shared S3 settings, RocksDB Nessie and the budget guards. It has **not been run on AWS yet.**

## 0. Prerequisites

You need `aws` (logged in), `eksctl`, `kubectl`, `helm` and Docker with buildx.

```bash
export AWS_REGION=us-west-2
export ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
export REGISTRY=$ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com
export TAG=$(git describe --tags --always --dirty)
export BUCKET=telemetry-lake-$ACCOUNT
```

## 1. Cluster

The appender image is built for amd64, so use x86 nodes (not Graviton). Don't use Fargate either, because the OTel Collector reads `/var/log/pods` from the host. The EKS profile's budgets fit three nodes with 4 vCPU and 16 GiB each.

`eks.yaml`:
```yaml
apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig
metadata:
  name: telemetry-lake
  region: us-west-2
  version: "1.33"            # any currently supported version
iam:
  withOIDC: true
addonsConfig:
  autoApplyPodIdentityAssociations: true
addons:
  - name: vpc-cni
  - name: coredns
  - name: kube-proxy
  - name: eks-pod-identity-agent
  - name: aws-ebs-csi-driver   # Kafka, Nessie, Loki and Prometheus volumes
  - name: metrics-server       # the query and worker autoscalers
managedNodeGroups:
  - name: default
    instanceType: m6i.xlarge
    minSize: 3
    desiredCapacity: 3
    maxSize: 6
    volumeSize: 50
    privateNetworking: true    # nodes need NAT egress (images, Maven Central)
```

```bash
eksctl create cluster -f eks.yaml
```

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
```

## 2. Bucket and keys

The services authenticate to S3 with static keys from a Secret that you create; the chart doesn't create it for AWS. Use an IAM user limited to the bucket:

```bash
aws s3api create-bucket --bucket $BUCKET --region $AWS_REGION \
  --create-bucket-configuration LocationConstraint=$AWS_REGION
aws iam create-user --user-name telemetry-lake
aws iam put-user-policy --user-name telemetry-lake --policy-name lake --policy-document "{
  \"Version\": \"2012-10-17\",
  \"Statement\": [
    {\"Effect\": \"Allow\", \"Action\": [\"s3:ListBucket\", \"s3:GetBucketLocation\"], \"Resource\": \"arn:aws:s3:::$BUCKET\"},
    {\"Effect\": \"Allow\", \"Action\": [\"s3:GetObject\", \"s3:PutObject\", \"s3:DeleteObject\", \"s3:AbortMultipartUpload\"], \"Resource\": \"arn:aws:s3:::$BUCKET/*\"}
  ]}"
aws iam create-access-key --user-name telemetry-lake   # note AccessKeyId and SecretAccessKey

kubectl create namespace telemetry-lake
kubectl create secret generic telemetry-lake-s3 -n telemetry-lake \
  --from-literal=access-key=<AccessKeyId> --from-literal=secret-key=<SecretAccessKey>
```

Every S3 consumer reads these keys through `secretKeyRef`: Nessie, the appender, query, the query workers, compaction and nessie-gc. External Secrets can sync the Secret from AWS Secrets Manager instead of `kubectl create`. The chart only needs a Secret named `s3.existingSecret` with the keys `access-key` and `secret-key`.

The chart creates the `telemetry-lake` namespace itself. If you created it above for the Secret, let Helm adopt it:
```bash
kubectl label namespace telemetry-lake app.kubernetes.io/managed-by=Helm
kubectl annotate namespace telemetry-lake meta.helm.sh/release-name=telemetry-lake meta.helm.sh/release-namespace=default
```

## 3. Images in ECR

Use a unique tag rather than `latest`: with `pullPolicy: IfNotPresent`, nodes never pull a newer `latest`.

```bash
aws ecr get-login-password | docker login --username AWS --password-stdin $REGISTRY
for c in ingester appender query ui compaction loadgen; do
  aws ecr describe-repositories --repository-names telemetry-lake/$c >/dev/null 2>&1 ||
    aws ecr create-repository --repository-name telemetry-lake/$c >/dev/null
  docker buildx build --platform linux/amd64 --build-arg TELEMETRY_LAKE_VERSION=$TAG \
    -f docker/$c/Dockerfile -t $REGISTRY/telemetry-lake/$c:$TAG --push .
done
```

MinIO isn't deployed on AWS, so its image isn't needed.

## 4. Install

```bash
images=""
for c in ingester appender query ui compaction loadgen; do
  images="$images --set $c.image.repository=$REGISTRY/telemetry-lake/$c --set $c.image.tag=$TAG"
done
helm install telemetry-lake helm/telemetry-lake -f helm/telemetry-lake/values-eks.yaml \
  --set s3.bucket=$BUCKET --set s3.region=$AWS_REGION \
  --set s3.endpoint=https://s3.$AWS_REGION.amazonaws.com \
  --set observability.grafana.adminPassword=$(openssl rand -hex 16) $images
```

The chart refuses to render with a clear message in these cases:
- `s3.bucket`, `s3.endpoint`, the S3 Secret or the Grafana password is missing;
- a memory budget doesn't fit its container: the appender's DuckDB limit plus 512 MiB must fit its pod, and the query service's and workers' DataFusion limits must fit theirs.

## 5. Check

```bash
kubectl get pods,pvc -n telemetry-lake     # PVCs Bound (gp3), pods Ready
kubectl get hpa -n telemetry-lake          # targets show %, not <unknown>
kubectl port-forward -n telemetry-lake svc/ui 8080:80          # Load test tab: a short run
kubectl port-forward -n telemetry-lake svc/grafana 3000:3000   # Status → Targets all up
```

Nessie keeps its catalog on the `nessie-data` volume. After `kubectl delete pod -l app=nessie`, the logs table should still answer queries once the pod is back.

## Exposure

The UI, Grafana and the ingester are ClusterIP Services.
- **Agents outside the cluster** should send to an **internal** load balancer: `--set ingester.service.type=LoadBalancer`, then annotate the Service with `service.beta.kubernetes.io/aws-load-balancer-scheme: internal`.
- **The UI and Grafana** stay behind port-forward or an authenticated ingress. The UI has no authentication of its own, and it can start load tests.

## Still open

- **Kafka** is one broker with replication factor 1, on one EBS volume in one availability zone. For durability, move to Amazon MSK. Point `ingester.kafka.brokers` and `appender.kafka.brokers` at it, and `kafka.enabled=false` drops the in-chart broker and its exporter.
- **Nessie** runs as one replica on RocksDB. For HA, Nessie supports a JDBC store (RDS PostgreSQL).
- **Credentials** are static keys. IRSA / Pod Identity would need code changes: the appender's DuckDB S3 secret, opendal in the query service, Spark in compaction, and Nessie's auth type.
- **Alertmanager** isn't deployed, so alert rules only show in Prometheus.
