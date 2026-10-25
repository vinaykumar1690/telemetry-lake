{{/*
Expand the name of the chart.
*/}}
{{- define "telemetry-lake.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "telemetry-lake.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "telemetry-lake.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "telemetry-lake.labels" -}}
helm.sh/chart: {{ include "telemetry-lake.chart" . }}
{{ include "telemetry-lake.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "telemetry-lake.selectorLabels" -}}
app.kubernetes.io/name: {{ include "telemetry-lake.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
S3 endpoint: the in-chart MinIO, or s3.endpoint (required without MinIO)
*/}}
{{- define "telemetry-lake.s3Endpoint" -}}
{{- if .Values.minio.enabled -}}
http://minio:9000
{{- else -}}
{{- required "s3.endpoint is required when minio.enabled is false (e.g. https://s3.us-west-2.amazonaws.com)" .Values.s3.endpoint -}}
{{- end -}}
{{- end }}

{{/*
S3 addressing: path for MinIO, virtual-hosted otherwise, unless s3.urlStyle says
*/}}
{{- define "telemetry-lake.s3UrlStyle" -}}
{{- .Values.s3.urlStyle | default (ternary "path" "vhost" .Values.minio.enabled) -}}
{{- end }}

{{/*
Secret holding the S3 keys (access-key, secret-key): s3.existingSecret, or the
chart's s3-credentials (MinIO's root keys, or s3.accessKey/s3.secretKey)
*/}}
{{- define "telemetry-lake.s3Secret" -}}
{{- if .Values.s3.existingSecret -}}
{{- .Values.s3.existingSecret -}}
{{- else if or .Values.minio.enabled (and .Values.s3.accessKey .Values.s3.secretKey) -}}
s3-credentials
{{- else -}}
{{- fail "S3 credentials: set s3.existingSecret to a Secret with access-key and secret-key, or pass s3.accessKey and s3.secretKey" -}}
{{- end -}}
{{- end }}

{{/*
S3 settings for every service that reads or writes the bucket
*/}}
{{- define "telemetry-lake.s3Env" -}}
- name: S3_ENDPOINT
  value: {{ include "telemetry-lake.s3Endpoint" . | quote }}
- name: S3_BUCKET
  value: {{ required "s3.bucket is required" .Values.s3.bucket | quote }}
- name: S3_REGION
  value: {{ .Values.s3.region | quote }}
- name: S3_URL_STYLE
  value: {{ include "telemetry-lake.s3UrlStyle" . | quote }}
- name: S3_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "telemetry-lake.s3Secret" . }}
      key: access-key
- name: S3_SECRET_KEY
  valueFrom:
    secretKeyRef:
      name: {{ include "telemetry-lake.s3Secret" . }}
      key: secret-key
{{- end }}

{{/*
storageClassName line for a PVC spec, or nothing (the cluster's default class).
Usage: {{- include "telemetry-lake.storageClass" (dict "root" . "indent" 8) }}
*/}}
{{- define "telemetry-lake.storageClass" -}}
{{- with .root.Values.storageClass }}
{{ printf "storageClassName: %q" . | indent $.indent }}
{{- end }}
{{- end }}

{{/*
Nessie endpoint URL
*/}}
{{- define "telemetry-lake.nessieEndpoint" -}}
http://nessie:19120/iceberg/
{{- end }}

{{/*
Kafka bootstrap servers
*/}}
{{- define "telemetry-lake.kafkaBootstrap" -}}
kafka:9092
{{- end }}

{{/*
Environment shared by the load generator's controller and workers
*/}}
{{- define "telemetry-lake.loadgenEnv" -}}
{{- $lg := .Values.loadgen -}}
- name: HTTP_PORT
  value: {{ $lg.port | quote }}
- name: INGEST_URL
  value: {{ $lg.ingestUrl | default (printf "http://ingester.%s.svc.cluster.local:%v" .Values.namespace .Values.ingester.service.port) | quote }}
- name: MAX_RECORDS_PER_SECOND
  value: {{ $lg.maxRecordsPerSecond | int64 | quote }}
- name: MAX_IN_FLIGHT
  value: {{ $lg.maxInFlight | quote }}
- name: MAX_REQUEST_BYTES
  value: {{ $lg.maxRequestBytes | default .Values.ingester.limits.maxDecompressedBytes | int64 | quote }}
- name: REQUEST_TIMEOUT_SECONDS
  value: {{ $lg.requestTimeoutSeconds | quote }}
{{- end }}

{{/*
Pod annotations that make the chart's Prometheus scrape a pod.
Usage: include "telemetry-lake.scrapeAnnotations" (dict "root" . "port" 9464)
*/}}
{{- define "telemetry-lake.scrapeAnnotations" -}}
{{- if .root.Values.observability.enabled }}
prometheus.io/scrape: "true"
prometheus.io/port: {{ .port | quote }}
prometheus.io/path: {{ .path | default "/metrics" | quote }}
{{- end }}
{{- end }}

{{/*
A memory quantity in MiB ("1536Mi", "4Gi", "1.5Gi")
*/}}
{{- define "telemetry-lake.mib" -}}
{{- $q := toString . -}}
{{- if hasSuffix "Gi" $q -}}
{{- mulf (trimSuffix "Gi" $q | float64) 1024 | int -}}
{{- else if hasSuffix "Mi" $q -}}
{{- trimSuffix "Mi" $q | float64 | int -}}
{{- else -}}
{{- fail (printf "memory limit %q: use Mi or Gi" $q) -}}
{{- end -}}
{{- end }}

{{/*
Memory budgets that fit their containers; renders nothing. The appender
needs about 512 MiB beyond DuckDB's limit while it catches up a backlog
(measured in the load tests), and DuckDB must hold MAX_STAGED_MB.
*/}}
{{- define "telemetry-lake.checkBudgets" -}}
{{- $a := .Values.appender -}}
{{- if $a.enabled -}}
{{- $limit := include "telemetry-lake.mib" $a.resources.limits.memory | int -}}
{{- $duck := int $a.duckdb.memoryLimitMB -}}
{{- if gt (add $duck 512) $limit -}}
{{- fail (printf "appender: duckdb.memoryLimitMB (%d) + 512 MiB exceeds resources.limits.memory (%d MiB)" $duck $limit) -}}
{{- end -}}
{{- if lt $duck (int $a.buffer.maxStagedMB) -}}
{{- fail (printf "appender: duckdb.memoryLimitMB (%d) is below buffer.maxStagedMB (%d)" $duck (int $a.buffer.maxStagedMB)) -}}
{{- end -}}
{{- end -}}
{{- $q := .Values.query -}}
{{- if $q.enabled -}}
{{- $qlimit := include "telemetry-lake.mib" $q.resources.limits.memory | int -}}
{{- if gt (int $q.memoryLimitMB) $qlimit -}}
{{- fail (printf "query: memoryLimitMB (%d) exceeds resources.limits.memory (%d MiB)" (int $q.memoryLimitMB) $qlimit) -}}
{{- end -}}
{{- if $q.workers.enabled -}}
{{- $wlimit := include "telemetry-lake.mib" $q.workers.resources.limits.memory | int -}}
{{- if gt (int $q.workers.memoryLimitMB) $wlimit -}}
{{- fail (printf "query.workers: memoryLimitMB (%d) exceeds resources.limits.memory (%d MiB)" (int $q.workers.memoryLimitMB) $wlimit) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
One of this repository's images: <images.registry>/<repository>:<tag> when a
registry is set (ECR on AWS), else <repository>:<tag> as built locally. The tag
is images.tag when set, else the component's.
Usage: include "telemetry-lake.image" (dict "root" . "image" .Values.appender.image)
*/}}
{{- define "telemetry-lake.image" -}}
{{- $root := .root -}}
{{- $tag := $root.Values.images.tag | default .image.tag -}}
{{- if $root.Values.images.registry -}}
{{- printf "%s/%s:%s" (trimSuffix "/" $root.Values.images.registry) .image.repository $tag -}}
{{- else if not $root.Values.minio.enabled -}}
{{- fail "images.registry is required outside a local deployment: push the images to ECR and set images.registry (docs/eks.md)" -}}
{{- else -}}
{{- printf "%s:%s" .image.repository $tag -}}
{{- end -}}
{{- end }}
