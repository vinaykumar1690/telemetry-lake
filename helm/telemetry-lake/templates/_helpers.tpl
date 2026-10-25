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
MinIO endpoint URL
*/}}
{{- define "telemetry-lake.minioEndpoint" -}}
http://minio:9000
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
