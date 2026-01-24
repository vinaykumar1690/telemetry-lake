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
