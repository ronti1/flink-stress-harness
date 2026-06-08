{{/* Common naming + label helpers. */}}

{{- define "harness.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "harness.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "harness.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "harness.labels" -}}
app.kubernetes.io/name: {{ include "harness.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "harness.prometheus.fullname" -}}
{{- printf "%s-prometheus" (include "harness.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "harness.grafana.fullname" -}}
{{- printf "%s-grafana" (include "harness.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
