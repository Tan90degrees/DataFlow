{{- define "dataflow.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "dataflow.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "dataflow.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "dataflow.labels" -}}
app.kubernetes.io/name: {{ include "dataflow.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "dataflow.apiServiceAccountName" -}}
{{- if .Values.serviceAccount.apiName -}}{{ .Values.serviceAccount.apiName }}{{- else -}}{{ include "dataflow.fullname" . }}-api{{- end -}}
{{- end -}}

{{- define "dataflow.controllerServiceAccountName" -}}
{{- if .Values.serviceAccount.controllerName -}}{{ .Values.serviceAccount.controllerName }}{{- else -}}{{ include "dataflow.fullname" . }}-controller{{- end -}}
{{- end -}}

{{- define "dataflow.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) -}}
{{- end -}}
