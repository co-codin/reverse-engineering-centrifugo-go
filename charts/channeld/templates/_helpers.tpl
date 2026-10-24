{{- define "channeld.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "channeld.fullname" -}}
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

{{- define "channeld.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "channeld.labels" -}}
helm.sh/chart: {{ include "channeld.chart" . }}
{{ include "channeld.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "channeld.selectorLabels" -}}
app.kubernetes.io/name: {{ include "channeld.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "channeld.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "channeld.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "channeld.configJSON" -}}
{
  "port": {{ .Values.config.port }},
  "host": {{ .Values.config.host | quote }},
  "node_name": {{ .Values.config.nodeName | quote }},
  "http_api": {
    "key": {{ .Values.config.httpApi.key | quote }},
    "insecure": {{ .Values.config.httpApi.insecure }}
  },
  "client": {
    "token_hmac_secret": {{ .Values.config.client.tokenHmacSecret | quote }},
    "anonymous": {{ .Values.config.client.anonymous }},
    "ping_interval": {{ .Values.config.client.pingInterval | quote }}
  },
  "channel": {
    "history_size": {{ .Values.config.channel.historySize }},
    "history_ttl": {{ .Values.config.channel.historyTtl | quote }},
    "presence": {{ .Values.config.channel.presence }},
    "join_leave": {{ .Values.config.channel.joinLeave }},
    "force_recovery": {{ .Values.config.channel.forceRecovery }},
    "force_positioning": {{ .Values.config.channel.forcePositioning }},
    "allow_subscribe_for_client": {{ .Values.config.channel.allowSubscribeForClient }},
    "allow_publish_for_subscriber": {{ .Values.config.channel.allowPublishForSubscriber }},
    "namespaces": [
      {{- range $i, $ns := .Values.config.channel.namespaces }}
      {{- if $i }},{{ end }}
      {
        "name": {{ $ns.name | quote }}
        {{- if hasKey $ns "allowPublishForSubscriber" }},
        "allow_publish_for_subscriber": {{ $ns.allowPublishForSubscriber }}
        {{- end }}
        {{- if hasKey $ns "allowPublishForClient" }},
        "allow_publish_for_client": {{ $ns.allowPublishForClient }}
        {{- end }}
      }
      {{- end }}
    ]
  }
}
{{- end }}
