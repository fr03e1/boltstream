{{- define "clickhouse.name" -}}
boltstream-producer
{{- end -}}
{{- define "clickhouse.fullname" -}}
{{ include "clickhouse.name" . }}
{{- end -}}