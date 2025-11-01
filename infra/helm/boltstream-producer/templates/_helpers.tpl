{{- define "boltstream-producer.name" -}}
boltstream-producer
{{- end -}}
{{- define "boltstream-producer.fullname" -}}
{{ include "boltstream-producer.name" . }}
{{- end -}}