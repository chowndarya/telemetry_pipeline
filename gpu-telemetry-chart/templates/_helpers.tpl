{{- define "gpu-telemetry.fullImage" -}}
{{ .Values.image.registry }}/{{ . }}:{{ $.Values.image.tag }}
{{- end }}