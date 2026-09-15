{{- define "ouf-ingestion.name" -}}{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}{{- end -}}
