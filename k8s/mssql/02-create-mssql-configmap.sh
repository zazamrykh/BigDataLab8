#!/bin/bash

# Создаем ConfigMap из SQL скриптов
kubectl create configmap mssql-init-scripts \
  --namespace food-clustering \
  --from-file=../../scripts/mssql/01_create_database.sql \
  --from-file=../../scripts/mssql/02_create_tables.sql \
  --from-file=../../scripts/mssql/03_insert_test_data.sql \
  --dry-run=client -o yaml > ./02-mssql-init-configmap.yaml

echo "ConfigMap manifest created: 02-mssql-init-configmap.yaml"
