#!/bin/bash

# Переходим в директорию скрипта
cd "$(dirname "$0")"

# Применяем манифест сервиса-алиаса
echo "Applying MSSQL alias service..."
kubectl apply -f ./mssql-alias-service.yaml

# Проверяем, что сервис создан
echo "Checking service status..."
kubectl get service mssql -n food-clustering

echo "Done! Now PySpark should be able to connect to MS SQL Server using the name 'mssql'."
echo "Try running the clustering again with:"
echo "curl -X 'POST' 'http://localhost:8000/cluster' -H 'accept: application/json' -H 'Content-Type: application/json' -d '{\"use_sample_data\": true, \"use_datamart\": false}'"
