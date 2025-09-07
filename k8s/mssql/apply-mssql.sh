#!/bin/bash

# Переходим в директорию k8s
cd "$(dirname "$0")"

# Создаем ConfigMap для SQL скриптов
echo "Creating ConfigMap for SQL scripts..."
./02-create-mssql-configmap.sh

# Применяем манифесты в правильном порядке
echo "Applying Kubernetes manifests..."
kubectl apply -f ../00-namespace.yaml
kubectl apply -f ./01-mssql-pvc.yaml
kubectl apply -f ./02-env-configmap.yaml
kubectl apply -f ./02-db-secret.yaml
kubectl apply -f ./02-mssql-init-configmap.yaml
kubectl apply -f ./03-mssql-deployment.yaml

# Ждем, пока MS SQL Server запустится
echo "Waiting for MS SQL Server to start..."
kubectl wait --namespace food-clustering --for=condition=ready pod --selector=app=mssql --timeout=120s

# Применяем Job для инициализации базы данных
echo "Applying initialization job..."
kubectl apply -f ./04-mssql-init-job.yaml

echo "Done! Check the status of the pods with:"
echo "kubectl get pods -n food-clustering"
