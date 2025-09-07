#!/bin/bash

# Переходим в директорию скрипта
cd "$(dirname "$0")"

# Применяем манифесты в правильном порядке
echo "Applying Datamart Kubernetes manifests..."
kubectl apply -f ./01-datamart-pvc.yaml
kubectl apply -f ./02-datamart-deployment.yaml

# Ждем, пока Datamart запустится
echo "Waiting for Datamart to start..."
kubectl wait --namespace food-clustering --for=condition=ready pod --selector=app=datamart --timeout=240s

echo "Done! Check the status of the pods with:"
echo "kubectl get pods -n food-clustering"

echo ""
echo "Datamart API доступен через NodePort: http://<node-ip>:30880"
echo ""
echo "Для запуска ETL пайплайна выполните:"
echo "./run-etl-job.sh"
echo ""
echo "Для проверки работоспособности API выполните:"
echo "./run-healthcheck.sh"
