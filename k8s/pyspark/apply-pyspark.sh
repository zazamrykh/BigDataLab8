#!/bin/bash

# Переходим в директорию скрипта
cd "$(dirname "$0")"

# Применяем манифесты в правильном порядке
echo "Applying PySpark Kubernetes manifests..."
kubectl apply -f ./01-pyspark-pvc.yaml
kubectl apply -f ./02-pyspark-configmap.yaml
kubectl apply -f ./03-pyspark-deployment.yaml

# Ждем, пока PySpark запустится
echo "Waiting for PySpark to start..."
kubectl wait --namespace food-clustering --for=condition=ready pod --selector=app=app --timeout=120s

echo "Done! Check the status of the pods with:"
echo "kubectl get pods -n food-clustering"

echo ""
echo "API доступен через:"
echo "1. NodePort: http://<node-ip>:30800/docs"
echo "2. Port-forward: запустите скрипт ../port-forward-app.sh для доступа через http://localhost:8000/docs"
echo ""
echo "Для тестирования API внутри кластера запустите:"
echo "./run-test-job.sh"
