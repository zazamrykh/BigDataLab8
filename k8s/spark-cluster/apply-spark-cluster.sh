#!/bin/bash

# Переходим в директорию скрипта
cd "$(dirname "$0")"

# Применяем манифесты в правильном порядке
echo "Applying Spark Cluster Kubernetes manifests..."
kubectl apply -f ./00-spark-namespace.yaml
kubectl apply -f ./01-spark-serviceaccount.yaml
kubectl apply -f ./02-spark-configmap.yaml
kubectl apply -f ./03-spark-master.yaml
kubectl apply -f ./04-spark-worker.yaml

# Ждем, пока Spark Master запустится
echo "Waiting for Spark Master to start..."
kubectl wait --namespace spark-cluster --for=condition=ready pod --selector=app=spark-master --timeout=120s

echo "Done! Check the status of the pods with:"
echo "kubectl get pods -n spark-cluster"

echo ""
echo "Spark UI доступен через:"
echo "http://<node-ip>:30808"
