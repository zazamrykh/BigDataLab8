#!/bin/bash

# Проверяем статус подов в namespace food-clustering
echo "Checking pod status..."
kubectl get pods -n food-clustering -l app=app

# Проверяем логи PySpark
echo -e "\nChecking PySpark logs..."
kubectl logs -n food-clustering -l app=app --tail=20

# Проверяем сервисы
echo -e "\nChecking services..."
kubectl get services -n food-clustering -l app=app

# Проверяем PVC
echo -e "\nChecking PVC..."
kubectl get pvc -n food-clustering pyspark-model-data

echo -e "\nDone!"
