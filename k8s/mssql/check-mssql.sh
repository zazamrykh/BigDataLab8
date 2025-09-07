#!/bin/bash

# Проверяем статус подов в namespace food-clustering
echo "Checking pod status..."
kubectl get pods -n food-clustering

# Проверяем логи MS SQL Server
echo -e "\nChecking MS SQL Server logs..."
kubectl logs -n food-clustering -l app=mssql --tail=20

# Проверяем статус Job для инициализации базы данных
echo -e "\nChecking initialization job status..."
kubectl get jobs -n food-clustering

# Проверяем логи Job для инициализации базы данных
echo -e "\nChecking initialization job logs..."
INIT_POD=$(kubectl get pods -n food-clustering -l job-name=mssql-init -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [ -n "$INIT_POD" ]; then
  kubectl logs -n food-clustering $INIT_POD
else
  echo "Initialization job pod not found"
fi

# Проверяем сервисы
echo -e "\nChecking services..."
kubectl get services -n food-clustering
