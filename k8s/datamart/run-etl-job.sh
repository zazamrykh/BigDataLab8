#!/bin/bash

# Скрипт для запуска ETL пайплайна витрины данных

# Удаляем предыдущий Job, если он существует
kubectl delete job datamart-etl -n food-clustering --ignore-not-found

# Применяем манифест Job
kubectl apply -f 03-datamart-etl-job.yaml

echo "ETL Job запущен. Проверяем логи..."
sleep 3

# Получаем имя пода Job
JOB_POD=$(kubectl get pods -n food-clustering -l job-name=datamart-etl -o jsonpath='{.items[0].metadata.name}')

if [ -z "$JOB_POD" ]; then
  echo "Под Job не найден. Проверьте статус Job:"
  kubectl get job datamart-etl -n food-clustering
  exit 1
fi

# Ждем завершения Job
echo "Ожидаем завершения ETL Job..."
kubectl wait --for=condition=complete job/datamart-etl -n food-clustering --timeout=300s

# Выводим логи пода
echo "Логи ETL Job:"
kubectl logs $JOB_POD -n food-clustering

# Проверяем статус Job
JOB_STATUS=$(kubectl get job datamart-etl -n food-clustering -o jsonpath='{.status.succeeded}')

if [ "$JOB_STATUS" == "1" ]; then
  echo "ETL Job успешно завершен!"
else
  echo "ETL Job завершился с ошибкой. Проверьте логи."
  exit 1
fi
