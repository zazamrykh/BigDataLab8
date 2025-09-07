#!/bin/bash

# Скрипт для запуска проверки работоспособности API витрины данных

# Удаляем предыдущий Job, если он существует
kubectl delete job datamart-healthcheck -n food-clustering --ignore-not-found

# Применяем манифест Job
kubectl apply -f 04-datamart-healthcheck-job.yaml

echo "Healthcheck Job запущен. Проверяем логи..."
sleep 3

# Получаем имя пода Job
JOB_POD=$(kubectl get pods -n food-clustering -l job-name=datamart-healthcheck -o jsonpath='{.items[0].metadata.name}')

if [ -z "$JOB_POD" ]; then
  echo "Под Job не найден. Проверьте статус Job:"
  kubectl get job datamart-healthcheck -n food-clustering
  exit 1
fi

# Ждем завершения Job
echo "Ожидаем завершения Healthcheck Job..."
kubectl wait --for=condition=complete job/datamart-healthcheck -n food-clustering --timeout=30s

# Выводим логи пода
echo "Логи Healthcheck Job:"
kubectl logs $JOB_POD -n food-clustering

# Проверяем статус Job
JOB_STATUS=$(kubectl get job datamart-healthcheck -n food-clustering -o jsonpath='{.status.succeeded}')

if [ "$JOB_STATUS" == "1" ]; then
  echo "Healthcheck Job успешно завершен! API витрины данных работает."
else
  echo "Healthcheck Job завершился с ошибкой. API витрины данных недоступен."
  exit 1
fi
