#!/bin/bash

# Скрипт для запуска тестового Job для проверки API сервиса модели

# Удаляем предыдущий Job, если он существует
kubectl delete job pyspark-api-test -n food-clustering --ignore-not-found

# Применяем манифест Job
kubectl apply -f 04-pyspark-test-job.yaml

echo "Тестовый Job запущен. Проверяем логи..."
sleep 3

# Получаем имя пода Job
JOB_POD=$(kubectl get pods -n food-clustering -l job-name=pyspark-api-test -o jsonpath='{.items[0].metadata.name}')

if [ -z "$JOB_POD" ]; then
  echo "Под Job не найден. Проверьте статус Job:"
  kubectl get job pyspark-api-test -n food-clustering
  exit 1
fi

# Ждем завершения Job
echo "Ожидаем завершения Job..."
kubectl wait --for=condition=complete job/pyspark-api-test -n food-clustering --timeout=30s

# Выводим логи пода
echo "Логи тестового Job:"
kubectl logs $JOB_POD -n food-clustering

# Проверяем статус Job
JOB_STATUS=$(kubectl get job pyspark-api-test -n food-clustering -o jsonpath='{.status.succeeded}')

if [ "$JOB_STATUS" == "1" ]; then
  echo "Тест успешно завершен!"
else
  echo "Тест завершился с ошибкой. Проверьте логи."
  exit 1
fi
