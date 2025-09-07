#!/bin/bash

# Скрипт для настройки port-forward для доступа к API приложения

# Получаем имя пода app
APP_POD=$(kubectl get pods -n food-clustering -l app=app -o jsonpath='{.items[0].metadata.name}')

if [ -z "$APP_POD" ]; then
  echo "Под app не найден"
  exit 1
fi

echo "Настраиваем port-forward для пода $APP_POD..."
echo "API будет доступен по адресу http://localhost:8000/docs"
echo "Нажмите Ctrl+C для завершения"

# Настраиваем port-forward
kubectl port-forward -n food-clustering $APP_POD 8000:8000
