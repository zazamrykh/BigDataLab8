#!/bin/bash

# Скрипт для запуска кластеризации через API

# Проверяем, доступен ли API через port-forward
if ! curl -s http://localhost:8000/health > /dev/null; then
  echo "API не доступен через localhost:8000"
  echo "Запустите port-forward с помощью скрипта ../port-forward-app.sh"
  exit 1
fi

echo "Запускаем кластеризацию через API..."

# Отправляем запрос на кластеризацию
curl -X POST http://localhost:8000/cluster \
  -H "Content-Type: application/json" \
  -d '{
    "use_sample_data": false,
    "use_datamart": false,
    "n_clusters": 5
  }'

echo ""
echo "Запрос на кластеризацию отправлен."
echo "Проверьте логи пода для получения результатов:"
echo "kubectl logs -n food-clustering -l app=app"
