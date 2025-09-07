#!/bin/bash

# Скрипт для сборки и загрузки образов в Docker Hub

# Переходим в корневую директорию проекта
cd "$(dirname "$0")/.."

# Запрашиваем имя пользователя Docker Hub
read -p "Введите ваше имя пользователя Docker Hub: " DOCKERHUB_USERNAME

# Проверяем, что пользователь ввел имя пользователя
if [ -z "$DOCKERHUB_USERNAME" ]; then
  echo "Имя пользователя не может быть пустым"
  exit 1
fi

# Входим в Docker Hub
echo "Выполняется вход в Docker Hub..."
docker login

# Собираем образ приложения
echo "Собираем образ приложения..."
docker build -t food-clustering-app:latest -f docker/Dockerfile .

# Собираем образ витрины данных
echo "Собираем образ витрины данных..."
docker build -t food-clustering-datamart:latest -f docker/Dockerfile.datamart .

# Тегируем образы для Docker Hub
echo "Тегируем образы для Docker Hub..."
docker tag food-clustering-app:latest $DOCKERHUB_USERNAME/food-clustering-app:latest
docker tag food-clustering-datamart:latest $DOCKERHUB_USERNAME/food-clustering-datamart:latest

# Загружаем образы в Docker Hub
echo "Загружаем образы в Docker Hub..."
docker push $DOCKERHUB_USERNAME/food-clustering-app:latest
docker push $DOCKERHUB_USERNAME/food-clustering-datamart:latest

echo "Образы успешно загружены в Docker Hub"
echo "Теперь вам нужно обновить манифесты Kubernetes, чтобы использовать эти образы"
echo "Замените 'food-clustering-app:latest' на '$DOCKERHUB_USERNAME/food-clustering-app:latest'"
echo "Замените 'food-clustering-datamart:latest' на '$DOCKERHUB_USERNAME/food-clustering-datamart:latest'"

# Обновляем манифесты Kubernetes
read -p "Хотите автоматически обновить манифесты Kubernetes? (y/n): " UPDATE_MANIFESTS

if [ "$UPDATE_MANIFESTS" = "y" ] || [ "$UPDATE_MANIFESTS" = "Y" ]; then
  echo "Обновляем манифесты Kubernetes..."

  # Обновляем манифест PySpark
  sed -i "s|image: food-clustering-app:latest|image: $DOCKERHUB_USERNAME/food-clustering-app:latest|g" k8s/pyspark/03-pyspark-deployment.yaml

  # Создаем директорию для Scala Spark, если она еще не существует
  mkdir -p k8s/scala-spark

  echo "Манифесты успешно обновлены"
fi

echo "Готово!"
