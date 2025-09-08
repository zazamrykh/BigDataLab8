# Food Clustering Helm Chart

Helm Chart для развертывания приложения Food Clustering в Kubernetes.

## Компоненты

- **Namespace**: `food-clustering`
- **MS SQL Server**: База данных для хранения продуктов и результатов кластеризации
- **PySpark API**: API для кластеризации продуктов
- **Datamart**: Витрина данных на Scala Spark

## Установка

```bash
# Установка
helm install food-clustering ./helm/food-clustering

# Обновление
helm upgrade food-clustering ./helm/food-clustering

# Удаление
helm uninstall food-clustering
```

## Конфигурация

Настройки можно изменить в файле `values.yaml`:

- Ресурсные ограничения для всех компонентов
- Образы контейнеров
- Размеры хранилища
- Порты сервисов
- Секреты для базы данных

## Доступ к сервисам

- **MS SQL Server**: Доступен внутри кластера по адресу `mssql-service:1433`
- **PySpark API**: Доступен через NodePort `30800` (http://<node-ip>:30800/docs)
- **Datamart**: Доступен через NodePort `30880` (http://<node-ip>:30880)
