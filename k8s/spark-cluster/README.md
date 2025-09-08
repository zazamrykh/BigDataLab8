# Spark Cluster в Kubernetes

## Обзор

Отдельный Spark-кластер в Kubernetes для вычислений, состоящий из:
- 1 Spark Master
- 2 Spark Worker

## Компоненты

- **Namespace**: `spark-cluster`
- **ServiceAccount**: `spark` с необходимыми разрешениями
- **ConfigMap**: `spark-config` с настройками Spark
- **Deployment**: `spark-master` для Spark Master
- **Deployment**: `spark-worker` для Spark Worker
- **Service**: `spark-master` для доступа к Spark Master

## Важные настройки

- Spark Master UI доступен через NodePort: `30808`
- Spark Master доступен внутри кластера по адресу: `spark-master:7077`
- Ресурсные ограничения:
  - Spark Master: 1Gi RAM (limit: 2Gi), 500m CPU (limit: 1000m)
  - Spark Worker: 2Gi RAM (limit: 4Gi), 500m CPU (limit: 1000m)

## Примечания по конфигурации

- Для Spark Master используется переменная окружения `SPARK_MASTER_PORT: "7077"` вместо `SPARK_MASTER_HOST`, чтобы избежать ошибки `NumberFormatException` при запуске
- Для Spark Worker используется переменная окружения `SPARK_MASTER_URL: "spark-master:7077"` для подключения к мастеру

## Применение

```bash
./apply-spark-cluster.sh
```

## Проверка статуса

```bash
kubectl get pods -n spark-cluster
kubectl get services -n spark-cluster
```

## Доступ к Spark UI

```
http://<node-ip>:30808
