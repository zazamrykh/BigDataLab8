# Оптимизации Kubernetes-инфраструктуры

## Ресурсные ограничения

Добавлены ресурсные ограничения для всех компонентов системы для оптимального использования ресурсов кластера:

### MS SQL Server
```yaml
resources:
  requests:
    memory: "2Gi"
    cpu: "500m"
  limits:
    memory: "4Gi"
    cpu: "1000m"
```

### PySpark API
```yaml
resources:
  requests:
    memory: "2Gi"
    cpu: "500m"
  limits:
    memory: "6Gi"
    cpu: "1000m"
```

### Datamart
```yaml
resources:
  requests:
    memory: "2Gi"
    cpu: "500m"
  limits:
    memory: "6Gi"
    cpu: "1000m"
```

### Spark Master
```yaml
resources:
  requests:
    memory: "1Gi"
    cpu: "500m"
  limits:
    memory: "2Gi"
    cpu: "1000m"
```

### Spark Worker
```yaml
resources:
  requests:
    memory: "2Gi"
    cpu: "500m"
  limits:
    memory: "4Gi"
    cpu: "1000m"
```

## Отдельный Spark-кластер

Создан отдельный Spark-кластер в Kubernetes для вычислений, который может использоваться для распределенной обработки данных. Кластер состоит из:
- 1 Spark Master
- 2 Spark Worker

Spark UI доступен через NodePort 30808.

## Helm Chart

Создан Helm Chart для упрощения развертывания всех компонентов системы. Helm Chart включает:
- Namespace
- MS SQL Server с инициализацией
- PySpark API
- Datamart
- Все необходимые ConfigMap, Secret и Service

Helm Chart позволяет настраивать параметры развертывания через файл values.yaml.
