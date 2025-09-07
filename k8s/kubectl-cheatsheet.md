# Шпаргалка по командам kubectl

## Основные команды для работы с подами

### Просмотр подов (аналог docker compose ps)
```bash
# Просмотр всех подов в текущем namespace
kubectl get pods

# Просмотр всех подов в определенном namespace
kubectl get pods -n food-clustering

# Просмотр всех подов во всех namespace
kubectl get pods --all-namespaces

# Просмотр подов с дополнительной информацией
kubectl get pods -n food-clustering -o wide

# Просмотр подов с определенным лейблом
kubectl get pods -n food-clustering -l app=mssql
```

### Просмотр логов пода (аналог docker logs)
```bash
# Просмотр логов конкретного пода
kubectl logs -n food-clustering <имя-пода>

# Просмотр логов пода с определенным лейблом
kubectl logs -n food-clustering -l app=mssql

# Просмотр последних N строк логов
kubectl logs -n food-clustering <имя-пода> --tail=100

# Следить за логами в реальном времени (аналог docker logs -f)
kubectl logs -n food-clustering <имя-пода> -f

# Просмотр логов конкретного контейнера в поде (если в поде несколько контейнеров)
kubectl logs -n food-clustering <имя-пода> -c <имя-контейнера>
```

### Подробная информация о поде
```bash
# Получение подробной информации о поде
kubectl describe pod -n food-clustering <имя-пода>

# Получение подробной информации о подах с определенным лейблом
kubectl describe pod -n food-clustering -l app=mssql
```

### Выполнение команд внутри пода (аналог docker exec)
```bash
# Выполнение команды в поде
kubectl exec -n food-clustering <имя-пода> -- <команда>

# Пример: выполнение команды ls в поде
kubectl exec -n food-clustering <имя-пода> -- ls -la

# Запуск интерактивного шелла в поде
kubectl exec -it -n food-clustering <имя-пода> -- /bin/bash
```

### Удаление подов
```bash
# Удаление конкретного пода
kubectl delete pod -n food-clustering <имя-пода>

# Удаление всех подов с определенным лейблом
kubectl delete pod -n food-clustering -l app=mssql

# forecegully
kubectl delete pod <PODNAME> --grace-period=0 --force --namespace <NAMESPACE>
```

## Работа с другими ресурсами Kubernetes

### Просмотр сервисов
```bash
# Просмотр всех сервисов в namespace
kubectl get services -n food-clustering

# Просмотр конкретного сервиса
kubectl get service -n food-clustering <имя-сервиса>
```

### Просмотр деплойментов
```bash
# Просмотр всех деплойментов в namespace
kubectl get deployments -n food-clustering

# Просмотр конкретного деплоймента
kubectl get deployment -n food-clustering <имя-деплоймента>
```

### Просмотр PersistentVolumeClaims (PVC)
```bash
# Просмотр всех PVC в namespace
kubectl get pvc -n food-clustering

# Просмотр конкретного PVC
kubectl get pvc -n food-clustering <имя-pvc>
```

### Просмотр ConfigMaps
```bash
# Просмотр всех ConfigMaps в namespace
kubectl get configmaps -n food-clustering

# Просмотр конкретного ConfigMap
kubectl get configmap -n food-clustering <имя-configmap>
```

### Просмотр Secrets
```bash
# Просмотр всех Secrets в namespace
kubectl get secrets -n food-clustering

# Просмотр конкретного Secret
kubectl get secret -n food-clustering <имя-secret>
```

### Просмотр Jobs
```bash
# Просмотр всех Jobs в namespace
kubectl get jobs -n food-clustering

# Просмотр конкретного Job
kubectl get job -n food-clustering <имя-job>
```

## Отладка и диагностика

### Port-forwarding (для доступа к сервисам внутри кластера)
```bash
# Перенаправление порта сервиса на локальный порт
kubectl port-forward -n food-clustering service/<имя-сервиса> <локальный-порт>:<порт-сервиса>

# Пример: перенаправление порта 8080 сервиса на локальный порт 8080
kubectl port-forward -n food-clustering service/app-service 8080:8080
```

### Просмотр событий в кластере
```bash
# Просмотр всех событий в namespace
kubectl get events -n food-clustering

# Просмотр событий, отсортированных по времени
kubectl get events -n food-clustering --sort-by='.lastTimestamp'
```

### Проверка статуса кластера
```bash
# Проверка статуса узлов кластера
kubectl get nodes

# Проверка статуса компонентов кластера
kubectl get componentstatuses
```

## Применение и обновление манифестов

```bash
# Применение манифеста
kubectl apply -f <путь-к-файлу.yaml>

# Применение всех манифестов в директории
kubectl apply -f <путь-к-директории>

# Удаление ресурсов, описанных в манифесте
kubectl delete -f <путь-к-файлу.yaml>
```

## Полезные опции для всех команд

```bash
# Вывод в формате YAML
kubectl get pod -n food-clustering <имя-пода> -o yaml

# Вывод в формате JSON
kubectl get pod -n food-clustering <имя-пода> -o json

# Вывод только имен ресурсов
kubectl get pods -n food-clustering --no-headers -o custom-columns=":metadata.name"
