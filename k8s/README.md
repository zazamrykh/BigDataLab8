# Миграция на Kubernetes

## Подготовка

1. Убедитесь, что у вас установлен и запущен Minikube:
   ```bash
   minikube status
   ```

   Если Minikube не запущен, запустите его:
   ```bash
   minikube start
   ```

2. Убедитесь, что у вас установлен kubectl и он настроен на работу с Minikube:
   ```bash
   kubectl cluster-info
   ```

## Миграция MS SQL Server

### Шаг 1: Создание namespace и ConfigMap

Применяем манифесты для создания namespace и ConfigMap:

```bash
# Запускаем скрипт для применения всех манифестов
./apply-mssql.sh
```

Этот скрипт выполнит следующие действия:
1. Создаст ConfigMap для SQL скриптов
2. Применит манифесты для namespace, PVC, ConfigMap и Secret
3. Запустит Deployment для MS SQL Server
4. Дождется запуска MS SQL Server
5. Применит Job для инициализации базы данных

### Шаг 2: Проверка статуса

Проверяем статус MS SQL Server и инициализации базы данных:

```bash
# Запускаем скрипт для проверки статуса
./check-mssql.sh
```

Этот скрипт выведет:
1. Статус подов в namespace food-clustering
2. Логи MS SQL Server
3. Статус Job для инициализации базы данных
4. Логи Job для инициализации базы данных
5. Список сервисов в namespace food-clustering

### Шаг 3: Проверка доступности MS SQL Server

Для проверки доступности MS SQL Server и выполнения SQL-запросов используйте скрипт run-test-job.sh:

```bash
# Запускаем скрипт для проверки MS SQL Server через Job
./run-test-job.sh
```

Этот скрипт:
1. Создает Job внутри кластера Kubernetes для проверки MS SQL Server
2. Ожидает запуска пода и отслеживает его статус
3. Выводит логи пода с результатами проверки
4. Проверяет соединение с MS SQL Server
5. Проверяет наличие базы данных food_clustering и таблиц

Этот метод более надежен, так как выполняет проверку внутри кластера Kubernetes, избегая проблем с сетевым доступом.

## Структура манифестов

- `00-namespace.yaml` - Namespace для проекта
- `01-mssql-pvc.yaml` - PersistentVolumeClaim для хранения данных MS SQL Server
- `02-env-configmap.yaml` - ConfigMap для переменных окружения
- `02-db-secret.yaml` - Secret для хранения паролей
- `02-mssql-init-configmap.yaml` - ConfigMap для скриптов инициализации базы данных (создается скриптом 02-create-mssql-configmap.sh)
- `03-mssql-deployment.yaml` - Deployment и Service для MS SQL Server
- `04-mssql-init-job.yaml` - Job для инициализации базы данных

## Скрипты

- `02-create-mssql-configmap.sh` - Скрипт для создания ConfigMap с SQL скриптами
- `apply-mssql.sh` - Скрипт для применения всех манифестов
- `check-mssql.sh` - Скрипт для проверки статуса MS SQL Server
- `test-mssql-job.yaml` - Манифест для создания Job, который проверяет MS SQL Server
- `run-test-job.sh` - Скрипт для запуска Job и просмотра его логов

## Организация файлов

Для лучшей организации файлов в будущем, когда будут добавлены манифесты для других сервисов (PySpark, Scala Spark), рекомендуется создать поддиректории для каждого сервиса:

```
k8s/
├── 00-namespace.yaml
├── mssql/
│   ├── 01-mssql-pvc.yaml
│   ├── 02-env-configmap.yaml
│   ├── 02-db-secret.yaml
│   ├── 02-mssql-init-configmap.yaml
│   ├── 03-mssql-deployment.yaml
│   ├── 04-mssql-init-job.yaml
│   ├── apply-mssql.sh
│   ├── check-mssql.sh
│   ├── test-mssql-job.yaml
│   └── run-test-job.sh
├── pyspark/
│   └── ... (будущие файлы для PySpark)
├── scala-spark/
│   └── ... (будущие файлы для Scala Spark)
└── README.md
```

Это позволит лучше организовать файлы и избежать путаницы при добавлении новых сервисов.
