#!/bin/bash

# Скрипт для диагностики проблем с подключением к MS SQL Server

echo "=== Диагностика проблем с подключением к MS SQL Server ==="

# Проверяем статус подов
echo -e "\n=== Статус подов ==="
kubectl get pods -n food-clustering

# Проверяем статус сервисов
echo -e "\n=== Статус сервисов ==="
kubectl get services -n food-clustering

# Проверяем логи MS SQL Server
echo -e "\n=== Логи MS SQL Server ==="
MSSQL_POD=$(kubectl get pods -n food-clustering -l app=mssql -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [ -n "$MSSQL_POD" ]; then
  kubectl logs -n food-clustering $MSSQL_POD --tail=20
else
  echo "Под MS SQL Server не найден"
fi

# Проверяем логи PySpark
echo -e "\n=== Логи PySpark ==="
APP_POD=$(kubectl get pods -n food-clustering -l app=app -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [ -n "$APP_POD" ]; then
  kubectl logs -n food-clustering $APP_POD --tail=20
else
  echo "Под PySpark не найден"
fi

# Проверяем ресурсы MS SQL Server
echo -e "\n=== Ресурсы MS SQL Server ==="
if [ -n "$MSSQL_POD" ]; then
  kubectl describe pod -n food-clustering $MSSQL_POD | grep -A 10 "Containers:"
else
  echo "Под MS SQL Server не найден"
fi

# Проверяем сетевую связность между подами
echo -e "\n=== Проверка сетевой связности ==="
if [ -n "$APP_POD" ]; then
  echo "Проверяем подключение к MS SQL Server из пода PySpark..."
  kubectl exec -it -n food-clustering $APP_POD -- bash -c "apt-get update && apt-get install -y netcat && nc -zv mssql-service 1433"
else
  echo "Под PySpark не найден"
fi

# Проверяем DNS-разрешение
echo -e "\n=== Проверка DNS-разрешения ==="
if [ -n "$APP_POD" ]; then
  echo "Проверяем DNS-разрешение имени mssql-service из пода PySpark..."
  kubectl exec -it -n food-clustering $APP_POD -- bash -c "apt-get update && apt-get install -y dnsutils && nslookup mssql-service"
else
  echo "Под PySpark не найден"
fi

# Создаем временный под для тестирования подключения к MS SQL Server
echo -e "\n=== Создание временного пода для тестирования подключения ==="
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: mssql-test-pod
  namespace: food-clustering
spec:
  containers:
  - name: mssql-tools
    image: mcr.microsoft.com/mssql-tools
    command: ["sleep", "3600"]
  restartPolicy: Never
EOF

echo "Ожидаем запуска временного пода..."
kubectl wait --for=condition=ready pod/mssql-test-pod -n food-clustering --timeout=60s

echo -e "\n=== Тестирование подключения к MS SQL Server из временного пода ==="
kubectl exec -it -n food-clustering mssql-test-pod -- bash -c "/opt/mssql-tools/bin/sqlcmd -S mssql-service -U sa -P YourStrong@Passw0rd -Q 'SELECT 1' -l 5"

echo -e "\n=== Проверка конфигурации JDBC в PySpark ==="
if [ -n "$APP_POD" ]; then
  echo "Проверяем наличие JDBC драйвера в поде PySpark..."
  kubectl exec -it -n food-clustering $APP_POD -- bash -c "ls -la /opt/mssql-jdbc.jar"
else
  echo "Под PySpark не найден"
fi

# Удаляем временный под
echo -e "\n=== Удаление временного пода ==="
kubectl delete pod -n food-clustering mssql-test-pod

echo -e "\n=== Диагностика завершена ==="
