#!/bin/bash

# Удаляем предыдущий Job, если он существует
kubectl delete job -n food-clustering mssql-test 2>/dev/null || true

# Применяем манифест Job
echo "Applying test job..."
kubectl apply -f ./test-mssql-job.yaml

# Ждем, пока Job запустится
echo "Waiting for job to start..."
sleep 5

# Получаем имя пода Job
JOB_POD=$(kubectl get pods -n food-clustering -l job-name=mssql-test -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

if [ -z "$JOB_POD" ]; then
  echo "Job pod not found, waiting longer..."
  sleep 10
  JOB_POD=$(kubectl get pods -n food-clustering -l job-name=mssql-test -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)

  if [ -z "$JOB_POD" ]; then
    echo "Job pod still not found, please check manually with:"
    echo "kubectl get pods -n food-clustering"
    exit 1
  fi
fi

echo "Found job pod: $JOB_POD"

# Ждем, пока под запустится
echo "Waiting for pod to be ready..."
POD_STATUS=""
MAX_RETRIES=30
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
  POD_STATUS=$(kubectl get pod -n food-clustering $JOB_POD -o jsonpath='{.status.phase}' 2>/dev/null)
  echo "Pod status: $POD_STATUS (attempt $RETRY_COUNT of $MAX_RETRIES)"

  # Проверяем статус пода
  if [ "$POD_STATUS" = "Running" ]; then
    echo "Pod is running!"
    break
  elif [ "$POD_STATUS" = "Succeeded" ]; then
    echo "Pod has completed successfully!"
    break
  elif [ "$POD_STATUS" = "Failed" ]; then
    echo "Pod has failed!"
    kubectl describe pod -n food-clustering $JOB_POD
    echo "You can check logs with:"
    echo "kubectl logs -n food-clustering $JOB_POD"
    exit 1
  fi

  RETRY_COUNT=$((RETRY_COUNT + 1))
  sleep 5
done

if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
  echo "Pod did not reach Running or Succeeded state within timeout. Current status: $POD_STATUS"
  echo "Checking pod details:"
  kubectl describe pod -n food-clustering $JOB_POD
  echo "You can check logs later with:"
  echo "kubectl logs -n food-clustering $JOB_POD"
  exit 1
fi

# Следим за логами пода
echo "Showing logs from job pod..."
if [ "$POD_STATUS" = "Running" ]; then
  kubectl logs -n food-clustering $JOB_POD -f
elif [ "$POD_STATUS" = "Succeeded" ]; then
  kubectl logs -n food-clustering $JOB_POD
fi

# Проверяем статус Job
echo -e "\nChecking job status..."
kubectl get job -n food-clustering mssql-test
