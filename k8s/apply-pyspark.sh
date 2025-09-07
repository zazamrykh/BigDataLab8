#!/bin/bash

# Переходим в директорию pyspark
cd "$(dirname "$0")/pyspark"

# Запускаем скрипт apply-pyspark.sh из директории pyspark
./apply-pyspark.sh

# Возвращаемся в исходную директорию
cd ..

echo "PySpark deployment completed. You can check the status with:"
echo "./check-pyspark.sh"
