#!/bin/bash

# Переходим в директорию pyspark
cd "$(dirname "$0")/pyspark"

# Запускаем скрипт check-pyspark.sh из директории pyspark
./check-pyspark.sh

# Возвращаемся в исходную директорию
cd ..
