#!/bin/bash

# Переходим в директорию mssql
cd "$(dirname "$0")/mssql"

# Запускаем скрипт run-test-job.sh из директории mssql
./run-test-job.sh

# Возвращаемся в исходную директорию
cd ..
