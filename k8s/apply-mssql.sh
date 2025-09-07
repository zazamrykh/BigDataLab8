#!/bin/bash

# Переходим в директорию mssql
cd "$(dirname "$0")/mssql"

# Запускаем скрипт apply-mssql.sh из директории mssql
./apply-mssql.sh

# Возвращаемся в исходную директорию
cd ..

echo "MS SQL Server deployment completed. You can check the status with:"
echo "./check-mssql.sh"
