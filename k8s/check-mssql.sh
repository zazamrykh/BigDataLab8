#!/bin/bash

# Переходим в директорию mssql
cd "$(dirname "$0")/mssql"

# Запускаем скрипт check-mssql.sh из директории mssql
./check-mssql.sh

# Возвращаемся в исходную директорию
cd ..
