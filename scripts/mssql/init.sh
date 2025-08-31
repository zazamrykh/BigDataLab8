#!/bin/bash
set -e

# Ждем, пока MS SQL Server запустится
sleep 30s

# Выполняем скрипты инициализации
/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -i /docker-entrypoint-initdb.d/01_create_database.sql
/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -i /docker-entrypoint-initdb.d/02_create_tables.sql
/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -i /docker-entrypoint-initdb.d/03_insert_test_data.sql

echo "Инициализация базы данных завершена."
