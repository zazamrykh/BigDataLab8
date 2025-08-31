-- Скрипт для создания базы данных food_clustering
-- Этот скрипт выполняется при инициализации MS SQL Server в Docker

-- Проверяем, существует ли база данных
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'food_clustering')
BEGIN
    -- Создаем базу данных
    CREATE DATABASE food_clustering;
    PRINT 'DATABASE food_clustering succ created.';
END
ELSE
BEGIN
    PRINT 'DB food_clustering already exists.';
END

-- Переключаемся на созданную базу данных
USE food_clustering;
GO
