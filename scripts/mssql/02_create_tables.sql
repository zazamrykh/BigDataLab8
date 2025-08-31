-- Скрипт для создания таблиц в базе данных food_clustering
-- Этот скрипт выполняется после создания базы данных

USE food_clustering;
GO

-- Таблица food_products
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'food_products')
BEGIN
    CREATE TABLE food_products (
        id INT IDENTITY(1,1) PRIMARY KEY,
        code NVARCHAR(100) NOT NULL,
        product_name NVARCHAR(255),
        [energy-kcal] FLOAT,
        fat FLOAT,
        carbohydrates FLOAT,
        proteins FLOAT,
        sugars FLOAT,
        created_at DATETIME DEFAULT GETDATE()
    );
    PRINT 'table food_products successfully created.';
END
ELSE
BEGIN
    PRINT 'Table food_products already existing.';
END

-- Таблица clustering_results без foreign key
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'clustering_results')
BEGIN
    CREATE TABLE clustering_results (
        id INT IDENTITY(1,1) PRIMARY KEY,
        product_id INT NOT NULL,
        cluster_id INT NOT NULL,
        created_at DATETIME DEFAULT GETDATE()
        -- FOREIGN KEY удалён
    );
    PRINT 'Table clustering_results successfully created (without foreign key).';
END
ELSE
BEGIN
    PRINT 'Table clustering_results already exists.';
END

-- Таблица cluster_centers
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'cluster_centers')
BEGIN
    CREATE TABLE cluster_centers (
        id INT IDENTITY(1,1) PRIMARY KEY,
        cluster_id INT NOT NULL,
        feature_name NVARCHAR(100) NOT NULL,
        feature_value FLOAT NOT NULL,
        created_at DATETIME DEFAULT GETDATE()
    );
    PRINT 'table cluster_centers successfully created.';
END
ELSE
BEGIN
    PRINT 'table cluster_centers already exists.';
END

-- Индексы
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_food_products_code')
BEGIN
    CREATE INDEX IX_food_products_code ON food_products(code);
    PRINT 'idx IX_food_products_code successfully created.';
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_clustering_results_product_id')
BEGIN
    CREATE INDEX IX_clustering_results_product_id ON clustering_results(product_id);
    PRINT 'idx IX_clustering_results_product_id successfully created.';
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_clustering_results_cluster_id')
BEGIN
    CREATE INDEX IX_clustering_results_cluster_id ON clustering_results(cluster_id);
    PRINT 'idx IX_clustering_results_cluster_id successfully created.';
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_cluster_centers_cluster_id')
BEGIN
    CREATE INDEX IX_cluster_centers_cluster_id ON cluster_centers(cluster_id);
    PRINT 'idx IX_cluster_centers_cluster_id successfully created.';
END
