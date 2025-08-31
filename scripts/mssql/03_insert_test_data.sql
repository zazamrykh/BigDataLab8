-- Скрипт для заполнения таблицы food_products тестовыми данными
-- Этот скрипт выполняется после создания таблиц

-- Переключаемся на базу данных food_clustering
USE food_clustering;
GO

-- Проверяем, есть ли уже данные в таблице
IF (SELECT COUNT(*) FROM food_products) = 0
BEGIN
    -- Вставляем тестовые данные
    INSERT INTO food_products (code, product_name, [energy-kcal], fat, carbohydrates, proteins, sugars)
    VALUES
        ('1', 'Test Product 1', 100.0, 5.0, 10.0, 2.0, 5.0),
        ('2', 'Test Product 2', 200.0, 10.0, 20.0, 5.0, 10.0),
        ('3', 'Test Product 3', 300.0, 15.0, 30.0, 10.0, 15.0),
        ('4', 'Test Product 4', 400.0, 20.0, 40.0, 15.0, 20.0),
        ('5', 'Test Product 5', 500.0, 25.0, 50.0, 20.0, 25.0);

    PRINT 'test data succ add in to food_products.';
END
ELSE
BEGIN
    PRINT 'table food_products alrd cont data.';
END
