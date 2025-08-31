# Инструкция по настройке и запуску проекта

Данная инструкция поможет вам настроить виртуальное окружение, установить необходимые зависимости и запустить проект кластеризации продуктов питания.

## Предварительные требования

- Python 3.12.3
- Java 8+ (необходим для PySpark)
- Docker и Docker Compose (для запуска в контейнерах)
- MS SQL Server (или использование Docker-контейнера)
- Доступ к интернету для загрузки зависимостей

## Настройка виртуального окружения

### 1. Создание виртуального окружения

#### Windows

```bash
# Создание виртуального окружения
python -m venv venv

# Активация виртуального окружения
venv\Scripts\activate
```

#### Linux/macOS

```bash
# Создание виртуального окружения
python3 -m venv venv

# Активация виртуального окружения
source venv/bin/activate
```

### 2. Установка зависимостей

После активации виртуального окружения установите необходимые зависимости:

```bash
pip install -r requirements.txt
```

### 3. Настройка переменных окружения для PySpark

#### Windows

```bash
# Установка переменных окружения для PySpark
set PYSPARK_PYTHON=python
set PYSPARK_DRIVER_PYTHON=python
```

#### Linux/macOS

```bash
# Установка переменных окружения для PySpark
export PYSPARK_PYTHON=python
export PYSPARK_DRIVER_PYTHON=python
```

## Запуск проекта

### 1. Запуск основного приложения локально

```bash
# Запуск с полным набором данных
python src/main.py

# Запуск с тестовым набором данных
python src/main.py --sample

# Запуск с сохранением модели
python src/main.py --save-model
```

### 2. Запуск FastAPI сервиса локально

```bash
# Запуск FastAPI сервиса
python src/api/main.py
```

После запуска API будет доступен по адресу http://localhost:8000 со следующими эндпоинтами:
- GET /health - Проверка работоспособности сервиса
- POST /cluster - Эндпоинт для кластеризации данных

Также доступна автоматически сгенерированная документация API:
- http://localhost:8000/docs - Swagger UI
- http://localhost:8000/redoc - ReDoc

### 3. Запуск с использованием Docker

#### Настройка переменных окружения

Перед запуском с использованием Docker, убедитесь, что файл `.env` содержит правильные настройки:

```bash
# Настройки MS SQL Server
MSSQL_SA_PASSWORD=YourStrong@Passw0rd
MSSQL_PID=Developer

# Настройки подключения к базе данных
DB_SERVER=mssql
DB_PORT=1433
DB_NAME=food_clustering
DB_USER=sa
DB_PASSWORD=YourStrong@Passw0rd
```

#### Запуск контейнеров

```bash
# Сборка и запуск контейнеров
docker-compose up -d

# Просмотр логов
docker-compose logs -f app

# Остановка контейнеров
docker-compose down
```

Это запустит:
1. Контейнер с MS SQL Server
2. Контейнер с приложением и FastAPI сервисом

API будет доступен по адресу http://localhost:8000

### 4. Запуск тестов

#### Запуск всех тестов

```bash
# Запуск всех тестов с использованием unittest
python -m unittest discover tests

# Запуск всех тестов с использованием pytest
pytest tests/
```

#### Запуск конкретного теста

```bash
# Запуск конкретного теста с использованием unittest
python -m unittest tests.test_config

# Запуск конкретного теста с использованием pytest
pytest tests/test_config.py
```

#### Запуск тестов с отчетом о покрытии

```bash
# Запуск тестов с отчетом о покрытии
pytest --cov=src tests/
```

## Настройка MS SQL Server

### 1. Локальная установка MS SQL Server

Если вы хотите использовать локальную установку MS SQL Server, выполните следующие шаги:

1. Установите MS SQL Server (Express или Developer Edition)
2. Создайте базу данных `food_clustering`
3. Выполните скрипты из директории `scripts/mssql` для создания таблиц и тестовых данных
4. Обновите настройки подключения в файле `config.ini` в секции `MSSQL`

### 2. Использование Docker для MS SQL Server

Если вы используете Docker для запуска MS SQL Server, все необходимые настройки уже включены в `docker-compose.yml` и скрипты инициализации.

## Проверка работоспособности

### Проверка локального запуска

После успешной установки и запуска проекта вы должны увидеть:

1. Логи о загрузке и обработке данных
2. Информацию о кластеризации
3. Сообщение о сохранении визуализаций в директории `results/`

### Проверка FastAPI сервиса

После запуска FastAPI сервиса вы можете проверить его работоспособность:

1. Откройте в браузере http://localhost:8000/docs
2. Выполните запрос к эндпоинту `/health`
3. Выполните запрос к эндпоинту `/cluster` с параметром `use_sample_data: true`

### Проверка Docker-контейнеров

После запуска Docker-контейнеров вы можете проверить их работоспособность:

1. Проверьте, что контейнеры запущены: `docker-compose ps`
2. Проверьте логи контейнеров: `docker-compose logs -f`
3. Проверьте доступность API: http://localhost:8000/docs

## Возможные проблемы и их решения

### Проблемы с PySpark

Если у вас возникают проблемы с запуском PySpark, убедитесь, что:

1. Java установлена и доступна в PATH
2. Переменные окружения PYSPARK_PYTHON и PYSPARK_DRIVER_PYTHON установлены правильно
3. Версия PySpark совместима с вашей версией Java

Для проверки установки Java выполните:

```bash
java -version
```

### Проблемы с MS SQL Server

Если у вас возникают проблемы с подключением к MS SQL Server:

1. Проверьте, что сервер запущен и доступен
2. Проверьте настройки подключения в файле `config.ini`
3. Проверьте, что порт 1433 не заблокирован брандмауэром
4. Проверьте, что драйвер JDBC для MS SQL Server доступен

Для проверки подключения к MS SQL Server можно использовать:

```bash
# С использованием sqlcmd (если установлен)
sqlcmd -S localhost,1433 -U sa -P 'YourStrong@Passw0rd' -Q "SELECT @@VERSION"

# С использованием pyodbc
python -c "import pyodbc; conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost,1433;DATABASE=food_clustering;UID=sa;PWD=YourStrong@Passw0rd'); print(conn.execute('SELECT @@VERSION').fetchone()[0])"
```

### Проблемы с Docker

Если у вас возникают проблемы с Docker:

1. Проверьте, что Docker и Docker Compose установлены и запущены
2. Проверьте, что порты 1433 и 8000 не заняты другими приложениями
3. Проверьте логи контейнеров: `docker-compose logs -f`
4. Перезапустите контейнеры: `docker-compose down && docker-compose up -d`

Для проверки статуса контейнеров:

```bash
# Проверка статуса контейнеров
docker-compose ps

# Проверка логов MS SQL Server
docker-compose logs -f mssql

# Проверка логов приложения
docker-compose logs -f app
```

### Проблемы с зависимостями

Если у вас возникают проблемы с установкой зависимостей, попробуйте:

```bash
# Обновить pip
pip install --upgrade pip

# Установить зависимости по одной
pip install pyspark==4.0.0
pip install pandas==2.3.1
pip install fastapi==0.116.1
# и т.д.
```

### Проблемы с Python 3.12.3

Если у вас возникают проблемы совместимости с Python 3.12.3, возможно, некоторые библиотеки еще не полностью поддерживают эту версию. В этом случае вы можете:

1. Создать виртуальное окружение с более старой версией Python (например, 3.10 или 3.11)
2. Или попробовать установить более новые версии библиотек (без указания конкретной версии в requirements.txt)

```bash
# Установка без указания версий
pip install pyspark pandas numpy matplotlib fastapi uvicorn pyodbc pymssql sqlalchemy
```

### Проблемы с JDBC драйвером для MS SQL Server

Если у вас возникают проблемы с JDBC драйвером для MS SQL Server:

1. Убедитесь, что драйвер доступен в classpath для PySpark
2. Скачайте последнюю версию драйвера с сайта Microsoft
3. Укажите путь к драйверу при запуске PySpark:

```bash
# Запуск PySpark с указанием пути к JDBC драйверу
export PYSPARK_SUBMIT_ARGS="--driver-class-path /path/to/mssql-jdbc.jar --jars /path/to/mssql-jdbc.jar pyspark-shell"
```

## Дополнительная информация

Для получения дополнительной информации о проекте обратитесь к файлу README.md.
