FROM python:3.12-bookworm

# Рабочая директория
WORKDIR /app

# Переменные окружения Python
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Установка OpenJDK для PySpark, системных зависимостей и curl/gnupg
RUN apt-get update -o Acquire::Check-Valid-Until=false -o Acquire::Check-Date=false \
    && apt-get install -y --no-install-recommends \
       openjdk-17-jdk-headless \
       unixodbc \
       unixodbc-dev \
       curl \
       gnupg \
       ca-certificates \
       apt-transport-https \
    && rm -rf /var/lib/apt/lists/*

# Настройка JAVA_HOME для PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Настройка Microsoft репозитория для ODBC и mssql-tools
RUN curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
       > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y \
       msodbcsql18 \
       mssql-tools \
    && rm -rf /var/lib/apt/lists/*

# Добавляем sqlcmd в PATH
ENV PATH="$PATH:/opt/mssql-tools/bin"

# Скачиваем JDBC драйвер для MS SQL
RUN curl -L -o /opt/mssql-jdbc.jar https://repo1.maven.org/maven2/com/microsoft/sqlserver/mssql-jdbc/12.2.0.jre8/mssql-jdbc-12.2.0.jre8.jar

# ---- Оптимизация кеша ----
# Копируем только requirements.txt для кеширования установки зависимостей
COPY requirements.txt .

# Устанавливаем Python-зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Теперь копируем весь код проекта
COPY . .

# Создаем директорию для моделей
RUN mkdir -p model

# Экспорт пути к JDBC драйверу для Spark
ENV SPARK_CLASSPATH=/opt/mssql-jdbc.jar

# Запуск FastAPI
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
