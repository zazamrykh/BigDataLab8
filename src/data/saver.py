"""
Module for saving clustering results to MS SQL Server.
"""
from typing import Dict, List, Optional
import json
import requests

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, FloatType, StringType, TimestampType
from pyspark.sql import Row
from pyspark.sql.functions import col, current_timestamp, when, first, explode, size, lit


from src.utils.config import config
from src.utils.logger import logger


class DataSaver:
    """
    Class for saving clustering results to MS SQL Server.
    """
    def __init__(self, spark: SparkSession):
        """
        Initialize data saver.

        Args:
            spark: SparkSession for working with data.
        """
        self.spark = spark

        # MS SQL Server settings
        self.mssql_server = config.get('MSSQL', 'server')
        self.mssql_port = config.get('MSSQL', 'port')
        self.mssql_database = config.get('MSSQL', 'database')
        self.mssql_username = config.get('MSSQL', 'username')
        self.mssql_password = config.get('MSSQL', 'password')
        self.mssql_driver = config.get('MSSQL', 'driver')
        self.mssql_input_table = config.get('MSSQL', 'input_table')
        self.mssql_output_table = config.get('MSSQL', 'output_table')

        # Настройки для витрины данных
        self.datamart_host = config.get('DATAMART', 'host', fallback='datamart')
        self.datamart_port = config.getint('DATAMART', 'port', fallback=8080)

    def save_clustering_results(self, df_clustered: DataFrame) -> None:
        """
        Save clustering results to MS SQL Server.

        Args:
            df_clustered: DataFrame with clustering results.
        """
        logger.info(f"Saving clustering results to MS SQL Server: {self.mssql_server}:{self.mssql_port}/{self.mssql_database}")

        try:
            jdbc_url = f"jdbc:sqlserver://{self.mssql_server}:{self.mssql_port};databaseName={self.mssql_database};encrypt=false;trustServerCertificate=true"

            connection_properties = {
                "user": self.mssql_username,
                "password": self.mssql_password,
                "driver": self.mssql_driver
            }

            # Определяем идентификатор продукта
            if "id" in df_clustered.columns:
                product_id_col = F.col("id").cast(IntegerType())
            elif "code" in df_clustered.columns:
                product_id_col = F.col("code").cast(StringType())
            else:
                # Если нет ни id, ни code — создаем уникальный id
                product_id_col = F.monotonically_increasing_id().cast(IntegerType())

            # Приводим названия колонок к тем, что в таблице SQL
            df_to_save = df_clustered.select(
                product_id_col.alias("product_id"),
                F.col("cluster").cast(IntegerType()).alias("cluster_id"),
                F.current_timestamp().alias("created_at")
            )

            # Сохраняем в таблицу
            df_to_save.write \
                .jdbc(url=jdbc_url,
                    table=self.mssql_output_table,
                    mode="append",
                    properties=connection_properties)

            logger.info(f"Clustering results successfully saved to {self.mssql_output_table}")

        except Exception as e:
            logger.error(f"Error saving clustering results to MS SQL Server: {e}")
            raise



    def save_cluster_centers(self, cluster_centers: list, feature_names: list[str]) -> None:
        """
        Save cluster centers to MS SQL Server.

        Args:
            cluster_centers: list of cluster center rows, e.g., list of lists or dicts
            feature_names: list of feature names corresponding to columns in cluster_centers
        """
        logger.info(f"Saving cluster centers to MS SQL Server: {self.mssql_server}:{self.mssql_port}/{self.mssql_database}")

        try:
            jdbc_url = f"jdbc:sqlserver://{self.mssql_server}:{self.mssql_port};databaseName={self.mssql_database};encrypt=false;trustServerCertificate=true"
            connection_properties = {
                "user": self.mssql_username,
                "password": self.mssql_password,
                "driver": self.mssql_driver
            }

            # Преобразуем list в Spark DataFrame
            rows = [Row(cluster_id=i+1, **{name: val for name, val in zip(feature_names, center)})
                    for i, center in enumerate(cluster_centers)]
            df_centers = self.spark.createDataFrame(rows)

            # Переводим в длинный формат для записи в таблицу
            df_to_save = df_centers.select(
                "cluster_id",
                F.explode(
                    F.array([
                        F.struct(F.lit(name).alias("feature_name"), F.col(name).cast("double").alias("feature_value"))
                        for name in feature_names
                    ])
                ).alias("feature")
            ).select(
                "cluster_id",
                F.col("feature.feature_name"),
                F.col("feature.feature_value"),
                F.current_timestamp().alias("created_at")
            )

            df_to_save.write \
                .jdbc(url=jdbc_url,
                    table="cluster_centers",
                    mode="append",
                    properties=connection_properties)

            logger.info("Cluster centers successfully saved to cluster_centers table")

        except Exception as e:
            logger.error(f"Error saving cluster centers to MS SQL Server: {e}")
            raise




    def insert_food_products(self, df: DataFrame, num_rows: int = 10) -> int:
        """
        Insert a batch of food products into MS SQL Server.
        Converts array<struct> product_name into string before insertion.
        """

        logger.info("Schema before insertion:")
        df.printSchema()

        logger.info("Top 10 rows before insertion:")
        df.show(truncate=False, n=10)

        # Ограничиваем количество строк
        df_batch = df.limit(num_rows)

        # Преобразуем product_name в строку (берём первый элемент text, если массив не пуст)
        if "product_name" in df_batch.columns:
            df_batch = df_batch.withColumn(
                "product_name",
                when(size(col("product_name")) > 0, col("product_name")[0]["text"])
                .otherwise(lit(None))
            )

        # Берём только колонки, которые есть в таблице
        target_cols = ["code", "product_name", "energy-kcal", "fat", "carbohydrates", "proteins", "sugars"]
        existing_cols = [c for c in target_cols if c in df_batch.columns]

        # Добавляем created_at
        df_to_save = df_batch.select(*[col(c) for c in existing_cols]).withColumn("created_at", current_timestamp())

        logger.info("Final DataFrame for insertion:")
        df_to_save.show(truncate=False, n=10)

        # JDBC-соединение
        jdbc_url = f"jdbc:sqlserver://{self.mssql_server}:{self.mssql_port};databaseName={self.mssql_database};encrypt=false;trustServerCertificate=true"
        props = {
            "user": self.mssql_username,
            "password": self.mssql_password,
            "driver": self.mssql_driver
        }

        # Вставка через JDBC
        df_to_save.write.jdbc(url=jdbc_url, table="food_products", mode="append", properties=props)

        return df_to_save.count()

    def save_clustering_results_via_datamart(self, df_clustered: DataFrame) -> None:
        """
        Save clustering results via Data Mart API.

        Args:
            df_clustered: DataFrame with clustering results.
        """
        logger.info(f"Saving clustering results via Data Mart API: http://{self.datamart_host}:{self.datamart_port}/api/clustering-results")

        try:
            # Выводим схему DataFrame для отладки
            logger.info("DataFrame schema before conversion:")
            df_clustered.printSchema()

            # Выводим первые несколько строк для отладки
            logger.info("DataFrame sample data:")
            df_clustered.show(5, truncate=False)

            # Преобразуем DataFrame в pandas DataFrame
            pandas_df = df_clustered.limit(1000).toPandas()

            # Логируем типы данных в pandas DataFrame
            logger.info(f"Pandas DataFrame dtypes: {pandas_df.dtypes}")

            # Функция для безопасного преобразования любого объекта в JSON-сериализуемый формат
            def safe_convert(obj):
                # Проверяем тип объекта
                obj_type = type(obj).__name__
                # logger.info(f"Converting object of type: {obj_type}")

                # Если это None, просто возвращаем его
                if obj is None:
                    return None

                # Если это примитивный тип, возвращаем его
                if isinstance(obj, (int, float, str, bool)):
                    return obj

                # Если это DenseVector
                if hasattr(obj, 'tolist'):
                    return obj.tolist()

                # Если это SparseVector или другой объект с методом toArray
                if hasattr(obj, 'toArray'):
                    return obj.toArray().tolist()

                # Если это SparseVector с методом values
                if hasattr(obj, 'values') and hasattr(obj.values, 'tolist'):
                    return obj.values.tolist()

                # Если это список или кортеж, рекурсивно преобразуем его элементы
                if isinstance(obj, (list, tuple)):
                    return [safe_convert(item) for item in obj]

                # Если это словарь, рекурсивно преобразуем его значения
                if isinstance(obj, dict):
                    return {k: safe_convert(v) for k, v in obj.items()}

                # Если ничего не подошло, преобразуем в строку
                return str(obj)

            # Преобразуем все сложные типы в JSON-сериализуемые
            for col in pandas_df.columns:
                if pandas_df[col].dtype.name == 'object':
                    # Логируем информацию о столбце
                    if len(pandas_df[col]) > 0:
                        first_val = pandas_df[col].iloc[0]
                        logger.info(f"Column {col} first value type: {type(first_val).__name__}")

                        # Применяем безопасное преобразование
                        pandas_df[col] = pandas_df[col].apply(safe_convert)

            # Преобразуем в список словарей для JSON
            data = pandas_df.to_dict(orient='records')

            # Логируем первый элемент данных для отладки
            if data:
                logger.info(f"First data item sample: {str(data[0])[:500]}...")

            # Проверяем, что данные можно сериализовать в JSON
            try:
                json_data = json.dumps({"data": data})
                logger.info(f"JSON serialization successful, data size: {len(json_data)} bytes")
            except Exception as e:
                logger.error(f"JSON serialization failed: {e}")
                # Если сериализация не удалась, попробуем найти проблемные поля
                for i, item in enumerate(data):
                    for key, value in item.items():
                        try:
                            json.dumps({key: value})
                        except Exception as e:
                            logger.error(f"Problem with item {i}, field {key}, value type {type(value).__name__}: {e}")
                            # Заменяем проблемное значение на строку
                            item[key] = str(value)

                # Пробуем еще раз сериализовать
                json_data = json.dumps({"data": data})
                logger.info("JSON serialization successful after fixing problematic fields")

            # Отправляем запрос к API витрины данных
            response = requests.post(
                f"http://{self.datamart_host}:{self.datamart_port}/api/clustering-results",
                json={"data": data}
            )

            if response.status_code != 200:
                raise Exception(f"Failed to save clustering results via Data Mart API: {response.status_code} {response.text}")

            # Парсим JSON-ответ
            result = response.json()

            if not result.get("success"):
                raise Exception(f"Data Mart API returned error: {result.get('message')}")

            logger.info("Clustering results successfully saved via Data Mart API")

        except Exception as e:
            logger.error(f"Error saving clustering results via Data Mart API: {e}")
            raise

    def save_cluster_centers_via_datamart(self, cluster_centers: list, feature_names: list[str]) -> None:
        """
        Save cluster centers via Data Mart API.

        Args:
            cluster_centers: list of cluster center rows, e.g., list of lists or dicts
            feature_names: list of feature names corresponding to columns in cluster_centers
        """
        logger.info(f"Saving cluster centers via Data Mart API: http://{self.datamart_host}:{self.datamart_port}/api/cluster-centers")

        try:
            # Логируем информацию о центрах кластеров
            logger.info(f"Cluster centers type: {type(cluster_centers).__name__}")
            if cluster_centers:
                logger.info(f"First center type: {type(cluster_centers[0]).__name__}")
                logger.info(f"First center sample: {str(cluster_centers[0])[:100]}...")

            # Функция для безопасного преобразования любого объекта в JSON-сериализуемый формат
            def safe_convert(obj):
                # Проверяем тип объекта
                obj_type = type(obj).__name__

                # Если это None, просто возвращаем его
                if obj is None:
                    return None

                # Если это примитивный тип, возвращаем его
                if isinstance(obj, (int, float, str, bool)):
                    return obj

                # Если это DenseVector
                if hasattr(obj, 'tolist'):
                    return obj.tolist()

                # Если это SparseVector или другой объект с методом toArray
                if hasattr(obj, 'toArray'):
                    return obj.toArray().tolist()

                # Если это SparseVector с методом values
                if hasattr(obj, 'values') and hasattr(obj.values, 'tolist'):
                    return obj.values.tolist()

                # Если это список или кортеж, рекурсивно преобразуем его элементы
                if isinstance(obj, (list, tuple)):
                    return [safe_convert(item) for item in obj]

                # Если это словарь, рекурсивно преобразуем его значения
                if isinstance(obj, dict):
                    return {k: safe_convert(v) for k, v in obj.items()}

                # Если ничего не подошло, преобразуем в строку
                return str(obj)

            # Преобразуем центры кластеров в список списков для JSON
            centers_list = [safe_convert(center) for center in cluster_centers]

            # Логируем преобразованные данные
            logger.info(f"Converted centers list type: {type(centers_list).__name__}")
            if centers_list:
                logger.info(f"First converted center sample: {str(centers_list[0])[:100]}...")

            # Проверяем, что данные можно сериализовать в JSON
            try:
                json_data = json.dumps({"centers": centers_list, "featureNames": feature_names})
                logger.info(f"JSON serialization successful, data size: {len(json_data)} bytes")
            except Exception as e:
                logger.error(f"JSON serialization failed: {e}")
                # Если сериализация не удалась, преобразуем все в строки
                centers_list = [str(center) for center in centers_list]
                feature_names = [str(name) for name in feature_names]

                # Пробуем еще раз сериализовать
                json_data = json.dumps({"centers": centers_list, "featureNames": feature_names})
                logger.info("JSON serialization successful after converting to strings")

            # Отправляем запрос к API витрины данных
            response = requests.post(
                f"http://{self.datamart_host}:{self.datamart_port}/api/cluster-centers",
                json={"centers": centers_list, "featureNames": feature_names}
            )

            if response.status_code != 200:
                raise Exception(f"Failed to save cluster centers via Data Mart API: {response.status_code} {response.text}")

            # Парсим JSON-ответ
            result = response.json()

            if not result.get("success"):
                raise Exception(f"Data Mart API returned error: {result.get('message')}")

            logger.info("Cluster centers successfully saved via Data Mart API")

        except Exception as e:
            logger.error(f"Error saving cluster centers via Data Mart API: {e}")
            raise
