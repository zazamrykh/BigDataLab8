"""
Module for saving clustering results to MS SQL Server.
"""
from typing import Dict, List, Optional

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
