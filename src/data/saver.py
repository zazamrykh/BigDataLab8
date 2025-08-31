"""
Module for saving clustering results to MS SQL Server.
"""
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, FloatType, StringType, TimestampType

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
            # jdbc_url = f"jdbc:sqlserver://{self.mssql_server}:{self.mssql_port};databaseName={self.mssql_database}"
            jdbc_url = f"jdbc:sqlserver://{self.mssql_server}:{self.mssql_port};databaseName={self.mssql_database};encrypt=false;trustServerCertificate=true"

            connection_properties = {
                "user": self.mssql_username,
                "password": self.mssql_password,
                "driver": self.mssql_driver
            }

            # Если колонки 'id' нет, используем 'code' как идентификатор
            if "id" in df_clustered.columns:
                id_col = F.col("id").cast(IntegerType())
            elif "code" in df_clustered.columns:
                id_col = F.col("code").cast(StringType())
            else:
                # Если нет ни id, ни code — создаем уникальный id
                id_col = F.monotonically_increasing_id().cast(IntegerType())

            df_to_save = df_clustered.select(
                id_col.alias("id"),
                F.col("cluster").cast(IntegerType()),
                F.current_timestamp().alias("created_at")
            )

            df_to_save.write \
                .jdbc(url=jdbc_url,
                    table=self.mssql_output_table,
                    mode="append",
                    properties=connection_properties)

            logger.info(f"Clustering results successfully saved to {self.mssql_output_table}")

        except Exception as e:
            logger.error(f"Error saving clustering results to MS SQL Server: {e}")
            raise
