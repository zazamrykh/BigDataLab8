"""
Module for loading data from OpenFoodFacts.
"""
import os
import urllib.request
from typing import Optional, Dict

import pandas as pd
from pyspark.sql import DataFrame, SparkSession

from src.utils.config import config
from src.utils.logger import logger


class DataLoader:
    """
    Class for loading data from OpenFoodFacts.
    """
    def __init__(self, spark: SparkSession):
        """
        Initialize data loader.

        Args:
            spark: SparkSession for working with data.
        """
        self.spark = spark
        self.data_url = config.get('DATA', 'data_url')
        self.data_dir = config.get('DATA', 'data_dir')
        self.sample_size = config.getint('DATA', 'sample_size')
        self.data_format = config.get('DATA', 'data_format', fallback='parquet')

        # MS SQL Server settings
        self.mssql_server = config.get('MSSQL', 'server')
        self.mssql_port = config.get('MSSQL', 'port')
        self.mssql_database = config.get('MSSQL', 'database')
        self.mssql_username = config.get('MSSQL', 'username')
        self.mssql_password = config.get('MSSQL', 'password')
        self.mssql_driver = config.get('MSSQL', 'driver')
        self.mssql_input_table = config.get('MSSQL', 'input_table')
        self.mssql_output_table = config.get('MSSQL', 'output_table')

        # Create data directory if it doesn't exist
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

    def download_data(self, force_download: bool = False) -> str:
        """
        Download data from OpenFoodFacts.

        Args:
            force_download: Force download even if file already exists.

        Returns:
            Path to downloaded file.
        """
        # Get filename from URL
        url_path = self.data_url.split('?')[0]  # Remove query parameters
        filename = os.path.basename(url_path)

        # Add extension if not present
        if not filename.endswith(f'.{self.data_format}'):
            filename = f"{filename}.{self.data_format}"

        filepath = os.path.join(self.data_dir, filename)

        # Check if file exists
        if os.path.exists(filepath) and not force_download:
            logger.info(f"File {filepath} already exists, skipping download.")
            return filepath

        # Download file
        logger.info(f"Downloading data from {self.data_url}...")
        try:
            urllib.request.urlretrieve(self.data_url, filepath)
            logger.info(f"Data successfully downloaded to {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Error downloading data: {e}")
            raise

    # --- вставка в src/loader.py (замена тела load_data) ---
    def load_data(self, filepath: Optional[str] = None) -> DataFrame:
        if filepath is None:
            filepath = os.path.join(self.data_dir, os.path.basename(self.data_url))

        if not os.path.exists(filepath):
            logger.warning(f"File {filepath} does not exist. Attempting to download data...")
            filepath = self.download_data()

        logger.info(f"Loading data from {filepath} with format {self.data_format}...")

        try:
            # --- get available columns from parquet schema (fast metadata read) ---
            if self.data_format == 'parquet':
                all_columns = self.spark.read.parquet(filepath).schema.names
            else:
                # for csv/json we'll simply let Spark infer schema on read
                all_columns = None

            # basic columns we always want
            base_cols = ["code", "product_name", "nutriments"]

            # Try to include features from config if they exist in file (for flat columns)
            desired_features = config.getlist('FEATURES', 'nutriment_features')
            # normalize desired names (strings)
            desired_features = [f.strip() for f in desired_features if f.strip()]

            columns_to_read = []
            # If we have schema (parquet), filter desired features by existence
            if all_columns is not None:
                for c in base_cols + desired_features:
                    if c in all_columns:
                        columns_to_read.append(c)
                # ensure we at least read nutriments (for extracting nutrients)
                if "nutriments" not in columns_to_read and "nutriments" in all_columns:
                    columns_to_read.append("nutriments")
            else:
                # fallback: read base cols + desired features (Spark will ignore missing on select)
                columns_to_read = base_cols + desired_features

            logger.info(f"Columns to read: {columns_to_read}")

            # Now read only these columns
            if self.data_format == 'parquet':
                reader = self.spark.read.parquet(filepath).select(*columns_to_read)
            elif self.data_format == 'csv':
                reader = self.spark.read.csv(filepath, header=True, inferSchema=True, sep='\t')
            elif self.data_format == 'json':
                reader = self.spark.read.json(filepath)
            else:
                raise ValueError(f"Unsupported file format: {self.data_format}")

            # Apply sample limit *after* selecting columns (reduces scanned data)
            if self.sample_size > 0:
                logger.info(f"Taking first {self.sample_size} records (after selecting columns)...")
                df = reader.limit(self.sample_size)
            else:
                df = reader

            logger.info("DataFrame schema (loaded):")
            df.printSchema()
            logger.info(f"DataFrame columns: {df.columns[:50]}")

            # NOTE: avoid expensive df.count() on huge data unless necessary
            try:
                cnt = df.count()
                logger.info(f"Data successfully loaded: {cnt} records")
            except Exception:
                logger.info("Skipping df.count() because it may be expensive for large datasets")

            return df

        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise

    def load_data_from_mssql(self) -> DataFrame:
        """
        Load data from MS SQL Server.

        Returns:
            Spark DataFrame with data from MS SQL Server.
        """
        logger.info(f"Loading data from MS SQL Server: {self.mssql_server}:{self.mssql_port}/{self.mssql_database}")

        try:
            # Формируем URL для подключения к MS SQL Server
            # jdbc_url = f"jdbc:sqlserver://{self.mssql_server}:{self.mssql_port};databaseName={self.mssql_database}"
            jdbc_url = f"jdbc:sqlserver://{self.mssql_server}:{self.mssql_port};databaseName={self.mssql_database};encrypt=false;trustServerCertificate=true"

            # Настройки подключения
            connection_properties = {
                "user": self.mssql_username,
                "password": self.mssql_password,
                "driver": self.mssql_driver
            }

            # Загружаем данные из таблицы
            df = self.spark.read \
                .jdbc(url=jdbc_url,
                      table=self.mssql_input_table,
                      properties=connection_properties)

            # Выводим схему данных
            logger.info("DataFrame schema (loaded from MS SQL Server):")
            df.printSchema()
            logger.info(f"DataFrame columns: {df.columns[:50]}")

            # Подсчитываем количество записей
            try:
                cnt = df.count()
                logger.info(f"Data successfully loaded from MS SQL Server: {cnt} records")
            except Exception:
                logger.info("Skipping df.count() because it may be expensive for large datasets")

            return df

        except Exception as e:
            logger.error(f"Error loading data from MS SQL Server: {e}")
            raise

    def load_sample_data(self) -> DataFrame:
        """
        Load a small sample of data for testing, names match config.
        Returns:
            Spark DataFrame with sample data.
        """
        logger.info("Loading test sample data...")

        # Используем имена признаков из конфига
        from src.utils.config import config
        features = [f.strip() for f in config.getlist("FEATURES", "nutriment_features")]

        data = [
            {"code": "1", "product_name": "Test Product 1", **{features[0]: 100.0, features[1]: 5.0, features[2]: 10.0, features[3]: 2.0, features[4]: 5.0}},
            {"code": "2", "product_name": "Test Product 2", **{features[0]: 200.0, features[1]: 10.0, features[2]: 20.0, features[3]: 5.0, features[4]: 10.0}},
            {"code": "3", "product_name": "Test Product 3", **{features[0]: 300.0, features[1]: 15.0, features[2]: 30.0, features[3]: 10.0, features[4]: 15.0}},
            {"code": "4", "product_name": "Test Product 4", **{features[0]: 400.0, features[1]: 20.0, features[2]: 40.0, features[3]: 15.0, features[4]: 20.0}},
            {"code": "5", "product_name": "Test Product 5", **{features[0]: 500.0, features[1]: 25.0, features[2]: 50.0, features[3]: 20.0, features[4]: 25.0}},
        ]

        import pandas as pd
        pdf = pd.DataFrame(data)

        # Преобразуем в Spark DataFrame
        df = self.spark.createDataFrame(pdf)
        logger.info(f"Test sample data successfully loaded: {df.count()} records")
        return df
