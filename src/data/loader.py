"""
Module for loading data from OpenFoodFacts.
"""
import os
import urllib.request
from typing import Optional

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


    def load_sample_data(self) -> DataFrame:
        """
        Load a small sample of data for testing.

        Returns:
            Spark DataFrame with sample data.
        """
        logger.info("Loading test sample data...")

        # Instead of using nested dictionaries, create a flattened structure
        # with individual nutriment columns
        data = [
            {"code": "1", "product_name": "Test Product 1", "energy_100g": 100.0, "fat_100g": 5.0, "carbohydrates_100g": 10.0, "proteins_100g": 2.0, "sugars_100g": 5.0, "fiber_100g": 1.0, "salt_100g": 0.1},
            {"code": "2", "product_name": "Test Product 2", "energy_100g": 200.0, "fat_100g": 10.0, "carbohydrates_100g": 20.0, "proteins_100g": 5.0, "sugars_100g": 10.0, "fiber_100g": 2.0, "salt_100g": 0.2},
            {"code": "3", "product_name": "Test Product 3", "energy_100g": 300.0, "fat_100g": 15.0, "carbohydrates_100g": 30.0, "proteins_100g": 10.0, "sugars_100g": 15.0, "fiber_100g": 3.0, "salt_100g": 0.3},
            {"code": "4", "product_name": "Test Product 4", "energy_100g": 400.0, "fat_100g": 20.0, "carbohydrates_100g": 40.0, "proteins_100g": 15.0, "sugars_100g": 20.0, "fiber_100g": 4.0, "salt_100g": 0.4},
            {"code": "5", "product_name": "Test Product 5", "energy_100g": 500.0, "fat_100g": 25.0, "carbohydrates_100g": 50.0, "proteins_100g": 20.0, "sugars_100g": 25.0, "fiber_100g": 5.0, "salt_100g": 0.5}
        ]

        # Create Pandas DataFrame
        pdf = pd.DataFrame(data)

        # Convert to Spark DataFrame with explicit schema
        df = self.spark.createDataFrame(pdf)

        logger.info(f"Test sample data successfully loaded: {df.count()} records")
        return df
