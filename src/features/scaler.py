"""
Module for scaling features for clustering.
"""
from typing import Dict, List, Optional

from pyspark.ml.feature import StandardScaler, MinMaxScaler, MaxAbsScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType

from src.utils.config import config
from src.utils.logger import logger


class FeatureScaler:
    """
    Class for scaling features for clustering.
    """
    def __init__(self):
        """
        Initialize feature scaler.
        """
        self.scaling_method = config.get('FEATURES', 'scaling_method')
        self.feature_columns = config.getlist('FEATURES', 'nutriment_features')

    def scale_features(self, df: DataFrame) -> DataFrame:
        """
        Scale features for clustering.

        Args:
            df: DataFrame with feature columns.

        Returns:
            DataFrame with scaled features.
        """
        logger.info(f"Scaling features using {self.scaling_method} method...")

        # Assemble features into a vector
        assembler = VectorAssembler(
            inputCols=self.feature_columns,
            outputCol="features"
        )
        df_assembled = assembler.transform(df)

        # Scale features based on the selected method
        if self.scaling_method == "standard":
            # Standardization: (x - mean) / stddev
            scaler = StandardScaler(
                inputCol="features",
                outputCol="scaled_features",
                withStd=True,
                withMean=True
            )
        elif self.scaling_method == "minmax":
            # Min-Max scaling: (x - min) / (max - min)
            scaler = MinMaxScaler(
                inputCol="features",
                outputCol="scaled_features",
                min=0.0,
                max=1.0
            )
        elif self.scaling_method == "maxabs":
            # Max-Abs scaling: x / max(abs(x))
            scaler = MaxAbsScaler(
                inputCol="features",
                outputCol="scaled_features"
            )
        else:
            raise ValueError(f"Unsupported scaling method: {self.scaling_method}")

        # Fit the scaler and transform the data
        scaler_model = scaler.fit(df_assembled)
        df_scaled = scaler_model.transform(df_assembled)

        logger.info("Feature scaling completed")
        return df_scaled

    def extract_scaled_features(self, df: DataFrame) -> Dict[str, List[float]]:
        """
        Extract scaled features for visualization.

        Args:
            df: DataFrame with scaled features.

        Returns:
            Dictionary with feature names and their scaled values.
        """
        # Check if scaled_features column exists
        if "scaled_features" not in df.columns:
            raise ValueError("Column 'scaled_features' not found in DataFrame")

        # Extract individual features from the scaled_features vector
        result = {}
        for i, feature in enumerate(self.feature_columns):
            # Create a new column with the extracted feature
            df_with_feature = df.withColumn(
                f"scaled_{feature}",
                F.col("scaled_features").getItem(i).cast(DoubleType())
            )

            # Collect the values
            values = [row[f"scaled_{feature}"] for row in df_with_feature.collect()]
            result[feature] = values

        return result
