"""
Module for K-means clustering model.
"""
from typing import Dict, List, Optional, Tuple

from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

from src.utils.config import config
from src.utils.logger import logger


class KMeansClusterer:
    """
    Class for K-means clustering model.
    """
    def __init__(self, model=None):
        """
        Initialize K-means clusterer.
        """
        self.n_clusters = config.getint('MODEL', 'n_clusters')
        self.max_iterations = config.getint('MODEL', 'max_iterations')
        self.random_seed = config.getint('DEFAULT', 'random_seed')
        self.model = model

    def train(self, df: DataFrame) -> KMeansModel:
        """
        Train K-means clustering model.

        Args:
            df: DataFrame with scaled features.

        Returns:
            Trained K-means model.
        """
        logger.info(f"Training K-means model with {self.n_clusters} clusters...")

        # Check if scaled_features column exists
        if "scaled_features" not in df.columns:
            raise ValueError("Column 'scaled_features' not found in DataFrame")

        # Create K-means model
        kmeans = KMeans(
            k=self.n_clusters,
            seed=self.random_seed,
            maxIter=self.max_iterations,
            featuresCol="scaled_features",
            predictionCol="cluster"
        )

        # Train model
        self.model = kmeans.fit(df)

        logger.info("K-means model training completed")
        return self.model

    def predict(self, df: DataFrame) -> DataFrame:
        """
        Predict clusters for data.

        Args:
            df: DataFrame with scaled features.

        Returns:
            DataFrame with cluster predictions.
        """
        if self.model is None:
            raise ValueError("Model not trained. Call train() first.")

        logger.info("Predicting clusters...")

        # Predict clusters
        df_with_predictions = self.model.transform(df)

        # Convert cluster column to integer type
        df_with_predictions = df_with_predictions.withColumn(
            "cluster",
            F.col("cluster").cast(IntegerType())
        )

        logger.info("Cluster prediction completed")
        return df_with_predictions

    def evaluate(self, df: DataFrame) -> float:
        """
        Evaluate clustering model using Silhouette score.

        Args:
            df: DataFrame with cluster predictions.

        Returns:
            Silhouette score (ranges from -1 to 1, higher is better).
        """
        logger.info("Evaluating clustering model...")

        # Check if required columns exist
        if "scaled_features" not in df.columns or "cluster" not in df.columns:
            raise ValueError("Columns 'scaled_features' and 'cluster' are required for evaluation")

        # Create evaluator
        evaluator = ClusteringEvaluator(
            predictionCol="cluster",
            featuresCol="scaled_features",
            metricName="silhouette"
        )

        # Calculate silhouette score
        silhouette = evaluator.evaluate(df)

        logger.info(f"Silhouette score: {silhouette:.4f}")
        return silhouette

    def get_cluster_centers(self) -> List[List[float]]:
        """
        Get cluster centers.

        Returns:
            List of cluster centers.
        """
        if self.model is None:
            raise ValueError("Model not trained. Call train() first.")

        # Get cluster centers
        centers = self.model.clusterCenters()

        # Convert to list of lists
        return [center.tolist() for center in centers]

    def get_cluster_sizes(self, df: DataFrame) -> Dict[int, int]:
        """
        Get number of samples in each cluster.

        Args:
            df: DataFrame with cluster predictions.

        Returns:
            Dictionary with cluster indices and their sizes.
        """
        if "cluster" not in df.columns:
            raise ValueError("Column 'cluster' not found in DataFrame")

        # Count samples in each cluster
        cluster_counts = df.groupBy("cluster").count().collect()

        # Convert to dictionary
        return {row["cluster"]: row["count"] for row in cluster_counts}

    def get_cluster_samples(self, df: DataFrame, n_samples: int = 5) -> Dict[int, List[Dict]]:
        """
        Get sample products from each cluster.

        Args:
            df: DataFrame with cluster predictions.
            n_samples: Number of samples to get from each cluster.

        Returns:
            Dictionary with cluster indices and lists of sample products.
        """
        if "cluster" not in df.columns:
            raise ValueError("Column 'cluster' not found in DataFrame")

        # Get samples from each cluster
        samples = {}

        for cluster_idx in range(self.n_clusters):
            # Filter products in this cluster
            cluster_df = df.filter(F.col("cluster") == cluster_idx)

            # Take n_samples or all if less
            cluster_samples = cluster_df.limit(n_samples).collect()

            # Convert to list of dictionaries
            samples[cluster_idx] = [
                {col: row[col] for col in row.asDict() if col not in ["features", "scaled_features"]}
                for row in cluster_samples
            ]

        return samples
