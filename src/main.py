"""
Main module for food products clustering application.
"""
import os
import sys
import time
from typing import Dict, List, Optional, Tuple

# Add the project root directory to Python path
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from pyspark.sql import SparkSession

from src.data.loader import DataLoader
from src.data.preprocessor import DataPreprocessor
from src.features.scaler import FeatureScaler
from src.models.kmeans import KMeansClusterer
from src.utils.config import config
from src.utils.logger import logger
from src.visualization.visualizer import ClusterVisualizer


def create_spark_session() -> SparkSession:
    """
    Create Spark session with configuration from config.ini.

    Returns:
        SparkSession instance.
    """
    # Get Spark configuration from config.ini
    app_name = config.get('SPARK', 'app_name')
    master = config.get('SPARK', 'master')
    driver_memory = config.get('SPARK', 'driver_memory')
    executor_memory = config.get('SPARK', 'executor_memory')
    executor_cores = config.getint('SPARK', 'executor_cores')
    num_executors = config.getint('SPARK', 'num_executors')
    shuffle_partitions = config.getint('SPARK', 'shuffle_partitions')
    default_parallelism = config.getint('SPARK', 'default_parallelism')
    serializer = config.get('SPARK', 'serializer')

    logger.info(f"Creating Spark session with app name: {app_name}")

    # Create Spark session with configuration
    spark = SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.executor.memory", executor_memory) \
        .config("spark.executor.cores", str(executor_cores)) \
        .config("spark.executor.instances", str(num_executors)) \
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions)) \
        .config("spark.default.parallelism", str(default_parallelism)) \
        .config("spark.serializer", serializer) \
        .getOrCreate()

    # Set log level
    spark.sparkContext.setLogLevel("ERROR")

    # Log Spark configuration
    logger.info(f"Spark session created: {spark.version}")
    logger.info(f"Spark configuration:")
    logger.info(f"  Master: {master}")
    logger.info(f"  Driver Memory: {driver_memory}")
    logger.info(f"  Executor Memory: {executor_memory}")
    logger.info(f"  Executor Cores: {executor_cores}")
    logger.info(f"  Number of Executors: {num_executors}")
    logger.info(f"  Shuffle Partitions: {shuffle_partitions}")
    logger.info(f"  Default Parallelism: {default_parallelism}")
    logger.info(f"  Serializer: {serializer}")

    return spark


def run_clustering_pipeline(use_sample_data: bool = False) -> Dict:
    """
    Run the complete clustering pipeline.

    Args:
        use_sample_data: Whether to use sample data instead of real data.

    Returns:
        Dictionary with clustering results.
    """
    logger.info("Starting clustering pipeline")
    start_time = time.time()

    # Create Spark session
    spark = create_spark_session()

    try:
        # 1. Load data
        logger.info("Step 1: Loading data")
        data_loader = DataLoader(spark)

        if use_sample_data:
            df = data_loader.load_sample_data()
        else:
            df = data_loader.load_data()

        logger.info(f"Loaded {df.count()} records")

        # 2. Preprocess data
        logger.info("Step 2: Preprocessing data")
        preprocessor = DataPreprocessor()
        df_preprocessed = preprocessor.preprocess_data(df)
        logger.info(f"Preprocessed data: {df_preprocessed.count()} records")

        # 3. Scale features
        logger.info("Step 3: Scaling features")
        scaler = FeatureScaler()
        df_scaled = scaler.scale_features(df_preprocessed)

        # 4. Train clustering model
        logger.info("Step 4: Training clustering model")
        clusterer = KMeansClusterer()
        model = clusterer.train(df_scaled)

        # 5. Predict clusters
        logger.info("Step 5: Predicting clusters")
        df_clustered = clusterer.predict(df_scaled)

        # 6. Evaluate model
        logger.info("Step 6: Evaluating model")
        silhouette = clusterer.evaluate(df_clustered)

        # 7. Get cluster information
        logger.info("Step 7: Getting cluster information")
        cluster_centers = clusterer.get_cluster_centers()
        cluster_sizes = clusterer.get_cluster_sizes(df_clustered)
        cluster_samples = clusterer.get_cluster_samples(df_clustered)

        # 8. Visualize results
        logger.info("Step 8: Visualizing results")
        visualizer = ClusterVisualizer()

        # Visualize cluster sizes
        visualizer.visualize_cluster_sizes(cluster_sizes)

        # Visualize cluster centers
        feature_names = preprocessor.get_feature_columns()
        visualizer.visualize_cluster_centers(cluster_centers, feature_names)

        # Visualize feature distributions for each feature
        for feature in feature_names:
            visualizer.visualize_feature_distribution(df_clustered, feature)

        # Visualize 2D clusters for some feature pairs
        if len(feature_names) >= 2:
            visualizer.visualize_2d_clusters(df_clustered, feature_names[0], feature_names[1])

        # Generate cluster report
        report_path = visualizer.generate_cluster_report(
            cluster_centers, feature_names, cluster_sizes, cluster_samples
        )

        # 9. Package results
        logger.info("Step 9: Packaging results")
        results = {
            "model": model,
            "silhouette": silhouette,
            "cluster_centers": cluster_centers,
            "cluster_sizes": cluster_sizes,
            "cluster_samples": cluster_samples,
            "report_path": report_path
        }

        # Log execution time
        execution_time = time.time() - start_time
        logger.info(f"Clustering pipeline completed in {execution_time:.2f} seconds")

        return results

    finally:
        # Stop Spark session
        logger.info("Stopping Spark session")
        spark.stop()


def save_model(model_path: str = "model") -> None:
    """
    Save trained model and artifacts.

    Args:
        model_path: Path to save model.
    """
    logger.info(f"Saving model to {model_path}")

    # Create model directory if it doesn't exist
    if not os.path.exists(model_path):
        os.makedirs(model_path)

    # Run clustering pipeline
    results = run_clustering_pipeline()

    # Save model
    model = results["model"]
    model.save(os.path.join(model_path, "kmeans_model"))

    # Save model metadata
    with open(os.path.join(model_path, "metadata.txt"), "w") as f:
        f.write(f"Silhouette Score: {results['silhouette']}\n")
        f.write(f"Number of Clusters: {len(results['cluster_centers'])}\n")
        f.write(f"Cluster Sizes: {results['cluster_sizes']}\n")

    logger.info(f"Model saved to {model_path}")


def main() -> None:
    """
    Main function.
    """
    logger.info("Starting Food Products Clustering application")

    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description="Food Products Clustering")
    parser.add_argument("--sample", action="store_true", help="Use sample data")
    parser.add_argument("--save-model", action="store_true", help="Save trained model")
    args = parser.parse_args()

    try:
        if args.save_model:
            save_model()
        else:
            run_clustering_pipeline(use_sample_data=args.sample)

        logger.info("Application completed successfully")

    except Exception as e:
        logger.error(f"Error in application: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
