"""
FastAPI service for food products clustering.
"""
import os
import sys
from typing import Dict, List, Optional

# Add the project root directory to Python path
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

from fastapi import FastAPI, HTTPException, Depends
from pyspark.sql import SparkSession

from src.api.models import ClusteringRequest, ClusteringResponse, HealthResponse
from src.data.loader import DataLoader
from src.data.saver import DataSaver
from src.models.model_loader import ModelLoader
from src.models.kmeans import KMeansClusterer
from src.data.preprocessor import DataPreprocessor
from src.features.scaler import FeatureScaler
from src.utils.config import config
from src.utils.logger import logger



# Создаем FastAPI приложение
app = FastAPI(
    title="Food Products Clustering API",
    description="API for clustering food products using KMeans algorithm",
    version="1.0.0"
)


# Функция для создания Spark-сессии
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
        .config("spark.jars", "file:///opt/mssql-jdbc.jar") \
        .config("spark.driver.extraClassPath", "/opt/mssql-jdbc.jar") \
        .config("spark.executor.extraClassPath", "/opt/mssql-jdbc.jar") \
        .getOrCreate()



    # Set log level
    spark.sparkContext.setLogLevel("ERROR")

    return spark


# Зависимость для получения Spark-сессии
def get_spark() -> SparkSession:
    """
    Get Spark session.

    Returns:
        SparkSession instance.
    """
    spark = create_spark_session()
    try:
        yield spark
    finally:
        # Не закрываем Spark-сессию, так как она будет использоваться для всех запросов
        pass


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check() -> HealthResponse:
    """
    Health check endpoint.

    Returns:
        HealthResponse with status and version.
    """
    return HealthResponse(
        status="ok",
        version="1.0.0"
    )


@app.post("/cluster", response_model=ClusteringResponse, tags=["Clustering"])
async def cluster_data(
    request: ClusteringRequest,
    spark: SparkSession = Depends(get_spark)
) -> ClusteringResponse:
    """
    Cluster data from MS SQL Server.

    Args:
        request: ClusteringRequest with parameters.
        spark: SparkSession instance.

    Returns:
        ClusteringResponse with clustering results.
    """
    try:
        # Загружаем модель
        logger.info("Loading model...")
        model_loader = ModelLoader(spark)
        model = model_loader.get_model()

        if model is None:
            raise HTTPException(status_code=500, detail="Failed to load model")

        # Загружаем данные
        logger.info("Loading data...")
        data_loader = DataLoader(spark)

        if request.use_sample_data:
            df = data_loader.load_sample_data()
        else:
            df = data_loader.load_data_from_mssql()

        # Препроцессинг и масштабирование
        preprocessor = DataPreprocessor()
        df_preprocessed = preprocessor.preprocess_data(df)

        scaler = FeatureScaler()
        df_scaled = scaler.scale_features(df_preprocessed)

        # Предсказание кластеров
        clusterer = KMeansClusterer(model=model)
        df_clustered = clusterer.predict(df_scaled)


        # Оцениваем модель
        logger.info("Evaluating model...")
        silhouette = clusterer.evaluate(df_clustered)

        # Получаем информацию о кластерах
        logger.info("Getting cluster information...")
        cluster_centers = clusterer.get_cluster_centers()
        cluster_sizes = clusterer.get_cluster_sizes(df_clustered)

        # Сохраняем результаты в MS SQL Server
        logger.info("Saving results to MS SQL Server...")
        data_saver = DataSaver(spark)
        data_saver.save_clustering_results(df_clustered)

        # Получаем имена признаков
        feature_names = preprocessor.get_feature_columns()

        # Сохраняем центры кластеров
        data_saver.save_cluster_centers(cluster_centers, feature_names)

        # Формируем ответ
        return ClusteringResponse(
            success=True,
            message="Clustering completed successfully",
            cluster_count=len(cluster_centers),
            silhouette_score=silhouette,
            cluster_sizes={str(k): v for k, v in cluster_sizes.items()}
        )

    except Exception as e:
        logger.error(f"Error in clustering: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    # Для локального запуска
    import uvicorn

    host = config.get('API', 'host')
    port = config.getint('API', 'port')
    debug = config.getboolean('API', 'debug')
    reload = config.getboolean('API', 'reload')

    uvicorn.run("src.api.main:app", host=host, port=port, reload=reload)
