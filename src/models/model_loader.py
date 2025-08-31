"""
Module for loading saved KMeans model.
"""
import os
from typing import Optional, Dict

from pyspark.ml.clustering import KMeansModel
from pyspark.sql import SparkSession

from src.utils.config import config
from src.utils.logger import logger
from src.data.loader import DataLoader
from src.data.preprocessor import DataPreprocessor
from src.features.scaler import FeatureScaler
from src.models.kmeans import KMeansClusterer


class ModelLoader:
    """
    Class for loading saved KMeans model.
    """
    def __init__(self, spark: SparkSession):
        """
        Initialize model loader.

        Args:
            spark: SparkSession for working with data.
        """
        self.spark = spark
        self.model_path = config.get("MODEL", "model_path", fallback="/app/model/kmeans_model")
        self.model = None

    def load_model(self, train_if_not_exists: bool = True) -> Optional[KMeansModel]:
        """
        Load saved KMeans model.

        Args:
            train_if_not_exists: Whether to train a new model if the saved model does not exist.

        Returns:
            Loaded KMeans model or None if model not found and train_if_not_exists is False.
        """
        # Преобразуем путь к модели в абсолютный
        abs_model_path = os.path.abspath(self.model_path)
        logger.info(f"Loading model from {abs_model_path}")

        # Проверяем, существует ли директория с моделью
        model_exists = os.path.exists(abs_model_path)

        if not model_exists:
            logger.warning(f"Model path {abs_model_path} does not exist")

            if train_if_not_exists:
                logger.info("Training new model...")
                return self._train_and_save_model()
            else:
                return None

        try:
            # Загружаем модель
            self.model = KMeansModel.load(abs_model_path)
            logger.info("Model successfully loaded")
            return self.model
        except Exception as e:
            logger.error(f"Error loading model: {e}")

            if train_if_not_exists:
                logger.info("Training new model due to loading error...")
                return self._train_and_save_model()

            return None


    def _train_and_save_model(self) -> Optional[KMeansModel]:
        """
        Train a new model and save it.

        Returns:
            Trained KMeans model or None if training failed.
        """
        try:
            # 1. Загрузка данных
            logger.info("Step 1: Loading data")
            data_loader = DataLoader(self.spark)
            df = data_loader.load_data()
            logger.info(f"Loaded {df.count()} records")

            # 2. Предобработка данных
            logger.info("Step 2: Preprocessing data")
            preprocessor = DataPreprocessor()
            df_preprocessed = preprocessor.preprocess_data(df)
            logger.info(f"Preprocessed data: {df_preprocessed.count()} records")

            # 3. Масштабирование признаков
            logger.info("Step 3: Scaling features")
            scaler = FeatureScaler()
            df_scaled = scaler.scale_features(df_preprocessed)

            # 4. Обучение модели кластеризации
            logger.info("Step 4: Training clustering model")
            clusterer = KMeansClusterer()
            model = clusterer.train(df_scaled)

            # Создаем директорию для модели, если её нет
            model_dir = os.path.dirname(self.model_path)
            if not os.path.exists(model_dir):
                os.makedirs(model_dir)

            # Сохраняем модель
            model.save(self.model_path)
            logger.info(f"Model saved to {self.model_path}")

            self.model = model
            return model

        except Exception as e:
            logger.error(f"Error training model: {e}")
            return None

    def get_model(self, train_if_not_exists: bool = True) -> Optional[KMeansModel]:
        """
        Get loaded model or load it if not loaded yet.

        Args:
            train_if_not_exists: Whether to train a new model if the saved model does not exist.

        Returns:
            Loaded KMeans model or None if model not found and train_if_not_exists is False.
        """
        if self.model is None:
            return self.load_model(train_if_not_exists)
        return self.model
