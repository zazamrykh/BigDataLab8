"""
Tests for K-means clustering model.
"""
import unittest
from unittest.mock import patch, MagicMock

import numpy as np
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import DataFrame

from src.models.kmeans import KMeansClusterer


class TestKMeansClusterer(unittest.TestCase):
    """
    Test cases for KMeansClusterer class.
    """
    def setUp(self):
        """
        Set up test environment.
        """
        # Create mock DataFrame
        self.mock_df = MagicMock(spec=DataFrame)

        # Configure mock for columns property
        self.mock_df.columns = ["code", "product_name", "scaled_features", "cluster"]

        # Configure mock for groupBy method
        mock_grouped = MagicMock()
        mock_count = MagicMock()
        mock_count.collect.return_value = [
            {"cluster": 0, "count": 10},
            {"cluster": 1, "count": 20},
            {"cluster": 2, "count": 30}
        ]
        mock_grouped.count.return_value = mock_count
        self.mock_df.groupBy.return_value = mock_grouped

        # Configure mock for filter method
        self.mock_df.filter.return_value = self.mock_df

        # Configure mock for limit method
        self.mock_df.limit.return_value = self.mock_df

        # Configure mock for collect method
        mock_row = MagicMock()
        mock_row.asDict.return_value = {
            "code": "123",
            "product_name": "Test Product",
            "energy_100g": 100.0,
            "fat_100g": 5.0,
            "proteins_100g": 10.0,
            "cluster": 0
        }
        self.mock_df.collect.return_value = [mock_row]

        # Create KMeansClusterer instance
        with patch('src.utils.config.config') as mock_config:
            # Configure the mock config
            mock_config.getint.side_effect = lambda section, option, fallback=None: {
                ('MODEL', 'n_clusters'): 3,
                ('MODEL', 'max_iterations'): 20,
                ('DEFAULT', 'random_seed'): 42
            }.get((section, option), fallback)

            self.clusterer = KMeansClusterer()

    def test_train(self):
        """
        Test train method.
        """
        # Create a real KMeans instance but mock its behavior
        with patch('pyspark.ml.clustering.KMeans') as MockKMeansClass:
            # Configure mocks
            mock_kmeans = MagicMock()
            mock_kmeans_model = MagicMock()
            # Avoid using spec=KMeansModel which causes issues with super()
            mock_kmeans.fit.return_value = mock_kmeans_model
            MockKMeansClass.return_value = mock_kmeans

        # Call train
        model = self.clusterer.train(self.mock_df)

        # Check that KMeans was created with correct parameters
        mock_kmeans_class.assert_called_once_with(
            k=3,
            seed=42,
            maxIter=20,
            featuresCol="scaled_features",
            predictionCol="cluster"
        )

        # Check that fit method was called
        mock_kmeans.fit.assert_called_once_with(self.mock_df)

        # Check returned model
        self.assertEqual(model, mock_kmeans_model)
        self.assertEqual(self.clusterer.model, mock_kmeans_model)

    def test_predict_no_model(self):
        """
        Test predict method when model is not trained.
        """
        # Call predict without training
        with self.assertRaises(ValueError):
            self.clusterer.predict(self.mock_df)

    def test_predict(self):
        """
        Test predict method.
        """
        # Configure mock model
        mock_model = MagicMock(spec=KMeansModel)
        mock_model.transform.return_value = self.mock_df
        self.clusterer.model = mock_model

        # Call predict
        result_df = self.clusterer.predict(self.mock_df)

        # Check that transform method was called
        mock_model.transform.assert_called_once_with(self.mock_df)

        # Check returned DataFrame
        self.assertEqual(result_df, self.mock_df)

    def test_evaluate_no_columns(self):
        """
        Test evaluate method when required columns are missing.
        """
        # Configure mock for columns property
        self.mock_df.columns = ["code", "product_name"]

        # Call evaluate
        with self.assertRaises(ValueError):
            self.clusterer.evaluate(self.mock_df)

    def test_evaluate(self):
        """
        Test evaluate method.
        """
        # Create a real ClusteringEvaluator instance but mock its behavior
        with patch('pyspark.ml.evaluation.ClusteringEvaluator') as MockEvaluatorClass:
            # Configure mocks
            mock_evaluator = MagicMock()
            mock_evaluator.evaluate.return_value = 0.75
            MockEvaluatorClass.return_value = mock_evaluator

        # Call evaluate
        silhouette = self.clusterer.evaluate(self.mock_df)

        # Check that ClusteringEvaluator was created with correct parameters
        mock_evaluator_class.assert_called_once_with(
            predictionCol="cluster",
            featuresCol="scaled_features",
            metricName="silhouette"
        )

        # Check that evaluate method was called
        mock_evaluator.evaluate.assert_called_once_with(self.mock_df)

        # Check returned silhouette score
        self.assertEqual(silhouette, 0.75)

    def test_get_cluster_centers_no_model(self):
        """
        Test get_cluster_centers method when model is not trained.
        """
        # Call get_cluster_centers without training
        with self.assertRaises(ValueError):
            self.clusterer.get_cluster_centers()

    def test_get_cluster_centers(self):
        """
        Test get_cluster_centers method.
        """
        # Configure mock model
        mock_model = MagicMock(spec=KMeansModel)
        mock_center1 = np.array([1.0, 2.0, 3.0])
        mock_center2 = np.array([4.0, 5.0, 6.0])
        mock_center3 = np.array([7.0, 8.0, 9.0])
        mock_model.clusterCenters.return_value = [mock_center1, mock_center2, mock_center3]
        self.clusterer.model = mock_model

        # Call get_cluster_centers
        centers = self.clusterer.get_cluster_centers()

        # Check that clusterCenters method was called
        mock_model.clusterCenters.assert_called_once()

        # Check returned centers
        self.assertEqual(len(centers), 3)
        self.assertEqual(centers[0], [1.0, 2.0, 3.0])
        self.assertEqual(centers[1], [4.0, 5.0, 6.0])
        self.assertEqual(centers[2], [7.0, 8.0, 9.0])

    def test_get_cluster_sizes_no_cluster(self):
        """
        Test get_cluster_sizes method when cluster column is missing.
        """
        # Configure mock for columns property
        self.mock_df.columns = ["code", "product_name"]

        # Call get_cluster_sizes
        with self.assertRaises(ValueError):
            self.clusterer.get_cluster_sizes(self.mock_df)

    def test_get_cluster_sizes(self):
        """
        Test get_cluster_sizes method.
        """
        # Call get_cluster_sizes
        sizes = self.clusterer.get_cluster_sizes(self.mock_df)

        # Check that groupBy method was called
        self.mock_df.groupBy.assert_called_once_with("cluster")

        # Check returned sizes
        self.assertEqual(len(sizes), 3)
        self.assertEqual(sizes[0], 10)
        self.assertEqual(sizes[1], 20)
        self.assertEqual(sizes[2], 30)

    def test_get_cluster_samples_no_cluster(self):
        """
        Test get_cluster_samples method when cluster column is missing.
        """
        # Configure mock for columns property
        self.mock_df.columns = ["code", "product_name"]

        # Call get_cluster_samples
        with self.assertRaises(ValueError):
            self.clusterer.get_cluster_samples(self.mock_df)

    def test_get_cluster_samples(self):
        """
        Test get_cluster_samples method.
        """
        # Call get_cluster_samples
        samples = self.clusterer.get_cluster_samples(self.mock_df)

        # Check that filter method was called for each cluster
        self.assertEqual(self.mock_df.filter.call_count, 3)

        # Check returned samples
        self.assertEqual(len(samples), 3)
        self.assertIn(0, samples)
        self.assertIn(1, samples)
        self.assertIn(2, samples)
        self.assertEqual(len(samples[0]), 1)
        self.assertEqual(samples[0][0]["code"], "123")
        self.assertEqual(samples[0][0]["product_name"], "Test Product")


if __name__ == '__main__':
    unittest.main()
