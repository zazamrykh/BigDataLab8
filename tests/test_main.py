"""
Tests for main module.
"""
import os
import unittest
from unittest.mock import patch, MagicMock

from pyspark.sql import SparkSession

from src.main import create_spark_session, run_clustering_pipeline, save_model, main


class TestMain(unittest.TestCase):
    """
    Test cases for main module.
    """
    def setUp(self):
        """
        Set up test environment.
        """
        # Create mock SparkSession
        self.mock_spark = MagicMock(spec=SparkSession)
        self.mock_spark.version = "3.1.2"
        self.mock_spark.sparkContext = MagicMock()

        # Create mock SparkSession.builder
        self.mock_builder = MagicMock()
        self.mock_builder.appName.return_value = self.mock_builder
        self.mock_builder.master.return_value = self.mock_builder
        self.mock_builder.config.return_value = self.mock_builder
        self.mock_builder.getOrCreate.return_value = self.mock_spark

    @patch('pyspark.sql.SparkSession.builder', new_callable=MagicMock)
    @patch('src.utils.config.config')
    def test_create_spark_session(self, mock_config, mock_builder):
        """
        Test create_spark_session function.
        """
        # Configure mocks
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = self.mock_spark

        # Configure mock config
        mock_config.get.side_effect = lambda section, option, fallback=None: {
            ('SPARK', 'app_name'): 'TestApp',
            ('SPARK', 'master'): 'local[*]',
            ('SPARK', 'driver_memory'): '2g',
            ('SPARK', 'executor_memory'): '2g',
            ('SPARK', 'serializer'): 'org.apache.spark.serializer.KryoSerializer'
        }.get((section, option), fallback)

        mock_config.getint.side_effect = lambda section, option, fallback=None: {
            ('SPARK', 'executor_cores'): 2,
            ('SPARK', 'num_executors'): 2,
            ('SPARK', 'shuffle_partitions'): 100,
            ('SPARK', 'default_parallelism'): 8
        }.get((section, option), fallback)

        # Call create_spark_session
        spark = create_spark_session()

        # Check that builder methods were called with correct parameters
        mock_builder.appName.assert_called_once_with('TestApp')
        mock_builder.master.assert_called_once_with('local[*]')

        # Check that config method was called for each configuration
        self.assertEqual(mock_builder.config.call_count, 7)

        # Check that getOrCreate method was called
        mock_builder.getOrCreate.assert_called_once()

        # Check that setLogLevel method was called
        self.mock_spark.sparkContext.setLogLevel.assert_called_once_with("ERROR")

        # Check returned SparkSession
        self.assertEqual(spark, self.mock_spark)

    @patch('src.main.create_spark_session')
    @patch('src.data.loader.DataLoader')
    @patch('src.data.preprocessor.DataPreprocessor')
    @patch('src.features.scaler.FeatureScaler')
    @patch('src.models.kmeans.KMeansClusterer')
    @patch('src.visualization.visualizer.ClusterVisualizer')
    def test_run_clustering_pipeline(self, mock_visualizer_class, mock_clusterer_class,
                                    mock_scaler_class, mock_preprocessor_class,
                                    mock_loader_class, mock_create_spark_session):
        """
        Test run_clustering_pipeline function.
        """
        # Configure mocks
        mock_create_spark_session.return_value = self.mock_spark

        # Configure mock DataLoader
        mock_loader = MagicMock()
        mock_df = MagicMock()
        mock_df.count.return_value = 100
        mock_loader.load_data.return_value = mock_df
        mock_loader.load_sample_data.return_value = mock_df
        mock_loader_class.return_value = mock_loader

        # Configure mock DataPreprocessor
        mock_preprocessor = MagicMock()
        mock_preprocessor.preprocess_data.return_value = mock_df
        mock_preprocessor.get_feature_columns.return_value = ['energy_100g', 'fat_100g', 'proteins_100g']
        mock_preprocessor_class.return_value = mock_preprocessor

        # Configure mock FeatureScaler
        mock_scaler = MagicMock()
        mock_scaler.scale_features.return_value = mock_df
        mock_scaler_class.return_value = mock_scaler

        # Configure mock KMeansClusterer
        mock_clusterer = MagicMock()
        mock_model = MagicMock()
        mock_clusterer.train.return_value = mock_model
        mock_clusterer.predict.return_value = mock_df
        mock_clusterer.evaluate.return_value = 0.75
        mock_clusterer.get_cluster_centers.return_value = [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]]
        mock_clusterer.get_cluster_sizes.return_value = {0: 10, 1: 20, 2: 30}
        mock_clusterer.get_cluster_samples.return_value = {
            0: [{'code': '123', 'product_name': 'Product 1'}],
            1: [{'code': '456', 'product_name': 'Product 2'}],
            2: [{'code': '789', 'product_name': 'Product 3'}]
        }
        mock_clusterer_class.return_value = mock_clusterer

        # Configure mock ClusterVisualizer
        mock_visualizer = MagicMock()
        mock_visualizer.generate_cluster_report.return_value = 'results/cluster_report.html'
        mock_visualizer_class.return_value = mock_visualizer

        # Call run_clustering_pipeline with default parameters
        results = run_clustering_pipeline()

        # Check that create_spark_session was called
        mock_create_spark_session.assert_called_once()

        # Check that DataLoader was created and load_data was called
        mock_loader_class.assert_called_once_with(self.mock_spark)
        mock_loader.load_data.assert_called_once()

        # Check that DataPreprocessor was created and preprocess_data was called
        mock_preprocessor_class.assert_called_once()
        mock_preprocessor.preprocess_data.assert_called_once_with(mock_df)

        # Check that FeatureScaler was created and scale_features was called
        mock_scaler_class.assert_called_once()
        mock_scaler.scale_features.assert_called_once_with(mock_df)

        # Check that KMeansClusterer was created and methods were called
        mock_clusterer_class.assert_called_once()
        mock_clusterer.train.assert_called_once_with(mock_df)
        mock_clusterer.predict.assert_called_once_with(mock_df)
        mock_clusterer.evaluate.assert_called_once_with(mock_df)
        mock_clusterer.get_cluster_centers.assert_called_once()
        mock_clusterer.get_cluster_sizes.assert_called_once_with(mock_df)
        mock_clusterer.get_cluster_samples.assert_called_once_with(mock_df)

        # Check that ClusterVisualizer was created and methods were called
        mock_visualizer_class.assert_called_once()
        mock_visualizer.visualize_cluster_sizes.assert_called_once_with({0: 10, 1: 20, 2: 30})
        mock_visualizer.visualize_cluster_centers.assert_called_once_with(
            [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]],
            ['energy_100g', 'fat_100g', 'proteins_100g']
        )

        # Check that spark.stop was called
        self.mock_spark.stop.assert_called_once()

        # Check returned results
        self.assertEqual(results['model'], mock_model)
        self.assertEqual(results['silhouette'], 0.75)
        self.assertEqual(results['cluster_centers'], [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]])
        self.assertEqual(results['cluster_sizes'], {0: 10, 1: 20, 2: 30})
        self.assertEqual(results['report_path'], 'results/cluster_report.html')

        # Call run_clustering_pipeline with sample_data=True
        mock_create_spark_session.reset_mock()
        self.mock_spark.reset_mock()

        results = run_clustering_pipeline(use_sample_data=True)

        # Check that load_sample_data was called
        mock_loader.load_sample_data.assert_called_once()

    @patch('os.path.exists')
    @patch('os.makedirs')
    @patch('src.main.run_clustering_pipeline')
    def test_save_model(self, mock_run_clustering_pipeline, mock_makedirs, mock_exists):
        """
        Test save_model function.
        """
        # Configure mocks
        mock_exists.return_value = False

        mock_model = MagicMock()
        mock_run_clustering_pipeline.return_value = {
            'model': mock_model,
            'silhouette': 0.75,
            'cluster_centers': [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]],
            'cluster_sizes': {0: 10, 1: 20, 2: 30}
        }

        # Mock open function
        with patch('builtins.open', unittest.mock.mock_open()) as mock_file:
            # Call save_model
            save_model()

            # Check that directory was created
            mock_makedirs.assert_called_once_with('model')

            # Check that model.save was called
            mock_model.save.assert_called_once_with(os.path.join('model', 'kmeans_model'))

            # Check that file was opened for writing
            mock_file.assert_called_once_with(os.path.join('model', 'metadata.txt'), 'w')

            # Check that write method was called
            file_handle = mock_file()
            self.assertEqual(file_handle.write.call_count, 3)

    @patch('argparse.ArgumentParser')
    @patch('src.main.run_clustering_pipeline')
    @patch('src.main.save_model')
    def test_main(self, mock_save_model, mock_run_clustering_pipeline, mock_arg_parser):
        """
        Test main function.
        """
        # Configure mocks
        mock_parser = MagicMock()
        mock_args = MagicMock()
        mock_args.sample = False
        mock_args.save_model = False
        mock_parser.parse_args.return_value = mock_args
        mock_arg_parser.return_value = mock_parser

        # Call main
        main()

        # Check that run_clustering_pipeline was called with correct parameters
        mock_run_clustering_pipeline.assert_called_once_with(use_sample_data=False)

        # Check that save_model was not called
        mock_save_model.assert_not_called()

        # Reset mocks
        mock_run_clustering_pipeline.reset_mock()

        # Configure args for save_model
        mock_args.save_model = True

        # Call main
        main()

        # Check that save_model was called
        mock_save_model.assert_called_once()

        # Check that run_clustering_pipeline was not called
        mock_run_clustering_pipeline.assert_not_called()


if __name__ == '__main__':
    unittest.main()
