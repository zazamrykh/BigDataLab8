"""
Tests for visualization module.
"""
import os
import unittest
from unittest.mock import patch, MagicMock, mock_open

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import DataFrame

from src.visualization.visualizer import ClusterVisualizer


class TestClusterVisualizer(unittest.TestCase):
    """
    Test cases for ClusterVisualizer class.
    """
    def setUp(self):
        """
        Set up test environment.
        """
        # Create mock DataFrame
        self.mock_df = MagicMock(spec=DataFrame)

        # Configure mock for select method
        mock_select_df = MagicMock()
        mock_select_df.toPandas.return_value = pd.DataFrame({
            'cluster': [0, 0, 1, 1, 2],
            'energy_100g': [100, 150, 200, 250, 300]
        })
        self.mock_df.select.return_value = mock_select_df

        # Configure mock for distinct method
        mock_distinct_df = MagicMock()
        mock_distinct_df.count.return_value = 3
        self.mock_df.select('cluster').distinct.return_value = mock_distinct_df

        # Sample data for tests
        self.cluster_sizes = {0: 10, 1: 20, 2: 30}
        self.cluster_centers = [
            [100.0, 5.0, 10.0],
            [200.0, 10.0, 20.0],
            [300.0, 15.0, 30.0]
        ]
        self.feature_names = ['energy_100g', 'fat_100g', 'proteins_100g']
        self.cluster_samples = {
            0: [{'code': '123', 'product_name': 'Product 1', 'energy_100g': 100.0}],
            1: [{'code': '456', 'product_name': 'Product 2', 'energy_100g': 200.0}],
            2: [{'code': '789', 'product_name': 'Product 3', 'energy_100g': 300.0}]
        }

        # Create ClusterVisualizer instance
        self.visualizer = ClusterVisualizer()

        # Create output directory if it doesn't exist
        if not os.path.exists(self.visualizer.output_dir):
            os.makedirs(self.visualizer.output_dir)

    def tearDown(self):
        """
        Clean up after tests.
        """
        # Close any open matplotlib figures
        plt.close('all')

    @patch('src.visualization.visualizer.plt.figure')
    @patch('src.visualization.visualizer.plt.bar')
    @patch('src.visualization.visualizer.plt.savefig')
    @patch('src.visualization.visualizer.plt.close')
    def test_visualize_cluster_sizes(self, mock_close, mock_savefig, mock_bar, mock_figure):
        """
        Test visualize_cluster_sizes method.
        """
        # Call visualize_cluster_sizes
        output_path = self.visualizer.visualize_cluster_sizes(self.cluster_sizes)

        # Check that figure was created
        mock_figure.assert_called_once()

        # Check that bar chart was created
        mock_bar.assert_called_once()

        # Check that figure was saved
        mock_savefig.assert_called_once()

        # Check that figure was closed
        mock_close.assert_called_once()

        # Check returned output path
        self.assertEqual(output_path, os.path.join(self.visualizer.output_dir, 'cluster_sizes.png'))

    @patch('src.visualization.visualizer.plt.figure')
    @patch('src.visualization.visualizer.plt.imshow')
    @patch('src.visualization.visualizer.plt.colorbar')
    @patch('src.visualization.visualizer.plt.savefig')
    @patch('src.visualization.visualizer.plt.close')
    def test_visualize_cluster_centers(self, mock_close, mock_savefig, mock_colorbar, mock_imshow, mock_figure):
        """
        Test visualize_cluster_centers method.
        """
        # Call visualize_cluster_centers
        output_path = self.visualizer.visualize_cluster_centers(
            self.cluster_centers, self.feature_names
        )

        # Check that figure was created
        mock_figure.assert_called_once()

        # Check that heatmap was created
        mock_imshow.assert_called_once()

        # Check that colorbar was added
        mock_colorbar.assert_called_once()

        # Check that figure was saved
        mock_savefig.assert_called_once()

        # Check that figure was closed
        mock_close.assert_called_once()

        # Check returned output path
        self.assertEqual(output_path, os.path.join(self.visualizer.output_dir, 'cluster_centers.png'))

    @patch('src.visualization.visualizer.plt.figure')
    @patch('src.visualization.visualizer.plt.boxplot')
    @patch('src.visualization.visualizer.plt.savefig')
    @patch('src.visualization.visualizer.plt.close')
    def test_visualize_feature_distribution(self, mock_close, mock_savefig, mock_boxplot, mock_figure):
        """
        Test visualize_feature_distribution method.
        """
        # Call visualize_feature_distribution
        output_path = self.visualizer.visualize_feature_distribution(
            self.mock_df, 'energy_100g'
        )

        # Check that figure was created
        mock_figure.assert_called_once()

        # Check that boxplot was created
        mock_boxplot.assert_called_once()

        # Check that figure was saved
        mock_savefig.assert_called_once()

        # Check that figure was closed
        mock_close.assert_called_once()

        # Check returned output path
        self.assertEqual(output_path, os.path.join(self.visualizer.output_dir, 'energy_100g_distribution.png'))

    @patch('src.visualization.visualizer.plt.figure')
    @patch('src.visualization.visualizer.plt.scatter')
    @patch('src.visualization.visualizer.plt.savefig')
    @patch('src.visualization.visualizer.plt.close')
    def test_visualize_2d_clusters(self, mock_close, mock_savefig, mock_scatter, mock_figure):
        """
        Test visualize_2d_clusters method.
        """
        # Call visualize_2d_clusters
        output_path = self.visualizer.visualize_2d_clusters(
            self.mock_df, 'energy_100g', 'fat_100g'
        )

        # Check that figure was created
        mock_figure.assert_called_once()

        # Check that scatter plot was created
        self.assertTrue(mock_scatter.called)

        # Check that figure was saved
        mock_savefig.assert_called_once()

        # Check that figure was closed
        mock_close.assert_called_once()

        # Check returned output path
        self.assertEqual(output_path, os.path.join(self.visualizer.output_dir, 'energy_100g_vs_fat_100g.png'))

    @patch('builtins.open', new_callable=mock_open)
    def test_generate_cluster_report(self, mock_file):
        """
        Test generate_cluster_report method.
        """
        # Call generate_cluster_report
        output_path = self.visualizer.generate_cluster_report(
            self.cluster_centers, self.feature_names, self.cluster_sizes, self.cluster_samples
        )

        # Check that file was opened for writing
        mock_file.assert_called_once_with(os.path.join(self.visualizer.output_dir, 'cluster_report.html'), 'w')

        # Check that write method was called
        file_handle = mock_file()
        self.assertTrue(file_handle.write.called)

        # Check returned output path
        self.assertEqual(output_path, os.path.join(self.visualizer.output_dir, 'cluster_report.html'))


if __name__ == '__main__':
    unittest.main()
