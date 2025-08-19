"""
Tests for data preprocessor module.
"""
import unittest
from unittest.mock import patch, MagicMock

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from src.data.preprocessor import DataPreprocessor


class TestDataPreprocessor(unittest.TestCase):
    """
    Test cases for DataPreprocessor class.
    """
    def setUp(self):
        """
        Set up test environment.
        """
        # Create mock DataFrame
        self.mock_df = MagicMock(spec=DataFrame)
        self.mock_df.count.return_value = 10

        # Configure mock for select method
        self.mock_df.select.return_value = self.mock_df

        # Configure mock for filter method
        self.mock_df.filter.return_value = self.mock_df

        # Configure mock for withColumn method
        self.mock_df.withColumn.return_value = self.mock_df

        # Create DataPreprocessor instance
        with patch('src.utils.config.config') as mock_config:
            # Configure the mock config
            mock_config.getfloat.side_effect = lambda section, option, fallback=None: {
                ('PREPROCESSING', 'min_values_threshold'): 0.5,
                ('PREPROCESSING', 'outlier_threshold'): 3.0
            }.get((section, option), fallback)

            mock_config.getboolean.side_effect = lambda section, option, fallback=None: {
                ('PREPROCESSING', 'remove_outliers'): True
            }.get((section, option), fallback)

            mock_config.getlist.side_effect = lambda section, option, fallback=None: {
                ('FEATURES', 'nutriment_features'): ['energy_100g', 'fat_100g', 'proteins_100g']
            }.get((section, option), fallback)

            self.preprocessor = DataPreprocessor()

    def test_preprocess_data_nested(self):
        """
        Test preprocess_data method with nested structure.
        """
        # Configure mock for columns property to simulate nested structure
        self.mock_df.columns = ["code", "product_name", "nutriments"]

        # Call preprocess_data
        result_df = self.preprocessor.preprocess_data(self.mock_df)

        # Check that select was called with correct arguments
        self.mock_df.select.assert_called_with("code", "product_name", "nutriments")

        # Check returned DataFrame
        self.assertEqual(result_df, self.mock_df)

    def test_preprocess_data_flat(self):
        """
        Test preprocess_data method with flat structure.
        """
        # Configure mock for columns property to simulate flat structure
        self.mock_df.columns = ["code", "product_name", "energy_100g", "fat_100g", "proteins_100g"]

        # Call preprocess_data
        result_df = self.preprocessor.preprocess_data(self.mock_df)

        # Check that select was called with correct arguments
        self.mock_df.select.assert_called_with("code", "product_name", "energy_100g", "fat_100g", "proteins_100g")

        # Check returned DataFrame
        self.assertEqual(result_df, self.mock_df)

    def test_extract_nutriment_features(self):
        """
        Test _extract_nutriment_features method.
        """
        # Configure mock for columns property
        self.mock_df.columns = ["code", "product_name", "nutriments"]

        # Call _extract_nutriment_features
        result_df = self.preprocessor._extract_nutriment_features(self.mock_df)

        # Check that select was called with correct arguments
        self.mock_df.select.assert_called_with("code", "product_name")

        # Check that withColumn was called for each feature
        self.assertEqual(self.mock_df.withColumn.call_count, 3)

        # Check returned DataFrame
        self.assertEqual(result_df, self.mock_df)

    def test_extract_nutriment_features_no_nutriments(self):
        """
        Test _extract_nutriment_features method when nutriments column is missing.
        """
        # Configure mock for columns property - no nutriments and no individual features
        self.mock_df.columns = ["code", "product_name"]

        # Call _extract_nutriment_features
        with self.assertRaises(ValueError):
            self.preprocessor._extract_nutriment_features(self.mock_df)

    def test_extract_nutriment_features_flat(self):
        """
        Test _extract_nutriment_features method with flat structure.
        """
        # Configure mock for columns property - individual features present
        self.mock_df.columns = ["code", "product_name", "energy_100g", "fat_100g", "proteins_100g"]

        # Call _extract_nutriment_features
        result_df = self.preprocessor._extract_nutriment_features(self.mock_df)

        # Check returned DataFrame - should return the input DataFrame as is
        self.assertEqual(result_df, self.mock_df)

    def test_remove_null_values(self):
        """
        Test _remove_null_values method.
        """
        # Call _remove_null_values
        result_df = self.preprocessor._remove_null_values(self.mock_df)

        # Check that filter was called
        self.mock_df.filter.assert_called_once()

        # Check returned DataFrame
        self.assertEqual(result_df, self.mock_df)

    @patch('pyspark.sql.functions.mean')
    @patch('pyspark.sql.functions.stddev')
    @patch('pyspark.sql.functions.abs')
    def test_remove_outliers(self, mock_abs, mock_stddev, mock_mean):
        """
        Test _remove_outliers method.
        """
        # Configure mocks
        mock_mean_col = MagicMock()
        mock_stddev_col = MagicMock()
        mock_abs.return_value = MagicMock()

        mock_mean.return_value = mock_mean_col
        mock_stddev.return_value = mock_stddev_col

        # Configure mock for select method to return stats
        mock_stats = MagicMock()
        mock_stats.collect.return_value = [{"mean": 10.0, "stddev": 2.0}]
        self.mock_df.select.return_value = mock_stats

        # Call _remove_outliers
        result_df = self.preprocessor._remove_outliers(self.mock_df)

        # Check that filter was called
        self.mock_df.filter.assert_called_once()

        # Check returned DataFrame
        self.assertEqual(result_df, self.mock_df)

    def test_get_feature_columns(self):
        """
        Test get_feature_columns method.
        """
        # Call get_feature_columns
        features = self.preprocessor.get_feature_columns()

        # Check returned features
        self.assertEqual(features, ['energy_100g', 'fat_100g', 'proteins_100g'])


if __name__ == '__main__':
    unittest.main()
