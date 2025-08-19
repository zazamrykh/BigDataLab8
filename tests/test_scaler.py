"""
Tests for feature scaler module.
"""
import unittest
from unittest.mock import patch, MagicMock

from pyspark.ml.feature import StandardScaler, MinMaxScaler, MaxAbsScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame

from src.features.scaler import FeatureScaler


class TestFeatureScaler(unittest.TestCase):
    """
    Test cases for FeatureScaler class.
    """
    def setUp(self):
        """
        Set up test environment.
        """
        # Create mock DataFrame
        self.mock_df = MagicMock(spec=DataFrame)

        # Configure mock for transform method
        self.mock_df.transform.return_value = self.mock_df

        # Configure mock for withColumn method
        self.mock_df.withColumn.return_value = self.mock_df

        # Configure mock for collect method
        mock_row = MagicMock()
        mock_row.__getitem__.side_effect = lambda x: 1.0
        self.mock_df.collect.return_value = [mock_row]

        # Create FeatureScaler instance
        with patch('src.utils.config.config') as mock_config:
            # Configure the mock config
            mock_config.get.side_effect = lambda section, option, fallback=None: {
                ('FEATURES', 'scaling_method'): 'standard'
            }.get((section, option), fallback)

            mock_config.getlist.side_effect = lambda section, option, fallback=None: {
                ('FEATURES', 'nutriment_features'): ['energy_100g', 'fat_100g', 'proteins_100g']
            }.get((section, option), fallback)

            self.scaler = FeatureScaler()

    @patch('src.features.scaler.VectorAssembler')
    @patch('src.features.scaler.StandardScaler')
    def test_scale_features_standard(self, mock_standard_scaler_class, mock_vector_assembler_class):
        """
        Test scale_features method with standard scaling.
        """
        # Configure mocks
        mock_vector_assembler = MagicMock()
        mock_vector_assembler.transform.return_value = self.mock_df
        mock_vector_assembler_class.return_value = mock_vector_assembler

        mock_standard_scaler = MagicMock()
        mock_standard_scaler_model = MagicMock()
        mock_standard_scaler_model.transform.return_value = self.mock_df
        mock_standard_scaler.fit.return_value = mock_standard_scaler_model
        mock_standard_scaler_class.return_value = mock_standard_scaler

        # Call scale_features
        result_df = self.scaler.scale_features(self.mock_df)

        # Check that VectorAssembler was created with correct parameters
        mock_vector_assembler_class.assert_called_once_with(
            inputCols=['energy_100g', 'fat_100g', 'proteins_100g'],
            outputCol="features"
        )

        # Check that StandardScaler was created with correct parameters
        mock_standard_scaler_class.assert_called_once_with(
            inputCol="features",
            outputCol="scaled_features",
            withStd=True,
            withMean=True
        )

        # Check that transform and fit methods were called
        mock_vector_assembler.transform.assert_called_once_with(self.mock_df)
        mock_standard_scaler.fit.assert_called_once_with(self.mock_df)
        mock_standard_scaler_model.transform.assert_called_once_with(self.mock_df)

        # Check returned DataFrame
        self.assertEqual(result_df, self.mock_df)

    @patch('src.features.scaler.VectorAssembler')
    @patch('src.features.scaler.MinMaxScaler')
    def test_scale_features_minmax(self, mock_minmax_scaler_class, mock_vector_assembler_class):
        """
        Test scale_features method with minmax scaling.
        """
        # Configure scaler to use minmax
        with patch.object(self.scaler, 'scaling_method', 'minmax'):
            # Configure mocks
            mock_vector_assembler = MagicMock()
            mock_vector_assembler.transform.return_value = self.mock_df
            mock_vector_assembler_class.return_value = mock_vector_assembler

            mock_minmax_scaler = MagicMock()
            mock_minmax_scaler_model = MagicMock()
            mock_minmax_scaler_model.transform.return_value = self.mock_df
            mock_minmax_scaler.fit.return_value = mock_minmax_scaler_model
            mock_minmax_scaler_class.return_value = mock_minmax_scaler

            # Call scale_features
            result_df = self.scaler.scale_features(self.mock_df)

            # Check that MinMaxScaler was created with correct parameters
            mock_minmax_scaler_class.assert_called_once_with(
                inputCol="features",
                outputCol="scaled_features",
                min=0.0,
                max=1.0
            )

            # Check returned DataFrame
            self.assertEqual(result_df, self.mock_df)

    @patch('src.features.scaler.VectorAssembler')
    @patch('src.features.scaler.MaxAbsScaler')
    def test_scale_features_maxabs(self, mock_maxabs_scaler_class, mock_vector_assembler_class):
        """
        Test scale_features method with maxabs scaling.
        """
        # Configure scaler to use maxabs
        with patch.object(self.scaler, 'scaling_method', 'maxabs'):
            # Configure mocks
            mock_vector_assembler = MagicMock()
            mock_vector_assembler.transform.return_value = self.mock_df
            mock_vector_assembler_class.return_value = mock_vector_assembler

            mock_maxabs_scaler = MagicMock()
            mock_maxabs_scaler_model = MagicMock()
            mock_maxabs_scaler_model.transform.return_value = self.mock_df
            mock_maxabs_scaler.fit.return_value = mock_maxabs_scaler_model
            mock_maxabs_scaler_class.return_value = mock_maxabs_scaler

            # Call scale_features
            result_df = self.scaler.scale_features(self.mock_df)

            # Check that MaxAbsScaler was created with correct parameters
            mock_maxabs_scaler_class.assert_called_once_with(
                inputCol="features",
                outputCol="scaled_features"
            )

            # Check returned DataFrame
            self.assertEqual(result_df, self.mock_df)

    @patch('src.features.scaler.VectorAssembler')
    def test_scale_features_unsupported(self, mock_vector_assembler_class):
        """
        Test scale_features method with unsupported scaling method.
        """
        # Configure scaler to use unsupported method
        with patch.object(self.scaler, 'scaling_method', 'unsupported'):
            # Configure mocks
            mock_vector_assembler = MagicMock()
            mock_vector_assembler.transform.return_value = self.mock_df
            mock_vector_assembler_class.return_value = mock_vector_assembler

            # Call scale_features with unsupported method
            with self.assertRaises(ValueError):
                self.scaler.scale_features(self.mock_df)

    def test_extract_scaled_features(self):
        """
        Test extract_scaled_features method.
        """
        # Configure mock for columns property
        self.mock_df.columns = ["code", "product_name", "scaled_features"]

        # Call extract_scaled_features
        result = self.scaler.extract_scaled_features(self.mock_df)

        # Check that withColumn was called for each feature
        self.assertEqual(self.mock_df.withColumn.call_count, 3)

        # Check returned dictionary
        self.assertIsInstance(result, dict)
        self.assertEqual(len(result), 3)
        self.assertIn('energy_100g', result)
        self.assertIn('fat_100g', result)
        self.assertIn('proteins_100g', result)

    def test_extract_scaled_features_no_scaled_features(self):
        """
        Test extract_scaled_features method when scaled_features column is missing.
        """
        # Configure mock for columns property
        self.mock_df.columns = ["code", "product_name"]

        # Call extract_scaled_features
        with self.assertRaises(ValueError):
            self.scaler.extract_scaled_features(self.mock_df)


if __name__ == '__main__':
    unittest.main()
