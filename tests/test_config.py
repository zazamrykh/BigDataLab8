"""
Tests for configuration module.
"""
import os
import unittest
from unittest.mock import patch, mock_open

from src.utils.config import Config


class TestConfig(unittest.TestCase):
    """
    Test cases for Config class.
    """
    def setUp(self):
        """
        Set up test environment.
        """
        # Sample config content for testing
        self.config_content = """
        [DEFAULT]
        log_level = INFO
        random_seed = 42

        [DATA]
        data_url = https://example.com/data.csv
        data_dir = data
        sample_size = 1000

        [FEATURES]
        nutriment_features = energy_100g,fat_100g,proteins_100g
        scaling_method = standard
        """

    @patch('os.path.exists')
    def test_init_file_not_found(self, mock_exists):
        """
        Test initialization when config file does not exist.
        """
        mock_exists.return_value = False

        with self.assertRaises(FileNotFoundError):
            Config('nonexistent.ini')

    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    def test_init_success(self, mock_file, mock_exists):
        """
        Test successful initialization.
        """
        mock_exists.return_value = True
        mock_file.return_value.__enter__.return_value.read.return_value = self.config_content

        config = Config('test.ini')
        self.assertIsNotNone(config)

    @patch('os.path.exists')
    @patch('configparser.ConfigParser.read')
    def test_get_methods(self, mock_read, mock_exists):
        """
        Test get methods.
        """
        mock_exists.return_value = True

        # Create a config with mocked configparser
        with patch('configparser.ConfigParser') as mock_configparser:
            # Configure the mock
            mock_instance = mock_configparser.return_value
            mock_instance.get.return_value = 'test_value'
            mock_instance.getint.return_value = 42
            mock_instance.getfloat.return_value = 3.14
            mock_instance.getboolean.return_value = True
            mock_instance.has_section.return_value = True
            mock_instance.__getitem__.return_value = {'key1': 'value1', 'key2': 'value2'}

            # Create config instance
            config = Config('test.ini')

            # Test get methods
            self.assertEqual(config.get('section', 'option'), 'test_value')
            self.assertEqual(config.getint('section', 'option'), 42)
            self.assertEqual(config.getfloat('section', 'option'), 3.14)
            self.assertEqual(config.getboolean('section', 'option'), True)

            # Test getlist method
            with patch.object(config, 'get', return_value='item1,item2,item3'):
                result = config.getlist('section', 'option')
                self.assertEqual(result, ['item1', 'item2', 'item3'])

            # Test get_all_section method
            result = config.get_all_section('section')
            self.assertEqual(result, {'key1': 'value1', 'key2': 'value2'})

    @patch('os.path.exists')
    @patch('configparser.ConfigParser.read')
    def test_getlist_with_empty_value(self, mock_read, mock_exists):
        """
        Test getlist method with empty value.
        """
        mock_exists.return_value = True

        # Create a config with mocked configparser
        with patch('configparser.ConfigParser') as mock_configparser:
            # Configure the mock
            mock_instance = mock_configparser.return_value

            # Create config instance
            config = Config('test.ini')

            # Test getlist with None value
            with patch.object(config, 'get', return_value=None):
                result = config.getlist('section', 'option')
                self.assertEqual(result, [])

                # Test with fallback
                result = config.getlist('section', 'option', fallback=['default'])
                self.assertEqual(result, ['default'])


if __name__ == '__main__':
    unittest.main()
