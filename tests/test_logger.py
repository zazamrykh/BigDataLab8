"""
Tests for logger module.
"""
import logging
import os
import unittest
from unittest.mock import patch, MagicMock

from src.utils.logger import setup_logger


class TestLogger(unittest.TestCase):
    """
    Test cases for logger module.
    """
    def setUp(self):
        """
        Set up test environment.
        """
        # Reset logging configuration before each test
        logging.root.handlers = []
        logging.root.setLevel(logging.NOTSET)

    def test_setup_logger_default(self):
        """
        Test setup_logger with default parameters.
        """
        with patch('src.utils.config.config') as mock_config:
            # Configure the mock
            mock_config.get.return_value = 'INFO'

            # Create logger
            logger = setup_logger()

            # Check logger properties
            self.assertEqual(logger.name, 'food_clustering')
            self.assertEqual(logger.level, logging.INFO)

            # Check handlers
            self.assertEqual(len(logger.handlers), 1)
            self.assertIsInstance(logger.handlers[0], logging.StreamHandler)

    def test_setup_logger_with_file(self):
        """
        Test setup_logger with log file.
        """
        with patch('src.utils.config.config') as mock_config:
            # Configure the mock
            mock_config.get.return_value = 'DEBUG'

            # Create temporary log directory
            log_dir = 'test_logs'
            log_file = os.path.join(log_dir, 'test.log')

            try:
                # Create logger with file
                with patch('os.makedirs') as mock_makedirs:
                    logger = setup_logger(log_level='DEBUG', log_file=log_file)

                    # Check logger properties
                    self.assertEqual(logger.name, 'food_clustering')
                    self.assertEqual(logger.level, logging.DEBUG)

                    # Check handlers
                    self.assertEqual(len(logger.handlers), 2)
                    self.assertIsInstance(logger.handlers[0], logging.StreamHandler)
                    self.assertIsInstance(logger.handlers[1], logging.FileHandler)

                    # Check if directory was created
                    mock_makedirs.assert_called_once_with(log_dir)
            finally:
                # Clean up
                if os.path.exists(log_file):
                    os.remove(log_file)
                if os.path.exists(log_dir):
                    os.rmdir(log_dir)

    def test_setup_logger_invalid_level(self):
        """
        Test setup_logger with invalid log level.
        """
        with patch('src.utils.config.config') as mock_config:
            # Configure the mock
            mock_config.get.return_value = 'INVALID_LEVEL'

            # Create logger with invalid level
            with self.assertRaises(ValueError):
                setup_logger()

    def test_setup_logger_existing_handlers(self):
        """
        Test setup_logger with existing handlers.
        """
        with patch('src.utils.config.config') as mock_config:
            # Configure the mock
            mock_config.get.return_value = 'INFO'

            # Create logger with existing handlers
            logger_name = 'test_logger'
            logger = logging.getLogger(logger_name)
            handler = logging.StreamHandler()
            logger.addHandler(handler)

            # Setup logger again
            logger = setup_logger(name=logger_name)

            # Check that old handlers were removed
            self.assertEqual(len(logger.handlers), 1)


if __name__ == '__main__':
    unittest.main()
