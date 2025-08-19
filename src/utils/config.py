"""
Module for working with project configuration.
"""
import configparser
import os
from typing import Any, Dict, List, Optional


class Config:
    """
    Class for working with project configuration.
    """
    def __init__(self, config_path: str = 'config.ini'):
        """
        Initialize configuration.

        Args:
            config_path: Path to the configuration file.
        """
        self.config_path = config_path
        self.config = configparser.ConfigParser()

        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        self.config.read(config_path)

    def get(self, section: str, option: str, fallback: Any = None) -> Any:
        """
        Get value from configuration.

        Args:
            section: Configuration section.
            option: Configuration option.
            fallback: Default value if option is not found.

        Returns:
            Option value.
        """
        return self.config.get(section, option, fallback=fallback)

    def getint(self, section: str, option: str, fallback: Optional[int] = None) -> int:
        """
        Get integer value from configuration.

        Args:
            section: Configuration section.
            option: Configuration option.
            fallback: Default value if option is not found.

        Returns:
            Integer value of the option.
        """
        return self.config.getint(section, option, fallback=fallback)

    def getfloat(self, section: str, option: str, fallback: Optional[float] = None) -> float:
        """
        Get float value from configuration.

        Args:
            section: Configuration section.
            option: Configuration option.
            fallback: Default value if option is not found.

        Returns:
            Float value of the option.
        """
        return self.config.getfloat(section, option, fallback=fallback)

    def getboolean(self, section: str, option: str, fallback: Optional[bool] = None) -> bool:
        """
        Get boolean value from configuration.

        Args:
            section: Configuration section.
            option: Configuration option.
            fallback: Default value if option is not found.

        Returns:
            Boolean value of the option.
        """
        return self.config.getboolean(section, option, fallback=fallback)

    def getlist(self, section: str, option: str, fallback: Optional[List] = None,
                delimiter: str = ',') -> List:
        """
        Get list of values from configuration.

        Args:
            section: Configuration section.
            option: Configuration option.
            fallback: Default value if option is not found.
            delimiter: List elements delimiter.

        Returns:
            List of values.
        """
        value = self.get(section, option)
        if value is None:
            return fallback if fallback is not None else []

        return [item.strip() for item in value.split(delimiter)]

    def get_all_section(self, section: str) -> Dict[str, str]:
        """
        Get all options from a section.

        Args:
            section: Configuration section.

        Returns:
            Dictionary with options and their values.
        """
        if not self.config.has_section(section):
            return {}

        return dict(self.config[section])


# Create a global configuration instance
config = Config()
