"""This file for configuration"""

from datetime import datetime
import yaml
from src.common.utils.paths import Paths
from src.common.constants.constants import PathFolder
from dotenv import load_dotenv

class Config:
    """Class for getting configuration"""

    @staticmethod
    def get_config(config_path:str, encoding: str='utf-8')-> dict:
        """read configuration from config.yml

        Args:
            config_path (str): path config.yml

        Returns:
            dict: return a configuration file
        """
        with open(config_path, 'r', encoding=encoding) as config_file:
            config = yaml.safe_load(config_file.read())
            return config

    @staticmethod
    def get_config_yml(section_key: str='') -> dict:
        """_summary_

        Args:
            section_key (str): _description_

        Returns:
            dict: _description_
        """
        config_path = Paths.get_file_path(PathFolder.CONFIG_YAML.value)
        config = Config.get_config(config_path)

        if section_key:
            return config[section_key]
        return config

    @staticmethod
    def load_config():
        """Load .env"""
        load_dotenv()


class TimeFormatter:
    """TimeFormatter for formatting time"""

    @staticmethod
    def format_dttime_now(pattern: str) -> str:
        """
        Format the current datetime as a string using a specific pattern.

        Args:
            pattern (str): The format string to use for the datetime.

        Returns:
            str: The formatted datetime string.
        """
        try:
            formatted_time = datetime.now().strftime(pattern)
        except ValueError:
            raise ValueError(f"Invalid datetime format string: {pattern}")
        return formatted_time
