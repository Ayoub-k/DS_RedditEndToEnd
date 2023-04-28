"""Logger
"""
import os
import logging.config
from datetime import datetime

from src.common.utils.paths import Paths
from src.common.utils.config import Config
from src.common.constants.constants import DateFormat, FileType, PathFolder

class Logger:
    """Logger class for logging infos
    """
    def __init__(self, name: str=__name__):

        self.__setup_logging()
        self.logger = logging.getLogger(name)

    def __setup_logging(self):
        """Set up configuration logger
        """
        log_dir = Paths.get_file_path(PathFolder.LOGS.value)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        config_path = Paths.get_file_path(PathFolder.CONFIG_YAML.value)
        file_name = f"{datetime.now().strftime(DateFormat.DATE_FILE_FORMAT.value)}{FileType.LOG.value}"
        log_filename = os.path.join(log_dir, file_name)
        config = Config.get_config(config_path)['logging']
        config['handlers']['file_handler']['filename'] = log_filename
        logging.config.dictConfig(config)

    def info(self, message:str):
        """log infos

        Args:
            message (str): message we want logged
        """
        self.logger.info(message)

    def warning(self, message:str):
        """log warnings

        Args:
            message (str): message we want logged
        """
        self.logger.warning(message)

    def error(self, message:str):
        """log errors

        Args:
            message (str): message we want logged
        """
        self.logger.error(message)

    def critical(self, message:str):
        """log criticals

        Args:
            message (str): message we want logged
        """
        self.logger.critical(message)

    def debug(self, message:str):
        """log debugs

        Args:
            message (str): message we want logged
        """
        self.logger.debug(message)
