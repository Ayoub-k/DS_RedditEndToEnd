"""File for storing constants
"""

from enum import Enum

class DateFormat(Enum):
    """For format date
    """
    DATE_FILE_FORMAT = '%Y-%m-%d'

class FileType(Enum):
    """file type 'extenstion of file'
    """
    LOG = '.log'

class PathFolder(Enum):
    """This class for storing paths folders and files
    """
    LOGS = 'logs'
    CONFIG_YAML = 'config/config.yml'

class S3FileType(Enum):
    """Supported file types for S3BucketConnector
    """

    CSV     = '.csv'
    PARQUET = '.parquet'

class TimeFilter(Enum):
    """Storing time filter
    """
    HOUR = 'hour'
    DAY = 'day'
    WEEK = 'week'
    MONTH = 'month'
    YEAR = 'year'
    ALL = 'all'
