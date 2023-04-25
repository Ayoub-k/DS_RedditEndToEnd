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
