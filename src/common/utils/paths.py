""" file for handling paths
"""
from pathlib import Path

class Paths:
    """
    Utility class for dealing with paths in the project.

    Provides methods for obtaining the root path of the
    project and the absolute path to a file in the project given its
    relative path.

    Usage:
    ------
    project_root = Paths.get_project_root()
    file_path = Paths.get_file_path('path/to/file')
    """
    # Define a function to get the root path of the project
    @staticmethod
    def get_project_root() -> Path:
        """Returns the root path of the project."""
        return Path(__file__).parent.parent.parent.parent.absolute()

    # Define a function to get the path to a file in the project
    @staticmethod
    def get_file_path(relative_path: str) -> Path:
        """Returns the absolute path to a file in the project given its relative path."""
        return Paths.get_project_root() / relative_path
