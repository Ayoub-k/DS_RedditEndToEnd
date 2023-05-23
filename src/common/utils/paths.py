""" file for handling paths
"""
from pathlib import Path

class Paths:
    """
    Utility class for dealing with paths in the project.

    Provides methods for obtaining the root path of the project and the absolute path to
    a file in the project given its relative path.

    Usage:
    ------
    To obtain the root path of the project:
        project_root = Paths.get_project_root()

    To obtain the absolute path to a file in the project given its relative path:
        file_path = Paths.get_file_path('path/to/file')
    """
    # Define a function to get the root path of the project
    @staticmethod
    def get_project_root() -> Path:
        """
        Returns the absolute path to the root directory of the project.

        Returns:
            A pathlib.Path object represe   nting the absolute path to the root directory
            of the project.
        """
        return Path(__file__).parent.parent.parent.parent.absolute()

    # Define a function to get the path to a file in the project
    @staticmethod
    def get_file_path(relative_path: str) -> Path:
        """
        Returns the absolute path to a file in the project given its relative path.

        Args:
            relative_path (str): The relative path of the file.

        Returns:
            Path: The absolute path of the file.
        """
        file_path = Paths.get_project_root() / relative_path
        if not file_path.exists():
            raise FileNotFoundError(f"The file {file_path} does not exist.")
        return file_path
