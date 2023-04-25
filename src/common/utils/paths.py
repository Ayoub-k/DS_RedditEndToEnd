""" file for handling paths
"""
import os
from pathlib import Path

# Define a function to get the root path of the project
def get_project_root() -> Path:
    """Returns the root path of the project."""
    return Path(__file__).parent.parent.parent.parent.absolute()

# Define a function to get the path to a file in the project
def get_file_path(relative_path: str) -> Path:
    """Returns the absolute path to a file in the project given its relative path."""
    return get_project_root() / relative_path
