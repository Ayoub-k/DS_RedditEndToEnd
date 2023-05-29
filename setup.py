""" Setup.py
"""

from typing import List
from setuptools import setup, find_packages



def get_requires() -> List[str]:
    """Get requirements from a file and return them as a list

    Args:
        file_path (str): Path to the requirements file

    Returns:
        List[str]: List of requirements
    """

    with open('Pipfile', encoding='utf-8') as file_obj:
        pipfile_content = [line.split('=')[0].strip() for line in file_obj.readlines()]

    # Extract the packages listed under [packages]
    packages_start = pipfile_content.index('[packages]') + 1
    packages_end = pipfile_content.index('[dev-packages]')
    pcks = pipfile_content[packages_start:packages_end]
    pcks = [pack for pack in pcks if not pack.startswith('#') and pack.strip()]

    return pcks

setup(
    name='data_engineer_project',
    version='1.0.0',
    description='Project data engineer End-To-End',
    long_description='Read the README.md file for more details.',
    author='Ayoub Elkhad',
    author_email='ayoubelkhaddouri@gmail.com',
    packages=find_packages(),
    install_requires=get_requires()
)
