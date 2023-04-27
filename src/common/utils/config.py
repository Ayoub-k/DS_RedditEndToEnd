"""This file for configuration"""


import yaml

def get_config(config_path:str)-> dict:
    """read configuration from config.yml

    Args:
        config_path (str): path config.yml

    Returns:
        dict: return a configuration file
    """
    with open(config_path, 'r', encoding='utf-8') as config_file:
        config = yaml.safe_load(config_file.read())
        return config
