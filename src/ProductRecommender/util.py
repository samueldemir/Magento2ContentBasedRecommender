"""
This is a util python file with helper functions.
"""
import logging
import os
import yaml
from pathlib import Path

from typing import Dict

logger = logging.getLogger("utilLogger")


def get_config_file(config_name) -> Dict:
    """
    This function tries to get the config files. Path should be relative!
    :param config_name:
    :return:
    """
    full_path = os.path.join(get_root(), "conf", config_name)
    check_file_exists(full_path)
    with open(full_path, encoding='utf8') as path_input_file:
        try:
            config_file = yaml.safe_load(path_input_file)
            logger.info(f"Loading of <{config_file}> was successful.")
        except yaml.YAMLError:
            logger.warning(f"The specified file <{config_file}> could not be loaded."
                           f"Please check if the YAML is correct validated.")
    return config_file


def check_file_exists(path_to_conf: str):
    """
    This function checks if a file exists on a specified filepath.
    :param path_to_conf:
    :type path_to_conf:
    :return:
    :rtype:
    """

    if not os.path.exists(path_to_conf):
        raise FileExistsError("The specified file could not be found.")


def get_root() -> Path:
    """
    This function takes the collects the root folder.
    :return:
    :rtype:
    """
    root_path = Path(__file__).parent.resolve()
    return root_path
