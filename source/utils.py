import logging
import toml
from typing import Optional, Dict, Any

def load_config(path: str, mode: str, logger: logging.Logger) -> Dict[str, Any]:
    try:
        with open(path, mode) as file:
            config = toml.load(file)
    except Exception as e:
        logger.error("An error occurred in load_config method..", exc_info=True)
        raise e
    else:
        return config

def read_hudi_options(file_path: str, logger: logging.Logger) -> Dict[str, Any]:
    try:
        with open(file_path, "r") as file:
            hudi_options = toml.load(file)["hoodie_options"]
        return hudi_options
    except Exception as e:
        # Handle the error if the file cannot be read or parsed
        logger.error("An error occurred while reading the TOML file:", exc_info=True)
        raise e
