import pyspark
from pyspark.sql import SparkSession
import toml
import logging
from log_utils import setup_logger
from typing import Optional, Any

def test_spark_obj((spark: Optional[Any], logger: logging.Logger) -> bool):
    try:
        logger.warning('Started the test_spark_obj method')
        if spark:
            logger.warning('Validation successful!')
            return True
        else:
            logger.warning('Validation failed!')
            return False
    except Exception as e:
        logger.error('An error occurred in test_spark_obj method:', exc_info=True)
        raise
