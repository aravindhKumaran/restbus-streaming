import pyspark
from pyspark.sql import SparkSession
import toml
import logging
from typing import Optional, Any
from utils import UtilsClass

class TestClass(UtilsClass):
     
    def __init__(self):  
        self.module_name = __name__
        super().__init__(self.module_name)

    def test_spark_obj(self, spark):
        try:
            self.logger.warning('Started the test_spark_obj method')
            if spark:
                self.logger.warning('Validation successful!')
                return True
            else:
                self.logger.warning('Validation failed!')
                return False
        except Exception as e:
            self.logger.error('An error occurred in test_spark_obj method:', exc_info=True)
            raise
