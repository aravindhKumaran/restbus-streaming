import pyspark
from pyspark.sql import SparkSession
import toml
import logging
from utils import UtilsClass
from typing import Optional, Union


class GetSparkClass(UtilsClass):
   
    def __init__(self):  
        self.module_name = __name__
        super().__init__(self.module_name)


    def get_spark_obj(self, envn: str, appName: str) -> Optional[SparkSession]:

        try:
            self.logger.info('Started get_spark_obj() method..')
            spark: Optional[SparkSession] = None
            
            if envn == 'dev':
                self.logger.info('Running on dev environment')
                packages = self.config['dev']['packages']
                serializer = self.config['dev']['serializer']
                spark = (SparkSession
                    .builder
                    .appName(appName)
                    .config("spark.serializer", serializer)
                    .config("spark.jars.packages", packages)
                    .getOrCreate()
                )
            elif envn == 'prod':
                self.logger.info('Running on prod environment')
                master = self.config['prod']['master']
                deployMode = self.config['prod']['deployMode']
                packages = self.config['prod']['packages']
                serializer = self.config['prod']['serializer']
                spark = (SparkSession
                    .builder
                    .master(master)
                    .appName(appName)
                    .config("spark.submit.deployMode", "client")
                    .config("spark.serializer", serializer)
                    .config("spark.jars.packages", packages)
                    .getOrCreate()
                )

        except Exception as e:
            self.logger.error('An error occurred in get_spark_obj method.. ', str(e))
            raise
        else:
            self.logger.info('Spark object created..')

        return spark
