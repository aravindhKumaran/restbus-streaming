import pyspark
from pyspark.sql import SparkSession
import toml
import logging
from utils import load_config
from log_utils import setup_logger
from typing import Optional, Union

def get_spark_obj(envn: str, appName: str, logger: logging.Logger) -> Optional[SparkSession]:
    config_path = '/home/ubuntu/aws_restbus_proj/source/config.toml'
    config = load_config(config_path, 'r', logger)
    try:
        logger.info('Started get_spark_obj() method..')
        spark: Optional[SparkSession] = None
        
        if envn == 'dev':
            logger.info('Running on dev environment')
            packages = config['dev']['packages']
            serializer = config['dev']['serializer']
            spark = (SparkSession
                .builder
                .config("spark.serializer", serializer)
                .config("spark.jars.packages", packages)
                .getOrCreate()
            )
        elif envn == 'prod':
            logger.info('Running on prod environment')
            master = config['prod']['master']
            deployMode = config['prod']['deployMode']
            packages = config['prod']['packages']
            serializer = config['prod']['serializer']
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
        logger.error('An error occurred in get_spark_obj method.. ', str(e))
        raise
    else:
        logger.info('Spark object created..')

    return spark
