import pyspark
from pyspark.sql import SparkSession
import toml
import logging
from utils import setup_logger, load_config

conf_path = '/home/ubuntu/aws_restbus_proj/source/config.toml'
config = load_config(conf_path, 'r')

loc_log = config['dev']['logs_dir']
s3_logs = config['prod']['logs_dir']
logger = setup_logger(log_level='INFO', log_file=loc_log)



def get_spark_obj(envn, appName):
    
    try:
        logger.info('Started get_spark_obj() method..')
        if envn == 'dev':
            logger.info('Running on dev environment')
            master = config['dev']['master']
            deployMode = config['dev']['deployMode']
        elif envn == 'prod':
            logger.info('Running on prod environment')
            master = config['prod']['master']
            deployMode = config['prod']['deployMode']

        spark = (SparkSession
                .builder
                .master(master)
                .appName(appName)
                .config("spark.submit.deployMode", "client")
                .getOrCreate()
            )

    except Exception as e:
        logger.error('An error occured in get_spark_obj method.. ', str(e))
        raise
    else:
        logger.info('Spark object created..')

    return spark