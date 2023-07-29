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



def test_spark_obj(spark):
    
    try:
        logger.warning('Started the test_spark_obj method')
        spark
        print(f"spark object created: {spark}")
    except Exception as e:
        logger.error('An error occured in test_spark_obj method.. ', str(e))
        raise
    else:
        logger.warning('Validation done..!')