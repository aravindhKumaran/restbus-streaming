# Import packages
import traceback
from typing import Optional
import toml

from properties.schema import struct_schema
from transformations_helper import TransformationClass
from create_spark import GetSparkClass
from tests import TestClass
from utils import UtilsClass


def main():
    try: 
        #creating objects 
        sparkObj = GetSparkClass()
        t = TransformationClass()
        tc = TestClass()
        

        if envn == 'dev':
            logger = utils_obj.create_dev_logger("app.log", mode="a")

            logger.info('logger object is created and file location is local dir')
            logger.info('Started main() method...')
            
            spark = sparkObj.get_spark_obj(envn, spark_app_name)
      
            if tc.test_spark_obj(spark):
                df = t.read_stream(spark, spark_read_format, dev_bootstrap_server, kafka_topic, kafka_offset)
                if df is not None:
                    deserialized_df = t.deserialize_df(schema, df)
                    if deserialized_df is not None:
                        transformed_df = t.transform_df(deserialized_df)
                        if transformed_df is not None:
                            t.write_to_console_local(transformed_df)
                        else:
                            logger.error("transformed_df() returned None.")
                    else:
                        logger.error("deserialize_df() returned None.")
                else:
                    logger.error("read_stream() returned None.")
            else:
                logger.error("test_spark_obj() returned False or spark object is None. Exiting the program.")

            
        elif envn == 'prod':
            logger = utils_obj.create_dev_logger("app.log", mode="a")

            logger.info('logger object is created and file location is s3')
            logger.info('Started main() method...')
            spark = sparkObj.get_spark_obj(envn, spark_app_name)
        
            if tc.test_spark_obj(spark):
                df = t.read_stream(spark, spark_read_format, prod_bootstrap_server, kafka_topic, kafka_offset)
                if df is not None:
                    deserialized_df = t.deserialize_df(schema, df)
                    if deserialized_df is not None:
                        transformed_df = t.transform_df(deserialized_df)
                        if transformed_df is not None:
                            t.write_stream(transformed_df, s3_checkpoint, query_name)
                        else:
                            logger.error("transformed_df() returned None.")
                    else:
                        logger.error("deserialize_df() returned None.")
                else:
                    logger.error("read_stream() returned None.")
            else:
                logger.error("test_spark_obj() returned False or spark object is None. Exiting the program.")  

        else:
            logger.error("Environment is not set")
            sys.exit(1)   
               
    except Exception as e:
        logger.error("An error occurred when calling main() method...", exc_info=True)
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    
    utils_obj = UtilsClass(__name__)
    config = utils_obj.config

    # Environmental variables
    # Dev
    dev_log_level = config['dev']['log_level']
    loc_log = config['dev']['logs_dir']
    dev_bootstrap_server = config['dev']['bootstrap_server']

    # Prod
    prod_bootstrap_server = config['prod']['bootstrap_server']
    prod_log_level = config['prod']['log_level']
    s3_logs = config['prod']['logs_dir']
    s3_checkpoint = config['prod']['checkpoint_location']
    s3_base_path = config['prod']['s3_base_path']

    # Kafka
    kafka_topic = config['kafka']['topic']
    kafka_offset = config['kafka']['offset']

    # Spark
    envn = config['spark']['envn']
    spark_app_name = config['spark']['app_name']
    schema = struct_schema
    spark_read_format = config['spark']['read_format'] 
    spark_write_mode = config['spark']['write_mode']
    query_name = config['spark']['query_name']
    
    #calling main()
    main()

