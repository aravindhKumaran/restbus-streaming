# Import packages
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import traceback
from typing import Optional

from create_spark import get_spark_obj
from validate import test_spark_obj
from utils import load_config, read_hudi_options
from schema import struct_schema
from log_utils import setup_logger

def read_stream(spark: SparkSession, format: str, bootstrap_server: str, topic: str, offset: str, logger) -> Optional:
    try:
        df = (spark 
                .readStream 
                .format(format) 
                .option("kafka.bootstrap.servers", bootstrap_server) 
                .option("subscribe", topic) 
                .option("startingOffsets", offset) 
                .load()
        )
        return df  # Return the DataFrame if read is successful
    except Exception as e:
        logger.error("An error occurred in read_stream method..", exc_info=True)
        return None 


def deserialize_df(schema: StructType, df, logger) -> Optional:
    try:
        deserialized_df = (df.select
            (col("value").cast("string").alias("str_value"))
            .withColumn("jsonData",from_json(col("str_value"), schema))
        )
    except Exception as e:
        logger.error("An error occurred in deserialize_df method..", exc_info=True)
    else:
        return deserialized_df

def transform_df(deserialized_df, logger) -> Optional:
    try:
        transformed_df = deserialized_df.select("jsonData.payload.after.*")
    except Exception as e:
        logger.error("An error occurred in transformed_df method..", exc_info=True)
    else:
        return transformed_df

def write_batch(batch_df, batch_id: int, s3_base_path: str, spark_write_mode: str, hudi_options: Optional[dict], logger) -> None:
    try:
        # Check if hudi_options is None
        if hudi_options is None:
            raise ValueError("Hudi options are not available. Please check the TOML file.")
        else:
            (batch_df.write.format("org.apache.hudi") 
                .options(**hudi_options) 
                .mode(spark_write_mode) 
                .save(s3_base_path)
            )
    except Exception as e:
        logger.error("An error occurred in write_batch method..", exc_info=True)

def write_stream(transformed_df, checkpointLocation: str, queryName: str, write_batch, logger) -> None:
    try:
        (transformed_df.writeStream 
            .option("checkpointLocation", checkpointLocation) 
            .queryName(queryName) 
            .foreachBatch(write_batch) 
            .start() 
            .awaitTermination()
        )
    except Exception as e:
        logger.error("An error occurred when calling write_stream() method ...", exc_info=True)

def write_to_console_local(transformed_df, logger) -> None:
    try:
        (transformed_df.writeStream
            .format("console")
            .outputMode("append")
            .start()
            .awaitTermination()
        )
    except Exception as e:
        logger.error("An error occurred in write_to_console_local() method:", exc_info=True)

def main(logger, loc_log):
    try: 
        if envn == 'dev':
            logger = setup_logger(log_level='INFO', log_file=loc_log)
            logger.info('logger object is created and file location is local dir')
            logger.info('Started main() method...')
            spark = get_spark_obj(envn, spark_app_name, logger)
        
            if test_spark_obj(spark, logger):
                df = read_stream(spark, spark_read_format, dev_bootstrap_server, kafka_topic, kafka_offset, logger)
                if df is not None:
                    deserialized_df = deserialize_df(schema, df, logger)
                    if deserialized_df is not None:
                        transformed_df = transform_df(deserialized_df, logger)
                        if transformed_df is not None:
                            write_to_console_local(transformed_df, logger)
                        else:
                            logger.error("transformed_df() returned None.")
                    else:
                        logger.error("deserialize_df() returned None.")
                else:
                    logger.error("read_stream() returned None.")
            else:
                logger.error("test_spark_obj() returned False or spark object is None. Exiting the program.")

            
        elif envn == 'prod':
            logger = setup_logger(log_level='INFO', log_file=loc_log)
            logger.info('logger object is created and file location is s3')
            logger.info('Started main() method...')
            spark = get_spark_obj(envn, spark_app_name, logger)
        
            if test_spark_obj(spark, logger):
                df = read_stream(spark, spark_read_format, dev_bootstrap_server, kafka_topic, kafka_offset, logger)
                if df is not None:
                    deserialized_df = deserialize_df(schema, df, logger)
                    if deserialized_df is not None:
                        transformed_df = transform_df(deserialized_df, logger)
                        if transformed_df is not None:
                            hudi_options = read_hudi_options(hoodie_options_path, logger)
                            if hudi_options is not None:
                                write_stream(transformed_df, s3_checkpoint, query_name, write_batch, logger)
                            else:
                                logger.error("read_hudi_options() returned None.")
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
    conf_path = '/home/ubuntu/aws_restbus_proj/source/config.toml'
    loc_log = '/home/ubuntu/aws_restbus_proj/logs/script_logs/streaming.log'  
    
    # Logger setup
    logger = setup_logger(log_level='INFO', log_file=loc_log)
    config = load_config(conf_path, 'r', logger)
    
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

    hoodie_options_path = '/home/ubuntu/aws_restbus_proj/source/config.toml'
    
    #calling main()
    main(logger, loc_log)
    logger.info('Application done!!!')
