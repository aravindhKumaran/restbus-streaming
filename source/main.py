#=====================
#   import packages
#=====================
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

from create_spark import get_spark_obj
from validate import test_spark_obj
import sys
import logging
import hudi_config
from utils import setup_logger, load_config, read_hudi_options
import schema


def read_stream(sparkObj,format,bootstrap_server,topic,offset):
    
    try:
        df = (spark 
                .readStream 
                .format(format) 
                .option("kafka.bootstrap.servers", bootstrap_server) 
                .option("subscribe", topic) 
                .option("startingOffsets", offset) 
                .load()
        )
    except Exception as e:
        logger.error("An error occured in read_kafka method..", exc_info=True)
    else:
        return df

def deserialize_df(schema, df):
    try:
        deserialized_df = (df.select
            (col("value").cast("string").alias("str_value"))
            .withColumn("jsonData",from_json(col("str_value"), schema))
        )
    except Exception as e:
        logger.error("An error occured in deserialize_df method..", exc_info=True)
    else:
        return deserialized_df

def transformed_df(deserialized_df):
    try:
        transformed_df = deserialized_df.select("jsonData.payload.after.*")
    except Exception as e:
        logger.error("An error occured in transformed_df method..", exc_info=True)
    else:
        return transformed_df


def write_batch(batch_df: DataFrame, batch_id: int, s3_path: str, mode: str, hudi_options: dict):
    try:
         # Check if hudi_options is None
        if hudi_options is None:
            raise ValueError("Hudi options are not available. Please check the TOML file.")
        else:
            (batch_df.write.format("org.apache.hudi") 
                .options(**hudi_options) 
                .mode(mode) 
                .save(s3_path)
            )
    except Exception as e:
        logger.error("An error occurred in write_batch method..", exc_info=True)
    

def write_stream(transformed_df,checkpointLocation,queryName,write_batch):
    try:
        (transformed_df
            .writeStream 
            .option("checkpointLocation", checkpointLocation) 
            .queryName(queryName) 
            .foreachBatch(write_batch) 
            .start() 
            .awaitTermination()
        )
    except Exception as e:
        logger.error("An error occurred when calling write_stream() method ...", exc_info=True)


#=====================
#   Main method
#=====================

def main():

    config_path = '/home/ubuntu/aws_restbus_proj/source/config.toml'
    config = load_config(config_path, 'r')

    #====================================================================================
    #environmental variables
    #dev
    loc_log = config['dev']['logs_dir']
    dev_bootstrap_server = config['dev']['bootstrap_server']

    #prod
    s3_logs = config['prod']['logs_dir']
    checkpoint_location = config['prod']['checkpoint_location']

    #kafka
    kafka_topic = config['kafka']['topic']
    kafka_offset = config['kafka']['offset']

    #spark
    spark_read_format = config['spark']['read_format'] 
    schema =  schema.struct_schema  
    hoodie_options_path = '/home/ubuntu/aws_restbus_proj/source/config.toml'

    #=======================================================================================

    logger = setup_logger(log_level='INFO', log_file=loc_log)
    logger.INFO('logger object is created')
    logger.INFO('Started main() method...')
    
    
    try: 
        
        spark = get_spark_obj('dev', 'development')
        
        if test_spark_obj(spark):
            df = read_stream(spark, spark_read_format, dev_bootstrap_server, kafka_topic, kafka_offset)
            
            deserialized_df = deserialize_df(schema, df)
            
            transformed_df = transformed_df(deserialized_df)
            
            hudi_options = read_hudi_options(file_path)
            
            write_batch(batch_df: DataFrame, batch_id: int, s3_path: str, mode: str, hudi_options)
            
            write_stream(transformed_df, checkpoint_location, 'restbus_streaming', write_batch)
            
        else:
            logger.error("Spark validation failed. Exiting the program.")
            sys.exit(1)           
    except Exception as e:
        logger.error("An error occurred when calling main() method...", exc_info=True)
        sys.exit(1)



if __name__ == '__main__':

    main()
    logger.info('Application done!!!')
