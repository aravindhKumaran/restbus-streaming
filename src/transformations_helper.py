from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from typing import Optional
from utils import UtilsClass

class TransformationClass(UtilsClass):
    
    def __init__(self):  
        self.module_name = __name__
        super().__init__(self.module_name)


    def read_stream(self, spark: SparkSession, format: str, bootstrap_server: str, topic: str, offset: str) -> Optional:
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
            self.logger.error("An error occurred in read_stream method..", exc_info=True)
            return None 


    def deserialize_df(self, schema: StructType, df) -> Optional:
        try:
            deserialized_df = (df.select
                (col("value").cast("string").alias("str_value"))
                .withColumn("jsonData",from_json(col("str_value"), schema))
            )
        except Exception as e:
            self.logger.error("An error occurred in deserialize_df method..", exc_info=True)
        else:
            return deserialized_df

    def transform_df(self, deserialized_df) -> Optional:
        try:
            transformed_df = deserialized_df.select("jsonData.payload.after.*")
        except Exception as e:
            self.logger.error("An error occurred in transformed_df method..", exc_info=True)
        else:
            return transformed_df

    def write_batch(self, batch_df, batch_id: int) -> None:
        try:
            s3_base_path = self.config['prod']['s3_base_path']
            spark_write_mode = self.config['spark']['write_mode']
            hudi_options = self.read_hudi_options(super().self.conf_path)
            
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
            self.logger.error("An error occurred in write_batch method..", exc_info=True)

    def write_stream(self, transformed_df, checkpointLocation: str, queryName: str) -> None:
        try:
            (transformed_df.writeStream 
                .option("checkpointLocation", checkpointLocation) 
                .queryName(queryName) 
                .foreachBatch(self.write_batch)
                .start() 
                .awaitTermination()
            )
        except Exception as e:
            self.logger.error("An error occurred when calling write_stream() method ...", exc_info=True)

    def write_to_console_local(self, transformed_df) -> None:
        try:
            (transformed_df.writeStream
                .format("console")
                .outputMode("append")
                .start()
                .awaitTermination()
            )
        except Exception as e:
            self.logger.error("An error occurred in write_to_console_local() method:", exc_info=True)
