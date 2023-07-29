from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


BOOTSTRAP_SERVERS='b-2.restbus.wc8w4g.c2.kafka.ca-central-1.amazonaws.com:9092,b-1.restbus.wc8w4g.c2.kafka.ca-central-1.amazonaws.com:9092,b-3.restbus.wc8w4g.c2.kafka.ca-central-1.amazonaws.com:9092'

checkpoint_location = "s3://local-restbus/checkpoint/"
s3_path = "s3://local-restbus/routes/"


if __name__ == "__main__":

    spark = SparkSession.builder.getOrCreate()

    schema = StructType([
    StructField("payload", StructType([
        StructField("after", StructType([
            StructField("record_id", IntegerType(), True),
            StructField("id", IntegerType(), True),
            StructField("routeId", IntegerType(), True),
            StructField("directionId", StringType(), True),
            StructField("predictable", IntegerType(), True),
            StructField("secsSinceReport", IntegerType(), True),
            StructField("kph", IntegerType(), True),
            StructField("heading", IntegerType(), True),
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True),
            StructField("leadingVehicleId", IntegerType(), True),
            StructField("event_time", LongType(), True)
        ]))
    ]))
])


    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("subscribe", "dbserver1.demo.bus_status") \
        .option("startingOffsets", "latest") \
        .load()

    transform_df = (
        df.select(col("value").cast("string").alias("str_value"))
        .withColumn("jsonData",from_json(col("str_value"),schema))
        .select("jsonData.payload.after.*")
        )

    table_name = 'bus_status'
    hudi_options = {
        'hoodie.table.name': table_name,
        'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
        'hoodie.datasource.write.recordkey.field': 'record_id',
        'hoodie.datasource.write.partitionpath.field': 'routeId',
        'hoodie.datasource.write.table.name': table_name,
        'hoodie.datasource.write.operation': 'upsert',
        'hoodie.datasource.write.precombine.field': 'event_time',
        'hoodie.upsert.shuffle.parallelism': 100,
        'hoodie.insert.shuffle.parallelism': 100
   }

   

    def write_batch(batch_df, batch_id):
        batch_df.write.format("org.apache.hudi") \
        .options(**hudi_options) \
        .mode("append") \
        .save(s3_path)

    transform_df.writeStream.option("checkpointLocation", checkpoint_location).queryName("rest-bus-streaming").foreachBatch(write_batch).start().awaitTermination()