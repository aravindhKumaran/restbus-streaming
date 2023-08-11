import pyspark
from pyspark.sql.types import *

struct_schema = StructType([
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