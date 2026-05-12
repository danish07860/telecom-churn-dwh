# from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

import sys
import os

sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../..")
    )
)
from spark_jobs.utils.spark_session import create_spark_session
spark = create_spark_session(
    "TelecomCallsIngestion"
)

# # Create Spark session
# spark = SparkSession.builder \
#     .appName("TelecomCallsIngestion") \
#     .getOrCreate()

# Read raw calls CSV
df = spark.read.csv(
    "data/raw/calls.csv",
    header=True,
    inferSchema=True
)

# print("Raw Schema:")
# df.printSchema()

# Remove duplicates
cleaned_df = df.dropDuplicates()


cleaned_df = cleaned_df.filter(
    col("call_duration_minutes") > 0
)


cleaned_df = cleaned_df.withColumn(
    "network_type",
    upper(col("network_type"))
)


cleaned_df = cleaned_df.fillna({
    "tower_location": "UNKNOWN"
})

# print("Cleaned Calls Preview:")
# cleaned_df.show(5)


cleaned_df.write.mode("overwrite").parquet(
    "data/processed/calls"
)

print("Calls ingestion completed successfully")

spark.stop()