#from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, col
import sys
import os

sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../..")
    )
)
from spark_jobs.utils.spark_session import create_spark_session
spark = create_spark_session(
    "TelecomRechargesIngestion"
)
# Create Spark session
# spark = SparkSession.builder \
#     .appName("TelecomRechargesIngestion") \
#     .getOrCreate()

# Read raw recharge CSV
df = spark.read.csv(
    "data/raw/recharges.csv",
    header=True,
    inferSchema=True
)

print("Raw Schema:")
df.printSchema()

# Remove duplicates
cleaned_df = df.dropDuplicates()

# Remove invalid recharge amounts
cleaned_df = cleaned_df.filter(
    col("recharge_amount") > 0
)

# Standardize payment mode
cleaned_df = cleaned_df.withColumn(
    "payment_mode",
    upper(col("payment_mode"))
)

# Keep only successful recharges
cleaned_df = cleaned_df.filter(
    col("successful_flag") == 1
)

print("Cleaned Recharge Preview:")
cleaned_df.show(5)

# Write processed parquet
cleaned_df.write.mode("overwrite").parquet(
    "data/processed/recharges"
)

print("Recharge ingestion completed successfully")

spark.stop()