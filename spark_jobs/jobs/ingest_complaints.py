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
    "TelecomComplaintsIngestion"
)

# spark = SparkSession.builder \
#     .appName("TelecomComplaintsIngestion") \
#     .getOrCreate()


df = spark.read.csv(
    "data/raw/complaints.csv",
    header=True,
    inferSchema=True
)

# print("Raw Schema:")
# df.printSchema()


cleaned_df = df.dropDuplicates()


cleaned_df = cleaned_df.withColumn(
    "status",
    upper(col("status"))
)


cleaned_df = cleaned_df.filter(
    col("resolution_time_hours") >= 0
)


cleaned_df = cleaned_df.fillna({
    "complaint_type": "UNKNOWN"
})

# print("Cleaned Complaints Preview:")
# cleaned_df.show(5)


cleaned_df.write.mode("overwrite").parquet(
    "data/processed/complaints"
)

print("Complaints ingestion completed successfully")

spark.stop()