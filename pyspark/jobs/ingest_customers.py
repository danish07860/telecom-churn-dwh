from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder \
    .appName("TelecomCustomerIngestion") \
    .getOrCreate()


df = spark.read.csv(
    "data/raw/customers.csv",
    header=True,
    inferSchema=True
)


# print("Raw Schema:")
# df.printSchema()


cleaned_df = df.dropDuplicates()


cleaned_df = cleaned_df.fillna({
    "city": "Unknown",
    "state": "Unknown"
})


cleaned_df = cleaned_df.withColumnRenamed(
    "customer_name",
    "customer_full_name"
)


# print("Cleaned Data Preview:")
# cleaned_df.show(5)


cleaned_df.write.mode("overwrite").parquet(
    "data/processed/customers"
)

print("Customer ingestion completed successfully")


spark.stop()