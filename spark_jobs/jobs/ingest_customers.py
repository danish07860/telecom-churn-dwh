
from pyspark.sql.functions import col
import sys
import os

sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../..")
    )
)
from spark_jobs.utils.logger import get_logger
from spark_jobs.utils.spark_session import create_spark_session
spark = create_spark_session(
    "TelecomCustomerIngestion"
)
logger = get_logger(__name__)

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

logger.info("Customer ingestion completed successfully")


spark.stop()