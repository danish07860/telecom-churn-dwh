import sys
import os

BASE_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../.."
    )
)
sys.path.append(BASE_DIR)
from pyspark.sql.functions import (
    col,
    to_timestamp
)
from spark_jobs.utils.logger import get_logger
from spark_jobs.utils.spark_session import create_spark_session
spark = create_spark_session(
    "TelecomCustomerIngestion"
)
logger = get_logger(__name__)

RAW_PATH = os.path.join(
    BASE_DIR,
    "data/raw/customers.csv"
)

PROCESSED_PATH = os.path.join(
    BASE_DIR,
    "data/processed/customers"
)

WATERMARK_PATH = os.path.join(
    BASE_DIR,
    "metadata/customers_watermark.txt"
)

logger.info("Loading Raw Customer Data")

df = spark.read.csv(
    RAW_PATH,
    header=True,
    inferSchema=True
)


df = df.withColumn(

    "created_at",

    to_timestamp(
        col("created_at")
    )

)

with open(
    WATERMARK_PATH,
    "r"
) as f:

    last_watermark = (
        f.read().strip()
    )


logger.info(
    f"Last watermark: "
    f"{last_watermark}"
)

incremental_df = df.filter(

    col("created_at") > last_watermark

)

cleaned_df = incremental_df.dropDuplicates()


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


cleaned_df.write.mode(
    "append"
).parquet(
    PROCESSED_PATH
)

max_timestamp = cleaned_df.agg(
    {
        "created_at": "max"
    }
).collect()[0][0]


if max_timestamp:

    with open(
        WATERMARK_PATH,
        "w"
    ) as f:

        f.write(
            str(max_timestamp)
        )

    logger.info(
        f"Updated watermark: "
        f"{max_timestamp}"
    )


logger.info("Customer ingestion completed successfully")


spark.stop()