from pyspark.sql.functions import (
    col,
    upper,
    to_timestamp
)

import sys
import os


# -----------------------------
# Base Directory
# -----------------------------
BASE_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../.."
    )
)

sys.path.append(BASE_DIR)


# -----------------------------
# Imports
# -----------------------------
from spark_jobs.utils.spark_session import (
    create_spark_session
)

from spark_jobs.utils.logger import (
    get_logger
)


# -----------------------------
# Logger
# -----------------------------
logger = get_logger(
    "TelecomRechargesIncrementalIngestion"
)


# -----------------------------
# Spark Session
# -----------------------------
spark = create_spark_session(
    "TelecomRechargesIncrementalIngestion"
)


# -----------------------------
# File Paths
# -----------------------------
RAW_PATH = os.path.join(
    BASE_DIR,
    "data/raw/recharges.csv"
)

PROCESSED_PATH = os.path.join(
    BASE_DIR,
    "data/processed/recharges"
)

WATERMARK_PATH = os.path.join(
    BASE_DIR,
    "metadata/recharges_watermark.txt"
)


# -----------------------------
# Read Raw CSV
# -----------------------------
logger.info(
    "Reading raw recharge dataset"
)

df = spark.read.csv(

    RAW_PATH,

    header=True,

    inferSchema=True

)


# -----------------------------
# Convert Timestamp
# -----------------------------
df = df.withColumn(

    "created_at",

    to_timestamp(
        col("created_at")
    )

)


# -----------------------------
# Read Watermark
# -----------------------------
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


# -----------------------------
# Incremental Filtering
# -----------------------------
incremental_df = df.filter(

    col("created_at") > last_watermark

)


logger.info(
    f"New records found: "
    f"{incremental_df.count()}"
)


# -----------------------------
# Cleaning Logic
# -----------------------------
cleaned_df = (

    incremental_df

    .dropDuplicates()

    .filter(
        col("recharge_amount") > 0
    )

    .withColumn(
        "payment_mode",
        upper(col("payment_mode"))
    )

    .filter(
        col("successful_flag") == 1
    )

)


logger.info(
    "Recharge cleaning completed"
)


# -----------------------------
# Append Parquet
# -----------------------------
cleaned_df.write.mode(
    "append"
).parquet(
    PROCESSED_PATH
)


logger.info(
    "Incremental parquet append completed"
)


# -----------------------------
# Update Watermark
# -----------------------------
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


logger.info(
    "Incremental recharge ingestion completed"
)

spark.stop()