from pyspark.sql.functions import (
    col,
    upper,
    to_timestamp
)

import sys
import os


sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "../.."
        )
    )
)

from spark_jobs.utils.spark_session import (
    create_spark_session
)


# -----------------------------
# Spark Session
# -----------------------------
spark = create_spark_session(
    "TelecomCallsIncrementalIngestion"
)


# -----------------------------
# Read Raw Calls CSV
# -----------------------------
df = spark.read.csv(
    "data/raw/calls.csv",
    header=True,
    inferSchema=True
)


# -----------------------------
# Convert Timestamp
# -----------------------------
df = df.withColumn(
    "created_at",
    to_timestamp(col("created_at"))
)


# -----------------------------
# Read Last Watermark
# -----------------------------
watermark_file = (
    "metadata/calls_watermark.txt"
)

with open(watermark_file, "r") as f:

    last_watermark = (
        f.read().strip()
    )


print(
    f"Last watermark: {last_watermark}"
)


# -----------------------------
# Incremental Filtering
# -----------------------------
incremental_df = df.filter(
    col("created_at") > last_watermark
)


print(
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
        col("call_duration_minutes") > 0
    )
    .withColumn(
        "network_type",
        upper(col("network_type"))
    )
    .fillna({
        "tower_location": "UNKNOWN"
    })
)


# -----------------------------
# Append Parquet
# -----------------------------
cleaned_df.write.mode(
    "append"
).parquet(
    "data/processed/calls"
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
        watermark_file,
        "w"
    ) as f:

        f.write(
            str(max_timestamp)
        )

    print(
        f"Updated watermark: "
        f"{max_timestamp}"
    )


print(
    "Incremental calls ingestion completed"
)

spark.stop()