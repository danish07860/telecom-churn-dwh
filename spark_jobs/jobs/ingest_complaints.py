from pyspark.sql.functions import (
    upper,
    col,
    to_timestamp
)

import sys
import os


BASE_DIR = os.path.abspath(

    os.path.join(

        os.path.dirname(__file__),

        "../.."

    )

)


sys.path.append(BASE_DIR)


from spark_jobs.utils.logger import (
    get_logger
)

from spark_jobs.utils.spark_session import (
    create_spark_session
)


spark = create_spark_session(
    "TelecomComplaintsIngestion"
)

logger = get_logger(__name__)


RAW_PATH = os.path.join(

    BASE_DIR,

    "data/raw/complaints.csv"

)


PROCESSED_PATH = os.path.join(

    BASE_DIR,

    "data/processed/complaints"

)


WATERMARK_PATH = os.path.join(

    BASE_DIR,

    "metadata/complaints_watermark.txt"

)


logger.info(
    "Loading raw complaints data"
)


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


logger.info(
    f"New complaint records: "
    f"{incremental_df.count()}"
)


if incremental_df.count() == 0:

    logger.info(
        "No new complaint records found"
    )

    spark.stop()

    sys.exit()


cleaned_df = incremental_df.dropDuplicates()


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


logger.info(
    "Complaints ingestion completed successfully"
)


spark.stop()