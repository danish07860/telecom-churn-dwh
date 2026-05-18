import pandas as pd
import snowflake.connector

import sys
import os


BASE_DIR = os.path.abspath(

    os.path.join(

        os.path.dirname(__file__),

        "../.."

    )

)


sys.path.append(BASE_DIR)


from configs.snowflake_config import (
    SNOWFLAKE_CONFIG
)

from spark_jobs.utils.logger import (
    get_logger
)


logger = get_logger(__name__)


PARQUET_PATH = os.path.join(

    BASE_DIR,

    "data/processed/complaints"

)


WATERMARK_PATH = os.path.join(

    BASE_DIR,

    "metadata/snowflake_complaints_watermark.txt"

)


conn = snowflake.connector.connect(

    user=SNOWFLAKE_CONFIG["user"],

    password=SNOWFLAKE_CONFIG["password"],

    account=SNOWFLAKE_CONFIG["account"],

    warehouse=SNOWFLAKE_CONFIG["warehouse"],

    database=SNOWFLAKE_CONFIG["database"],

    schema=SNOWFLAKE_CONFIG["schema"],

    role=SNOWFLAKE_CONFIG["role"]

)


logger.info(
    "Connected to Snowflake"
)


df = pd.read_parquet(
    PARQUET_PATH
)


df["created_at"] = pd.to_datetime(
    df["created_at"]
)


with open(
    WATERMARK_PATH,
    "r"
) as f:

    last_watermark = (
        f.read().strip()
    )


logger.info(
    f"Last Snowflake watermark: "
    f"{last_watermark}"
)


incremental_df = df[
    df["created_at"] > last_watermark
]


logger.info(
    f"New records to load: "
    f"{len(incremental_df)}"
)


cur = conn.cursor()


cur.execute("""

CREATE TABLE IF NOT EXISTS stg_complaints (

    complaint_id INTEGER,

    customer_id INTEGER,

    complaint_type STRING,

    complaint_date TIMESTAMP,

    status STRING,

    resolution_time_hours INTEGER,

    created_at TIMESTAMP

)

""")


logger.info(
    "stg_complaints table ready"
)


if len(incremental_df) == 0:

    logger.info(
        "No new complaint records found"
    )

    cur.close()

    conn.close()

    sys.exit()


temp_csv_path = os.path.join(

    BASE_DIR,

    "temp_complaints_incremental.csv"

)


incremental_df.to_csv(

    temp_csv_path,

    index=False

)


logger.info(
    "Temporary incremental CSV created"
)


put_command = f"""

PUT file://{temp_csv_path}
@complaints_stage
OVERWRITE = TRUE

"""


cur.execute(
    put_command
)


logger.info(
    "File uploaded to Snowflake stage"
)


copy_command = """

COPY INTO STAGING.stg_complaints

FROM @complaints_stage/temp_complaints_incremental.csv

FILE_FORMAT = (

    FORMAT_NAME = complaints_csv_format

)

"""


cur.execute(
    copy_command
)


logger.info(
    "COPY INTO bulk loading completed"
)


if len(incremental_df) > 0:

    latest_timestamp = (
        incremental_df["created_at"].max()
    )

    with open(
        WATERMARK_PATH,
        "w"
    ) as f:

        f.write(
            str(latest_timestamp)
        )

    logger.info(
        f"Updated Snowflake watermark: "
        f"{latest_timestamp}"
    )


conn.commit()

cur.close()

conn.close()


logger.info(
    "Snowflake connection closed"
)


if os.path.exists(
    temp_csv_path
):

    os.remove(
        temp_csv_path
    )

    logger.info(
        "Temporary CSV removed"
    )