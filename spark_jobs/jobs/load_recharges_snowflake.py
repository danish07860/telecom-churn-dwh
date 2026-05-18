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

import pandas as pd
import snowflake.connector

from configs.snowflake_config import (
    SNOWFLAKE_CONFIG
)

from spark_jobs.utils.logger import (
    get_logger
)


# -----------------------------
# Base Directory
# -----------------------------
BASE_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../.."
    )
)


# -----------------------------
# Logger
# -----------------------------
logger = get_logger(__name__)


# -----------------------------
# File Paths
# -----------------------------
PARQUET_PATH = os.path.join(
    BASE_DIR,
    "data/processed/recharges"
)

WATERMARK_PATH = os.path.join(
    BASE_DIR,
    "metadata/snowflake_recharges_watermark.txt"
)


# -----------------------------
# Connect Snowflake
# -----------------------------
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

cur = conn.cursor()


# -----------------------------
# Read Processed Parquet
# -----------------------------
df = pd.read_parquet(
    PARQUET_PATH
)

logger.info(
    f"Loaded {len(df)} parquet records"
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
    f"Last Snowflake watermark: "
    f"{last_watermark}"
)


# -----------------------------
# Convert Timestamp
# -----------------------------
df["created_at"] = pd.to_datetime(
    df["created_at"]
)


# -----------------------------
# Incremental Filtering
# -----------------------------
incremental_df = df[
    df["created_at"] > last_watermark
]


logger.info(
    f"New records to load: "
    f"{len(incremental_df)}"
)


# -----------------------------
# Create Staging Table
# -----------------------------
cur.execute("""

CREATE TABLE IF NOT EXISTS stg_recharges (

    recharge_id INTEGER,
    customer_id INTEGER,
    recharge_date TIMESTAMP,
    recharge_amount FLOAT,
    payment_mode STRING,
    successful_flag INTEGER,
    created_at TIMESTAMP

)

""")


logger.info(
    "stg_recharges table ready"
)


# -----------------------------
# Export Incremental CSV
# -----------------------------
temp_csv_path = os.path.join(

    BASE_DIR,

    "temp_recharges_incremental.csv"

)

incremental_df.to_csv(

    temp_csv_path,

    index=False

)


logger.info(
    "Temporary incremental CSV created"
)


# -----------------------------
# Upload File To Stage
# -----------------------------
put_command = f"""

PUT file://{temp_csv_path}
@recharges_stage
OVERWRITE = TRUE

"""

cur.execute(
    put_command
)

logger.info(
    "File uploaded to Snowflake stage"
)


# -----------------------------
# Bulk Load Using COPY INTO
# -----------------------------
copy_command = """

COPY INTO STAGING.stg_recharges

FROM @recharges_stage/temp_recharges_incremental.csv

FILE_FORMAT = (
    FORMAT_NAME = recharges_csv_format
)

"""

cur.execute(
    copy_command
)

logger.info(
    "COPY INTO bulk loading completed"
)


# -----------------------------
# Update Watermark
# -----------------------------
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


# -----------------------------
# Commit & Close
# -----------------------------
conn.commit()

cur.close()

conn.close()

logger.info(
    "Snowflake connection closed"
)


# -----------------------------
# Remove Temp CSV
# -----------------------------
if os.path.exists(
    temp_csv_path
):

    os.remove(
        temp_csv_path
    )

    logger.info(
        "Temporary CSV removed"
    )