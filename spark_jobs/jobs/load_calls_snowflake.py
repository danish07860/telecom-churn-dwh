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
    "data/processed/calls"
)

WATERMARK_PATH = os.path.join(
    BASE_DIR,
    "metadata/snowflake_calls_watermark.txt"
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
with open(WATERMARK_PATH, "r") as f:

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

CREATE TABLE IF NOT EXISTS stg_calls (

    call_id INTEGER,
    customer_id INTEGER,
    call_date TIMESTAMP,
    call_duration_minutes FLOAT,
    network_type STRING,
    call_drop_flag INTEGER,
    tower_location STRING,
    created_at TIMESTAMP

)

""")


logger.info(
    "stg_calls table ready"
)


# -----------------------------
# Insert Incremental Records
# -----------------------------
for _, row in incremental_df.iterrows():

    cur.execute("""

    INSERT INTO stg_calls (

        call_id,
        customer_id,
        call_date,
        call_duration_minutes,
        network_type,
        call_drop_flag,
        tower_location,
        created_at

    )

    VALUES (

        %s, %s, %s, %s,
        %s, %s, %s, %s

    )

    """, (

        int(row["call_id"]),
        int(row["customer_id"]),
        row["call_date"].to_pydatetime(),
        float(row["call_duration_minutes"]),
        row["network_type"],
        int(row["call_drop_flag"]),
        row["tower_location"],
        row["created_at"].to_pydatetime()

    ))


logger.info(
    "Incremental records loaded"
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