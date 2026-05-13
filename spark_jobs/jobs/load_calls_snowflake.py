import sys
import os

sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../..")
    )
)

import pandas as pd
import snowflake.connector

from configs.snowflake_config import SNOWFLAKE_CONFIG
from spark_jobs.utils.logger import get_logger

logger = get_logger(__name__)

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=SNOWFLAKE_CONFIG["user"],
    password=SNOWFLAKE_CONFIG["password"],
    account=SNOWFLAKE_CONFIG["account"],
    warehouse=SNOWFLAKE_CONFIG["warehouse"],
    database=SNOWFLAKE_CONFIG["database"],
    schema=SNOWFLAKE_CONFIG["schema"],
    role=SNOWFLAKE_CONFIG["role"]
)

logger.info("Connected to Snowflake")

# Read processed parquet
df = pd.read_parquet(
    "data/processed/calls"
)
df["call_date"] = df["call_date"].astype(str)

logger.info(f"Loaded {len(df)} call records")

cur = conn.cursor()

# Create staging table
cur.execute("""
CREATE TABLE IF NOT EXISTS stg_calls (
    call_id INTEGER,
    customer_id INTEGER,
    call_date TIMESTAMP,
    call_duration_minutes FLOAT,
    network_type STRING,
    call_drop_flag INTEGER,
    tower_location STRING
)
""")

logger.info("stg_calls table ready")

# Insert records
for _, row in df.iterrows():

    cur.execute("""
    INSERT INTO stg_calls (
        call_id,
        customer_id,
        call_date,
        call_duration_minutes,
        network_type,
        call_drop_flag,
        tower_location
    )
    VALUES (
        %s, %s, %s, %s,
        %s, %s, %s
    )
    """, tuple(row))

logger.info("Call data loaded into Snowflake")

cur.close()
conn.close()

logger.info("Snowflake connection closed")