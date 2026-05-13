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
    "data/processed/recharges"
)

# Fix timestamp datatype
df["recharge_date"] = df["recharge_date"].astype(str)

logger.info(f"Loaded {len(df)} recharge records")

cur = conn.cursor()

# Create staging table
cur.execute("""
CREATE TABLE IF NOT EXISTS stg_recharges (
    recharge_id INTEGER,
    customer_id INTEGER,
    recharge_date TIMESTAMP,
    recharge_amount INTEGER,
    payment_mode STRING,
    successful_flag INTEGER
)
""")

logger.info("stg_recharges table ready")

# Insert records
for _, row in df.iterrows():

    cur.execute("""
    INSERT INTO stg_recharges (
        recharge_id,
        customer_id,
        recharge_date,
        recharge_amount,
        payment_mode,
        successful_flag
    )
    VALUES (
        %s, %s, %s,
        %s, %s, %s
    )
    """, tuple(row))

logger.info("Recharge data loaded into Snowflake")

cur.close()
conn.close()

logger.info("Snowflake connection closed")