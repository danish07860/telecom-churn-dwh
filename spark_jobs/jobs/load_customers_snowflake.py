import pandas as pd
import snowflake.connector
import sys
import os

sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../..")
    )
)
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
    "data/processed/customers"
)

logger.info(f"Loaded {len(df)} customer records")

# Create cursor
cur = conn.cursor()

# Create staging table
cur.execute("""
CREATE TABLE IF NOT EXISTS stg_customers (
    customer_id INTEGER,
    customer_full_name STRING,
    gender STRING,
    age INTEGER,
    city STRING,
    state STRING,
    plan_type STRING,
    monthly_charges INTEGER,
    tenure_months INTEGER,
    is_active BOOLEAN
)
""")

logger.info("stg_customers table ready")

# Insert records
for _, row in df.iterrows():

    cur.execute("""
    INSERT INTO stg_customers VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s
    )
    """, tuple(row))

logger.info("Customer data loaded into Snowflake")

cur.close()
conn.close()

logger.info("Snowflake connection closed")