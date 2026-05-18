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

from configs.snowflake_config import SNOWFLAKE_CONFIG
from spark_jobs.utils.logger import get_logger

logger = get_logger(__name__)
PARQUET_PATH = os.path.join(
    BASE_DIR,
    "data/processed/customers"
)

WATERMARK_PATH = os.path.join(
    BASE_DIR,
    "metadata/snowflake_customers_watermark.txt"
)
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
df = pd.read_parquet(PARQUET_PATH)
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
    is_active BOOLEAN,
    created_at TIMESTAMP
)
""")

logger.info("stg_customers table ready")
if len(incremental_df) == 0:

    logger.info(
        "No new customer records found"
    )

    conn.close()

    sys.exit()
# -----------------------------
# Export Incremental CSV
# -----------------------------
temp_csv_path = os.path.join(

    BASE_DIR,

    "temp_customers_incremental.csv"

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
@customers_stage
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

COPY INTO STAGING.stg_customers

FROM @customers_stage/temp_customers_incremental.csv

FILE_FORMAT = (
    FORMAT_NAME = customers_csv_format
)

"""

cur.execute(
    copy_command
)

logger.info(
    "COPY INTO bulk loading completed"
)
# # Insert records
# for _, row in df.iterrows():

#     cur.execute("""
#     INSERT INTO stg_customers VALUES (
#         %s, %s, %s, %s, %s,
#         %s, %s, %s, %s, %s
#     )
#     """, tuple(row))

# logger.info("Customer data loaded into Snowflake")
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
conn.commit()
cur.close()
conn.close()

logger.info("Snowflake connection closed")

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