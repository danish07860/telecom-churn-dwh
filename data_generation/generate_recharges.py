import pandas as pd
import random
from faker import Faker
from datetime import datetime
import os
import sys


# -----------------------------
# Base Directory
# -----------------------------
BASE_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        ".."
    )
)

sys.path.append(BASE_DIR)


# -----------------------------
# Logger
# -----------------------------
from spark_jobs.utils.logger import (
    get_logger
)

logger = get_logger(
    "RechargeDataGeneration"
)


# -----------------------------
# Faker
# -----------------------------
fake = Faker()

recharges = []


# -----------------------------
# Payment Modes
# -----------------------------
payment_modes = [

    "UPI",
    "CARD",
    "NETBANKING",
    "WALLET"

]


# -----------------------------
# Generate Incremental Batch
# -----------------------------
logger.info(
    "Generating incremental recharge batch"
)

for recharge_id in range(1, 101):

    recharges.append({

        "recharge_id": random.randint(
            100000,
            999999
        ),

        "customer_id": random.randint(
            1,
            1000
        ),

        "recharge_date": fake.date_time_this_year(),

        "recharge_amount": round(
            random.uniform(10, 500),
            2
        ),

        "payment_mode": random.choice(
            payment_modes
        ),

        "successful_flag": random.choice(
            [0, 1]
        ),

        "created_at": datetime.now()

    })


# -----------------------------
# Create DataFrame
# -----------------------------
df = pd.DataFrame(
    recharges
)

logger.info(
    f"Generated {len(df)} recharge records"
)


# -----------------------------
# File Path
# -----------------------------
RAW_PATH = os.path.join(
    BASE_DIR,
    "data/raw/recharges.csv"
)


# -----------------------------
# Append Incremental Batch
# -----------------------------
if os.path.exists(RAW_PATH):

    df.to_csv(

        RAW_PATH,

        mode="a",

        header=False,

        index=False

    )

    logger.info(
        "Incremental recharge batch appended"
    )

else:

    df.to_csv(

        RAW_PATH,

        index=False

    )

    logger.info(
        "New recharge dataset created"
    )


logger.info(
    "Recharge data generation completed"
)