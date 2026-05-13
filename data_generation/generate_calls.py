import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta
import os

file_path = "data/raw/calls.csv"
fake = Faker()

calls = []

network_types = ["4G", "5G", "VoLTE"]

start_id = random.randint(10000, 99999)

for call_id in range(start_id, start_id + 100):

    call_date = fake.date_time_between(
        start_date='-1y',
        end_date='now'
    )

    calls.append({
    "call_id": call_id,
    "customer_id": random.randint(1, 1000),
    "call_date": call_date,
    "call_duration_minutes": round(random.uniform(0.5, 60), 2),
    "network_type": random.choice(network_types),
    "call_drop_flag": random.choice([0, 1]),
    "tower_location": fake.city(),
    "created_at": datetime.now()
})

df = pd.DataFrame(calls)

if os.path.exists(file_path):

    df.to_csv(
        file_path,
        mode='a',
        header=False,
        index=False
    )

else:

    df.to_csv(
        file_path,
        index=False
    )

print("calls.csv generated successfully")