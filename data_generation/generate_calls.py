import pandas as pd
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

calls = []

network_types = ["4G", "5G", "VoLTE"]

for call_id in range(1, 10001):

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
        "tower_location": fake.city()
    })

df = pd.DataFrame(calls)

df.to_csv("data/raw/calls.csv", index=False)

print("calls.csv generated successfully")