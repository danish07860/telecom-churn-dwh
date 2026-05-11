import pandas as pd
import random
from faker import Faker

fake = Faker()

recharges = []

payment_modes = [
    "UPI",
    "Credit Card",
    "Debit Card",
    "Net Banking",
    "Wallet"
]

for recharge_id in range(1, 5001):

    recharge_amount = random.choice([
        199,
        239,
        299,
        399,
        499,
        599,
        799,
        999
    ])

    recharges.append({
        "recharge_id": recharge_id,
        "customer_id": random.randint(1, 1000),
        "recharge_date": fake.date_time_between(
            start_date='-1y',
            end_date='now'
        ),
        "recharge_amount": recharge_amount,
        "payment_mode": random.choice(payment_modes),
        "successful_flag": random.choice([0, 1])
    })

df = pd.DataFrame(recharges)

df.to_csv("data/raw/recharges.csv", index=False)

print("recharges.csv generated successfully")