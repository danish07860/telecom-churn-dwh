from faker import Faker
import pandas as pd
import random
import os
from datetime import datetime, timedelta
fake = Faker()

customers = []

for customer_id in range(1, 1001):

    customers.append({
        "customer_id": customer_id,
        "customer_name": fake.name(),
        "gender": random.choice(["Male", "Female"]),
        "age": random.randint(18, 70),
        "city": fake.city(),
        "state": fake.state(),
        "plan_type": random.choice(["Prepaid", "Postpaid"]),
        "monthly_charges": random.randint(200, 2000),
        "tenure_months": random.randint(1, 60),
        "is_active": random.choice([True, False]),
        "created_at": datetime.now()
    })

df = pd.DataFrame(customers)
file_path = "data/raw/customers.csv"
df.to_csv("data/raw/customers.csv", index=False)
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
print("customers.csv generated successfully")