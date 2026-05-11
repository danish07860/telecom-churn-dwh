from faker import Faker
import pandas as pd
import random

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
        "is_active": random.choice([True, False])
    })

df = pd.DataFrame(customers)

df.to_csv("data/raw/customers.csv", index=False)

print("customers.csv generated successfully")