import pandas as pd
import random

from faker import Faker
from datetime import datetime


fake = Faker()

complaints = []


complaint_types = [

    "Network Issue",
    "Billing Issue",
    "Call Drop",
    "Slow Internet",
    "Recharge Failure",
    "Poor Customer Service"

]


statuses = [

    "Open",
    "In Progress",
    "Resolved",
    "Closed"

]


for complaint_id in range(1, 3001):

    complaints.append({

        "complaint_id": complaint_id,

        "customer_id": random.randint(
            1,
            1000
        ),

        "complaint_type": random.choice(
            complaint_types
        ),

        "complaint_date": fake.date_time_between(

            start_date='-1y',

            end_date='now'

        ),

        "status": random.choice(
            statuses
        ),

        "resolution_time_hours": random.randint(
            1,
            168
        ),

        "created_at": datetime.now()

    })


df = pd.DataFrame(
    complaints
)


df.to_csv(

    "data/raw/complaints.csv",

    index=False

)


print(
    "complaints.csv generated successfully"
)