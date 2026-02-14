import random
from datetime import datetime
import pandas as pd
from faker import Faker
from typing import Literal
import sys 

fake = Faker("en_US")

Domain_Types = Literal["customer_order", "customer_master"]
S3_BUCKET_NAME = "ad-demo-bucket-205930608840"
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
PART_KEY = timestamp[:8]
FILE_NAME_KEY = timestamp[8:]
S3_PATH_CUSTOMER_ORDER  = f"s3://{S3_BUCKET_NAME}/customer_order/{PART_KEY}/customer_order_{FILE_NAME_KEY}.csv"
S3_PATH_CUSTOMER_MASTER = f"s3://{S3_BUCKET_NAME}/customer_master/{PART_KEY}/customer_master_{FILE_NAME_KEY}.csv"


def generate_customer_order_data(n_rows: int = 100):
    PRODUCT_NAMES = ["Laptop", "Phone", "Tablet", "Headphones", "Monitor"]
    rows = []
    for order_id in range(n_rows):
        quantity=random.randint(1, 1000)
        price_per_unit=round(random.uniform(50, 1500),2)
        rows.append({
            "order_id": order_id,
            "customer_id": str(random.randint(10000, 99999)),
            "product_name": random.choice(PRODUCT_NAMES),
            "quantity": quantity,
            "price_per_unit": price_per_unit,
            "order_date": datetime.now(),
            "total_amount":round(quantity*price_per_unit,2)
        })
    return pd.DataFrame(rows)

def generate_customer_master_data(n_rows: int = 100) -> pd.DataFrame:
    records = []

    for i in range(1, n_rows + 1):
        profile = fake.simple_profile()

        record = {
            "id": i,
            "name": profile["name"],
            "phone": fake.phone_number(),
            "email": profile["mail"],
            "dob": profile["birthdate"].strftime("%Y-%m-%d"),
            "address": fake.street_address() + ", " +
                       fake.city() + ", " +
                       fake.state_abbr()
        }
        records.append(record)
    return pd.DataFrame(records)


def write_to_s3(domain_type: Domain_Types) -> str:
    if domain_type == "customer_order":
        df = generate_customer_order_data(200)
        s3_path = S3_PATH_CUSTOMER_ORDER
        df.to_csv(s3_path, index=False)
    elif domain_type == "customer_master":
        df = generate_customer_master_data(200)
        s3_path = S3_PATH_CUSTOMER_MASTER
        df.to_csv(s3_path, index=False)
    return s3_path


if __name__ == "__main__":  
    domain_type = sys.argv[1]
    print(f"Generating {domain_type} data...")
    s3_path = write_to_s3(domain_type)
    print(f"Data saved to {s3_path}")


