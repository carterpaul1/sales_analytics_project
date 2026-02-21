import pandas as pd
import numpy as np
import os
from faker import Faker
from datetime import datetime, timedelta
import random

fake = Faker()

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
RAW_PATH = os.path.join(BASE_DIR, "data", "raw")

os.makedirs(RAW_PATH, exist_ok=True)

# -----------------------------
# Generate Customers
# -----------------------------

customers = []
for i in range(1, 201):
    customers.append({
        "customer_id": i,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email() if random.random() > 0.1 else None,
        "state": random.choice(["VA", "CA", "TX", "FL", "NY"])
    })

customers_df = pd.DataFrame(customers)
customers_df.to_csv(os.path.join(RAW_PATH, "customers.csv"), index=False)

# -----------------------------
# Generate Products
# -----------------------------

products = [
    {"product_id": 101, "product_name": "Laptop", "category": "Electronics", "price": 899.99},
    {"product_id": 102, "product_name": "Phone", "category": "Electronics", "price": 699.99},
    {"product_id": 103, "product_name": "Desk", "category": "Furniture", "price": 299.99},
    {"product_id": 104, "product_name": "Chair", "category": "Furniture", "price": 129.99},
    {"product_id": 105, "product_name": "Monitor", "category": "Electronics", "price": 199.99},
]

products_df = pd.DataFrame(products)
products_df.to_csv(os.path.join(RAW_PATH, "products.csv"), index=False)

# -----------------------------
# Generate Orders
# -----------------------------

orders = []
start_date = datetime(2024, 1, 1)

for i in range(1, 1001):
    orders.append({
        "order_id": i,
        "customer_id": random.randint(1, 200),
        "product_id": random.choice(products_df["product_id"]),
        "quantity": random.randint(1, 5),
        "order_date": start_date + timedelta(days=random.randint(0, 365))
    })

orders_df = pd.DataFrame(orders)
orders_df.to_csv(os.path.join(RAW_PATH, "orders.csv"), index=False)

print("✅ Large dataset generated successfully.")