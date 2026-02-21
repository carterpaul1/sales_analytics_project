import pandas as pd
import os
import logging
from datetime import datetime

# -------------------------------------------------
# Setup Paths
# -------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(__file__))  # points to sales_analytics_project root
RAW_PATH = os.path.join(BASE_DIR, "data", "raw")
CLEANED_PATH = os.path.join(BASE_DIR, "data", "cleaned")
LOG_PATH = os.path.join(BASE_DIR, "data", "cleaning_log.log")

os.makedirs(CLEANED_PATH, exist_ok=True)

# -------------------------------------------------
# Setup Logging
# -------------------------------------------------
logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logging.info("Starting data cleaning process.")

# -------------------------------------------------
# Load Data
# -------------------------------------------------
try:
    customers = pd.read_csv(os.path.join(RAW_PATH, "customers.csv"))
    products = pd.read_csv(os.path.join(RAW_PATH, "products.csv"))
    orders = pd.read_csv(os.path.join(RAW_PATH, "orders.csv"))
    logging.info("Raw data loaded successfully.")
except Exception as e:
    logging.error(f"Error loading raw data: {e}")
    raise

# -------------------------------------------------
# Data Quality Report (Before Cleaning)
# -------------------------------------------------
print("\n--- DATA QUALITY REPORT (BEFORE CLEANING) ---")
print("Customers:", customers.shape)
print("Products:", products.shape)
print("Orders:", orders.shape)

# -------------------------------------------------
# Clean Customers
# -------------------------------------------------
customers_before = len(customers)

# Remove missing emails
customers = customers.dropna(subset=["email"])
# Keep only valid emails
customers = customers[customers["email"].str.contains("@", na=False)]

customers_after = len(customers)
logging.info(f"Customers cleaned: {customers_before - customers_after} rows removed.")

# -------------------------------------------------
# Clean Products
# -------------------------------------------------
products_before = len(products)

# Remove zero or negative prices
products = products[products["price"] > 0]

products_after = len(products)
logging.info(f"Products cleaned: {products_before - products_after} rows removed.")

# -------------------------------------------------
# Clean Orders
# -------------------------------------------------
orders_before = len(orders)

# Remove negative quantities
orders = orders[orders["quantity"] > 0]

# Remove invalid customer_id
orders = orders[orders["customer_id"].isin(customers["customer_id"])]

# Remove invalid product_id
orders = orders[orders["product_id"].isin(products["product_id"])]

orders_after = len(orders)
logging.info(f"Orders cleaned: {orders_before - orders_after} rows removed.")

# -------------------------------------------------
# Merge Tables
# -------------------------------------------------
df = (
    orders
    .merge(customers, on="customer_id", how="inner")
    .merge(products, on="product_id", how="inner")
)

# -------------------------------------------------
# Transformations
# -------------------------------------------------
df["order_date"] = pd.to_datetime(df["order_date"])
df["year"] = df["order_date"].dt.year
df["month"] = df["order_date"].dt.month
df["month_name"] = df["order_date"].dt.month_name()
df["total_sales"] = df["quantity"] * df["price"]

# -------------------------------------------------
# Validation Checks
# -------------------------------------------------
assert df["price"].min() > 0, "Invalid price detected!"
assert df["quantity"].min() > 0, "Invalid quantity detected!"
assert df["total_sales"].min() >= 0, "Negative total sales detected!"

logging.info("Validation checks passed.")

# -------------------------------------------------
# Save Cleaned Data
# -------------------------------------------------
output_file = os.path.join(CLEANED_PATH, "sales_cleaned_for_powerbi.csv")
df.to_csv(output_file, index=False)
logging.info(f"Cleaned data saved: {output_file}")

# -------------------------------------------------
# Data Quality Report (After Cleaning)
# -------------------------------------------------
print("\n--- DATA QUALITY REPORT (AFTER CLEANING) ---")
print("Final dataset shape:", df.shape)
print("Total Revenue:", df["total_sales"].sum())
print("Average Order Value:", df["total_sales"].mean())

logging.info("Data cleaning process completed successfully.")
print("\n✅ Data cleaning completed successfully.")
print(f"Cleaned file saved to: {output_file}")