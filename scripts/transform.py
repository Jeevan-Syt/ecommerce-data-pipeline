import pandas as pd
import json
import os

RAW_PATH = "data/raw"
PROCESSED_PATH = "data/processed"

os.makedirs(PROCESSED_PATH, exist_ok=True)

def get_latest_file(prefix):
    files = [f for f in os.listdir(RAW_PATH) if f.startswith(prefix)]
    files.sort(reverse=True)
    return os.path.join(RAW_PATH, files[0])

# -------------------------
# PRODUCTS → dim_products
# -------------------------
def transform_products():
    file_path = get_latest_file("products")

    with open(file_path) as f:
        data = json.load(f)

    df = pd.json_normalize(data)

    df_products = df[[
        "id", "title", "price", "category"
    ]].rename(columns={
        "id": "product_id",
        "title": "product_name"
    })

    df_products.drop_duplicates(inplace=True)
    df_products.fillna("Unknown", inplace=True)

    df_products.to_csv(f"{PROCESSED_PATH}/dim_products.csv", index=False)
    print("dim_products created")

# -------------------------
# USERS → dim_users
# -------------------------
def transform_users():
    file_path = get_latest_file("users")

    with open(file_path) as f:
        data = json.load(f)

    df = pd.json_normalize(data)

    df_users = df[[
        "id", "name.firstname", "name.lastname", "email"
    ]].rename(columns={
        "id": "user_id",
        "name.firstname": "first_name",
        "name.lastname": "last_name"
    })

    df_users["full_name"] = df_users["first_name"] + " " + df_users["last_name"]

    df_users.drop_duplicates(inplace=True)
    df_users.fillna("Unknown", inplace=True)

    df_users.to_csv(f"{PROCESSED_PATH}/dim_users.csv", index=False)
    print("dim_users created")

# -------------------------
# CARTS → fact_orders
# -------------------------
def transform_carts():
    file_path = get_latest_file("carts")

    with open(file_path) as f:
        data = json.load(f)

    df = pd.json_normalize(data, record_path="products", meta=["id", "userId", "date"])

    df_orders = df.rename(columns={
        "id": "cart_id",
        "userId": "user_id",
        "productId": "product_id",
        "quantity": "quantity",
        "date": "order_date"
    })

    # Merge with products to get price
    df_products = pd.read_csv("data/processed/dim_products.csv")
    df_orders = df_orders.merge(df_products, on="product_id", how="left")

    # Calculate total price
    df_orders["total_price"] = df_orders["price"] * df_orders["quantity"]

    # Clean data
    df_orders["order_date"] = pd.to_datetime(df_orders["order_date"])
    df_orders.drop_duplicates(inplace=True)

    # 🔥 IMPORTANT: Keep only required columns (FIX)
    df_orders = df_orders[[
        "cart_id",
        "user_id",
        "product_id",
        "quantity",
        "order_date",
        "total_price"
    ]]

    df_orders.to_csv(f"{PROCESSED_PATH}/fact_orders.csv", index=False)
    print("fact_orders created")

# -------------------------
# MAIN
# -------------------------
def transform_all():
    transform_products()
    transform_users()
    transform_carts()

if __name__ == "__main__":
    transform_all()
