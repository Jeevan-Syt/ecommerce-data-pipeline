import pandas as pd
import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    user="jeevan",
    password="Jeevan@123",
    database="ecommerce_db"
)

cursor = conn.cursor()

def load_csv(file, table):
    df = pd.read_csv(file)

    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))

    query = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"

    for _, row in df.iterrows():
        cursor.execute(query, tuple(row))

    conn.commit()
    print(f"{table} loaded")

load_csv("data/processed/dim_products.csv", "dim_products")
load_csv("data/processed/dim_users.csv", "dim_users")
load_csv("data/processed/fact_orders.csv", "fact_orders")

cursor.close()
conn.close()
