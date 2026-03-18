import requests
import json
from datetime import datetime
import os

BASE_URL = "https://fakestoreapi.com"

def fetch_data(endpoint):
    url = f"{BASE_URL}/{endpoint}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def save_to_file(data, filename):
    os.makedirs("data/raw", exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = f"data/raw/{filename}_{timestamp}.json"
    
    with open(file_path, "w") as f:
        json.dump(data, f, indent=4)
    
    print(f"Saved {filename} data to {file_path}")

def extract_all():
    products = fetch_data("products")
    users = fetch_data("users")
    carts = fetch_data("carts")
    
    save_to_file(products, "products")
    save_to_file(users, "users")
    save_to_file(carts, "carts")

if __name__ == "__main__":
    extract_all()




