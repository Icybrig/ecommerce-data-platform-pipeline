from pathlib import Path
import random
from datetime import datetime, timedelta

import pandas as pd


RANDOM_SEED = 42
random.seed(RANDOM_SEED)

BASE_DIR = Path(__file__).resolve().parents[2]
OUTPUT_DIR = BASE_DIR / "data" / "raw"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def generate_customers(n=100):
    countries = ["France", "Germany", "UK", "Spain", "Italy"]

    rows = []

    for customer_id in range(1, n + 1):
        rows.append(
            {
                "customer_id": customer_id,
                "name": f"Customer_{customer_id}",
                "country": random.choice(countries),
                "signup_date": (
                    datetime(2025, 1, 1)
                    + timedelta(days=random.randint(0, 365))
                ).date(),
                "updated_at": datetime(2026, 1, 1),
            }
        )

    return pd.DataFrame(rows)


def generate_products(n=50):
    categories = [
        "Electronics",
        "Clothing",
        "Books",
        "Home",
        "Sports",
    ]

    rows = []

    for product_id in range(1, n + 1):
        rows.append(
            {
                "product_id": product_id,
                "product_name": f"Product_{product_id}",
                "category": random.choice(categories),
                "price": round(random.uniform(10, 500), 2),
                "updated_at": datetime(2026, 1, 1),
            }
        )

    return pd.DataFrame(rows)


def generate_orders(n=1000):
    rows = []

    start_date = datetime(2026, 1, 1)

    for order_id in range(1, n + 1):
        quantity = random.randint(1, 5)
        price = round(random.uniform(10, 300), 2)

        order_date = start_date + timedelta(
            days=random.randint(0, 180)
        )

        rows.append(
            {
                "order_id": order_id,
                "customer_id": random.randint(1, 100),
                "product_id": random.randint(1, 50),
                "quantity": quantity,
                "amount": round(quantity * price, 2),
                "order_date": order_date.date(),
                "updated_at": order_date,
            }
        )

    return pd.DataFrame(rows)


def main():
    print("Generating sample e-commerce data...")

    customers = generate_customers()
    products = generate_products()
    orders = generate_orders()

    customers.to_csv(
        OUTPUT_DIR / "customers.csv",
        index=False,
    )

    products.to_csv(
        OUTPUT_DIR / "products.csv",
        index=False,
    )

    orders.to_csv(
        OUTPUT_DIR / "orders.csv",
        index=False,
    )

    print(f"Customers: {len(customers)}")
    print(f"Products:  {len(products)}")
    print(f"Orders:    {len(orders)}")
    print(f"Output:    {OUTPUT_DIR}")


if __name__ == "__main__":
    main()