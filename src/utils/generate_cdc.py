from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_PATH = PROJECT_ROOT / "data" / "raw"
CDC_PATH = RAW_PATH / "cdc"

CDC_PATH.mkdir(parents=True, exist_ok=True)


def generate_orders_cdc():
    """
    Simulate new and updated orders arriving from the source system.
    """

    rows = [
        # Existing order -> UPDATE
        {
            "order_id": 1,
            "customer_id": 10,
            "product_id": 5,
            "quantity": 3,
            "amount": 299.99,
            "order_date": "2026-01-05",
            "updated_at": "2026-09-16 10:00:00",
        },
        {
            "order_id": 2,
            "customer_id": 20,
            "product_id": 8,
            "quantity": 2,
            "amount": 199.99,
            "order_date": "2026-01-10",
            "updated_at": "2026-09-16 10:05:00",
        },
        {
            "order_id": 3,
            "customer_id": 30,
            "product_id": 12,
            "quantity": 5,
            "amount": 599.99,
            "order_date": "2026-01-15",
            "updated_at": "2026-09-16 10:10:00",
        },

        # New orders -> INSERT
        {
            "order_id": 1001,
            "customer_id": 40,
            "product_id": 15,
            "quantity": 2,
            "amount": 150.00,
            "order_date": "2026-09-16",
            "updated_at": "2026-09-16 11:00:00",
        },
        {
            "order_id": 1001,
            "customer_id": 40,
            "product_id": 15,
            "quantity": 2,
            "amount": 180.00,
            "order_date": "2026-09-16",
            "updated_at": "2026-09-16 12:00:00",
        },
        {
            "order_id": 1002,
            "customer_id": 50,
            "product_id": 20,
            "quantity": 1,
            "amount": 250.00,
            "order_date": "2026-09-16",
            "updated_at": "2026-09-16 11:05:00",
        },
    ]

    df = pd.DataFrame(rows)

    output_path = CDC_PATH / "orders_cdc.csv"

    df.to_csv(output_path, index=False)

    print(f"Generated CDC file: {output_path}")
    print(f"CDC records: {len(df)}")


if __name__ == "__main__":
    generate_orders_cdc()