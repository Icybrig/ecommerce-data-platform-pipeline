from pathlib import Path

from pyspark.sql.functions import col


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER_PATH = PROJECT_ROOT / "data" / "silver"


def test_orders_no_null_primary_key(spark):
    orders_df = (
        spark.read
        .format("delta")
        .load(str(SILVER_PATH / "orders"))
    )

    invalid_count = (
        orders_df
        .filter(col("order_id").isNull())
        .count()
    )

    assert invalid_count == 0


def test_orders_positive_quantity(spark):
    orders_df = (
        spark.read
        .format("delta")
        .load(str(SILVER_PATH / "orders"))
    )

    invalid_count = (
        orders_df
        .filter(col("quantity") <= 0)
        .count()
    )

    assert invalid_count == 0


def test_orders_positive_amount(spark):
    orders_df = (
        spark.read
        .format("delta")
        .load(str(SILVER_PATH / "orders"))
    )

    invalid_count = (
        orders_df
        .filter(col("amount") <= 0)
        .count()
    )

    assert invalid_count == 0


def test_orders_no_duplicate_order_id(spark):
    orders_df = (
        spark.read
        .format("delta")
        .load(str(SILVER_PATH / "orders"))
    )

    duplicate_count = (
        orders_df
        .groupBy("order_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    assert duplicate_count == 0


def test_customers_no_duplicate_customer_id(spark):
    customers_df = (
        spark.read
        .format("delta")
        .load(str(SILVER_PATH / "customers"))
    )

    duplicate_count = (
        customers_df
        .groupBy("customer_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    assert duplicate_count == 0


def test_products_no_duplicate_product_id(spark):
    products_df = (
        spark.read
        .format("delta")
        .load(str(SILVER_PATH / "products"))
    )

    duplicate_count = (
        products_df
        .groupBy("product_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    assert duplicate_count == 0