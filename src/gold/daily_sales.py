from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import (
    count,
    sum,
)
from delta import configure_spark_with_delta_pip


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_PATH = PROJECT_ROOT / "data" / "silver"
GOLD_PATH = PROJECT_ROOT / "data" / "gold"


def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("Ecommerce-Gold-Daily-Sales")
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def main():
    GOLD_PATH.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()

    try:
        orders_path = str(SILVER_PATH / "orders")

        print(f"Loading Silver orders: {orders_path}")

        orders_df = spark.read.format("delta").load(orders_path)

        print(f"Silver orders count: {orders_df.count()}")

        daily_sales_df = (
            orders_df
            .groupBy("order_date")
            .agg(
                count("order_id").alias("total_orders"),
                sum("quantity").alias("total_quantity"),
                sum("amount").cast(DecimalType(18, 2)).alias("total_revenue"),
            )
            .orderBy("order_date")
        )

        print("Daily sales:")
        daily_sales_df.show(20, truncate=False)

        output_path = str(GOLD_PATH / "daily_sales")

        (
            daily_sales_df
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(output_path)
        )

        print(f"Gold table written to: {output_path}")
        print(f"Gold rows: {daily_sales_df.count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()