from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    row_number,
)
from pyspark.sql.window import Window


PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_PATH = PROJECT_ROOT / "data" / "bronze"
SILVER_PATH = PROJECT_ROOT / "data" / "silver"


def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("Ecommerce-Silver-Transformation")
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


def deduplicate(df, key_column):
    """
    Keep the latest record for each business key.
    """

    window = (
        Window
        .partitionBy(key_column)
        .orderBy(col("updated_at").desc())
    )

    return (
        df
        .withColumn("_row_number", row_number().over(window))
        .filter(col("_row_number") == 1)
        .drop("_row_number")
    )


def transform_customers(spark):
    source_path = str(BRONZE_PATH / "customers")
    target_path = str(SILVER_PATH / "customers")

    print(f"Reading Bronze: {source_path}")

    df = spark.read.format("delta").load(source_path)

    # Explicit type casting
    df = (
        df
        .withColumn("customer_id", col("customer_id").cast("integer"))
        .withColumn("signup_date", col("signup_date").cast("date"))
        .withColumn("updated_at", col("updated_at").cast("timestamp"))
    )

    # Data quality rules
    df = df.filter(
        col("customer_id").isNotNull()
        & col("country").isNotNull()
    )

    # Deduplication
    df = deduplicate(df, "customer_id")

    print(f"customers: {df.count()} rows")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target_path)
    )

    print(f"Written Silver: {target_path}")


def transform_products(spark):
    source_path = str(BRONZE_PATH / "products")
    target_path = str(SILVER_PATH / "products")

    print(f"Reading Bronze: {source_path}")

    df = spark.read.format("delta").load(source_path)

    # Explicit type casting
    df = (
        df
        .withColumn("product_id", col("product_id").cast("integer"))
        .withColumn("price", col("price").cast("double"))
        .withColumn("updated_at", col("updated_at").cast("timestamp"))
    )

    # Data quality rules
    df = df.filter(
        col("product_id").isNotNull()
        & col("product_name").isNotNull()
        & col("price").isNotNull()
        & (col("price") > 0)
    )

    # Deduplication
    df = deduplicate(df, "product_id")

    print(f"products: {df.count()} rows")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target_path)
    )

    print(f"Written Silver: {target_path}")


def transform_orders(spark):
    source_path = str(BRONZE_PATH / "orders")
    target_path = str(SILVER_PATH / "orders")

    print(f"Reading Bronze: {source_path}")

    df = spark.read.format("delta").load(source_path)

    # Explicit type casting
    df = (
        df
        .withColumn("order_id", col("order_id").cast("integer"))
        .withColumn("customer_id", col("customer_id").cast("integer"))
        .withColumn("product_id", col("product_id").cast("integer"))
        .withColumn("quantity", col("quantity").cast("integer"))
        .withColumn("amount", col("amount").cast("double"))
        .withColumn("order_date", col("order_date").cast("date"))
        .withColumn("updated_at", col("updated_at").cast("timestamp"))
    )

    # Data quality rules
    df = df.filter(
        col("order_id").isNotNull()
        & col("customer_id").isNotNull()
        & col("product_id").isNotNull()
        & col("quantity").isNotNull()
        & (col("quantity") > 0)
        & col("amount").isNotNull()
        & (col("amount") > 0)
        & col("order_date").isNotNull()
    )

    # Deduplication
    df = deduplicate(df, "order_id")

    print(f"orders: {df.count()} rows")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target_path)
    )

    print(f"Written Silver: {target_path}")


def main():
    SILVER_PATH.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()

    try:
        transform_customers(spark)
        transform_products(spark)
        transform_orders(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()