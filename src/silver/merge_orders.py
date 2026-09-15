from pathlib import Path

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    row_number,
)
from pyspark.sql.window import Window


PROJECT_ROOT = Path(__file__).resolve().parents[2]

CDC_PATH = PROJECT_ROOT / "data" / "raw" / "cdc" / "orders_cdc.csv"
SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "orders"


def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("Ecommerce-Silver-CDC-Merge")
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
    spark = create_spark_session()

    try:
        # ---------------------------------------------------------
        # 1. Read CDC data
        # ---------------------------------------------------------
        print(f"Reading CDC: {CDC_PATH}")

        cdc_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(CDC_PATH))
        )

        # ---------------------------------------------------------
        # 2. Explicit schema
        # ---------------------------------------------------------
        cdc_df = (
            cdc_df
            .withColumn("order_id", col("order_id").cast("integer"))
            .withColumn("customer_id", col("customer_id").cast("integer"))
            .withColumn("product_id", col("product_id").cast("integer"))
            .withColumn("quantity", col("quantity").cast("integer"))
            .withColumn("amount", col("amount").cast("double"))
            .withColumn("order_date", col("order_date").cast("date"))
            .withColumn("updated_at", col("updated_at").cast("timestamp"))
        )

        # ---------------------------------------------------------
        # 3. Data quality
        # ---------------------------------------------------------
        cdc_df = cdc_df.filter(
            col("order_id").isNotNull()
            & col("customer_id").isNotNull()
            & col("product_id").isNotNull()
            & col("quantity").isNotNull()
            & (col("quantity") > 0)
            & col("amount").isNotNull()
            & (col("amount") > 0)
            & col("order_date").isNotNull()
            & col("updated_at").isNotNull()
        )

        # ---------------------------------------------------------
        # 4. Add ingestion metadata
        # ---------------------------------------------------------
        cdc_df = (
            cdc_df
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_source", lit("orders_cdc.csv"))
        )

        print(f"CDC records before dedup: {cdc_df.count()}")

        # ---------------------------------------------------------
        # 5. Deduplicate CDC records
        # ---------------------------------------------------------
        window = (
            Window
            .partitionBy("order_id")
            .orderBy(col("updated_at").desc())
        )

        cdc_df = (
            cdc_df
            .withColumn("_row_number", row_number().over(window))
            .filter(col("_row_number") == 1)
            .drop("_row_number")
        )

        print(f"CDC records after dedup: {cdc_df.count()}")

        print("CDC after deduplication:")

        (
            cdc_df
            .orderBy("order_id")
            .show(truncate=False)
        )

        # ---------------------------------------------------------
        # 6. Load existing Silver Delta table
        # ---------------------------------------------------------
        print(f"Loading Silver: {SILVER_PATH}")

        delta_table = DeltaTable.forPath(
            spark,
            str(SILVER_PATH),
        )

        # ---------------------------------------------------------
        # 7. MERGE CDC into Silver
        # ---------------------------------------------------------
        print("Running Delta MERGE...")

        (
            delta_table.alias("target")
            .merge(
                cdc_df.alias("source"),
                "target.order_id = source.order_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print("MERGE completed successfully.")

        # ---------------------------------------------------------
        # 8. Verify result
        # ---------------------------------------------------------
        result_df = (
            spark.read
            .format("delta")
            .load(str(SILVER_PATH))
        )

        print(f"Silver row count: {result_df.count()}")

        print("Updated orders:")

        (
            result_df
            .filter(
                col("order_id").isin(
                    [1, 2, 3, 1001, 1002]
                )
            )
            .select(
                "order_id",
                "customer_id",
                "product_id",
                "quantity",
                "amount",
                "updated_at",
            )
            .orderBy("order_id")
            .show(truncate=False)
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()