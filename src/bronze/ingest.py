from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from delta import configure_spark_with_delta_pip


PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_PATH = PROJECT_ROOT / "data" / "raw"
BRONZE_PATH = PROJECT_ROOT / "data" / "bronze"


def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("Ecommerce-Bronze-Ingestion")
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


def ingest_table(spark, table_name):
    source_path = str(RAW_PATH / f"{table_name}.csv")
    target_path = str(BRONZE_PATH / table_name)

    print(f"Reading: {source_path}")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(source_path)
    )

    # Add ingestion metadata
    df = (
        df.withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit(f"{table_name}.csv"))
    )

    print(f"{table_name}: {df.count()} rows")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target_path)
    )

    print(f"Written to: {target_path}")


def main():
    BRONZE_PATH.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()

    try:
        for table_name in ["customers", "products", "orders"]:
            ingest_table(spark, table_name)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()