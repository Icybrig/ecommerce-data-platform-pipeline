from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("Ecommerce-Data-Quality-Tests")
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


@pytest.fixture(scope="module")
def spark():
    spark = create_spark_session()

    yield spark

    spark.stop()