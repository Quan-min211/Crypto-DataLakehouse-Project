"""
check_delta_paths.py
====================
Quick diagnostic: verify which GCS paths are valid Delta tables.

Run inside spark-master container:
    spark-submit --packages io.delta:delta-spark_2.12:3.2.1 \
                 /processing/check_delta_paths.py
"""

import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from gcs_auth import apply_gcs_auth

from pyspark.sql import SparkSession
from delta.tables import DeltaTable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
log = logging.getLogger("check_delta_paths")

SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")


def create_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("check_delta_paths")
        .master(SPARK_MASTER)
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "512m")
        .config("spark.cores.max", "1")
    )
    builder = apply_gcs_auth(builder)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


paths = [
    "gs://crypto-lakehouse-group8/bronze",
    "gs://crypto-lakehouse-group8/bronze/crypto_ticks",
    "gs://crypto-lakehouse-group8/silver",
    "gs://crypto-lakehouse-group8/silver/crypto_ticks",
    "gs://crypto-lakehouse-group8/gold",
]

spark = create_spark()

for p in paths:
    ok = DeltaTable.isDeltaTable(spark, p)
    print(f"{p} => is_delta={ok}")

spark.stop()
