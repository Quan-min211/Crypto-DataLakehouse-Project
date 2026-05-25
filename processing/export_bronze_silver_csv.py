"""
export_bronze_silver_csv.py
===========================
Export sample data from Bronze and Silver Delta tables to CSV files
for documentation / reporting purposes.

Output:
    /docs/bronze_sample.csv
    /docs/silver_sample.csv

These paths are relative to the project root mounted via docker-compose:
    ./processing:/processing
    ./docs:/docs            (needs to be added to docker-compose volumes)

Run inside spark-master container:
    spark-submit --packages io.delta:delta-spark_2.12:3.2.1 \
                 /processing/export_bronze_silver_csv.py
"""

import logging
import os
import sys

# Add processing dir to path so gcs_auth is importable when spark-submit is used
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from gcs_auth import apply_gcs_auth

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
log = logging.getLogger("export_csv")

# ── Config ────────────────────────────────────────────────────────────────────
SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
BRONZE_PATH  = "gs://crypto-lakehouse-group8/bronze"
SILVER_PATH  = "gs://crypto-lakehouse-group8/silver"

# Output inside the container — mapped to ./docs on the host via docker-compose
CSV_OUT_DIR  = "/docs"


def create_spark() -> SparkSession:
    log.info("Connecting to Spark Master: %s", SPARK_MASTER)
    builder = (
        SparkSession.builder
        .appName("export_bronze_silver_csv")
        .master(SPARK_MASTER)
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "512m")
        # Only need 1 core for this lightweight export job
        .config("spark.cores.max", "1")
        .config("spark.executor.cores", "1")
    )
    builder = apply_gcs_auth(builder)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def export_to_csv(spark: SparkSession, delta_path: str, csv_name: str, limit: int = 500000):
    """Read a Delta table, take a sample, and write a single CSV file."""
    log.info("Reading Delta table: %s", delta_path)
    df = spark.read.format("delta").load(delta_path)

    total_count = df.count()
    log.info("Total rows in %s: %d", delta_path, total_count)

    # Take a sample (limit rows) to keep CSV manageable for docs
    sample_df = df.limit(limit)

    # Write as single CSV file to a temp Spark output directory
    tmp_out = f"{CSV_OUT_DIR}/_tmp_{csv_name}"
    log.info("Writing CSV to temp dir: %s", tmp_out)
    (
        sample_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(tmp_out)
    )
    log.info("✅ Exported %s → %s (sampled %d of %d rows)",
             delta_path, tmp_out, min(limit, total_count), total_count)


def main():
    log.info("=== Export Bronze & Silver → CSV starting ===")
    spark = create_spark()

    try:
        export_to_csv(spark, BRONZE_PATH, "bronze_sample")
        export_to_csv(spark, SILVER_PATH, "silver_sample")
        log.info("=== Export complete! CSV files are in %s ===", CSV_OUT_DIR)
    except Exception as exc:
        log.critical("Export failed: %s", exc, exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
