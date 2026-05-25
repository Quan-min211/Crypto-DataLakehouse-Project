from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("VacuumDryRunCount").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

paths = {
    "bronze": "gs://crypto-lakehouse-group8/bronze",
    "silver": "gs://crypto-lakehouse-group8/silver",
    "gold": "gs://crypto-lakehouse-group8/gold",
}

for name, path in paths.items():
    print(f"\n===== {name.upper()} =====")
    try:
        df = spark.sql(f"VACUUM delta.`{path}` RETAIN 168 HOURS DRY RUN")
        count = df.count()
        print(f"candidates_to_delete={count}")
        df.show(20, truncate=False)
    except Exception as exc:
        print(f"vacuum_dry_run_error={exc}")

spark.stop()
