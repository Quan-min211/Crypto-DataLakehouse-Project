from silver_to_gold import create_spark, read_silver, build_ohlcv_candles
spark = create_spark()
df = read_silver(spark)
ohlcv = build_ohlcv_candles(df, "1 minute")
ohlcv.printSchema()
spark.stop()
