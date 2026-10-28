
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    window,
    col,
    count,
    avg,
    min,
    max,
    sum as spark_sum,
)

# Chemins des tables Delta
SILVER_PATH = "/opt/app/data/silver/trades"
GOLD_PATH = "/opt/app/data/gold/minute_metrics"
CHECKPOINT_PATH = "/opt/app/data/checkpoints/gold"

# Initialisation de Spark et Delta Lake
spark = (
    SparkSession.builder
    .appName("MarketDataGold")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension",
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Lecture continue des données propres depuis Silver
silver_stream = (
    spark.readStream
    .format("delta")
    .load(SILVER_PATH)
)

# Agrégation des transactions par minute et par symbole
gold_stream = (
    silver_stream
    .filter(
        col("event_time").isNotNull()
        & col("price").isNotNull()
        & col("quantity").isNotNull()
    )
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("symbol"),
    )
    .agg(
        count("*").alias("trade_count"),
        avg("price").alias("avg_price"),
        min("price").alias("min_price"),
        max("price").alias("max_price"),
        spark_sum("quantity").alias("total_btc"),
        spark_sum(
            col("price") * col("quantity")
        ).alias("total_value_usdt"),
    )
    .select(
        col("symbol"),
        col("window.start").alias("minute_start"),
        col("window.end").alias("minute_end"),
        col("trade_count"),
        col("avg_price"),
        col("min_price"),
        col("max_price"),
        col("total_btc"),
        col("total_value_usdt"),
    )
)

# Écriture des indicateurs dans Gold
query = (
    gold_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start(GOLD_PATH)
)

print("Gold calcule les indicateurs par minute.")
print(f"Destination : {GOLD_PATH}")

query.awaitTermination()
