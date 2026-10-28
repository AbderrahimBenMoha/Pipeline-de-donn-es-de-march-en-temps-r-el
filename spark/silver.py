from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    current_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
    DecimalType,
)

# Chemins des tables Delta
BRONZE_PATH = "/opt/app/data/bronze/trades"
SILVER_PATH = "/opt/app/data/silver/trades"
CHECKPOINT_PATH = "/opt/app/data/checkpoints/silver"

# Initialiser Spark et Delta Lake
spark = (
    SparkSession.builder
    .appName("MarketDataSilver")
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

# Définir la structure attendue du JSON brut
event_schema = StructType([
    StructField("source", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("trade_id", LongType(), True),
    StructField("price", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("trade_time", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("buyer_is_market_maker", BooleanType(), True),
])

# Lire les nouveaux fichiers de la table Bronze
bronze_stream = (
    spark.readStream
    .format("delta")
    .load(BRONZE_PATH)
)

# Transformer le JSON en colonnes structurées
parsed_stream = bronze_stream.withColumn(
    "event",
    from_json(col("raw_json"), event_schema),
)

silver_stream = parsed_stream.select(
    col("event.source").alias("source"),
    col("event.symbol").alias("symbol"),
    col("event.trade_id").alias("trade_id"),
    col("event.price").cast(
        DecimalType(20, 8)
    ).alias("price"),
    col("event.quantity").cast(
        DecimalType(20, 8)
    ).alias("quantity"),
    to_timestamp(
        col("event.trade_time")
    ).alias("trade_time"),
    to_timestamp(
        col("event.event_time")
    ).alias("event_time"),
    col("event.buyer_is_market_maker").alias(
        "buyer_is_market_maker"
    ),
    col("ingested_at").alias("bronze_ingested_at"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    current_timestamp().alias("silver_processed_at"),
)

# Ne conserver que les transactions valides
valid_stream = silver_stream.filter(
    col("source").isNotNull()
    & col("symbol").isNotNull()
    & col("trade_id").isNotNull()
    & (col("price") > 0)
    & (col("quantity") > 0)
    & col("trade_time").isNotNull()
    & col("event_time").isNotNull()
)

# Déduplication avec une fenêtre de temps pour limiter
# l'état conservé en mémoire par Spark
deduplicated_stream = (
    valid_stream
    .withWatermark("trade_time", "10 minutes")
    .dropDuplicates(["trade_id", "trade_time"])
)

# Enregistrer les données propres dans une nouvelle table Delta
query = (
    deduplicated_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start(SILVER_PATH)
)

print("Silver lit les données de Bronze.")
print(f"Destination : {SILVER_PATH}")

query.awaitTermination()
