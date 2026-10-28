from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


# Configuration
KAFKA_SERVERS = "kafka:29092"
KAFKA_TOPIC = "market-data"

BRONZE_PATH = "/opt/app/data/bronze/trades"
CHECKPOINT_PATH = "/opt/app/data/checkpoints/bronze"


# Initialisation de Spark et de Delta Lake
spark = (
    SparkSession.builder
    .appName("MarketDataBronzeIngestion")
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


# Lecture continue des messages Kafka
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)


# Sélection des informations utiles du message Kafka
raw_stream = kafka_stream.selectExpr(
    "CAST(value AS STRING) AS raw_json",
    "topic",
    "partition",
    "offset",
    "timestamp AS kafka_timestamp",
)


# Traitement de chaque micro-lot de données
def save_bronze(batch_df, batch_id):
    bronze_batch = batch_df.withColumn(
        "ingested_at",
        current_timestamp(),
    )

    # Stockage des événements bruts dans Delta Lake
    (
        bronze_batch.write
        .format("delta")
        .mode("append")
        .save(BRONZE_PATH)
    )

    print(f"\nMicro-lot {batch_id} enregistré dans Bronze")

    # Afficher quelques événements pour vérifier le résultat
    batch_df.select("raw_json").show(5, truncate=False)


# Lancement du pipeline
query = (
    raw_stream.writeStream
    .foreachBatch(save_bronze)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

print("Spark écoute le topic Kafka market-data...")
print(f"Les données brutes seront stockées dans : {BRONZE_PATH}")

query.awaitTermination()
