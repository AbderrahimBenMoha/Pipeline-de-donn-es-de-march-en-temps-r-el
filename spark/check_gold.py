
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

GOLD_PATH = "/opt/app/data/gold/minute_metrics"

spark = (
    SparkSession.builder
    .appName("CheckGold")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension",
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Lire la table Gold enregistrée dans Delta Lake
gold_df = spark.read.format("delta").load(GOLD_PATH)

print("\n=== STRUCTURE DE LA TABLE GOLD ===")
gold_df.printSchema()

print("\n=== INDICATEURS CALCULÉS ===")
gold_df.orderBy(
    col("minute_start").desc()
).show(10, truncate=False)

print(f"Nombre de lignes dans Gold : {gold_df.count()}")

spark.stop()
