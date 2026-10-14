
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path
import time

BASE = Path("demo/edges_1pct")
OUT = Path("demo/results")
OUT.mkdir(parents=True, exist_ok=True)

start = time.time()

spark = (
    SparkSession.builder
    .appName("WikiFlowDemo")
    .master("local[2]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.default.parallelism", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    edges = spark.read.parquet(str(BASE)).cache()
    edges.count()

    # A1: Top 20 articles by total incoming clicks
    top_articles = (
        edges.filter(F.col("type") == "link")
        .groupBy("curr")
        .agg(F.sum("n").alias("total_clicks"))
        .orderBy(F.desc("total_clicks"))
        .limit(20)
    )
    top_articles.toPandas().to_csv(
        OUT / "a1_top_articles.csv", index=False
    )

    # A1: Total clicks per traffic source
    traffic_sources = (
        spark.read.parquet("clean/sources")
        .groupBy("type")
        .agg(F.sum("n").alias("total_clicks"))
        .orderBy(F.desc("total_clicks"))
    )
    traffic_sources.toPandas().to_csv(
        OUT / "traffic_sources.csv", index=False
    )

    # A2: Top 20 transitions by click count
    links = edges.filter(F.col("type") == "link")

    top_count = (
        links.select("prev", "curr", "n", "p")
        .orderBy(F.desc("n"))
        .limit(20)
    )
    top_count.toPandas().to_csv(
        OUT / "a2_top_by_count.csv", index=False
    )

    # A2: Top 20 transitions by probability, with n >= 1000
    top_prob = (
        links.filter(F.col("n") >= 1000)
        .select("prev", "curr", "n", "p")
        .orderBy(F.desc("p"), F.desc("n"))
        .limit(20)
    )
    top_prob.toPandas().to_csv(
        OUT / "a2_top_by_prob.csv", index=False
    )

    print("\nDemo completed successfully.")
    print("Output folder:", OUT)
    print("Elapsed seconds:", round(time.time() - start, 2))

finally:
    spark.stop()

