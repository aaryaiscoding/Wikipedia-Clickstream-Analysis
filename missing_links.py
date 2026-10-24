
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from bootstrap_sample_data import ensure_sample_data

ensure_sample_data()

spark = (
    SparkSession.builder
    .appName("WikiFlowMissingLinks")
    .master("local[2]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.default.parallelism", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    edges = spark.read.parquet("clean/edges")

    missing = (
        edges.filter(F.col("type") == "other")
        .groupBy("prev", "curr")
        .agg(F.sum("n").alias("n"))
        .orderBy(F.desc("n"))
        .limit(50)
    )

    missing.toPandas().to_csv(
        "results/missing_links.csv",
        index=False
    )

    print("Created results/missing_links.csv")
    print("Top 50 other-type transitions exported.")

finally:
    spark.stop()

