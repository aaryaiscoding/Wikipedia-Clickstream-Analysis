import tempfile
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from bootstrap_sample_data import ensure_sample_data

ensure_sample_data()

spark_dir = Path(tempfile.gettempdir()) / "wikiflow-spark"
spark_dir.mkdir(parents=True, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("VerifyProbabilities")
    .master("local[1]")
    .config("spark.local.dir", str(spark_dir))
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.hadoop.parquet.hadoop.vectored.io.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

edges = spark.read.parquet("clean/edges")

print("Columns:", edges.columns)

sample_prev = [
    row["prev"]
    for row in edges.select("prev").distinct().limit(5).collect()
]

for article in sample_prev:
    total = (
        edges.filter(F.col("prev") == article)
        .agg(F.sum("p").alias("total"))
        .first()["total"]
    )
    print(f"{article}: sum(p) = {total}")

spark.stop()
