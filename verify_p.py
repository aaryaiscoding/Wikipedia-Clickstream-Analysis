from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("VerifyProbabilities")
    .master("local[1]")
    .config("spark.local.dir", "/home/aarya/spark-tmp")
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
