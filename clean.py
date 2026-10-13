
from pyspark.sql import SparkSession, Window, functions as F
import os

spark = (
    SparkSession.builder
    .appName("Clean")
    .master("local[1]")
    .config("spark.local.dir", "/home/aarya/spark-tmp")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.files.maxPartitionBytes", str(16 * 1024 * 1024))
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.hadoop.parquet.hadoop.vectored.io.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = spark.read.parquet("clickstream_raw_parquet")
log = []

# Count input rows
log.append(("raw rows", raw.count()))

# Remove nulls and non-positive counts
d = raw.dropna().filter(F.col("n") > 0)
log.append(("after dropping nulls and n <= 0", d.count()))

# Separate source categories
is_src = F.col("prev").startswith("other-")
sources = d.filter(is_src)

log.append(("sources rows (other-*)", sources.count()))

# Keep valid article-to-article transitions, excluding self-links
edges = (
    d.filter(~is_src)
     .filter(F.col("type").isin("link", "other"))
     .filter(F.col("prev") != F.col("curr"))
)

log.append(("edges after removing self-links", edges.count()))

# Keep Main_Page transitions separately
is_main = (
    (F.col("prev") == "Main_Page") |
    (F.col("curr") == "Main_Page")
)

main_page = edges.filter(is_main)
edges = edges.filter(~is_main)

log.append(("main_page rows (kept separately)", main_page.count()))
log.append(("final edges", edges.count()))

# Clean titles
edges = edges.withColumn(
    "title_clean",
    F.regexp_replace("curr", "_", " ")
)

# Calculate transition probabilities per source article.
# Use aggregation + join instead of an unbounded window.
totals = edges.groupBy("prev").agg(
    F.sum("n").alias("total_n")
)

edges = (
    edges.join(totals, on="prev", how="left")
         .withColumn("p", F.col("n") / F.col("total_n"))
         .drop("total_n")
)

# Write each output
os.makedirs("clean", exist_ok=True)

sources.write.mode("overwrite").parquet("clean/sources")
edges.write.mode("overwrite").parquet("clean/edges")
main_page.write.mode("overwrite").parquet("clean/main_page")

# Save preprocessing log
with open("cleaning_log.txt", "w") as f:
    for step, count in log:
        line = f"{step}: {count:,}"
        print(line)
        f.write(line + "\n")

print("Cleaning completed successfully!")
spark.stop()

