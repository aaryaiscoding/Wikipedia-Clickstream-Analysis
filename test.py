from pathlib import Path

from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("WikiClickstream")
         .config("spark.driver.memory", "4g").getOrCreate())
raw_path = Path("clickstream-enwiki-2026-09.tsv.gz")
parquet_path = Path("clickstream_raw_parquet")

if raw_path.is_file():
    df = spark.read.csv(
        str(raw_path),
        sep="\t",
        schema="prev STRING, curr STRING, type STRING, n LONG",
    )
    df.write.mode("overwrite").parquet(str(parquet_path))
else:
    from bootstrap_sample_data import ensure_sample_data

    ensure_sample_data()
    df = spark.read.parquet(str(parquet_path))

df.show(5, truncate=False)
print("Total rows:", df.count())
print("Parquet data is ready.")
spark.stop()
