from __future__ import annotations

import os
from pathlib import Path
import tempfile

from pyspark.sql import SparkSession, functions as F


SAMPLE_ROWS = [
    ("other-search", "2026_Asian_Games", "external", 1200),
    ("other-empty", "Python_(programming_language)", "external", 800),
    ("other-internal", "Wikipedia", "external", 150),
    ("2026_Asian_Games", "2026_Asian_Games_medal_table", "link", 585),
    ("2026_Asian_Games", "India_at_the_2026_Asian_Games", "link", 210),
    ("2026_Asian_Games", "2026_Asian_Games", "link", 5),
    ("Python_(programming_language)", "Python_software", "link", 400),
    ("Python_(programming_language)", "Guido_van_Rossum", "other", 120),
    ("Python_software", "Python_(programming_language)", "link", 260),
    ("Guido_van_Rossum", "Python_(programming_language)", "other", 180),
    ("Main_Page", "2026_Asian_Games", "link", 1500),
    ("2026_Asian_Games", "Main_Page", "link", 900),
    ("Wikipedia", "Main_Page", "link", 300),
    ("Main_Page", "Wikipedia", "link", 500),
    ("Main_Page", "Wikipedia", "other", 200),
    ("2026_Asian_Games", "India_at_the_2026_Asian_Games", "other", 80),
    ("India_at_the_2026_Asian_Games", "2026_Asian_Games", "link", 110),
    ("Wikipedia", "Python_(programming_language)", "link", 220),
    ("Python_(programming_language)", "Wikipedia", "link", 200),
    ("Python_software", "List_of_Python_software", "link", 110),
    ("List_of_Python_software", "Python_software", "link", 90),
]


def _build_spark() -> SparkSession:
    spark_dir = Path(tempfile.gettempdir()) / "wikiflow-spark"
    spark_dir.mkdir(parents=True, exist_ok=True)
    spark = (
        SparkSession.builder
        .appName("WikiFlowBootstrap")
        .master("local[1]")
        .config("spark.local.dir", str(spark_dir))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def build_sample_data() -> None:
    spark = _build_spark()
    try:
        raw = spark.createDataFrame(SAMPLE_ROWS, ["prev", "curr", "type", "n"])
        raw.write.mode("overwrite").parquet("clickstream_raw_parquet")

        d = raw.dropna().filter(F.col("n") > 0)
        is_src = F.col("prev").startswith("other-")
        sources = d.filter(is_src)

        edges = (
            d.filter(~is_src)
            .filter(F.col("type").isin("link", "other"))
            .filter(F.col("prev") != F.col("curr"))
        )

        is_main = (F.col("prev") == "Main_Page") | (F.col("curr") == "Main_Page")
        main_page = edges.filter(is_main)
        edges = edges.filter(~is_main)

        edges = edges.withColumn("title_clean", F.regexp_replace("curr", "_", " "))
        totals = edges.groupBy("prev").agg(F.sum("n").alias("total_n"))
        edges = (
            edges.join(totals, on="prev", how="left")
            .withColumn("p", F.col("n") / F.col("total_n"))
            .drop("total_n")
        )

        os.makedirs("clean", exist_ok=True)
        sources.write.mode("overwrite").parquet("clean/sources")
        edges.write.mode("overwrite").parquet("clean/edges")
        main_page.write.mode("overwrite").parquet("clean/main_page")

        demo_dir = Path("demo/edges_1pct")
        demo_dir.mkdir(parents=True, exist_ok=True)
        edges.write.mode("overwrite").parquet(str(demo_dir))
    finally:
        spark.stop()


def ensure_sample_data() -> None:
    raw_ok = Path("clickstream_raw_parquet").exists()
    clean_ok = Path("clean").exists() and Path("clean/edges").exists()
    demo_ok = Path("demo/edges_1pct").exists()
    if not (raw_ok and clean_ok and demo_ok):
        build_sample_data()


if __name__ == "__main__":
    ensure_sample_data()
    print("Sample WikiFlow data is ready.")
