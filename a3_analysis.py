
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from bootstrap_sample_data import ensure_sample_data


def main():
    ensure_sample_data()
    spark = (
        SparkSession.builder
        .appName("WikiFlowA3Analysis")
        .master("local[2]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.default.parallelism", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Loading internal article links...")

        edges = (
            spark.read.parquet("clean/edges")
            .filter(col("type") == "link")
            .select("prev", "curr", "n")
        )

        print("Finding two-way article pairs...")

        # Keep each pair only once by imposing a consistent article order.
        forward = edges.select(
            col("prev").alias("article_a"),
            col("curr").alias("article_b"),
            col("n").alias("clicks_a_to_b")
        )

        reverse = edges.select(
            col("curr").alias("article_a"),
            col("prev").alias("article_b"),
            col("n").alias("clicks_b_to_a")
        )

        # A pair qualifies only when links exist in both directions.
        pairs = (
            forward
            .join(
                reverse,
                on=["article_a", "article_b"],
                how="inner"
            )
            .filter(col("article_a") < col("article_b"))
            .select(
                col("article_a"),
                col("article_b"),
                col("clicks_a_to_b"),
                col("clicks_b_to_a"),
                (
                    col("clicks_a_to_b") + col("clicks_b_to_a")
                ).alias("total_clicks")
            )
            .orderBy(col("total_clicks").desc())
            .limit(2000)
        )

        print("Top two-way pairs:")
        pairs.show(10, truncate=False)

        pairs.coalesce(1).write \
            .mode("overwrite") \
            .option("header", True) \
            .csv("results/a3_two_way_output")

        print("A3 two-way pair analysis completed.")
        print("Output: results/a3_two_way_output/")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()

