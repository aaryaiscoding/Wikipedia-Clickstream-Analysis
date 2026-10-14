
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
    SparkSession.builder
    .appName("WikiFlowA1Analysis")
    .master("local[2]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.default.parallelism", "4")
    .getOrCreate()
)

    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("Loading cleaned datasets...")

        edges = spark.read.parquet("clean/edges")
        main_page = spark.read.parquet("clean/main_page")
        sources = spark.read.parquet("clean/sources")

        # Standardize the columns before combining the datasets.
        edge_clicks = edges.select("curr", "n")
        main_page_clicks = main_page.select("curr", "n")
        source_clicks = sources.select("curr", "n")

        all_clicks = (
            edge_clicks
            .unionByName(main_page_clicks)
            .unionByName(source_clicks)
        )

        # A1: Top 20 articles by total incoming clicks.
        print("Calculating top articles...")

        top_articles = (
            all_clicks
            .groupBy("curr")
            .agg(F.sum("n").alias("clicks"))
            .orderBy(F.desc("clicks"))
            .limit(20)
            .select(
                F.col("curr"),
                F.col("clicks")
            )
        )

        # Spark writes CSV output as a directory.
        # Coalesce the small top-20 result into one CSV file.
        top_articles.coalesce(1).write.mode("overwrite").option(
            "header", True
        ).csv("results/a1_top_articles_output")

        # A1: Total clicks by source label.
        print("Calculating traffic sources...")

        traffic_sources = (
            sources
            .groupBy("prev")
            .agg(F.sum("n").alias("clicks"))
            .orderBy(F.desc("clicks"))
            .select(
                F.col("prev"),
                F.col("clicks")
            )
        )

        traffic_sources.coalesce(1).write.mode("overwrite").option(
            "header", True
        ).csv("results/traffic_sources_output")

        print("A1 analysis completed.")
        print("Top 20 articles:")
        top_articles.show(20, truncate=False)

        print("Traffic sources:")
        traffic_sources.show(30, truncate=False)

        print("CSV output directories:")
        print("results/a1_top_articles_output/")
        print("results/traffic_sources_output/")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

