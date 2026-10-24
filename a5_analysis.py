
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from bootstrap_sample_data import ensure_sample_data


def main():
    ensure_sample_data()
    spark = (
        SparkSession.builder
        .appName("WikiFlowA5Analysis")
        .master("local[2]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.default.parallelism", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Loading internal article transitions...")

        edges = (
            spark.read.parquet("clean/edges")
            .filter(col("type") == "link")
            .select("prev", "curr", "n")
        )

        # Four starting articles from different subject areas.
        starts = [
            ("2026_Asian_Games", "asian_games"),
            ("Artificial_intelligence", "artificial_intelligence"),
            ("India", "india"),
            ("Python_(programming_language)", "python")
        ]

        for article, filename in starts:
            print(f"Extracting subgraph around: {article}")

            # Include links leaving or entering the selected article.
            subgraph = edges.filter(
                (col("prev") == article) | (col("curr") == article)
            )

            output_path = f"results/a5_subgraph_{filename}_output"

            subgraph.coalesce(1).write \
                .mode("overwrite") \
                .option("header", True) \
                .csv(output_path)

            print(f"Saved: {output_path}")

        print("A5 subgraph extraction completed.")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()

