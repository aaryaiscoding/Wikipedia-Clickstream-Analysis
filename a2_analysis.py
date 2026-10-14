
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = (
        SparkSession.builder
        .appName("WikiFlowA2Analysis")
        .master("local[2]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.default.parallelism", "4")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Loading cleaned edge data...")

        edges = spark.read.parquet("clean/edges")

        # Analyze internal article-to-article links only.
        transitions = edges.filter(col("type") == "link")

        print("Calculating top 20 transitions by click count...")

        top_by_count = (
            transitions
            .select("prev", "curr", "n")
            .orderBy(col("n").desc())
            .limit(20)
        )

        top_by_count.show(20, truncate=False)

        top_by_count.coalesce(1).write \
            .mode("overwrite") \
            .option("header", True) \
            .csv("results/a2_top_by_count_output")

        print("Calculating top 20 transitions by probability (n >= 1000)...")

        top_by_prob = (
            transitions
            .filter(col("n") >= 1000)
            .select("prev", "curr", "n", "p")
            .orderBy(col("p").desc())
            .limit(20)
        )

        top_by_prob.show(20, truncate=False)

        top_by_prob.coalesce(1).write \
            .mode("overwrite") \
            .option("header", True) \
            .csv("results/a2_top_by_prob_output")

        print("A2 analysis completed.")
        print("Output directories:")
        print("results/a2_top_by_count_output/")
        print("results/a2_top_by_prob_output/")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()

