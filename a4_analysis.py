
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

def main():
    spark = (
        SparkSession.builder
        .appName("WikiFlowA4Analysis")
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

        # For each A -> B -> C path, estimate readers using:
        # (clicks A -> B * clicks B -> C) / total outgoing clicks from B.
        outgoing = (
            edges.groupBy("prev")
            .agg(spark_sum("n").alias("outgoing_total"))
            .withColumnRenamed("prev", "middle")
        )

        first = edges.select(
            col("prev").alias("step1"),
            col("curr").alias("middle"),
            col("n").alias("n12")
        )

        second = edges.select(
            col("prev").alias("middle"),
            col("curr").alias("step3"),
            col("n").alias("n23")
        )

        paths = (
            first.join(second, on="middle", how="inner")
            .join(outgoing, on="middle", how="inner")
            .filter(col("step1") != col("step3"))
            .withColumn(
                "est_readers",
                (
                    col("n12").cast("double") *
                    col("n23").cast("double") /
                    col("outgoing_total")
                )
            )
            .select("step1", "middle", "step3", "est_readers")
            .withColumnRenamed("middle", "step2")
            .orderBy(col("est_readers").desc())
            .limit(50)
        )

        print("Top estimated two-step paths:")
        paths.show(10, truncate=False)

        paths.coalesce(1).write \
            .mode("overwrite") \
            .option("header", True) \
            .csv("results/a4_paths_output")

        print("A4 analysis completed.")
        print("Output: results/a4_paths_output/")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()

