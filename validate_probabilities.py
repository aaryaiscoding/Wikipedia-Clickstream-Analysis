import argparse
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from bootstrap_sample_data import ensure_sample_data


def main():
    ensure_sample_data()
    parser = argparse.ArgumentParser(
        description="Validate outgoing transition probabilities in WikiFlow edges."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(__file__).parent / "clean",
        help="Directory containing the edges dataset.",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("WikiFlowProbabilityValidation").getOrCreate()
    try:
        edges = spark.read.parquet(str(args.data_dir / "edges"))

        probability_sums = edges.groupBy("prev").agg(
            F.sum("p").alias("probability_sum"),
            F.count("*").alias("outgoing_edges"),
        )

        print("Number of source pages:", probability_sums.count())

        tolerance = 0.001
        invalid = probability_sums.filter(
            F.abs(F.col("probability_sum") - 1.0) > tolerance
        )

        print(
            "Source pages with probability sums outside tolerance:",
            invalid.count(),
        )

        print("\nExamples of invalid probability sums:")
        invalid.orderBy(
            F.abs(F.col("probability_sum") - 1.0).desc()
        ).show(10, truncate=False)

        print("\nProbability-sum statistics:")
        probability_sums.select(
            F.min("probability_sum").alias("min_sum"),
            F.max("probability_sum").alias("max_sum"),
            F.avg("probability_sum").alias("avg_sum"),
        ).show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()