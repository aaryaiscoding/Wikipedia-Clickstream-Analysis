import argparse
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from bootstrap_sample_data import ensure_sample_data


def main():
    ensure_sample_data()
    parser = argparse.ArgumentParser(
        description="Find duplicate transitions in the WikiFlow edges dataset."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(__file__).parent / "clean",
        help="Directory containing the edges dataset.",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("WikiFlowDuplicateValidation").getOrCreate()
    try:
        edges = spark.read.parquet(str(args.data_dir / "edges"))

        duplicate_groups = (
            edges.groupBy("prev", "curr", "type")
            .count()
            .filter(F.col("count") > 1)
        )

        print("Duplicate transition groups:", duplicate_groups.count())

        print("Example duplicate groups:")
        duplicate_groups.orderBy(F.desc("count")).show(20, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()