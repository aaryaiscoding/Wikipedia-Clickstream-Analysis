import argparse
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def main():
    parser = argparse.ArgumentParser(
        description="Summarize the internal link graph in WikiFlow edges."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(__file__).parent / "clean",
        help="Directory containing the edges dataset.",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("WikiFlowGraphSummary")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        edges = spark.read.parquet(str(args.data_dir / "edges"))

        # Keep only actual internal Wikipedia links.
        links = edges.filter(F.col("type") == "link")

        print("\n=== WikiFlow Graph Summary ===")
        print("Internal link rows:", links.count())
        print("Unique source pages:", links.select("prev").distinct().count())
        print("Unique destination pages:", links.select("curr").distinct().count())
        print(
            "Unique source-destination pairs:",
            links.select("prev", "curr").distinct().count(),
        )

        print("\nMost frequent internal transitions:")
        links.orderBy(F.desc("n")).select("prev", "curr", "n", "p").show(
            10,
            truncate=False,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()