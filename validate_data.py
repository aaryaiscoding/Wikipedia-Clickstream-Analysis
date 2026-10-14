import argparse
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def main():
    parser = argparse.ArgumentParser(description="Validate the cleaned WikiFlow Parquet datasets.")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(__file__).parent / "clean",
        help="Directory containing the edges, main_page, and sources datasets.",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("WikiFlowDataValidation").getOrCreate()
    try:
        datasets = {
            name: spark.read.parquet(str(args.data_dir / name))
            for name in ("edges", "main_page", "sources")
        }
        required = ("prev", "curr", "type", "n")

        for name, dataframe in datasets.items():
            print(f"\n{'=' * 55}")
            print(f"VALIDATING: {name}")
            print("=" * 55)
            print("Rows:", dataframe.count())

            missing = [column for column in required if column not in dataframe.columns]
            if missing:
                print("Missing required columns:", ", ".join(missing))

            for column in required:
                if column in dataframe.columns:
                    null_count = dataframe.filter(F.col(column).isNull()).count()
                    print(f"Null {column}:", null_count)
                else:
                    print(f"Null {column}: skipped (column missing)")

            if "n" in dataframe.columns:
                print(
                    "Negative click counts:",
                    dataframe.filter(F.col("n") < 0).count(),
                )
                print(
                    "Zero click counts:",
                    dataframe.filter(F.col("n") == 0).count(),
                )
                print("Total clicks:", dataframe.agg(F.sum("n")).first()[0])
            else:
                print("Click count checks: skipped (column n missing)")

            if "type" in dataframe.columns:
                print("\nTraffic types:")
                dataframe.groupBy("type").count().orderBy(
                    F.desc("count")
                ).show(20, truncate=False)
            else:
                print("Traffic types: skipped (column type missing)")

        print("\nEDGE PROBABILITY CHECK")
        edges = datasets["edges"]
        if "p" in edges.columns:
            edges.select(
                F.min("p").alias("min_p"),
                F.max("p").alias("max_p"),
                F.avg("p").alias("avg_p"),
                F.sum(F.when(F.col("p").isNull(), 1).otherwise(0)).alias("null_p_count"),
            ).show()
        else:
            print("Skipped: edges dataset has no p column")

        print("\nValidation complete.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()