import argparse
from pathlib import Path

from pyspark.sql import SparkSession

from bootstrap_sample_data import ensure_sample_data


def main():
    ensure_sample_data()
    parser = argparse.ArgumentParser(description="Inspect the cleaned WikiFlow Parquet datasets.")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(__file__).parent / "clean",
        help="Directory containing the edges, main_page, and sources datasets.",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("WikiFlowDataInspector").getOrCreate()
    try:
        for folder in ("edges", "main_page", "sources"):
            dataset_dir = args.data_dir / folder
            print(f"\n{'=' * 50}")
            print(f"FOLDER: {folder}")
            print("=" * 50)
            dataframe = spark.read.parquet(str(dataset_dir))
            dataframe.printSchema()
            dataframe.show(5, truncate=False)
            print("Total rows:", dataframe.count())
            print("Columns:", dataframe.columns)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()