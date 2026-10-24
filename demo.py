
from pathlib import Path
import time

import networkx as nx
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col

from bootstrap_sample_data import ensure_sample_data

ensure_sample_data()

BASE = Path("demo/edges_1pct")
OUT = Path("demo/results")
OUT.mkdir(parents=True, exist_ok=True)

start = time.time()

spark = (
    SparkSession.builder
    .appName("WikiFlowDemo")
    .master("local[2]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.default.parallelism", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    edges = spark.read.parquet(str(BASE)).cache()
    edges.count()

    # A1: Top 20 articles by total incoming clicks
    top_articles = (
        edges.filter(F.col("type") == "link")
        .groupBy("curr")
        .agg(F.sum("n").alias("total_clicks"))
        .orderBy(F.desc("total_clicks"))
        .limit(20)
    )
    top_articles.toPandas().to_csv(
        OUT / "a1_top_articles.csv", index=False
    )

    # A1: Total clicks per traffic source
    traffic_sources = (
        spark.read.parquet("clean/sources")
        .groupBy("type")
        .agg(F.sum("n").alias("total_clicks"))
        .orderBy(F.desc("total_clicks"))
    )
    traffic_sources.toPandas().to_csv(
        OUT / "traffic_sources.csv", index=False
    )

    # A2: Top 20 transitions by click count
    links = edges.filter(F.col("type") == "link")

    top_count = (
        links.select("prev", "curr", "n", "p")
        .orderBy(F.desc("n"))
        .limit(20)
    )
    top_count.toPandas().to_csv(
        OUT / "a2_top_by_count.csv", index=False
    )

    # A2: Top 20 transitions by probability, with n >= 1000
    top_prob = (
        links.filter(F.col("n") >= 1000)
        .select("prev", "curr", "n", "p")
        .orderBy(F.desc("p"), F.desc("n"))
        .limit(20)
    )
    top_prob.toPandas().to_csv(
        OUT / "a2_top_by_prob.csv", index=False
    )

    # A3: Top two-way article pairs
    forward = links.select(
        col("prev").alias("article_a"),
        col("curr").alias("article_b"),
        col("n").alias("clicks_a_to_b"),
    )
    reverse = links.select(
        col("curr").alias("article_a"),
        col("prev").alias("article_b"),
        col("n").alias("clicks_b_to_a"),
    )
    two_way = (
        forward.join(reverse, on=["article_a", "article_b"], how="inner")
        .filter(col("article_a") < col("article_b"))
        .withColumn(
            "total_clicks",
            col("clicks_a_to_b") + col("clicks_b_to_a"),
        )
        .orderBy(F.desc("total_clicks"))
        .limit(2000)
    )
    two_way_pd = two_way.toPandas()
    two_way_pd.to_csv(OUT / "a3_two_way.csv", index=False)

    graph = nx.Graph()
    for row in two_way_pd.itertuples(index=False):
        graph.add_edge(
            row.article_a,
            row.article_b,
            weight=float(row.total_clicks),
        )
    clusters = []
    if graph.number_of_edges():
        communities = nx.community.louvain_communities(
            graph,
            weight="weight",
            resolution=1.0,
            seed=42,
        )
        communities.sort(key=lambda group: (-len(group), min(group)))
        clusters = sorted(
            (f"C{index:02d}", article)
            for index, community in enumerate(communities, start=1)
            for article in community
        )
    import pandas as pd

    pd.DataFrame(clusters, columns=["cluster", "article"]).to_csv(
        OUT / "a3_clusters.csv", index=False
    )

    # A4: Top estimated two-step reading paths
    outgoing = (
        links.groupBy("prev")
        .agg(F.sum("n").alias("outgoing_total"))
        .withColumnRenamed("prev", "middle")
    )
    first = links.select(
        col("prev").alias("step1"),
        col("curr").alias("middle"),
        col("n").alias("n12"),
    )
    second = links.select(
        col("prev").alias("middle"),
        col("curr").alias("step3"),
        col("n").alias("n23"),
    )
    paths = (
        first.join(second, on="middle", how="inner")
        .join(outgoing, on="middle", how="inner")
        .filter(col("step1") != col("step3"))
        .withColumn(
            "est_readers",
            col("n12").cast("double") * col("n23").cast("double")
            / col("outgoing_total"),
        )
        .select("step1", col("middle").alias("step2"), "step3", "est_readers")
        .orderBy(F.desc("est_readers"))
        .limit(50)
    )
    paths.toPandas().to_csv(OUT / "a4_paths.csv", index=False)

    # A5: Incoming and outgoing links around four example topics
    topics = [
        ("2026_Asian_Games", "asian_games"),
        ("Artificial_intelligence", "artificial_intelligence"),
        ("India", "india"),
        ("Python_(programming_language)", "python"),
    ]
    for article, filename in topics:
        subgraph = links.filter(
            (col("prev") == article) | (col("curr") == article)
        ).select("prev", "curr", "n")
        subgraph.toPandas().to_csv(
            OUT / f"a5_subgraph_{filename}.csv",
            index=False,
        )

    print("\nDemo completed successfully.")
    print("Output folder:", OUT)
    print("A1-A5 outputs:", len(list(OUT.glob("*.csv"))), "CSV files")
    print("Elapsed seconds:", round(time.time() - start, 2))

finally:
    spark.stop()
