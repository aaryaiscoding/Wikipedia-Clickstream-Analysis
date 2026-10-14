
import csv
from pathlib import Path

import networkx as nx
from networkx.algorithms.community import louvain_communities

INPUT = Path("results/a3_two_way.csv")
OUTPUT = Path("results/a3_clusters.csv")


def main():
    graph = nx.Graph()

    with INPUT.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            a = row["article_a"]
            b = row["article_b"]
            weight = float(row["total_clicks"])
            graph.add_edge(a, b, weight=weight)

    print("Articles:", graph.number_of_nodes())
    print("Two-way links:", graph.number_of_edges())

    communities = louvain_communities(
        graph,
        weight="weight",
        resolution=1.0,
        seed=42,
    )

    communities = sorted(
        communities,
        key=lambda group: (-len(group), min(group))
    )

    assignments = {}
    for index, community in enumerate(communities, start=1):
        cluster = f"C{index:02d}"
        for article in community:
            assignments[article] = cluster

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["cluster", "article"])
        for article in sorted(assignments):
            writer.writerow([assignments[article], article])

    print("Communities:", len(communities))
    print("Saved:", OUTPUT)
    print("\nLargest communities:")
    for index, community in enumerate(communities[:10], start=1):
        print(f"C{index:02d}: {len(community)} articles")


if __name__ == "__main__":
    main()
