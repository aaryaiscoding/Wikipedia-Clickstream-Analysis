"""Render charts from the analysis CSVs in results/ into graphs/."""

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
from matplotlib.ticker import FuncFormatter

RESULTS = Path("results")
OUT = Path("graphs")
OUT.mkdir(exist_ok=True)

BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
INK, INK2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"

plt.rcParams.update({
    "figure.facecolor": SURFACE,
    "axes.facecolor": SURFACE,
    "axes.edgecolor": GRID,
    "axes.labelcolor": INK2,
    "axes.titlecolor": INK,
    "axes.titlesize": 14,
    "axes.titleweight": "bold",
    "axes.titlelocation": "left",
    "xtick.color": INK2,
    "ytick.color": INK,
    "font.size": 10,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.spines.left": False,
})


def nice(title):
    return title.replace("_", " ")


def human(x, _=None):
    for size, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(x) >= size:
            return f"{x / size:.1f}{suffix}".replace(".0", "")
    return f"{x:.0f}"


def header(fig, title, subtitle):
    h = fig.get_figheight()
    fig.text(0.01, 1 - 0.3 / h, title, ha="left", va="center",
             fontsize=14, fontweight="bold", color=INK)
    fig.text(0.01, 1 - 0.6 / h, subtitle, ha="left", va="center",
             fontsize=10, color=INK2)
    fig.tight_layout(rect=(0, 0, 1, 1 - 0.8 / h))


def hbar(labels, values, title, subtitle, filename, xlabel="Clicks", fmt=human, color=BLUE):
    fig, ax = plt.subplots(figsize=(10, 0.38 * len(labels) + 1.6))
    y = range(len(labels))[::-1]
    ax.barh(list(y), values, color=color, height=0.7)
    ax.set_yticks(list(y), labels)
    ax.tick_params(axis="y", length=0)
    ax.xaxis.set_major_formatter(FuncFormatter(fmt))
    ax.grid(axis="x", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_xlabel(xlabel)
    # Direct-label only the leading bar to keep the chart quiet.
    ax.text(values[0], max(y), "  " + fmt(values[0]), va="center", color=INK2)
    ax.set_xlim(0, max(values) * 1.18)
    header(fig, title, subtitle)
    fig.savefig(OUT / filename, dpi=150)
    plt.close(fig)
    print("Saved:", OUT / filename)


def a1_top_articles():
    df = pd.read_csv(RESULTS / "a1_top_articles.csv")
    main = df.iloc[0]
    rest = df.iloc[1:]
    hbar(
        [nice(a) for a in rest["article"]], rest["total_clicks"].tolist(),
        "A1 - Most-visited articles (Sept 2026)",
        f"Total incoming clicks. Main Page excluded ({human(main['total_clicks'])} clicks) "
        "so the other bars stay readable.",
        "a1_top_articles.png",
    )


def a1_traffic_sources():
    df = pd.read_csv(RESULTS / "traffic_sources.csv")
    total = df["total_clicks"].sum()
    share = (df["total_clicks"] / total * 100).tolist()
    labels = [s.replace("other-", "").capitalize() for s in df["source"]]
    hbar(
        labels, share,
        "A1 - Where readers arrive from",
        f"Share of {human(total)} clicks from outside article links",
        "a1_traffic_sources.png", xlabel="Share of clicks (%)",
        fmt=lambda v, _=None: f"{v:.1f}%",
    )


def a2_transitions():
    df = pd.read_csv(RESULTS / "a2_top_by_count.csv")
    hbar(
        [f"{nice(p)}  ->  {nice(c)}" for p, c in zip(df["prev"], df["curr"])],
        df["n"].tolist(),
        "A2 - Busiest article-to-article links",
        "Top 20 internal-link transitions by click count",
        "a2_top_by_count.png",
    )
    df = pd.read_csv(RESULTS / "a2_top_by_prob.csv")
    hbar(
        [f"{nice(p)}  ->  {nice(c)}" for p, c in zip(df["prev"], df["curr"])],
        df["p"].tolist(),
        "A2 - Most predictable next clicks",
        "Top 20 transitions by probability p (n >= 1,000)",
        "a2_top_by_prob.png", xlabel="Transition probability p",
        fmt=lambda v, _=None: f"{v:.2f}",
    )


def a3_clusters():
    clusters = pd.read_csv(RESULTS / "a3_clusters.csv")
    sizes = clusters["cluster"].value_counts()

    fig, ax = plt.subplots(figsize=(10, 4.5))
    bins = range(2, sizes.max() + 2)
    ax.hist(sizes, bins=bins, color=BLUE, rwidth=0.85)
    ax.set_yscale("log")
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_xlabel("Articles in community")
    ax.set_ylabel("Number of communities (log)")
    header(fig, "A3 - Louvain community sizes",
           f"{len(sizes)} communities across {len(clusters)} articles; most are pairs")
    fig.savefig(OUT / "a3_cluster_sizes.png", dpi=150)
    plt.close(fig)
    print("Saved:", OUT / "a3_cluster_sizes.png")

    # Network view of the three largest communities.
    pairs = pd.read_csv(RESULTS / "a3_two_way.csv")
    membership = dict(zip(clusters["article"], clusters["cluster"]))
    top = list(sizes.index[:3])
    colors = dict(zip(top, (BLUE, ORANGE, AQUA)))

    graph = nx.Graph()
    for row in pairs.itertuples():
        a, b = row.article_a, row.article_b
        if membership.get(a) in top and membership.get(a) == membership.get(b):
            graph.add_edge(a, b, weight=row.total_clicks)

    strength = dict(graph.degree(weight="weight"))
    fig, axes = plt.subplots(1, 3, figsize=(21, 8))
    for ax, c in zip(axes, top):
        sub = graph.subgraph(n for n in graph if membership[n] == c)
        pos = nx.kamada_kawai_layout(sub)
        biggest = max(strength[n] for n in sub)
        nx.draw_networkx_edges(sub, pos, ax=ax, edge_color="#c9c8c2", width=0.8)
        nx.draw_networkx_nodes(
            sub, pos, ax=ax, node_color=colors[c],
            node_size=[40 + 700 * strength[n] / biggest for n in sub],
            edgecolors=SURFACE, linewidths=1.5,
        )
        # Label the most-clicked articles in the community.
        names = sorted(sub, key=strength.get, reverse=True)[:6]
        nx.draw_networkx_labels(
            sub, pos, {n: nice(n) for n in names}, ax=ax, font_size=9,
            font_color=INK, verticalalignment="bottom",
            bbox=dict(boxstyle="round,pad=0.15", fc=SURFACE, ec="none", alpha=0.8),
        )
        ax.set_title(f"{c}: {sizes[c]} articles", fontsize=11, color=INK2)
        ax.set_axis_off()
    header(fig, "A3 - Three largest navigation communities",
           "Louvain communities on the 2,000 two-way pairs; node size = total two-way clicks")
    fig.savefig(OUT / "a3_cluster_network.png", dpi=150)
    plt.close(fig)
    print("Saved:", OUT / "a3_cluster_network.png")


def a4_paths():
    df = pd.read_csv(RESULTS / "a4_paths.csv").head(20)
    hbar(
        [f"{nice(a)} -> {nice(b)} -> {nice(c)}"
         for a, b, c in zip(df["step1"], df["step2"], df["step3"])],
        df["est_readers"].tolist(),
        "A4 - Most-travelled two-step reading paths",
        "Top 20 paths; estimated readers = n(A->B) x n(B->C) / outgoing(B)",
        "a4_paths.png", xlabel="Estimated readers",
    )


def a5_subgraphs(top_n=15):
    starts = {
        "asian_games": "2026_Asian_Games",
        "artificial_intelligence": "Artificial_intelligence",
        "india": "India",
        "python": "Python_(programming_language)",
    }
    for key, center in starts.items():
        df = pd.read_csv(RESULTS / f"a5_subgraph_{key}.csv")
        incoming = df[df["curr"] == center].nlargest(top_n, "n")
        outgoing = df[df["prev"] == center].nlargest(top_n, "n")

        graph = nx.DiGraph()
        for row in incoming.itertuples():
            graph.add_edge(row.prev, center, n=row.n)
        for row in outgoing.itertuples():
            graph.add_edge(center, row.curr, n=row.n)

        fig, ax = plt.subplots(figsize=(13, 11))
        pos = nx.shell_layout(graph, [[center], [n for n in graph if n != center]])
        biggest = max(d["n"] for *_, d in graph.edges(data=True))
        for color, edges in ((BLUE, [(u, v) for u, v in graph.edges if v == center]),
                             (ORANGE, [(u, v) for u, v in graph.edges if u == center])):
            nx.draw_networkx_edges(
                graph, pos, edgelist=edges, ax=ax, edge_color=color,
                width=[0.8 + 5 * graph.edges[e]["n"] / biggest for e in edges],
                arrowsize=12, connectionstyle="arc3,rad=0.08",
                node_size=500, alpha=0.85,
            )
        nx.draw_networkx_nodes(graph, pos, ax=ax, node_size=[
            1600 if n == center else 220 for n in graph],
            node_color=[INK if n == center else "#c9c8c2" for n in graph],
            edgecolors=SURFACE, linewidths=1.5)
        nx.draw_networkx_labels(
            graph, pos, {n: nice(n) for n in graph if n != center},
            ax=ax, font_size=8, font_color=INK)
        ax.text(pos[center][0], pos[center][1] - 0.09, nice(center),
                ha="center", va="top", color=INK, fontsize=11, fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.3", fc=SURFACE, ec=GRID), zorder=5)
        ax.plot([], [], color=BLUE, lw=3, label=f"Top {len(incoming)} incoming links")
        ax.plot([], [], color=ORANGE, lw=3, label=f"Top {len(outgoing)} outgoing links")
        ax.legend(loc="lower left", frameon=False)
        ax.set_axis_off()
        ax.set_title(f"A5 - Navigation around \"{nice(center)}\" "
                     "(line width = clicks)", fontsize=14)
        fig.tight_layout()
        fig.savefig(OUT / f"a5_subgraph_{key}.png", dpi=150)
        plt.close(fig)
        print("Saved:", OUT / f"a5_subgraph_{key}.png")


def missing_links():
    df = pd.read_csv(RESULTS / "missing_links.csv").head(20)
    hbar(
        [f"{nice(p)}  ->  {nice(c)}" for p, c in zip(df["prev"], df["curr"])],
        df["n"].tolist(),
        "Candidate missing links",
        "Top 20 type='other' transitions: readers went A -> B without a direct link",
        "missing_links.png",
    )


if __name__ == "__main__":
    a1_top_articles()
    a1_traffic_sources()
    a2_transitions()
    a3_clusters()
    a4_paths()
    a5_subgraphs()
    missing_links()
