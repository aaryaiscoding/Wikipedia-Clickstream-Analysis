# WikiFlow

Cleaned datasets are stored as Spark Parquet output under `clean/`:

- `clean/edges/`
- `clean/main_page/`
- `clean/sources/`

Install PySpark with the Python interpreter used by this project:

```zsh
python3 -m pip install pyspark
```

Inspect the schemas and row counts:

```zsh
python3 inspect_data.py
```

Spark also requires Java to be installed and available on `PATH`.

## KPI Summary

Dataset: English Wikipedia Clickstream, September 2026.

### Traffic Sources

Total recorded source clicks: 3,652,620,427.

| Source category | Clicks | Share |
| --- | ---: | ---: |
| Search | 2,539,581,604 | 69.53% |
| Empty | 951,140,936 | 26.04% |
| Internal | 107,919,554 | 2.95% |
| External | 35,793,454 | 0.98% |
| Other | 18,184,879 | 0.50% |

Shares are calculated as `(category clicks / total recorded source clicks) × 100`; the `other` category is included in the total.

### Topic Clustering

- Algorithm: Louvain community detection
- Topic clusters: 651
- Article assignments: 2,538

Louvain identifies groups of articles that are strongly connected in the navigation graph. These clusters describe structural relationships and should not automatically be interpreted as definitive semantic topics.

### Dataset Metrics

- Internal transition rows: 21,241,212
- Unique source pages: 2,104,547
- Unique destination pages: 3,126,020

### Interpretation and Limitations

- Source shares describe recorded click counts associated with each source category.
- Clickstream transitions are aggregate navigation counts, not individual user sessions.
- Transitions with `type="other"` are candidate missing links, not confirmed broken or absent hyperlinks.
- Cluster results depend on graph construction and clustering configuration.