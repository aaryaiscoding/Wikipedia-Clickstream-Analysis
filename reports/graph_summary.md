# WikiFlow — Graph Summary

## 1. Purpose

Summarize the internal Wikipedia navigation graph using the cleaned clickstream dataset.

## 2. Results

- Internal link rows: 21,241,212
- Unique source pages: 2,104,547
- Unique destination pages: 3,126,020
- Unique source-destination pairs: 21,241,212
- Repeated source-destination pairs among link rows: 0

## 3. Edge Interpretation

- `prev`: source page
- `curr`: destination page
- `n`: observed transition count
- `p`: transition probability
- `type`: transition category

The graph is represented as directed transitions from source pages to destination pages.

## 4. Example

The most frequent transition in the observed output was:

- Source: `2026_Asian_Games`
- Destination: `2026_Asian_Games_medal_table`
- Transition count: 585,024
- Transition probability: approximately 0.2373

## 5. Limitations

These statistics describe the internal-link subset. They do not yet represent PageRank scores or establish which Wikipedia pages are globally most important.
