# WikiFlow — Data Quality Report

## 1. Overview

This report documents validation of the cleaned Wikipedia clickstream datasets used by WikiFlow.

## 2. Dataset Summary

| Dataset | Rows | Columns |
|---|---:|---|
| Edges | 21,654,016 | `prev`, `curr`, `type`, `n`, `title_clean`, `p` |
| Main Page | 367,939 | `prev`, `curr`, `type`, `n` |
| Sources | 11,531,299 | `prev`, `curr`, `type`, `n` |

## 3. Data Validation Results

- No null values were found in the checked columns.
- No negative click counts were found.
- No zero click counts were found.
- The `edges` dataset contains `link` and `other` transition types.
- The `main_page` dataset primarily contains `other` transitions.
- The `sources` dataset contains `external` transitions.

## 4. Probability Validation

The outgoing transition probabilities in `edges` were grouped by `prev` and summed.

- Source pages checked: 2,112,678
- Pages outside the probability-sum tolerance: 0
- Minimum probability sum: approximately 1.0
- Maximum probability sum: approximately 1.0
- Average probability sum: 1.0

This confirms that outgoing transition probabilities are normalized within floating-point precision.

## 5. Duplicate Validation

- Dataset checked: `clean/edges`
- Key checked: `prev`, `curr`, `type`
- Duplicate groups detected: 0

No duplicate groups were detected using this key.

## 6. Conclusion

The validation checks completed so far found no null values, invalid click counts, probability-normalization failures, or duplicate transition groups under the selected key.

The existing cleaned datasets were preserved. No probability recalculation, aggregation, or deletion was performed.

## 7. Scope and Limitations

These checks establish basic data quality. They do not independently verify the original clickstream extraction process, the correctness of the click counts, or the intended semantics of every transition type.