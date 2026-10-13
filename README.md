# Wikipedia Clickstream Data Processing with PySpark

## Overview

This project processes the September 2026 English Wikipedia Clickstream dataset using Apache Spark (PySpark) on Ubuntu through WSL2. It converts the raw compressed TSV data into Parquet format, cleans the dataset, separates source-category and Main Page records, and calculates transition probabilities between Wikipedia articles.

## Objectives

- Process a large-scale Wikipedia clickstream dataset using PySpark.
- Convert compressed TSV data into Parquet format.
- Clean and filter clickstream records.
- Separate source-category records and Main Page-related records from article transitions.
- Calculate transition probabilities for outgoing edges from each source article.
- Record preprocessing statistics for analysis and reporting.

## Technology Stack

| Component | Version / Details |
|---|---|
| Operating environment | Ubuntu on WSL2 |
| Python | 3.14.4 |
| Java | OpenJDK 17.0.20.1 |
| Apache PySpark | 4.2.0 |
| Input format | Gzipped TSV |
| Output format | Apache Parquet |

## Dataset

**Dataset:** English Wikipedia Clickstream, September 2026 release

**Input file:** `clickstream-enwiki-2026-09.tsv.gz`

The dataset contains four columns:

| Column | Description |
|---|---|
| `prev` | Previous page or source category |
| `curr` | Destination page |
| `type` | Clickstream transition type |
| `n` | Number of recorded transitions |

The compressed input file is approximately 457 MB.

## Processing Pipeline

### 1. Read the raw dataset

PySpark reads the compressed TSV file using an explicitly defined schema:

- `prev`: STRING
- `curr`: STRING
- `type`: STRING
- `n`: LONG

**Total records read:** 33,553,254

### 2. Convert to Parquet

The raw dataset is converted to Parquet format for subsequent processing.

Output directory: `clickstream_raw_parquet/`

### 3. Clean the dataset

The cleaning pipeline performs the following operations:

1. Removes rows containing null values and records where `n <= 0`.
2. Separates records whose `prev` value begins with `other-` into the `sources` dataset.
3. Filters article-transition records to retain the `link` and `other` types.
4. Removes self-links where `prev` equals `curr`.
5. Separates records involving `Main_Page` into `main_page`.
6. Creates the final `edges` dataset.
7. Adds `title_clean`, which replaces underscores in destination titles with spaces.
8. Calculates `p`, the transition probability for each destination relative to the total transition count for its source article.

The cleaned data is written to three Parquet directories:

- `clean/sources/`
- `clean/edges/`
- `clean/main_page/`

### 4. Validate transition probabilities

A verification script samples five source articles and checks that their outgoing transition probabilities sum to approximately 1.0. All five sampled articles passed this check.

## Preprocessing Results

| Processing stage | Records |
|---|---:|
| Raw dataset | 33,553,254 |
| After dropping nulls and `n <= 0` | 33,553,254 |
| Source-category records (`other-*`) | 11,531,299 |
| Edges after removing self-links | 22,021,955 |
| Main Page records kept separately | 367,939 |
| Final edges | 21,654,016 |

The null and non-positive-count filter did not remove any records in this dataset.

## Key Performance Indicators (KPIs)

| KPI | Result |
|---|---:|
| Dataset records processed | 33,553,254 |
| Original compressed TSV size | 457 MB |
| Raw Parquet output size | 773 MB |
| Conversion status | Successful |
| Parquet size change | Approximately 69.1% increase |

The Parquet output is larger than the original compressed TSV file. This is not a failed conversion: the input is already gzip-compressed, while the Parquet output uses a different storage and compression format. The size comparison is between the compressed TSV input and the resulting Parquet directory.

## Output Schema

The final `clean/edges/` dataset contains six columns:

| Column | Description |
|---|---|
| `prev` | Source article |
| `curr` | Destination article |
| `type` | Clickstream transition type |
| `n` | Observed transition count |
| `title_clean` | Destination title with underscores replaced by spaces |
| `p` | Transition probability for the source article |

## Project Structure

```text
wikipedia-clickstream/
├── README.md
├── .gitignore
├── test.py
├── clean.py
├── verify_p.py
└── cleaning_log.txt
```

The large input file and generated Parquet datasets are kept outside the Git repository. They can be shared separately through an appropriate data-storage service.

## Running the Project

Activate the Python virtual environment and install PySpark if necessary:

```bash
source ~/bda/bin/activate
pip install pyspark==4.2.0
```

Run the scripts in order:

```bash
python test.py
python clean.py
python verify_p.py
```

The input file `clickstream-enwiki-2026-09.tsv.gz` must be available in the working directory used by `test.py`. The raw Parquet output must be available before running `clean.py`.

## Limitations and Notes

- Processing performance depends on available RAM, disk space, and Spark configuration.
- The compressed TSV input is processed successfully, but its conversion to Parquet does not reduce storage size in this run.
- Probability validation was performed on five sampled source articles; it is a spot-check rather than a full-dataset validation.
- Generated data files are excluded from Git because of their size.

## Status

- [x] Raw dataset read successfully
- [x] Exact raw row count recorded
- [x] Raw Parquet conversion completed
- [x] Cleaning pipeline completed
- [x] Cleaning row counts recorded
- [x] Three cleaned Parquet datasets generated
- [x] Transition probabilities spot-checked
- [ ] Team handoff and data sharing completed

