# Python Data Engineering Lab

This project implements a data engineering pipeline that transforms semi-structured JSON/JSONL data into
analytics-ready datasets and validates their usability through lightweight dashboarding.

---

## Part A – Environment Setup

The Python environment and project structure were correctly set up.  
All scripts can be executed from scratch in a reproducible manner.

---

## Part B – Data Engineering Pipeline

### B.1 – Data Acquisition

Raw application metadata and user reviews were ingested and stored in JSONL format.

Implementation:
- `src/ingest_raw.py`

Outputs:
- `data/raw/note_taking_ai_apps.jsonl`
- `data/raw/note_taking_ai_reviews.jsonl`

---

### B.2 – Diagnosing and Transforming Raw Data

The raw datasets are provided in JSONL format and consist of two files: application metadata and user reviews.
An inspection of the raw files was performed to identify issues preventing direct analytical use.

#### Identified issues in the raw data

1. Redundant install metrics  
Multiple installation-related fields (`installs`, `minInstalls`, `realInstalls`) coexist, requiring the
selection of a single numeric field.

2. Non-uniform price representation  
Pricing information is spread across multiple fields and must be standardized into a single numeric value.

3. Non-tabular structures  
Nested or list-based fields (e.g. `histogram`, `categories`) are not directly suitable for tabular analytics.

4. Missing or null values in reviews  
Some review fields may be empty or missing and require appropriate handling.

5. Inconsistent date formats  
Temporal fields must be normalized to standard datetime values.

6. Join key consistency  
A stable join key (`appId` / `app_id`) is required between applications and reviews.

#### Transformation strategy

The raw data was transformed into clean, tabular datasets without modifying the original files. Only the
required fields were selected, basic normalization was applied, and the resulting datasets were written to
the `data/processed` folder.

Final dataset structures:
- Apps Catalog: `appId`, `title`, `developer`, `score`, `ratings`, `installs`, `genre`, `price`
- Apps Reviews: `app_id`, `app_name`, `reviewId`, `userName`, `score`, `content`, `thumbsUpCount`, `at`

Implementation:
- `src/transform_b2.py`

---

### B.3 – Aggregations and KPIs

Aggregations were computed on the transformed datasets to produce application-level KPIs and daily metrics.

Implementation:
- `src/compute_kpis.py`

Outputs:
- `data/processed/app_level_kpis.csv`
- `data/processed/daily_metrics.csv`

---

### B.4 – Lightweight Dashboarding (Consumer View)

A lightweight dashboard was built using only the processed datasets to validate that the pipeline produces
usable analytics data from a consumer perspective.

Implementation:
- `src/dashboard.py`

For convenience, a screenshot of the resulting dashboard is provided in the root directory:
- `dashboard.jpg`

This dashboard highlights application performance, review volume differences, and rating trends over time.

---

## Conclusion

The pipeline successfully converts semi-structured data into structured, analytics-ready datasets and
demonstrates a clear separation between data engineering and data consumption.
