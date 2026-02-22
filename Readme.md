# Python Data Engineering Lab : RAMDANI Nabil & EL MAIMOUNI Kenza

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
- `src/transform.py`

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

## Feedback
- Overall good job on the pipeline
- reviews_all is great when you want to get all the available reviews but it can be problematic sometimes because you might hit the rate limit easily, and Be hard to control especially in production. Try reviews with pagination.
- If you do so, writing with append in the loop is always better to prevent data loss if code crashes


--> Following the feedback received, `src/ingest_raw.py` was updated to replace `reviews_all` with a paginated approach using `reviews` and a `continuation_token`. Reviews are now written to disk in append mode inside the loop, preventing data loss in case of a crash mid-run.

## C. Pipeline Changes and Stress Testing

In this step, the pipeline was run against several modified versions of the upstream datasets to expose hidden assumptions and structural fragilities.

---

### C.1 – New Reviews Batch

The pipeline was run against a new batch of 10 reviews. One duplicate (`r_2002`) was detected and dropped, leaving 9 clean records. The pipeline performs a **full refresh** — the output file is overwritten on each run with no implicit continuity from previous batches. Reviews referencing apps absent from the catalog (`com.ghost.notes`, `com.newnote.ai`) are retained in the reviews table but cannot be joined to app metadata. Only 1 code change was needed: pointing to the new input file.

Implementation:

* `src/stress_test.py` → `run_c1_new_batch()`

Outputs:

* `data/processed/c1_reviews_batch2_clean.csv`
* `data/processed/app_level_kpis.csv`
* `data/processed/daily_metrics.csv`

---

### C.2 – Schema Drift in Reviews

The upstream system delivered a file with renamed columns (`rating` instead of `score`, `review_text` instead of `content`, etc.) and a different date format (`2025/02/05 08:00`). Without the remapping step, the pipeline would have produced silently incorrect results — no crash, just NaN columns. The fix was localized to a single mapping dictionary (`SCHEMA_DRIFT_MAPPING`). The multi-format `parse_timestamp()` function handled the new date format without any additional change.

Implementation:

* `src/stress_test.py` → `run_c2_schema_drift()`

Outputs:

* `data/processed/c2_reviews_schema_drift_clean.csv`

---

### C.3 – Dirty and Inconsistent Data Records

10 rows were loaded, of which 5 were dropped during transformation: 4 due to invalid scores (`"five"`, `-1`, `NaN`, `0`) and 1 due to an unparseable timestamp (`"not_a_date"`). Rows with `content="NULL"` were kept with an empty string, and `thumbsUpCount=NULL` was replaced with 0. Errors were caught early in the transformation layer and did not propagate into the KPI aggregations.

Implementation:

* `src/stress_test.py` → `run_c3_dirty_data()`

Outputs:

* `data/processed/c3_reviews_dirty_clean.csv`

---

### C.4 – Updated Applications Metadata

The updated apps file introduced two issues: a duplicate `appId` (`com.otter.ai`) and a malformed row where an unquoted comma in the `developer` field caused a `ParserError`. The parser error was handled by switching to `engine='python'` with `on_bad_lines='warn'`, which skipped the malformed line. The duplicate was resolved by keeping the first (original) entry. The final catalog contains 8 apps. The join with batch2 reviews produced 9 rows with no unmatched records.

Implementation:

* `src/stress_test.py` → `run_c4_updated_apps()`

Outputs:

* `data/processed/apps_catalog.csv`

---

### C.5 – New Business Logic: Sentiment vs Rating

A keyword-based heuristic (`detect_sentiment`) was added at the transformation layer to classify review text as positive, negative, or neutral. A contradiction is flagged when sentiment contradicts the numeric score (positive text + score ≤ 2, or negative text + score ≥ 4). No contradictions were detected in the current batch, which is expected given the small and relatively consistent dataset. The `detect_sentiment()` function can be replaced by a proper NLP model (e.g. VADER, TextBlob) with no changes to the rest of the pipeline.

Implementation:

* `src/stress_test.py` → `run_c5_sentiment_contradiction()`

Outputs:

* `data/processed/c5_reviews_with_sentiment.csv`
* `data/processed/c5_app_contradiction_kpis.csv`