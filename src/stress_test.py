"""
stress_test.py
Part C – Pipeline Stress Testing
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

DATA_RAW_DIR       = Path("data/raw")
DATA_PROCESSED_DIR = Path("data/processed")

BATCH2_PATH       = Path("data/raw/note_taking_ai_reviews_batch2.csv")
SCHEMA_DRIFT_PATH = Path("data/raw/note_taking_ai_reviews_schema_drift.csv")
DIRTY_PATH        = Path("data/raw/note_taking_ai_reviews_dirty.csv")
APPS_UPDATED_PATH = Path("data/raw/note_taking_ai_apps_updated.csv")

SCHEMA_DRIFT_MAPPING = {
    "appId":       "app_id",
    "appTitle":    "app_name",
    "review_id":   "reviewId",
    "username":    "userName",
    "rating":      "score",
    "review_text": "content",
    "likes":       "thumbsUpCount",
    "review_time": "at",
}

POSITIVE_KEYWORDS = [
    "great", "love", "excellent", "amazing", "fantastic",
    "good", "best", "perfect", "awesome", "nice", "clean",
]
NEGATIVE_KEYWORDS = [
    "bad", "worst", "terrible", "horrible", "crash", "broken",
    "useless", "bug", "slow", "expensive", "issue", "problem",
    "laggy", "drain", "stopped", "sync",
]


def setup_directories():
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
    DATA_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

def clean_installs(value) -> int:
    if pd.isna(value):
        return 0
    value = str(value).replace(",", "").replace("+", "").strip()
    try:
        return int(float(value))
    except ValueError:
        return 0


def clean_price(value) -> float:
    if pd.isna(value):
        return 0.0
    try:
        return float(str(value).replace("$", "").strip())
    except ValueError:
        return 0.0


def parse_timestamp(value) -> pd.Timestamp:
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M", "%Y-%m-%dT%H:%M:%S"]:
        try:
            return pd.Timestamp(datetime.strptime(str(value).strip(), fmt))
        except (ValueError, TypeError):
            continue
    return pd.NaT


def transform_apps(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    expected = ["appId", "title", "developer", "score", "ratings", "installs", "genre", "price"]
    df = df[[c for c in expected if c in df.columns]]
    df["installs"] = df["installs"].apply(clean_installs)
    df["price"]    = df["price"].apply(clean_price)
    df["score"]    = pd.to_numeric(df["score"], errors="coerce")
    df["ratings"]  = pd.to_numeric(df["ratings"], errors="coerce").fillna(0).astype(int)
    return df


def transform_reviews(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["score"] = pd.to_numeric(df["score"], errors="coerce")

    before = len(df)
    df = df[df["score"].between(1, 5, inclusive="both")]
    if len(df) < before:
        print(f"    ⚠ Dropped {before - len(df)} review(s) with invalid score")

    df["at"] = df["at"].apply(parse_timestamp)

    before = len(df)
    df = df[df["at"].notna()]
    if len(df) < before:
        print(f"    ⚠ Dropped {before - len(df)} review(s) with unparseable timestamp")

    df["content"] = df["content"].replace("NULL", "").fillna("")
    df["thumbsUpCount"] = pd.to_numeric(
        df["thumbsUpCount"].replace("NULL", pd.NA), errors="coerce"
    ).fillna(0).astype(int)

    return df


def compute_kpis(reviews: pd.DataFrame, output_dir: Path):
    app_kpis = reviews.groupby("app_id").agg(
        num_reviews    = ("reviewId", "count"),
        avg_rating     = ("score", "mean"),
        pct_low_rating = ("score", lambda x: round((x <= 2).sum() / len(x) * 100, 2)),
        first_review   = ("at", "min"),
        last_review    = ("at", "max"),
    ).reset_index()
    app_kpis.to_csv(output_dir / "app_level_kpis.csv", index=False)
    print(f"  ✓ app_level_kpis.csv saved ({len(app_kpis)} apps)")

    reviews["date"] = reviews["at"].dt.date
    daily = reviews.groupby("date").agg(
        daily_reviews    = ("reviewId", "count"),
        daily_avg_rating = ("score", "mean"),
    ).reset_index()
    daily.to_csv(output_dir / "daily_metrics.csv", index=False)
    print(f"  ✓ daily_metrics.csv saved ({len(daily)} days)")


def detect_sentiment(text: str) -> str:
    if not isinstance(text, str) or text.strip() == "":
        return "neutral"
    text_lower = text.lower()
    pos = sum(1 for kw in POSITIVE_KEYWORDS if kw in text_lower)
    neg = sum(1 for kw in NEGATIVE_KEYWORDS if kw in text_lower)
    if pos > neg:
        return "positive"
    elif neg > pos:
        return "negative"
    return "neutral"


def detect_contradiction(sentiment: str, score: float) -> bool:
    if sentiment == "positive" and score <= 2:
        return True
    if sentiment == "negative" and score >= 4:
        return True
    return False


# ── Scenarios ─────────────────────────────────────────────────────────────────

def run_c1_new_batch():
    print("\n" + "="*60)
    print("C.1 – NEW REVIEWS BATCH")
    print("="*60)

    df = pd.read_csv(BATCH2_PATH)
    print(f"  Loaded {len(df)} rows from batch2")

    before = len(df)
    df = df.drop_duplicates(subset=["reviewId"], keep="first")
    if len(df) < before:
        print(f"  ⚠ Dropped {before - len(df)} duplicate reviewId(s)")

    apps_path = DATA_PROCESSED_DIR / "apps_catalog.csv"
    if apps_path.exists():
        known_ids = set(pd.read_csv(apps_path)["appId"].dropna())
        orphans = df[~df["app_id"].isin(known_ids)]["app_id"].unique()
        if len(orphans):
            print(f"  ⚠ Reviews reference apps not in catalog: {list(orphans)}")
    else:
        print("  ℹ apps_catalog.csv not found – skipping orphan check")

    df = transform_reviews(df)
    out = DATA_PROCESSED_DIR / "c1_reviews_batch2_clean.csv"
    df.to_csv(out, index=False)
    print(f"  ✓ Saved {len(df)} clean reviews → {out}")

    compute_kpis(df, DATA_PROCESSED_DIR)


def run_c2_schema_drift():
    print("\n" + "="*60)
    print("C.2 – SCHEMA DRIFT IN REVIEWS")
    print("="*60)

    df = pd.read_csv(SCHEMA_DRIFT_PATH)
    print(f"  Loaded {len(df)} rows from schema_drift")
    print(f"  Columns found:    {list(df.columns)}")

    df = df.rename(columns=SCHEMA_DRIFT_MAPPING)
    print(f"  Columns remapped: {list(df.columns)}")

    df = transform_reviews(df)
    out = DATA_PROCESSED_DIR / "c2_reviews_schema_drift_clean.csv"
    df.to_csv(out, index=False)
    print(f"  ✓ Saved {len(df)} clean reviews → {out}")

    compute_kpis(df, DATA_PROCESSED_DIR)


def run_c3_dirty_data():
    print("\n" + "="*60)
    print("C.3 – DIRTY AND INCONSISTENT DATA RECORDS")
    print("="*60)

    df = pd.read_csv(DIRTY_PATH)
    print(f"  Loaded {len(df)} rows from dirty dataset")
    print(f"  Raw score values:     {df['score'].tolist()}")
    print(f"  Raw timestamp values: {df['at'].tolist()}")

    df_clean = transform_reviews(df)
    out = DATA_PROCESSED_DIR / "c3_reviews_dirty_clean.csv"
    df_clean.to_csv(out, index=False)
    print(f"  ✓ Saved {len(df_clean)} valid reviews (from {len(df)}) → {out}")

    if len(df_clean) > 0:
        compute_kpis(df_clean, DATA_PROCESSED_DIR)
    else:
        print("  ⚠ No valid reviews remain – KPIs not computed")


def run_c4_updated_apps():
    print("\n" + "="*60)
    print("C.4 – UPDATED APPLICATIONS METADATA")
    print("="*60)

    # engine='python' handles lines with unquoted commas in fields
    df = pd.read_csv(APPS_UPDATED_PATH, engine='python', on_bad_lines='warn')
    print(f"  Loaded {len(df)} rows from apps_updated")

    dupes = df[df.duplicated(subset=["appId"], keep=False)]
    if len(dupes):
        print(f"  ⚠ Duplicate appId(s) found:")
        print(dupes[["appId", "title", "developer"]].to_string(index=False))

    before = len(df)
    df = df.drop_duplicates(subset=["appId"], keep="first")
    print(f"  ✓ Deduplicated: {before} → {len(df)} rows")

    missing_score = df[df["score"].isna()]
    if len(missing_score):
        print(f"  ⚠ {len(missing_score)} app(s) with missing score: {missing_score['appId'].tolist()}")

    df_clean = transform_apps(df)
    out = DATA_PROCESSED_DIR / "apps_catalog.csv"
    df_clean.to_csv(out, index=False)
    print(f"  ✓ Saved updated apps catalog ({len(df_clean)} apps) → {out}")

    reviews_path = DATA_PROCESSED_DIR / "c1_reviews_batch2_clean.csv"
    if reviews_path.exists():
        reviews = pd.read_csv(reviews_path)
        merged = reviews.merge(df_clean, left_on="app_id", right_on="appId", how="left")
        unmatched = merged[merged["title"].isna()]["app_id"].unique()
        if len(unmatched):
            print(f"  ⚠ {len(unmatched)} review app_id(s) unmatched in catalog: {list(unmatched)}")
        print(f"  ✓ Join check: {len(merged)} rows after left join")


def run_c5_sentiment_contradiction():
    print("\n" + "="*60)
    print("C.5 – SENTIMENT VS RATING CONTRADICTION")
    print("="*60)

    reviews_path = DATA_PROCESSED_DIR / "c1_reviews_batch2_clean.csv"
    if not reviews_path.exists():
        print("  ⚠ Run C.1 first to generate clean reviews.")
        return

    df = pd.read_csv(reviews_path)
    print(f"  Loaded {len(df)} reviews")

    df["sentiment"]     = df["content"].apply(detect_sentiment)
    df["contradiction"] = df.apply(
        lambda row: detect_contradiction(row["sentiment"], row["score"]), axis=1
    )

    df.to_csv(DATA_PROCESSED_DIR / "c5_reviews_with_sentiment.csv", index=False)
    print(f"  ✓ Extended reviews saved → data/processed/c5_reviews_with_sentiment.csv")

    contradiction_kpis = df.groupby("app_id").agg(
        num_reviews         = ("reviewId", "count"),
        avg_rating          = ("score", "mean"),
        contradiction_count = ("contradiction", "sum"),
        contradiction_rate  = ("contradiction", lambda x: round(x.mean() * 100, 2)),
    ).reset_index()

    contradiction_kpis.to_csv(DATA_PROCESSED_DIR / "c5_app_contradiction_kpis.csv", index=False)
    print(f"  ✓ Contradiction KPIs saved → data/processed/c5_app_contradiction_kpis.csv")

    print("\n  📊 Results:")
    print(contradiction_kpis.to_string(index=False))

    contradictions = df[df["contradiction"] == True][
        ["app_id", "userName", "score", "sentiment", "content"]
    ]
    if len(contradictions):
        print(f"\n  🔍 {len(contradictions)} contradictory review(s) found:")
        print(contradictions.to_string(index=False))
    else:
        print("\n  ✓ No contradictions detected in this batch")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("="*60)
    print("PART C – PIPELINE STRESS TESTING")
    print("="*60)

    setup_directories()

    run_c1_new_batch()
    run_c2_schema_drift()
    run_c3_dirty_data()
    run_c4_updated_apps()
    run_c5_sentiment_contradiction()

    print("\n" + "="*60)
    print("STRESS TEST COMPLETE")
    print("="*60)
    print(f"All outputs saved in: {DATA_PROCESSED_DIR}")

print("All observations based on the stress test results are documented in the README under Part C")

if __name__ == "__main__":
    main()