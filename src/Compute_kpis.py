from pathlib import Path
import pandas as pd

PROCESSED_DIR = Path("data/processed")

APPS_FILE = PROCESSED_DIR / "apps_catalog.csv"
REVIEWS_FILE = PROCESSED_DIR / "apps_reviews.csv"

OUT_APP_KPIS = PROCESSED_DIR / "app_level_kpis.csv"
OUT_DAILY_METRICS = PROCESSED_DIR / "daily_metrics.csv"


def main():
    # =========================
    # Load processed tables
    # =========================
    apps = pd.read_csv(APPS_FILE)
    reviews = pd.read_csv(REVIEWS_FILE, parse_dates=["at"])

    # =========================
    # OUTPUT 1 — App-Level KPIs
    # =========================
    app_kpis = (
        reviews
        .groupby("app_id")
        .agg(
            total_reviews=("reviewId", "count"),
            avg_score=("score", "mean"),
            min_score=("score", "min"),
            max_score=("score", "max"),
            total_thumbs_up=("thumbsUpCount", "sum"),
            first_review_date=("at", "min"),
            last_review_date=("at", "max"),
        )
        .reset_index()
    )

    # Join app title for readability
    app_kpis = app_kpis.merge(
        apps[["appId", "title"]],
        left_on="app_id",
        right_on="appId",
        how="left",
    ).drop(columns=["appId"])

    app_kpis.to_csv(OUT_APP_KPIS, index=False, encoding="utf-8")
    print(f"✅ Output 1 written: {OUT_APP_KPIS}")

    # =========================
    # OUTPUT 2 — Daily Metrics
    # =========================
    reviews["date"] = reviews["at"].dt.date

    daily_metrics = (
        reviews
        .groupby(["app_id", "date"])
        .agg(
            daily_reviews=("reviewId", "count"),
            daily_avg_score=("score", "mean"),
        )
        .reset_index()
    )

    # Join app title for readability
    daily_metrics = daily_metrics.merge(
        apps[["appId", "title"]],
        left_on="app_id",
        right_on="appId",
        how="left",
    ).drop(columns=["appId"])

    daily_metrics.to_csv(OUT_DAILY_METRICS, index=False, encoding="utf-8")
    print(f"✅ Output 2 written: {OUT_DAILY_METRICS}")


if __name__ == "__main__":
    main()
