from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

# =========================
# Paths
# =========================
PROCESSED_DIR = Path("data/processed")
APP_KPIS_FILE = PROCESSED_DIR / "app_level_kpis.csv"
DAILY_METRICS_FILE = PROCESSED_DIR / "daily_metrics.csv"


# =========================
# Load data
# =========================
def load_data():
    app_kpis = pd.read_csv(APP_KPIS_FILE)
    daily = pd.read_csv(DAILY_METRICS_FILE, parse_dates=["date"])
    return app_kpis, daily


# =========================
# Dashboard
# =========================
def build_dashboard(app_kpis: pd.DataFrame, daily: pd.DataFrame):
    # Sort apps by rating
    df = app_kpis.sort_values("avg_score", ascending=False)

    # Select top app for temporal analysis
    top_app = app_kpis.sort_values("total_reviews", ascending=False).iloc[0]
    app_id = top_app["app_id"]
    app_name = top_app["title"]

    daily_app = daily[daily["app_id"] == app_id].sort_values("date")

    # Smooth rating (rolling average)
    daily_app["rolling_score"] = daily_app["daily_avg_score"].rolling(
        window=14, min_periods=1
    ).mean()

    # =========================
    # Figure layout
    # =========================
    fig = plt.figure(figsize=(16, 10))
    gs = fig.add_gridspec(2, 2)

    # ---- Plot 1: Average rating per app
    ax1 = fig.add_subplot(gs[0, 0])
    ax1.barh(df["title"], df["avg_score"])
    ax1.set_title("Average User Rating per Application")
    ax1.set_xlabel("Average Rating")
    ax1.invert_yaxis()

    # ---- Plot 2: Review volume per app
    ax2 = fig.add_subplot(gs[0, 1])
    ax2.barh(df["title"], df["total_reviews"])
    ax2.set_title("Review Volume per Application")
    ax2.set_xlabel("Number of Reviews")
    ax2.invert_yaxis()

    # ---- Plot 3: Rating over time (smoothed)
    ax3 = fig.add_subplot(gs[1, :])
    ax3.plot(
        daily_app["date"],
        daily_app["rolling_score"],
        linewidth=2,
        color="darkblue",
    )
    ax3.set_title(f"Smoothed Average Rating Over Time — {app_name}")
    ax3.set_xlabel("Date")
    ax3.set_ylabel("Average Rating")
    ax3.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.show()


# =========================
# Main
# =========================
def main():
    app_kpis, daily = load_data()
    build_dashboard(app_kpis, daily)


if __name__ == "__main__":
    main()
