import json
import re
from pathlib import Path

import pandas as pd

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")

APPS_FILE = RAW_DIR / "note_taking_ai_apps.jsonl"
REVIEWS_FILE = RAW_DIR / "note_taking_ai_reviews.jsonl"

OUT_APPS = PROCESSED_DIR / "apps_catalog.csv"
OUT_REVIEWS = PROCESSED_DIR / "apps_reviews.csv"


def read_jsonl(path: Path) -> list[dict]:
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def parse_installs(value):
    """
    Convertit '10,000+' -> 10000, '1M+' -> 1000000 etc.
    Si déjà numérique -> retourne int.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)

    s = str(value).strip().upper()

    # exemples: "10,000+" / "1,000,000+"
    s = s.replace("+", "").replace(",", "")

    # exemples possibles: "1M", "2.5M"
    m = re.fullmatch(r"(\d+(\.\d+)?)([KMB])?", s)
    if not m:
        # si format inattendu, on tente d'extraire les chiffres
        digits = re.findall(r"\d+", s)
        return int("".join(digits)) if digits else None

    num = float(m.group(1))
    suf = m.group(3)
    mult = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}.get(suf, 1)
    return int(num * mult)


def transform_apps(apps_raw: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(apps_raw)

    # Assurer appId
    if "appId" not in df.columns:
        # tentative: parfois l'id est dans d'autres champs, sinon NaN
        df["appId"] = df.get("app_id")

    # installs: priorité realInstalls > minInstalls > installs (string)
    installs = None
    if "realInstalls" in df.columns:
        installs = df["realInstalls"]
    elif "minInstalls" in df.columns:
        installs = df["minInstalls"]
    elif "installs" in df.columns:
        installs = df["installs"].map(parse_installs)

    df["installs_clean"] = installs

    # price: si free=True -> 0, sinon price
    if "free" in df.columns and "price" in df.columns:
        df["price_clean"] = df.apply(lambda r: 0.0 if bool(r.get("free")) else r.get("price"), axis=1)
    elif "price" in df.columns:
        df["price_clean"] = df["price"]
    else:
        df["price_clean"] = None

    # conversions numériques
    df["score"] = pd.to_numeric(df.get("score"), errors="coerce")
    df["ratings"] = pd.to_numeric(df.get("ratings"), errors="coerce")
    df["price_clean"] = pd.to_numeric(df.get("price_clean"), errors="coerce")
    df["installs_clean"] = pd.to_numeric(df.get("installs_clean"), errors="coerce")

    # Table finale (ordre imposé)
    out = pd.DataFrame({
        "appId": df.get("appId"),
        "title": df.get("title"),
        "developer": df.get("developer"),
        "score": df.get("score"),
        "ratings": df.get("ratings"),
        "installs": df.get("installs_clean"),
        "genre": df.get("genre"),
        "price": df.get("price_clean"),
    })

    # Forcer types
    out["appId"] = out["appId"].astype("string")
    out["title"] = out["title"].astype("string")
    out["developer"] = out["developer"].astype("string")
    out["genre"] = out["genre"].astype("string")

    return out


def transform_reviews(reviews_raw: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(reviews_raw)

    # Normaliser les dates
    df["at"] = pd.to_datetime(df.get("at"), errors="coerce", utc=True)

    out = pd.DataFrame({
        "app_id": df.get("app_id"),
        "app_name": df.get("app_name"),
        "reviewId": df.get("reviewId"),
        "userName": df.get("userName"),
        "score": pd.to_numeric(df.get("score"), errors="coerce"),
        "content": df.get("content"),
        "thumbsUpCount": pd.to_numeric(df.get("thumbsUpCount"), errors="coerce"),
        "at": df.get("at"),
    })

    out["app_id"] = out["app_id"].astype("string")
    out["app_name"] = out["app_name"].astype("string")
    out["reviewId"] = out["reviewId"].astype("string")
    out["userName"] = out["userName"].astype("string")
    out["content"] = out["content"].astype("string")

    return out


def main():
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    apps_raw = read_jsonl(APPS_FILE)
    reviews_raw = read_jsonl(REVIEWS_FILE)

    apps_tbl = transform_apps(apps_raw)
    reviews_tbl = transform_reviews(reviews_raw)

    apps_tbl.to_csv(OUT_APPS, index=False, encoding="utf-8")
    reviews_tbl.to_csv(OUT_REVIEWS, index=False, encoding="utf-8")

    print(f"✅ Wrote: {OUT_APPS} (rows={len(apps_tbl)})")
    print(f"✅ Wrote: {OUT_REVIEWS} (rows={len(reviews_tbl)})")


if __name__ == "__main__":
    main()
