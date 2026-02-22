from google_play_scraper import app, search, reviews  # ← reviews_all remplacé par reviews
import json
from pathlib import Path
from typing import List, Dict
import time
from datetime import datetime

# Configuration
DATA_RAW_DIR = Path("data/raw")
SEARCH_QUERY = "AI note taking"
MAX_APPS = 20           # Adjust based on how much data you want
REVIEWS_BATCH_SIZE = 100  # Nombre de reviews récupérées par page

def setup_directories():
    """Create necessary directories if they don't exist"""
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"✓ Directory created: {DATA_RAW_DIR}")

def search_note_taking_apps(query: str, max_results: int = 50) -> List[str]:
    """
    Search for note-taking AI apps and return their app IDs
    
    Args:
        query: Search query string
        max_results: Maximum number of results to return
    
    Returns:
        List of app IDs
    """
    print(f"\n🔍 Searching for apps with query: '{query}'...")
    
    try:
        results = search(
            query,
            lang="en",
            country="us",
            n_hits=max_results
        )
        
        app_ids = [result['appId'] for result in results]
        print(f"✓ Found {len(app_ids)} apps")
        return app_ids
    
    except Exception as e:
        print(f"✗ Error during search: {e}")
        return []

def convert_datetime_to_string(obj):
    """
    Convert datetime objects to ISO format strings
    Recursively handles nested dictionaries and lists
    
    Args:
        obj: Object to convert (dict, list, or other)
    
    Returns:
        Converted object with datetime as strings
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: convert_datetime_to_string(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_datetime_to_string(item) for item in obj]
    else:
        return obj

def extract_app_metadata(app_id: str) -> Dict:
    """
    Extract detailed metadata for a single app
    
    Args:
        app_id: Google Play Store app ID
    
    Returns:
        Dictionary containing app metadata
    """
    try:
        result = app(
            app_id,
            lang='en',
            country='us'
        )
        # Convert datetime objects to strings
        return convert_datetime_to_string(result)
    
    except Exception as e:
        print(f"  ✗ Error extracting {app_id}: {e}")
        return None

def extract_apps_metadata(app_ids: List[str]) -> List[Dict]:
    """
    Extract metadata for multiple apps
    
    Args:
        app_ids: List of app IDs
    
    Returns:
        List of app metadata dictionaries
    """
    print(f"\n📱 Extracting metadata for {len(app_ids)} apps...")
    
    apps_data = []
    
    for i, app_id in enumerate(app_ids, 1):
        print(f"  [{i}/{len(app_ids)}] Extracting: {app_id}")
        
        app_data = extract_app_metadata(app_id)
        
        if app_data:
            apps_data.append(app_data)
            print(f"    ✓ Success")
        
        # Be respectful to the API - add small delay
        time.sleep(0.5)
    
    print(f"\n✓ Successfully extracted {len(apps_data)} apps metadata")
    return apps_data

def extract_app_reviews_paginated(app_id: str, filepath: Path, app_name: str = None) -> int:
    """
    Extract reviews for a single app using pagination and write directly to file (append mode).
    This avoids hitting rate limits and prevents data loss if the script crashes mid-run.
    
    Args:
        app_id: Google Play Store app ID
        filepath: Path to the JSONL file to append reviews to
        app_name: Optional app name for logging
    
    Returns:
        Total number of reviews extracted for this app
    """
    display_name = app_name if app_name else app_id
    print(f"  Extracting reviews for: {display_name}")

    total = 0
    continuation_token = None  # Premier appel sans token

    try:
        while True:
            # Récupère un batch de reviews (avec ou sans token de continuation)
            batch, continuation_token = reviews(
                app_id,
                lang='en',
                country='us',
                count=REVIEWS_BATCH_SIZE,
                continuation_token=continuation_token
            )

            if not batch:
                break  # Plus de reviews disponibles

            # Enrichir et convertir chaque review du batch
            for review in batch:
                review['app_id'] = app_id
                if app_name:
                    review['app_name'] = app_name
                review = convert_datetime_to_string(review)

                # ✅ Écriture en append immédiatement : pas de perte de données si crash
                with open(filepath, 'a', encoding='utf-8') as f:
                    json.dump(review, f, ensure_ascii=False)
                    f.write('\n')

            total += len(batch)
            print(f"    ... {total} reviews récupérées jusqu'ici")

            # Pas de token → on a tout récupéré
            if not continuation_token:
                break

            # Be respectful to the API
            time.sleep(1)

    except Exception as e:
        print(f"    ✗ Error extracting reviews for {display_name}: {e}")

    print(f"    ✓ Total extrait pour {display_name} : {total} reviews")
    return total

def extract_all_reviews(apps_data: List[Dict], filepath: Path) -> int:
    """
    Extract reviews for all apps using pagination, writing directly to file in append mode.
    
    Args:
        apps_data: List of app metadata dictionaries
        filepath: Path to the JSONL output file
    
    Returns:
        Total number of reviews extracted across all apps
    """
    print(f"\n💬 Extracting reviews for {len(apps_data)} apps...")

    # On repart d'un fichier vide à chaque nouvelle ingestion complète
    filepath.write_text("")

    total_reviews = 0

    for i, app_data in enumerate(apps_data, 1):
        app_id = app_data.get('appId')
        app_name = app_data.get('title')

        print(f"\n  [{i}/{len(apps_data)}] App: {app_name}")

        count = extract_app_reviews_paginated(app_id, filepath, app_name)
        total_reviews += count

        # Be respectful to the API between apps
        time.sleep(1)

    print(f"\n✓ Total reviews extracted: {total_reviews}")
    return total_reviews

def save_to_jsonl(data: List[Dict], filepath: Path):
    """
    Save data to JSONL format (one JSON object per line)
    
    Args:
        data: List of dictionaries to save
        filepath: Path to save the file
    """
    with open(filepath, 'w', encoding='utf-8') as f:
        for item in data:
            # Ensure datetime conversion before writing
            item_converted = convert_datetime_to_string(item)
            json.dump(item_converted, f, ensure_ascii=False)
            f.write('\n')
    
    print(f"✓ Saved {len(data)} records to {filepath}")

def save_to_json(data: List[Dict], filepath: Path):
    """
    Save data to JSON format
    
    Args:
        data: List of dictionaries to save
        filepath: Path to save the file
    """
    # Ensure datetime conversion before writing
    data_converted = convert_datetime_to_string(data)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data_converted, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Saved {len(data)} records to {filepath}")

def main():
    """Main execution function"""
    
    print("="*60)
    print("DATA INGESTION PIPELINE")
    print("Google Play Store - AI Note-Taking Apps")
    print("="*60)
    
    # Step 1: Setup
    setup_directories()
    
    # Step 2: Search for apps
    app_ids = search_note_taking_apps(SEARCH_QUERY, max_results=MAX_APPS)
    
    if not app_ids:
        print("\n✗ No apps found. Exiting.")
        return
    
    # Step 3: Extract apps metadata
    apps_data = extract_apps_metadata(app_ids)
    
    if not apps_data:
        print("\n✗ No apps metadata extracted. Exiting.")
        return
    
    # Step 4: Save apps metadata
    apps_file = DATA_RAW_DIR / "note_taking_ai_apps.jsonl"
    save_to_jsonl(apps_data, apps_file)
    
    # Step 5: Extract reviews avec pagination + écriture en append dans la boucle
    reviews_file = DATA_RAW_DIR / "note_taking_ai_reviews.jsonl"
    total_reviews = extract_all_reviews(apps_data, reviews_file)
    
    if total_reviews == 0:
        print("\n⚠ Warning: No reviews were extracted")
    
    # Summary
    print("\n" + "="*60)
    print("INGESTION COMPLETE")
    print("="*60)
    print(f"Apps extracted: {len(apps_data)}")
    print(f"Reviews extracted: {total_reviews}")
    print(f"\nFiles saved in: {DATA_RAW_DIR}")
    print("="*60)

if __name__ == "__main__":
    main()