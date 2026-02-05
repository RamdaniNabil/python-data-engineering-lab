"""
ingest_raw.py
Data Acquisition and Ingestion Script
Extracts AI note-taking apps metadata and reviews from Google Play Store
"""

from google_play_scraper import app, search, reviews_all
import json
from pathlib import Path
from typing import List, Dict
import time
from datetime import datetime

# Configuration
DATA_RAW_DIR = Path("data/raw")
SEARCH_QUERY = "AI note taking"
MAX_APPS = 20  # Adjust based on how much data you want

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

def extract_app_reviews(app_id: str, app_name: str = None) -> List[Dict]:
    """
    Extract all reviews for a single app
    
    Args:
        app_id: Google Play Store app ID
        app_name: Optional app name for logging
    
    Returns:
        List of review dictionaries
    """
    display_name = app_name if app_name else app_id
    
    try:
        print(f"  Extracting reviews for: {display_name}")
        
        result = reviews_all(
            app_id,
            sleep_milliseconds=0,
            lang='en',
            country='us'
        )
        
        # Add app_id to each review and convert datetime objects
        for review in result:
            review['app_id'] = app_id
            if app_name:
                review['app_name'] = app_name
            # Convert datetime objects in the review
            review = convert_datetime_to_string(review)
        
        # Convert the entire list to ensure all datetime objects are strings
        result = convert_datetime_to_string(result)
        
        print(f"    ✓ Extracted {len(result)} reviews")
        return result
    
    except Exception as e:
        print(f"    ✗ Error extracting reviews: {e}")
        return []

def extract_all_reviews(apps_data: List[Dict]) -> List[Dict]:
    """
    Extract reviews for all apps
    
    Args:
        apps_data: List of app metadata dictionaries
    
    Returns:
        List of all reviews from all apps
    """
    print(f"\n💬 Extracting reviews for {len(apps_data)} apps...")
    
    all_reviews = []
    
    for i, app_data in enumerate(apps_data, 1):
        app_id = app_data.get('appId')
        app_name = app_data.get('title')
        
        print(f"  [{i}/{len(apps_data)}] App: {app_name}")
        
        reviews = extract_app_reviews(app_id, app_name)
        all_reviews.extend(reviews)
        
        # Be respectful to the API
        time.sleep(1)
    
    print(f"\n✓ Total reviews extracted: {len(all_reviews)}")
    return all_reviews

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
    
    # Step 5: Extract reviews
    all_reviews = extract_all_reviews(apps_data)
    
    # Step 6: Save reviews
    if all_reviews:
        reviews_file = DATA_RAW_DIR / "note_taking_ai_reviews.jsonl"
        save_to_jsonl(all_reviews, reviews_file)
    else:
        print("\n⚠ Warning: No reviews were extracted")
    
    # Summary
    print("\n" + "="*60)
    print("INGESTION COMPLETE")
    print("="*60)
    print(f"Apps extracted: {len(apps_data)}")
    print(f"Reviews extracted: {len(all_reviews)}")
    print(f"\nFiles saved in: {DATA_RAW_DIR}")
    print("="*60)

if __name__ == "__main__":
    main()
