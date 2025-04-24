import requests
import datetime
import sqlite3
import os # *** Import os module ***
import json
import logging
import sys
from typing import List, Dict, Optional, Any

# --- Configuration ---
# Note: This API endpoint is not officially documented by Spotify and may change/break.
API_BASE_URL = "https://podcastcharts.byspotify.com/api/charts/top"
DEFAULT_REGION = "us"
PLATFORM_NAME_SPOTIFY = "Spotify" # Specific constant
# *** Define DB Path relative to this script ***
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data') # Define data directory path
DB_FILENAME = "podcasts.db"
DB_PATH = os.path.join(DATA_DIR, DB_FILENAME) # Full path to the database

LOG_LEVEL = logging.INFO
# --- Setup Logging ---
# Explicitly set the stream to standard output
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s',
    stream=sys.stdout 
)

# --- Ensure data directory exists ---
# *** Create the data directory if it doesn't exist ***
try:
    os.makedirs(DATA_DIR, exist_ok=True)
    logging.info(f"Ensured data directory exists: {DATA_DIR}")
except OSError as e:
    logging.error(f"Error creating data directory {DATA_DIR}: {e}")
    # Decide if you want to exit or continue without saving
    # exit(1)

def scrape_spotify_top100(region: str = DEFAULT_REGION) -> List[Dict[str, Any]]:
    """
    Scrapes the top 100 podcasts from the unofficial Spotify charts API for a given region.

    Args:
        region: The two-letter country code for the chart region (e.g., 'us', 'gb').

    Returns:
        A list of dictionaries, each representing a podcast entry, or an empty list on failure.
    """
    url = f"{API_BASE_URL}?region={region}"
    logging.info(f"Requesting Spotify chart data from: {url}")
    records = []
    try:
        response = requests.get(url, timeout=15) 
        logging.info(f"HTTP status: {response.status_code}")
        logging.debug(f"Response snippet: {response.text[:200]}...")
        response.raise_for_status()

        items = response.json()
        if not isinstance(items, list):
             logging.error(f"Unexpected API response format. Expected a list, got {type(items)}")
             return []

        logging.info(f"Parsed {len(items)} items from API.")
        today = str(datetime.date.today())

        for i, pod_data in enumerate(items[:100]):
             if not isinstance(pod_data, dict):
                  logging.warning(f"Skipping item at index {i}, expected dict, got {type(pod_data)}")
                  continue

             show_uri = pod_data.get("showUri", "")
             platform_podcast_id = show_uri.split(":")[-1] if show_uri and ':' in show_uri else None 

             records.append({
                 # *** Use specific platform constant ***
                 "platform": PLATFORM_NAME_SPOTIFY,
                 "rank": i + 1,
                 "title": pod_data.get("showName"),
                 "platform_podcast_id": platform_podcast_id,
                 "date": today
             })

    except requests.exceptions.Timeout:
        logging.error(f"Request timed out connecting to {url}")
        return []
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request failed: {e}")
        return []
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON response: {e}")
        logging.debug(f"Response text: {response.text[:500]}...")
        return []
    except Exception as e:
        logging.error(f"An unexpected error occurred during scraping: {e}")
        return []

    return records


def save_chart_data_to_db(records: List[Dict[str, Any]], db_path: str):
    """
    Saves scraped podcast chart records to the specified SQLite database file.

    Args:
        records: A list of podcast record dictionaries.
        db_path: The full path to the SQLite database file.
    """
    if not records:
        logging.warning("No records provided to save.")
        return

    db_dir = os.path.dirname(db_path)
    if not os.path.exists(db_dir):
        logging.error(f"Database directory does not exist: {db_dir}. Cannot save data.")
        return

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Top100Lists (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL,
                rank INTEGER NOT NULL,
                title TEXT,
                platform_podcast_id TEXT,
                date TEXT NOT NULL,
                UNIQUE(platform, rank, date)
            )
        ''')

        insert_count = 0
        ignore_count = 0
        first_platform = records[0]['platform'] if records else 'Unknown'

        for r in records:
            required_keys = ["platform", "rank", "date"]
            if not all(key in r for key in required_keys):
                 logging.error(f"Record missing required keys: {r}. Skipping.")
                 continue
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO Top100Lists(platform, rank, title, platform_podcast_id, date)
                    VALUES (?, ?, ?, ?, ?)
                ''', (r["platform"], r["rank"], r.get("title"), r.get("platform_podcast_id"), r["date"]))
                if cursor.rowcount > 0:
                    insert_count += 1
                else:
                    ignore_count += 1
            except sqlite3.Error as e:
                 logging.error(f"Failed to insert record: {r} - Error: {e}")

        conn.commit()
        logging.info(f"Database operation complete for {first_platform} data. Inserted: {insert_count}, Ignored (duplicates): {ignore_count} into {db_path}")

    except sqlite3.Error as e:
        logging.error(f"Database error accessing {db_path}: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            logging.debug(f"Database connection closed for {db_path}")


if __name__ == "__main__":
    logging.info("Starting Spotify Top 100 scrape...")
    scraped_data_spotify = scrape_spotify_top100() # Uses default region 'us'

    if scraped_data_spotify:
        # *** Pass the specific DB_PATH to the save function ***
        save_chart_data_to_db(scraped_data_spotify, DB_PATH)
        logging.info(f"Attempted to save {len(scraped_data_spotify)} Spotify rows to {DB_PATH}.")
    else:
        logging.warning("Spotify scraping returned no data. Nothing saved to the database.")

    logging.info("Script finished.")
    sys.exit(0) 