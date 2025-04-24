import requests
import datetime
import sqlite3
import json
import logging
import os 
import sys
from typing import List, Dict, Optional, Any

# --- Configuration ---
# Apple's RSS Feed Generator URL structure
APPLE_API_BASE_URL_TEMPLATE = "https://rss.marketingtools.apple.com/api/v2/{region}/podcasts/top/{limit}/podcasts.json"
DEFAULT_APPLE_REGION = "us"
DEFAULT_LIMIT = 100
PLATFORM_NAME_APPLE = "Apple" # Specific constant for Apple
# *** Define DB Path relative to this script ***
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data') # Define data directory path
DB_FILENAME = "podcasts.db"
DB_PATH = os.path.join(DATA_DIR, DB_FILENAME) # Full path to the database

LOG_LEVEL = logging.INFO

# --- Setup Logging ---
# Configure logging level and format
# Explicitly set the stream to standard output
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s',
    stream=sys.stdout # <-- Add this line
)
# --- Ensure data directory exists ---
# *** Create the data directory if it doesn't exist ***
try:
    os.makedirs(DATA_DIR, exist_ok=True) # exist_ok=True prevents error if dir already exists
    logging.info(f"Ensured data directory exists: {DATA_DIR}")
except OSError as e:
    logging.error(f"Error creating data directory {DATA_DIR}: {e}")
    # Decide if you want to exit or continue without saving
    # exit(1) # Uncomment to exit if directory creation fails


def scrape_apple_top_podcasts(
    region: str = DEFAULT_APPLE_REGION,
    limit: int = DEFAULT_LIMIT
) -> List[Dict[str, Any]]:
    """
    Scrapes the top podcasts from Apple's public RSS feed generator API for a given region.

    Args:
        region: The two-letter country code for the chart region (e.g., 'us', 'gb').
        limit: The number of top podcasts to fetch (e.g., 100).

    Returns:
        A list of dictionaries, each representing a podcast entry, or an empty list on failure.
    """
    url = APPLE_API_BASE_URL_TEMPLATE.format(region=region, limit=limit)
    logging.info(f"Requesting Apple chart data from: {url}")
    records = []
    try:
        response = requests.get(url, timeout=15) # Increased timeout slightly
        logging.info(f"HTTP status: {response.status_code}")
        logging.debug(f"Response snippet: {response.text[:200]}...")
        response.raise_for_status()

        data = response.json()

        feed_data = data.get("feed")
        if not feed_data or not isinstance(feed_data, dict):
             logging.error("API response missing 'feed' object or it's not a dictionary.")
             return []

        results = feed_data.get("results")
        if not results or not isinstance(results, list):
             logging.error("API response missing 'results' list within 'feed' or it's not a list.")
             return []

        logging.info(f"Parsed {len(results)} items from API response.")
        today = str(datetime.date.today())

        for i, pod_data in enumerate(results[:limit]):
             if not isinstance(pod_data, dict):
                  logging.warning(f"Skipping item at index {i}, expected dict, got {type(pod_data)}")
                  continue

             records.append({
                 "platform": PLATFORM_NAME_APPLE,
                 "rank": i + 1,
                 "title": pod_data.get("name"),
                 "platform_podcast_id": pod_data.get("id"),
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

    # Check if the directory exists before trying to connect
    db_dir = os.path.dirname(db_path)
    if not os.path.exists(db_dir):
        logging.error(f"Database directory does not exist: {db_dir}. Cannot save data.")
        return

    conn = None # Initialize conn to None
    try:
        # *** Connect using the provided db_path ***
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # *** Use standard SQLite syntax for CREATE TABLE ***
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
        first_platform = records[0]['platform'] if records else 'Unknown' # Get platform for logging

        for r in records:
            required_keys = ["platform", "rank", "date"]
            if not all(key in r for key in required_keys):
                 logging.error(f"Record missing required keys: {r}. Skipping.")
                 continue
            try:
                # *** Use standard SQLite syntax for INSERT OR IGNORE ***
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

        conn.commit() # Commit changes after the loop
        logging.info(f"Database operation complete for {first_platform} data. Inserted: {insert_count}, Ignored (duplicates): {ignore_count} into {db_path}")

    except sqlite3.Error as e:
        logging.error(f"Database error accessing {db_path}: {e}")
        if conn:
            conn.rollback() # Roll back changes if error occurs during transaction
    finally:
        if conn:
            conn.close() # Ensure connection is closed
            logging.debug(f"Database connection closed for {db_path}")


if __name__ == "__main__":
    logging.info("Starting Apple Top Podcasts scrape...")
    scraped_data_apple = scrape_apple_top_podcasts() # Uses default region/limit

    if scraped_data_apple:
        # *** Pass the specific DB_PATH to the save function ***
        save_chart_data_to_db(scraped_data_apple, DB_PATH)
        logging.info(f"Attempted to save {len(scraped_data_apple)} Apple Podcasts rows to {DB_PATH}.")
    else:
        logging.warning("Apple Podcasts scraping returned no data. Nothing saved.")

    logging.info("Script finished.")
    sys.exit(0) # <-- Add explicit successful exit