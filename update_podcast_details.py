import os
import sqlite3
import requests
import time
import hashlib
import difflib
import json
import logging
import sys
from typing import List, Dict, Optional, Any # Optional, for potential future use

# --- Configuration ---
# Define DB Path relative to this script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data') # Define data directory path
DB_FILENAME = "podcasts.db"
DB_PATH = os.path.join(DATA_DIR, DB_FILENAME) # Full path to the database

LOG_LEVEL = logging.INFO # Configure logging level

# *** Search Matching Configuration ***
SEARCH_ACCEPTANCE_THRESHOLD = 0.90 # Minimum ratio for fuzzy match acceptance (e.g., 0.90 = 90%)

# --- Setup Logging ---
# Explicitly set the stream to standard output
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s',
    stream=sys.stdout
)
# --- Ensure data directory exists ---
try:
    os.makedirs(DATA_DIR, exist_ok=True)
    logging.info(f"Ensured data directory exists: {DATA_DIR}")
except OSError as e:
    logging.error(f"Error creating data directory {DATA_DIR}: {e}")
    exit(1) # Exit if we can't create the essential data directory

# --- Load API Keys (Local Environment) ---
try:
    from dotenv import load_dotenv
    if load_dotenv():
        logging.info("Loaded environment variables from .env file.")
    else:
        logging.info(".env file not found. Reading variables directly from environment.")
except ImportError:
    logging.info(".env file not used (dotenv library not found). Reading variables directly from environment.")
    pass # dotenv not installed, proceed assuming variables are set in the OS environment

API_KEY = os.environ.get("PODCASTINDEX_API_KEY")
API_SECRET = os.environ.get("PODCASTINDEX_API_SECRET")

if not API_KEY or not API_SECRET:
    logging.error("Error: Podcast Index API keys not found in environment variables (PODCASTINDEX_API_KEY, PODCASTINDEX_API_SECRET).")
    exit(1) # Exit if keys are missing

BASE_URL = "https://api.podcastindex.org/api/1.0/"

# --- Helper Functions ---

def get_headers():
    """Generates the required authentication headers for the API."""
    auth_date = str(int(time.time()))
    auth_string = API_KEY + API_SECRET + auth_date
    authorization = hashlib.sha1(auth_string.encode("utf-8")).hexdigest()
    headers = {
        "User-Agent": "PodcastDashboard/Local/1.5-Cats", # Updated UA Version
        "X-Auth-Key": API_KEY,
        "X-Auth-Date": auth_date,
        "Authorization": authorization,
    }
    return headers

# --- Search Functions (With Stricter Matching) ---
# (Search functions remain unchanged from the previous version - included for completeness)
def search_byterm(query):
    """Search using 'search/byterm', prioritize exact match, then check fuzzy ratio threshold."""
    endpoint = "search/byterm"
    url = BASE_URL + endpoint
    params = {"q": query, "max": 10}
    headers = get_headers()
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        logging.debug(f"Raw response for '{query}': {response.text[:200]}...")
        response.raise_for_status()
        data = response.json()
        results = data.get("feeds") or data.get("results", [])
        if results:
            best_match = None
            best_ratio = 0
            query_lower_stripped = query.strip().lower() # Prepare query for comparison

            for res in results:
                candidate_title = res.get("title_original", "") or res.get("title", "")
                if not candidate_title: continue

                candidate_lower_stripped = candidate_title.strip().lower()

                # *** Check for Exact Match (Case-Insensitive, Trimmed) ***
                if query_lower_stripped == candidate_lower_stripped:
                    logging.info(f"Exact match found for '{query}': '{candidate_title}'")
                    best_match = res
                    best_ratio = 1.0 # Assign perfect ratio
                    break # Found exact match, no need to check further ratios

                # *** If no exact match yet, calculate ratio ***
                ratio = difflib.SequenceMatcher(None, query_lower_stripped, candidate_lower_stripped).ratio()
                logging.debug(f"  Comparing '{query}' vs '{candidate_title}' -> Ratio: {ratio:.2f}")
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_match = res

            # *** Check final result against threshold ***
            logging.info(f"Best ratio for '{query}': {best_ratio:.2f} (Match: '{best_match.get('title') if best_match else 'None'}')")
            if best_match and best_ratio >= SEARCH_ACCEPTANCE_THRESHOLD:
                return best_match
            else:
                if best_match: # Found a match but ratio too low
                    logging.info(f"No *good* match found for '{query}' (Best ratio {best_ratio:.2f} below threshold {SEARCH_ACCEPTANCE_THRESHOLD})")
                else: # No results found at all in the API response
                     logging.info(f"No results found for '{query}' in search response.")
        else:
            logging.info(f"No results found for '{query}' in search response.")

    except requests.exceptions.Timeout:
        logging.error(f"Request timed out for '{query}'")
    except requests.exceptions.RequestException as e:
        logging.error(f"Request Exception for '{query}': {e}")
    except json.JSONDecodeError as e:
        logging.error(f"JSON Decode Error for '{query}': {e} - Response was: {response.text[:200]}...")
    except Exception as e:
        logging.error(f"General Exception for '{query}': {e}", exc_info=True)
    return None

def search_bytitle(query):
    """Search using 'search/bytitle', prioritize exact match, then check fuzzy ratio threshold."""
    endpoint = "search/bytitle"
    url = BASE_URL + endpoint
    params = {"q": query, "max": 10}
    headers = get_headers()
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        logging.debug(f"Raw response for '{query}': {response.text[:200]}...")
        response.raise_for_status()
        data = response.json()
        feeds = data.get("feeds", []) # Note: search/bytitle primarily uses 'feeds' key
        if feeds:
            best_match = None
            best_ratio = 0
            query_lower_stripped = query.strip().lower()

            for feed in feeds:
                candidate_title = feed.get("title", "") # search/bytitle generally just has "title"
                if not candidate_title: continue

                candidate_lower_stripped = candidate_title.strip().lower()

                # *** Check for Exact Match (Case-Insensitive, Trimmed) ***
                if query_lower_stripped == candidate_lower_stripped:
                    logging.info(f"Exact match found for '{query}': '{candidate_title}'")
                    best_match = feed
                    best_ratio = 1.0
                    break

                # *** If no exact match yet, calculate ratio ***
                ratio = difflib.SequenceMatcher(None, query_lower_stripped, candidate_lower_stripped).ratio()
                logging.debug(f"  Comparing '{query}' vs '{candidate_title}' -> Ratio: {ratio:.2f}")
                if ratio > best_ratio:
                    best_ratio = ratio
                    best_match = feed

            # *** Check final result against threshold ***
            logging.info(f"Best ratio for '{query}': {best_ratio:.2f} (Match: '{best_match.get('title') if best_match else 'None'}')")
            if best_match and best_ratio >= SEARCH_ACCEPTANCE_THRESHOLD:
                return best_match
            else:
                if best_match:
                    logging.info(f"No *good* match found for '{query}' (Best ratio {best_ratio:.2f} below threshold {SEARCH_ACCEPTANCE_THRESHOLD})")
                else:
                     logging.info(f"No results found for '{query}' in search response.")
        else:
            logging.info(f"No results found for '{query}' in search response.")

    except requests.exceptions.Timeout:
        logging.error(f"Request timed out for '{query}'")
    except requests.exceptions.RequestException as e:
        logging.error(f"Request Exception for '{query}': {e}")
    except json.JSONDecodeError as e:
        logging.error(f"JSON Decode Error for '{query}': {e} - Response was: {response.text[:200]}...")
    except Exception as e:
        logging.error(f"General Exception for '{query}': {e}", exc_info=True)
    return None

def search_podcast_combined(query):
    """Try searching using byterm first; if that fails or match is poor, fall back to bytitle."""
    logging.info(f"\n--- Searching combined for: '{query}' ---")
    result = search_byterm(query)
    if not result:
        logging.info(f"-> byterm search failed or match too low, falling back to search/bytitle for: '{query}'")
        result = search_bytitle(query)

    if result:
        logging.info(f"--> Combined search found candidate: ID {result.get('id')}, Title: {result.get('title')}")
        return result
    else:
        logging.warning(f"--> Combined search FAILED for: '{query}' (No suitable match found via byterm or bytitle)")
        return None

# --- Detail Fetching Functions --- (Unchanged from previous local version)

def get_full_podcast_details_by_feed_id(feed_id):
    """Retrieve full podcast details using 'podcasts/byfeedid'."""
    endpoint = "podcasts/byfeedid"
    url = BASE_URL + endpoint
    params = {"id": feed_id, "pretty": 1}
    headers = get_headers()
    logging.info(f"--- Fetching details by Feed ID: {feed_id} ---")
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        logging.debug(f"Raw response for ID {feed_id}: {response.text[:200]}...")
        response.raise_for_status()
        data = response.json()
        if data.get("feed"):
            logging.debug(f"Successfully fetched details for Feed ID: {feed_id}")
            return data["feed"]
        else:
            logging.warning(f"Response OK, but no 'feed' data found for Feed ID: {feed_id}. Response: {data}")
            return None
    except requests.exceptions.Timeout:
        logging.error(f"Request timed out for feed ID {feed_id}")
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP Error {e.response.status_code} for Feed ID {feed_id}: {e.response.text[:200]}...")
    except requests.exceptions.RequestException as e:
        logging.error(f"Request Exception for Feed ID {feed_id}: {e}")
    except json.JSONDecodeError as e:
        logging.error(f"JSON Decode Error for Feed ID {feed_id}: {e} - Response was: {response.text[:200]}...")
    except Exception as e:
        logging.error(f"General Exception for Feed ID {feed_id}: {e}", exc_info=True)
    return None

def get_full_podcast_details_by_feed_url(feed_url):
    """Retrieve full podcast details using 'podcasts/byfeedurl'."""
    endpoint = "podcasts/byfeedurl"
    url = BASE_URL + endpoint
    params = {"url": feed_url, "pretty": 1}
    headers = get_headers()
    logging.info(f"--- Fetching details by Feed URL: {feed_url[:50]}... ---")
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        logging.debug(f"Raw response for URL {feed_url[:50]}...: {response.text[:200]}...")
        response.raise_for_status()
        data = response.json()
        if data.get("feed"):
            logging.debug(f"Successfully fetched details for Feed URL: {feed_url[:50]}...")
            return data["feed"]
        else:
            logging.warning(f"Response OK, but no 'feed' data found for Feed URL: {feed_url[:50]}... Response: {data}")
            return None
    except requests.exceptions.Timeout:
        logging.error(f"Request timed out for URL {feed_url[:50]}...")
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP Error {e.response.status_code} for URL {feed_url[:50]}...: {e.response.text[:200]}...")
    except requests.exceptions.RequestException as e:
        logging.error(f"Request Exception for URL {feed_url[:50]}...: {e}")
    except json.JSONDecodeError as e:
        logging.error(f"JSON Decode Error for URL {feed_url[:50]}...: {e} - Response was: {response.text[:200]}...")
    except Exception as e:
        logging.error(f"General Exception for URL {feed_url[:50]}...: {e}", exc_info=True)
    return None

def get_latest_episode_info(feed_id):
    """
    Fetches the latest 10 episodes for a feed ID.
    Returns a tuple: (average_duration_seconds, latest_episode_title)
    Returns (None, None) if calculation fails or no episodes found.
    """
    endpoint = "episodes/byfeedid"
    url = BASE_URL + endpoint
    params = {"id": feed_id, "max": 10, "pretty": 1}
    headers = get_headers()

    logging.info(f"--- Fetching latest episode info for Feed ID: {feed_id} ---")

    latest_episode_title = None
    average_duration = None

    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        logging.debug(f"Raw response for {feed_id}: {response.text[:150]}...")
        response.raise_for_status()
        data = response.json()
        items = data.get("items", [])

        if not items:
            logging.info(f"No episodes found for Feed ID: {feed_id}")
            return None, None

        latest_episode_title = items[0].get("title")
        logging.debug(f"Latest episode title: '{latest_episode_title}'")

        total_duration = 0
        valid_episode_count = 0
        for episode in items:
            duration = episode.get("duration")
            if duration and isinstance(duration, int) and duration > 0:
                total_duration += duration
                valid_episode_count += 1

        if valid_episode_count > 0:
            average_duration = int(total_duration / valid_episode_count)
            logging.debug(f"Avg duration (last {valid_episode_count}) for {feed_id}: {average_duration} sec")
        else:
            logging.info(f"No valid durations found in the last {len(items)} episodes for {feed_id}")

        return average_duration, latest_episode_title

    except requests.exceptions.Timeout:
         logging.error(f"Request timed out for feed ID {feed_id}")
    except requests.exceptions.HTTPError as e:
         logging.error(f"HTTP Error {e.response.status_code} for Feed ID {feed_id}: {e.response.text[:150]}...")
    except requests.exceptions.RequestException as e:
        logging.error(f"Request Exception for feed {feed_id}: {e}")
    except json.JSONDecodeError as e:
         logging.error(f"JSON Decode Error for feed {feed_id}: {e} - Response: {response.text[:150]}...")
    except Exception as e:
        logging.error(f"General Exception for feed {feed_id}: {e}", exc_info=True)

    return None, None


# (Keep imports and config/setup above)

# --- Main Database Update Function ---

def update_all_podcast_details(db_path: str):
    """Fetches podcast titles from Top100Lists, gets full details from PodcastIndex,
       and updates the Podcasts, Categories, and PodcastCategories tables
       in the specified SQLite DB."""

    conn = None
    cursor = None
    update_errors = 0
    category_link_errors = 0
    titles = [] # Initialize titles list

    try:
        # --- Phase 1: Connect and Schema Setup ---
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys = ON;")
        cursor = conn.cursor()
        logging.info(f"Connected to database: {db_path}")

        # Drop dependent table first if it exists
        cursor.execute("DROP TABLE IF EXISTS PodcastCategories;")
        # Drop main table
        cursor.execute("DROP TABLE IF EXISTS Podcasts;")
        logging.info("Dropped existing Podcasts and PodcastCategories tables (if they existed).")

        # Ensure ALL tables exist with the correct schema
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
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Podcasts (
                podcast_id INTEGER PRIMARY KEY, title TEXT, description TEXT, feed_url TEXT, image_url TEXT,
                episode_count INTEGER, avg_duration_last_10 INTEGER, latest_episode_title TEXT,
                last_update_time INTEGER, podcast_guid TEXT, original_url TEXT
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Categories (
                category_id INTEGER PRIMARY KEY,
                category_name TEXT NOT NULL UNIQUE
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS PodcastCategories (
                podcast_id INTEGER NOT NULL,
                category_id INTEGER NOT NULL,
                PRIMARY KEY (podcast_id, category_id),
                FOREIGN KEY (podcast_id) REFERENCES Podcasts(podcast_id) ON DELETE CASCADE,
                FOREIGN KEY (category_id) REFERENCES Categories(category_id) ON DELETE CASCADE
            )
        ''')
        conn.commit() # Commit schema changes
        logging.info("Ensured all required tables exist with current schema.")

        # --- Phase 2: Get Titles (Now AFTER schema setup is confirmed) ---
        # This specific operation can fail if Top100Lists doesn't exist or has issues
        try:
            cursor.execute("SELECT DISTINCT title FROM Top100Lists WHERE title IS NOT NULL AND title != ''")
            titles = [row[0] for row in cursor.fetchall()]
            logging.info(f"Found {len(titles)} unique podcast titles from Top100Lists.")
        except sqlite3.Error as e:
            # Log the specific error during title fetch but continue if possible
            # (though script might not do much without titles)
            logging.error(f"Database error getting titles from Top100Lists: {e}")
            # Titles list remains empty

        if not titles:
            logging.warning("No titles found in Top100Lists to process. Exiting update function.")
            # Exit cleanly if no titles found
            return # No need to proceed to Phase 3

        # --- Phase 3: Process Each Title (Loop inside the main try block) ---
        processed_count = 0
        for title in titles:
            processed_count += 1
            logging.info(f"\n======= Processing {processed_count}/{len(titles)}: '{title}' =======")

            # ... (3a. Search logic - unchanged) ...
            candidate = search_podcast_combined(title)
            if not candidate:
                time.sleep(1)
                continue

            # ... (3b. Get Full Details logic - unchanged) ...
            candidate_feed_id = candidate.get("id")
            full_details = None
            if candidate_feed_id:
                full_details = get_full_podcast_details_by_feed_id(candidate_feed_id)
            else:
                 logging.warning(f"Candidate for '{title}' found, but missing 'id'. Candidate data: {candidate}")

            if not full_details:
                 feed_url_from_candidate = candidate.get("url") or candidate.get("originalUrl")
                 if feed_url_from_candidate:
                     logging.info(f"-> Feed ID fetch failed or missing, falling back to Feed URL for '{title}'")
                     full_details = get_full_podcast_details_by_feed_url(feed_url_from_candidate)
                 else:
                     logging.warning(f"Cannot fall back to feed URL for '{title}', URL missing in candidate: {candidate}")

            # ... (3c. Process Details & Insert/Replace into DB - unchanged) ...
            if full_details:
                feed_id = full_details.get("id")
                if not feed_id:
                    logging.warning(f"!!! SKIPPING DB operations: 'id' field missing in full_details for '{title}'.")
                    continue

                podcast_title = full_details.get("title")
                # ... (extract other podcast details) ...
                description = full_details.get("description")
                feed_url = full_details.get("url")
                original_url = full_details.get("originalUrl")
                image_url = full_details.get("image") or full_details.get("artwork")
                episode_count = full_details.get("episodeCount")
                last_update_time = full_details.get("lastUpdateTime")
                categories_dict = full_details.get("categories") # Get the original dict
                podcast_guid = full_details.get("podcastGuid")

                avg_dur_10, latest_title = get_latest_episode_info(feed_id)

                if not feed_url: feed_url = candidate.get("url")
                if not original_url: original_url = candidate.get("originalUrl")

                logging.debug(f"+++ Preparing DB insert/replace for Podcast ID: {feed_id} +++")

                try:
                    # Insert/Replace main podcast data
                    cursor.execute('''
                        INSERT OR REPLACE INTO Podcasts
                        (podcast_id, title, description, feed_url, image_url, episode_count, avg_duration_last_10, latest_episode_title, last_update_time, podcast_guid, original_url)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        feed_id, podcast_title, description, feed_url, image_url, episode_count,
                        avg_dur_10, latest_title, last_update_time,
                        podcast_guid, original_url
                    ))
                    logging.debug(f"Podcast info inserted/replaced for ID: {feed_id}")

                    # Process Categories
                    del_success = True
                    try:
                        cursor.execute("DELETE FROM PodcastCategories WHERE podcast_id = ?", (feed_id,))
                        logging.debug(f"Deleted existing category links for podcast {feed_id} ({cursor.rowcount} rows)")
                    except sqlite3.Error as e_del:
                        category_link_errors += 1
                        del_success = False
                        logging.error(f"Error deleting old categories for podcast {feed_id}: {e_del}")

                    if del_success and categories_dict and isinstance(categories_dict, dict):
                        logging.debug(f"Processing {len(categories_dict)} categories for podcast {feed_id}")
                        for cat_id_str, cat_name in categories_dict.items():
                            if not cat_id_str or not cat_name: continue
                            try:
                                cat_id = int(cat_id_str)
                                cursor.execute('INSERT OR IGNORE INTO Categories (category_id, category_name) VALUES (?, ?)', (cat_id, cat_name))
                                cursor.execute('INSERT OR IGNORE INTO PodcastCategories (podcast_id, category_id) VALUES (?, ?)', (feed_id, cat_id))
                            except ValueError:
                                category_link_errors += 1
                                logging.warning(f"Invalid category ID format '{cat_id_str}' for podcast {feed_id}. Skipping category link.")
                            except sqlite3.Error as e_cat:
                                category_link_errors += 1
                                logging.error(f"Error processing category ID {cat_id_str}/{cat_name} for podcast {feed_id}: {e_cat}")
                    elif del_success:
                         logging.debug(f"No categories found or invalid format for podcast {feed_id}.")

                    conn.commit() # Commit all changes for this podcast
                    logging.info(f"### SUCCESS: Updated details and categories in DB for Feed ID: {feed_id} ('{title}') ###")

                except sqlite3.Error as e:
                    update_errors += 1
                    logging.error(f"!!! DATABASE ERROR processing main podcast data for Feed ID {feed_id} ('{title}'): {e} !!!")
                    if conn: conn.rollback() # Rollback this specific podcast's transaction

            else:
                 logging.warning(f"--- FAILED: Could not retrieve full details for '{title}' (Candidate ID: {candidate_feed_id}) ---")


            # API Rate Limiting
            time.sleep(1.5) # Pause between processing different podcasts


    # --- Catch potential errors from the entire process ---
    except sqlite3.Error as e:
        # This catches errors mainly from connect or initial schema setup
        logging.error(f"An SQLite error occurred: {e}", exc_info=True)
    except Exception as e:
        # Catch any other unexpected errors during processing
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)

    # --- Phase 4: Cleanup (This 'finally' now correctly pairs with the outer 'try') ---
    finally:
        logging.debug("Executing final database cleanup.")
        # Cursor closing is implicitly handled by closing connection in sqlite3 usually,
        # but explicit close is okay too. Let's simplify slightly.
        # if cursor:
        #     try: cursor.close(); logging.debug("Cursor closed.")
        #     except Exception as e_cur: logging.error(f"Error closing cursor: {e_cur}")
        if conn:
            try:
                conn.close()
                logging.info(f"\n======= Database connection closed ({db_path}). Update errors: {update_errors}. Category link errors: {category_link_errors} =======")
            except Exception as e_conn:
                logging.error(f"Error closing connection: {e_conn}")


# --- Main Execution Block --- (Keep as before)
if __name__ == "__main__":
    logging.info(f"Starting Podcast Index update process, using database: {DB_PATH}")
    update_all_podcast_details(DB_PATH)
    logging.info("Podcast Index update script finished.")
    sys.exit(0)