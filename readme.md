# Podcast Charts & Details Analysis Pipeline (Local Version)

## Project Overview

This project demonstrates an end-to-end data pipeline designed to collect, process, store, and enable analysis of podcast chart data and detailed podcast information. It automatically scrapes daily Top 100 podcast charts from Apple Podcasts and Spotify (US region), retrieves detailed metadata for these podcasts from the Podcast Index API, normalizes category data, and stores everything in a local SQLite database.

The primary goal is to create a clean, structured dataset suitable for analysis and visualization, showcased through an accompanying Tableau dashboard. This serves as a **Data Analysis Portfolio Project** highlighting skills in Python, API interaction, data modeling, SQL database management (including schema design and normalization), local automation, and data visualization with Tableau.

**Note:** This version runs entirely locally using Windows Task Scheduler. Cloud deployment concepts were explored but deferred for future work.

## Key Features & Skills Demonstrated

*   **Data Acquisition:** Automated daily scraping of Top 100 charts (Apple Podcasts API, unofficial Spotify API) and enrichment via the Podcast Index API.
*   **Data Storage & Modeling:** Utilization of SQLite for relational data storage. Design and implementation of a normalized schema, specifically separating multi-value categories into dedicated lookup (`Categories`) and junction (`PodcastCategories`) tables for efficient querying.
*   **Data Processing (Python):**
    *   Robust API interaction with error handling and rate limiting (`requests`, `time`).
    *   Effective linking of chart data to detailed metadata using fuzzy matching (`difflib`) combined with exact match prioritization and a **high acceptance threshold (0.90)**.
    *   Parsing and transformation of API responses (JSON).
    *   Data insertion and management within the SQLite database (`sqlite3`).
*   **SQL Implementation:**
    *   Schema creation (`CREATE TABLE`) with appropriate data types, primary keys, foreign keys (`ON DELETE CASCADE`), and unique constraints.
    *   Data manipulation using `INSERT OR IGNORE` and `INSERT OR REPLACE` for efficient data loading and updating.
    *   Implementation of data cleansing strategy (dropping/recreating `Podcasts` and `PodcastCategories` tables daily) to maintain relevance to current charts.
    *   Creation of SQL Views (`CREATE VIEW`) to encapsulate complex analytical queries.
*   **Local Automation:** Configured for daily execution using **Windows Task Scheduler**.
*   **Environment Management:** Secure handling of API keys using **`.env` files via the `python-dotenv` library**.
*   **Data Analysis & Visualization:** Preparation of data for analysis and creation of insightful visualizations in Tableau Public.

## Tech Stack

*   **Language:** Python 3.x
*   **Libraries:** `requests`, `sqlite3`, `logging`, `difflib`, `json`, `python-dotenv`
*   **Database:** SQLite 3
*   **Automation:** **Windows Task Scheduler**
*   **Visualization:** Tableau Public / Tableau Desktop

## Architecture & Data Flow (Local)

1.  **Chart Scraping (Daily):**
    *   `scrape_apple_top100.py` fetches Apple Top 100 -> Inserts/Ignores into `Top100Lists` table in `data/podcasts.db`.
    *   `scrape_spotify_top100.py` fetches Spotify Top 100 -> Inserts/Ignores into `Top100Lists` table in `data/podcasts.db`.
2.  **Details Update & Normalization (Daily, after scrapers):**
    *   `update_podcast_details.py`:
        *   Connects to `data/podcasts.db`.
        *   Drops existing `PodcastCategories` and `Podcasts` tables.
        *   Recreates `Podcasts` (no `categories` column) and `PodcastCategories` tables. Ensures `Top100Lists` and `Categories` tables exist.
        *   Reads distinct `title`s from the current `Top100Lists`.
        *   For each title, searches Podcast Index API for best match & ID.
        *   Fetches full details & latest episode info using Podcast Index ID.
        *   `INSERT OR REPLACE` podcast details into `Podcasts` table.
        *   Parses category dictionary.
        *   `INSERT OR IGNORE` unique categories into `Categories` table.
        *   `DELETE` existing links from `PodcastCategories` (handled by DROP TABLE).
        *   `INSERT OR IGNORE` current podcast-category links into `PodcastCategories`.
        *   Commits changes per podcast.
3.  **Database File:**
    *   `data/podcasts.db`: Contains all tables, updated daily by the scripts.
4.  **Visualization:**
    *   Tableau Desktop connects directly to the local `data/podcasts.db` file.
    *   Data extract published to Tableau Public.

## Database Schema

The project utilizes a local SQLite database (`data/podcasts.db`) comprised of four main tables designed to store chart history, podcast details, and normalized category information efficiently.

**SQL `CREATE TABLE` Statements:**

```sql
-- Stores daily chart rankings from different platforms
CREATE TABLE IF NOT EXISTS Top100Lists (
    id INTEGER PRIMARY KEY AUTOINCREMENT, -- Auto-generated row ID
    platform TEXT NOT NULL,         -- e.g., 'Apple', 'Spotify'
    rank INTEGER NOT NULL,          -- Rank on the specific day and platform
    title TEXT,                     -- Title as seen on the platform's chart
    platform_podcast_id TEXT,     -- ID from the specific platform (Apple ID, Spotify ID)
    date TEXT NOT NULL,             -- Date of ranking (YYYY-MM-DD format)
    UNIQUE(platform, rank, date)    -- Prevent duplicate ranks for same platform/day
);

-- Stores current details for podcasts found in Top100Lists, fetched from Podcast Index
-- This table is dropped and recreated daily to only contain details for currently relevant podcasts.
CREATE TABLE IF NOT EXISTS Podcasts (
    podcast_id INTEGER PRIMARY KEY, -- Podcast Index unique feed ID (used as the primary link)
    title TEXT,                     -- Title from Podcast Index (often more canonical)
    description TEXT,
    feed_url TEXT,                  -- Main RSS Feed URL
    image_url TEXT,                 -- URL for podcast artwork
    episode_count INTEGER,          -- Total episodes known by Podcast Index
    avg_duration_last_10 INTEGER,   -- Calculated average duration in seconds of the last 10 episodes
    latest_episode_title TEXT,      -- Title of the most recent episode found
    last_update_time INTEGER,       -- Unix timestamp of last update from Podcast Index
    podcast_guid TEXT,              -- Unique GUID from Podcast Index
    original_url TEXT               -- Original feed URL if different from the main one
);

-- Lookup table for unique category names and their corresponding IDs from Podcast Index
CREATE TABLE IF NOT EXISTS Categories (
    category_id INTEGER PRIMARY KEY, -- Category ID from Podcast Index
    category_name TEXT NOT NULL UNIQUE -- Category Name (e.g., 'Comedy', 'News')
);

-- Junction table linking Podcasts to Categories, resolving the many-to-many relationship
-- This table is dropped and recreated daily along with the Podcasts table.
CREATE TABLE IF NOT EXISTS PodcastCategories (
    podcast_id INTEGER NOT NULL,     -- Foreign key to Podcasts table
    category_id INTEGER NOT NULL,    -- Foreign key to Categories table
    PRIMARY KEY (podcast_id, category_id), -- Ensures unique podcast-category pairs
    -- Ensures that if a podcast is deleted (due to drop/recreate), its category links are also removed
    FOREIGN KEY (podcast_id) REFERENCES Podcasts(podcast_id) ON DELETE CASCADE,
    -- Ensures that if a category were ever deleted (unlikely here), its links would be removed
    FOREIGN KEY (category_id) REFERENCES Categories(category_id) ON DELETE CASCADE
);
```

---

## Key SQL Logic

*   **Preventing Duplicate Chart Entries:** Ensures data integrity when running scrapers daily.
    ```sql
    -- Used in scrape_apple_top100.py and scrape_spotify_top100.py
    INSERT OR IGNORE INTO Top100Lists(platform, rank, title, platform_podcast_id, date)
    VALUES (?, ?, ?, ?, ?);
    ```
*   **Updating Podcast Details:** Overwrites existing podcast details with the latest fetched data, identified by the unique Podcast Index ID.
    ```sql
    -- Used in update_podcast_details.py
    INSERT OR REPLACE INTO Podcasts
    (podcast_id, title, description, feed_url, image_url, episode_count, avg_duration_last_10, latest_episode_title, last_update_time, podcast_guid, original_url)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    ```
*   **Managing Category Definitions:** Ensures each unique category from the API exists only once in the lookup table.
    ```sql
    -- Used in update_podcast_details.py
    INSERT OR IGNORE INTO Categories (category_id, category_name)
    VALUES (?, ?);
    ```
*   **Managing Podcast-Category Links:** Adds current links, preventing duplicates (deletion handled by dropping table).
    ```sql
    -- Used in update_podcast_details.py
    -- DELETE FROM PodcastCategories WHERE podcast_id = ?; -- Not needed due to DROP TABLE strategy
    INSERT OR IGNORE INTO PodcastCategories (podcast_id, category_id)
    VALUES (?, ?);
    ```
*   **Selecting Titles for Update:** Retrieves the list of podcasts to process based on chart presence.
    ```sql
    -- Used in update_podcast_details.py
    SELECT DISTINCT title FROM Top100Lists WHERE title IS NOT NULL AND title != '';
    ```
*   **Data Cleansing via Schema Management:** Handled daily by `DROP TABLE IF EXISTS Podcasts;` and `DROP TABLE IF EXISTS PodcastCategories;` followed by `CREATE TABLE...` statements in `update_podcast_details.py`.

## SQL Views for Analysis

*(Assuming you created views similar to the music project, adapted for podcasts)*
The following SQL Views could be created directly in the SQLite database to pre-aggregate or reshape data, simplifying analysis in Tableau:

*   **`vw_CurrentPodcastDetailsWithCategories`**: Joins the latest Top 100 data with podcast details and category names.
*   **`vw_PodcastRankChanges`**: Calculates the day-over-day change in rank for each podcast on each platform using `LAG()`.
*   **`vw_PodcastTimeOnList`**: Counts the distinct number of days each podcast appeared on each platform list.
*   **`vw_PodcastPlatformOverlap`**: Identifies podcast titles present on both Apple and Spotify charts on the most recent day.
*   **`vw_PodcastNewEntries`**: Lists podcasts appearing on the latest list for a platform that were not present on the previous recorded day.
*   **`vw_PodcastCategoryCounts`**: Counts the number of podcasts associated with each category based on the latest data.

*(You would need to adapt the SQL from the Music project views to use the Podcast project table/column names if you implement these. Include specific `CREATE VIEW` SQL here if you have implemented them).*

## Setup & Usage

1.  **Prerequisites:** Python 3.x, Git (optional, for cloning).
2.  **Clone Repository:** `git clone https://github.com/jmforeman/PodcastDashboardLocal`
3.  **Navigate to Folder:** `cd PodcastDashboardLocal`
4.  **Python Environment (Recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # Linux/macOS OR venv\Scripts\activate # Windows
    ```
5.  **Install Dependencies:** `pip install -r requirements.txt` (ensure `requests`, `python-dotenv` are listed).
6.  **API Credentials:** Configure using the **`.env` file method**. Create the `.env` file in the project root:
    ```dotenv
    PODCASTINDEX_API_KEY="YOUR_KEY"
    PODCASTINDEX_API_SECRET="YOUR_SECRET"
    ```
    *(Ensure `.env` is in your `.gitignore`)*
7.  **Initial Run:** Execute scripts sequentially to populate the database:
    ```bash
    python scrape_apple_top100.py
    python scrape_spotify_top100.py
    python update_podcast_details.py
    ```
    This creates `data/podcasts.db`.
8.  **Automation:** Schedule the scripts (or a master script calling them) to run daily using **Windows Task Scheduler**. Ensure the scheduler runs them in the correct order and that the working directory is set correctly if using relative paths.
9.  **Viewing Data:** Connect Tableau Desktop or a DB Browser tool to `data/podcasts.db`. Note that Tableau requires a manual data source refresh to see updates made by the scripts.

## Visualizations (Tableau Dashboard)

A Tableau Public dashboard visualizes insights from this dataset, including current rankings, podcast details, category distributions, and platform comparisons.

**Link:** [**Podcast Dashboard on Tableau Public**](https://public.tableau.com/views/PodcastDashboardUpdatedDaily/PodcastsataGlance?:language=en-US&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)


## Challenges & Learnings

*   **Data Modeling:** Refactored the initial database design to normalize category data, improving query efficiency and analytical flexibility. This involved creating junction and lookup tables (`PodcastCategories`, `Categories`) and modifying data insertion logic.
*   **API Integration & Matching:** Developed robust search logic combining exact and fuzzy matching (`difflib`) with threshold adjustments (tuned to **0.90**) to reliably link chart titles to Podcast Index entries, handling variations in naming. Implemented error handling and rate limiting (`time.sleep`) for API calls.
*   **Schema Management & Data Cleansing:** Addressed the issue of stale podcast details by implementing a daily drop/recreate strategy for the `Podcasts` and `PodcastCategories` tables, ensuring data relevance to the current Top 100 lists. Also handled the one-time migration from an older schema containing a `categories` column in the `Podcasts` table.
*   **Automation:** Configured local automation using **Windows Task Scheduler**. Explored cloud options (GitHub Actions, GCS) providing valuable learning about CI/CD pipelines and cloud storage. Addressed issues with script output interpretation in automation tools by configuring Python logging to use `stdout`.
*   **SQL for Analysis:** Utilized SQL Views (conceptually, specific views may vary based on final implementation) to pre-calculate metrics like rank changes and category counts, demonstrating the power of performing transformations within the database for efficient analysis in downstream tools like Tableau.
*   **Security:** Managed API credentials securely using environment variables loaded via `.env` files instead of hardcoding. Learned about the importance of revoking exposed keys and understanding Git history implications.
*   **Tableau Interactivity:** Configured dashboard actions and filters for intuitive exploration.

## Future Enhancements

*   **Cloud Deployment:** Migrate the pipeline to a cloud platform (e.g., GitHub Actions + GCS/S3) for fully automated, serverless execution.
*   **Historical Detail Tracking:** Implement a strategy to retain historical podcast details if desired.
*   **Improved Matching:** Investigate mapping `platform_podcast_id` to Podcast Index `podcast_id` for more reliable joins.
*   **Error Alerting:** Integrate email or other notifications for script failures during automated runs.
*   **Expand Data Sources:** Include charts from other regions or podcast platforms.
*   **Deeper Analysis:** Perform time-series analysis on rank changes, category trends, or episode release patterns.

## Contact

Created by **Jason Foreman**
*   **GitHub:** [https://github.com/jmforeman/PodcastDashboardLocal](https://github.com/jmforeman/PodcastDashboardLocal)
*   **Email:** jmforeman02@gmail.com
*   **LinkedIn:** [www.linkedin.com/in/jason-foreman-191088233](https://www.linkedin.com/in/jason-foreman-191088233)
*   **Tableau Public:** [https://public.tableau.com/app/profile/jason.foreman/vizzes](https://public.tableau.com/app/profile/jason.foreman/vizzes)
