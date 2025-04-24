import sqlite3
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import os
import logging
import sys 
import math 
from datetime import datetime

# --- Configuration ---
# Path to service account key JSON file
# Use absolute path
KEY_FILE_PATH = r"C:\Users\jmfor\Documents\local_podcast_dashboard_pipeline\podcast-dashboard-pipeline-3ee4a06f8550.json"
# Path to SQLite database file
DB_FILE_PATH = r"C:\Users\jmfor\Documents\local_podcast_dashboard_pipeline\data\podcasts.db"

# ID of the Google Sheet
GOOGLE_SHEET_ID = "1-bPweWPzKlY-h4YjCzN4x7S0comscrvz_hQbErqc8ek"

# Cell to start writing data in each sheet 
START_CELL = "A1"

# Define queries and target worksheet names
QUERY_CONFIGS = [
    {
        "query": "SELECT * FROM vw_CurrentPodcastDetailsWithCategories",
        "worksheet_name": "Top100List" # Name of the first target worksheet (tab)
    },
    {
        "query": "SELECT * FROM vw_NewEntries",
        "worksheet_name": "NewEntries" # Name of the second target worksheet (tab)
    },
    {
        "query": "SELECT * FROM vw_PlatformOverlap",
        "worksheet_name": "PlatformOverlap" # Name of the third target worksheet (tab)
    },
    {
        "query": "SELECT * FROM vw_RankChanges",
        "worksheet_name": "RankChanges" # Name of the fourth target worksheet (tab)
    },
    {
        "query": "SELECT * FROM vw_TimeOnList",
        "worksheet_name": "TimeOnList" # Name of the fifth target worksheet (tab)
    }
    # Add more dictionaries here for other queries and their target sheets
]

# Logging Setup
LOG_LEVEL = logging.INFO
# Log file in the same directory as the script
LOG_FILE = os.path.join(os.path.dirname(__file__), 'update_gsheet.log')
logging.basicConfig(filename=LOG_FILE,
                    level=LOG_LEVEL,
                    format='%(asctime)s - %(levelname)s - %(message)s')
# Also log to console (stdout) for immediate feedback
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


# --- End Configuration ---

def update_multiple_google_sheets(db_path, sheet_id, query_configs, key_file_path):
    """
    Reads data from multiple SQLite queries (views/tables) and updates
    corresponding worksheets in a Google Sheet. Handles NaN/None values.

    Args:
        db_path (str): Path to the SQLite database file.
        sheet_id (str): The ID of the Google Sheet.
        query_configs (list): A list of dictionaries, each containing 'query' and 'worksheet_name'.
        key_file_path (str): Path to the Google service account JSON key file.
    """
    conn = None
    gc = None

    try:
        logging.info("Script started.")
        # Define Google API scopes
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive.file'
        ]

        # Authenticate using service account
        logging.info(f"Authenticating using key: {key_file_path}")
        credentials = Credentials.from_service_account_file(key_file_path, scopes=scopes)
        gc = gspread.authorize(credentials)

        # Open the main Google Sheet by ID
        logging.info(f"Opening Google Sheet ID: '{sheet_id}'")
        spreadsheet = gc.open_by_key(sheet_id)

        # Connect to SQLite database
        logging.info(f"Connecting to database: {db_path}")
        conn = sqlite3.connect(db_path)

        # Loop through each query configuration
        for config in query_configs:
            query = config["query"]
            worksheet_name = config["worksheet_name"]
            logging.info(f"--- Processing Worksheet: '{worksheet_name}' ---")

            try:
                # Read data from SQLite using pandas
                logging.info(f"Executing SQL query: {query[:100]}...")
                df = pd.read_sql_query(query, conn)
                logging.info(f"Successfully read {len(df)} rows from the database for '{worksheet_name}'.")

                # --- *** Data Cleaning Step (using Pandas) *** ---
                # Replace NaN and None with empty strings for JSON compatibility
                # fillna('') handles both None and NaN for object columns
                # Convert potential problematic floats AFTER reading from SQL
                for col in df.select_dtypes(include=['float']).columns:
                     # Check if the column actually contains NaNs before converting
                     if df[col].isnull().any():
                          df[col] = df[col].apply(lambda x: '' if pd.isna(x) else x)

                # Replace any remaining None values (often read as None from DB)
                df = df.fillna('') # Replace Python None/np.nan with empty string

                logging.debug(f"Data cleaned for worksheet '{worksheet_name}'.")
                # --- *** End of Data Cleaning Step *** ---

                # Prepare data including headers from the DataFrame
                # Convert DataFrame back to list of lists for gspread
                data_to_write = [df.columns.values.tolist()] + df.values.tolist()

                # Select the target worksheet
                try:
                    worksheet = spreadsheet.worksheet(worksheet_name)
                    logging.info(f"Found existing worksheet: '{worksheet_name}'.")
                except gspread.exceptions.WorksheetNotFound:
                    logging.warning(f"Worksheet '{worksheet_name}' not found. Skipping update for this sheet.")
                    continue

                # Clear existing data in the worksheet
                logging.info(f"Clearing existing data from worksheet '{worksheet_name}'.")
                worksheet.clear()

                # Write the data to the worksheet starting at START_CELL
                logging.info(f"Writing {len(data_to_write)} rows (incl. header) to '{worksheet_name}' starting at {START_CELL}.")
                worksheet.update(range_name=START_CELL, values=data_to_write, value_input_option='USER_ENTERED')

                logging.info(f"Successfully updated worksheet '{worksheet_name}'.")

            except sqlite3.Error as e:
                logging.error(f"SQLite query error for worksheet '{worksheet_name}': {e}", exc_info=True)
            except gspread.exceptions.APIError as e:
                 logging.error(f"Google Sheets API error for worksheet '{worksheet_name}': {e}", exc_info=True)
            # Catch the specific JSON error from requests if it bubbles up
            except requests.exceptions.InvalidJSONError as e_json:
                 logging.error(f"JSON Encoding error sending data for worksheet '{worksheet_name}'. Check NaN/Inf values: {e_json}", exc_info=True)
            except Exception as e:
                logging.error(f"Unexpected error processing worksheet '{worksheet_name}': {e}", exc_info=True)

    except FileNotFoundError as e:
        logging.error(f"Setup error - File not found: {e}")
    except gspread.exceptions.SpreadsheetNotFound:
         logging.error(f"Setup error - Google Sheet ID '{sheet_id}' not found or not shared correctly.")
    except gspread.exceptions.APIError as e_gspread_conn:
         logging.error(f"Setup error - Failed to connect to Google Sheets (check credentials/API access/scopes): {e_gspread_conn}")
    except sqlite3.Error as e:
        logging.error(f"Failed to connect to database {db_path}: {e}")
    except Exception as e:
        logging.error(f"A critical setup or connection error occurred: {e}", exc_info=True)
    finally:
        # Close the database connection
        if conn:
            conn.close()
            logging.info("Database connection closed.")
        logging.info("Script finished.")


if __name__ == "__main__":
    update_multiple_google_sheets(DB_FILE_PATH, GOOGLE_SHEET_ID, QUERY_CONFIGS, KEY_FILE_PATH)
    sys.exit(0) # Explicit successful exit