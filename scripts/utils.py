import logging
import sqlite3
import pandas as pd
import webbrowser
from contextlib import contextmanager
from typing import Any, Dict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("build.log"), logging.StreamHandler()])

class BuildError(Exception):
    """Custom exception for the build process."""
    pass

@contextmanager
def db_connection(db_name: str):
    """Context manager for database connections."""
    conn = None
    try:
        conn = sqlite3.connect(db_name)
        conn.execute("PRAGMA foreign_keys = ON;")
        yield conn
    except sqlite3.Error as e:
        raise BuildError(f"Database connection error: {e}")
    finally:
        if conn: conn.close()


def get_nested_value(data: Dict, path: str, default: Any = None) -> Any:
    """Safely retrieves a value from a nested dictionary/list structure."""
    keys = path.split('.')
    current = data
    for key in keys:
        if isinstance(current, dict): current = current.get(key)
        elif isinstance(current, list):
            try: current = current[int(key)]
            except (ValueError, IndexError): return default
        else: return default
        if current is None: return default
    return current


def open_icc_url(row: pd.Series):
    """Helper for constructing and opening the ICC match URL (for user verification)."""
    try:
        team1 = str(row['team1']).rsplit(' ', 1)[0].replace(' ', '-').lower()
        team2 = str(row['team2']).rsplit(' ', 1)[0].replace(' ', '-').lower()

        url = f"https://www.icc-cricket.com/matches/{row['icc_id']}/{team1}-vs-{team2}"
        print(f"Opening browser to: {url}")
        webbrowser.open_new(url)

    except Exception as e:
        print(f"Could not construct or open URL: {e}")


def get_files_to_process(db_name: str, json_files: list) -> list:
    """
    Filters json_files to only include those not already in the matches table.
    json_files is a list of tuples: (filename, full_path)
    """
    with db_connection(db_name) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT match_id FROM matches")
        done_ids = {str(row[0]) for row in cursor.fetchall()}

    return [
        (fn, path) for fn, path in json_files
        if fn.removesuffix('.json') not in done_ids
    ]