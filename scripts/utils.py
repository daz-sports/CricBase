import logging
import sqlite3
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