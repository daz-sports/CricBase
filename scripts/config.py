from dataclasses import dataclass

@dataclass
class Config:
    """Holds all configurable paths and settings for the build process."""
    # --- Database Settings ---
    DB_NAME: str = "CricBase_v1.db"
    BACKUP_DIR: str = "path/to/your/backup_directory"
    SCHEMA_VERSION: str = "1.0.0"

    # --- Input Data Paths ---
    CRICSHEET_JSON_DIR: str = "path/to/t20s_json_sample/"
    ADDITIONAL_JSON_DIR: str = "path/to/abandoned_pre_start/"
    REGISTRY_CSV_PATH: str = "path/to/registry_100725.csv"
    TEAMS_CSV_PATH: str = "path/to/teams_sample.csv"
    VENUES_CSV_PATH: str = "path/to/venues_sample.csv"
    VENUE_ALIASES_CSV_PATH: str = "path/to/venue_aliases_sample.csv"
    PLAYERS_CSV_PATH: str = "path/to/players_info_sample.csv"
    OFFICIALS_CSV_PATH: str = "path/to/officials_sample.csv"

    # --- ICC Scraper Settings ---
    USER_AGENT: str = "give-yourself-user-agent"
    SCRAPE_START_YEAR: int = 2024
    SCRAPE_START_MONTH: int = 1
    SCRAPE_END_YEAR: int = 2024
    SCRAPE_END_MONTH: int = 12