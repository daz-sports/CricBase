from dataclasses import dataclass

@dataclass
class Config:
    """Holds all configurable paths and settings for the build process."""
    # --- Database Settings ---
    DB_NAME: str = "CricBase_v1.db"
    BACKUP_DIR: str = "path/to/your/backup_directory"
    
    # --- Input Data Paths ---
    CRICSHEET_JSON_DIR: str = "path/to/cricsheet_json_files"
    REGISTRY_CSV_PATH: str = "path/to/registry_100725.csv"
    TEAMS_CSV_PATH: str = "path/to/teams_sample.csv"
    VENUES_CSV_PATH: str = "path/to/venues_sample.csv"
    VENUE_ALIASES_CSV_PATH: str = "path/to/venue_aliases_sample.csv"
    PLAYERS_CSV_PATH: str = "path/to/players_info_sample.csv"
    OFFICIALS_CSV_PATH: str = "path/to/officials_sample.csv"
