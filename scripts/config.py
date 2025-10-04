from dataclasses import dataclass

@dataclass
class Config:
    """Holds all configurable paths and settings for the build process."""
    # --- Database Settings ---
    # DB_NAME: str = "CricBase_v1.db"
    # BACKUP_DIR: str = "path/to/your/backup_directory"
    #
    # # --- Input Data Paths ---
    # CRICSHEET_JSON_DIR: str = "path/to/cricsheet_json_files"
    # REGISTRY_CSV_PATH: str = ""path/to/registry_100725.csv"
    # TEAMS_CSV_PATH: str = ""path/to/teams_sample.csv"
    # VENUES_CSV_PATH: str = ""path/to/venues_sample.csv"
    # VENUE_ALIASES_CSV_PATH: str = ""path/to/venue_aliases_sample.csv"
    # PLAYERS_CSV_PATH: str = ""path/to/players_info_sample.csv"
    # OFFICIALS_CSV_PATH: str = ""path/to/officials_sample.csv"

    # --- Database Settings ---
    DB_NAME: str = "/Users/stuartmini/PycharmProjects/CricBase/data/CricBase_2024_sample.db"
    BACKUP_DIR: str = "/Users/stuartmini/PycharmProjects/CricBase/data/Backups"
    SCHEMA_VERSION: str = "1.0.1"

    # --- Input Data Paths ---
    CRICSHEET_JSON_DIR: str = "/Users/stuartmini/PycharmProjects/CricBase/data/t20s_json_sample"
    REGISTRY_CSV_PATH: str = "/Users/stuartmini/PycharmProjects/CricBase/data/registry_100725.csv"
    TEAMS_CSV_PATH: str = "/Users/stuartmini/PycharmProjects/CricBase/data/teams_sample.csv"
    VENUES_CSV_PATH: str = "/Users/stuartmini/PycharmProjects/CricBase/data/venues_sample.csv"
    VENUE_ALIASES_CSV_PATH: str = "/Users/stuartmini/PycharmProjects/CricBase/data/venue_aliases_sample.csv"
    PLAYERS_CSV_PATH: str = "/Users/stuartmini/PycharmProjects/CricBase/data/players_info_sample.csv"
    OFFICIALS_CSV_PATH: str = "/Users/stuartmini/PycharmProjects/CricBase/data/officials_sample.csv"

