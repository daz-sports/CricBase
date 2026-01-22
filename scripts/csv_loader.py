import sqlite3
from typing import List, Tuple
import logging
import pandas as pd
from utils import db_connection, BuildError
from config import Config

class RegistryLoader:
    """Handles loading the registry into the database."""

    def __init__(self, db_name: str):
        self.db_name = db_name

    def _validate_registry_data(self, df: pd.DataFrame) -> bool:
        required_columns = ['identifier', 'name', 'unique_name']
        if not all(column in df.columns for column in required_columns):
            logging.error(f"Registry CSV is missing required columns. Found: {list(df.columns)}, Required: {required_columns}")
            return False
        for col in required_columns:
            if df[col].isnull().any() or (df[col] == '').any():
                logging.error(f"Registry CSV has missing or empty values in required NOT NULL column: '{col}'")
                return False
        return True

    def load_registry_from_csv(self, csv_path: str):
        logging.info("Starting to load registry...")
        try:
            df = pd.read_csv(csv_path, na_filter=False, dtype=str)
        except FileNotFoundError as e:
            logging.critical(f"Failed to read {csv_path}: {e}")
            raise BuildError(e)

        if not self._validate_registry_data(df):
            raise BuildError("Registry CSV validation failed.")

        db_columns = [
            'identifier', 'name', 'unique_name', 'key_cricinfo', 'key_cricinfo_2', 'key_bcci', 'key_bcci_2', 'key_bigbash',
            'key_cricbuzz', 'key_cricheroes', 'key_crichq', 'key_cricingif', 'key_cricketarchive',
            'key_cricketarchive_2', 'key_cricketworld', 'key_nvplay', 'key_nvplay_2', 'key_opta',
            'key_opta_2', 'key_pulse', 'key_pulse_2'
        ]

        people_to_insert: List[Tuple] = []

        for index, row in df.iterrows():
            person_tuple = tuple(row.get(col, None) for col in db_columns)
            people_to_insert.append(person_tuple)

        if not people_to_insert:
            logging.warning("No valid data found to insert into registry.")
            return

        with db_connection(self.db_name) as conn:
            cursor = conn.cursor()
            try:
                sql = f"""
                    INSERT OR IGNORE INTO registry ({', '.join(db_columns)})
                    VALUES ({', '.join(['?'] * len(db_columns))})
                """
                cursor.executemany(sql, people_to_insert)
                conn.commit()
                logging.info(f"Processed {cursor.rowcount} records into the registry table.")
            except sqlite3.Error as e:
                logging.error(f"A database error occurred during registry insertion: {e}")
                conn.rollback()
                raise BuildError(e)

class TeamsLoader:
    """
    Handles loading the teams table into the database for easy startup.
    There is the functionality to add new teams included elsewhere.
    """

    def __init__(self, db_name: str):
        self.db_name = db_name

    def _validate_teams_data(self, df: pd.DataFrame) -> bool:
        required_columns = ['team_id', 'format', 'full_name', 'abbreviation', 'sex', 'nation']
        if not all(column in df.columns for column in required_columns):
            logging.error(f"Teams dataframe is missing required columns. Found: {list(df.columns)}, Required: {required_columns}. \nPossible error in team_id creation process.")
            return False
        for col in required_columns:
            if df[col].isnull().any() or (df[col] == '').any():
                logging.error(f"Teams CSV has missing or empty values in required NOT NULL column: '{col}'")
                return False
        valid_sex = {'male', 'female'}
        if not set(df['sex'].unique()).issubset(valid_sex):
            logging.error("Invalid value found in 'sex' column. Must be 'male' or 'female'.")
            return False
        return True

    def load_teams_from_csv(self, csv_path: str):
        logging.info("Starting to load the teams...")
        try:
            df = pd.read_csv(csv_path, na_filter=False, dtype=str)
        except FileNotFoundError as e:
            logging.critical(f"Failed to read {csv_path}: {e}")
            raise BuildError(e)

        sex_map = {'male': 'M', 'female': 'F'}
        df['sex_code'] = df['sex'].map(sex_map)
        df['team_id'] = df['abbreviation'].str.split('-').str[0].str.lower() + df['sex_code'] + df['format']

        if not self._validate_teams_data(df):
            raise BuildError("Teams CSV/dataframe validation failed.")

        db_columns = [
            'team_id', 'format', 'full_name', 'short_name',
            'abbreviation', 'nickname', 'sex', 'nation'
        ]

        teams_to_insert: List[Tuple] = []

        for index, row in df.iterrows():
            team_tuple = tuple(row.get(col, None) for col in db_columns)
            teams_to_insert.append(team_tuple)

        if not teams_to_insert:
            logging.warning("No valid data found to insert into the teams table.")
            return

        with db_connection(self.db_name) as conn:
            cursor = conn.cursor()
            try:
                sql = f"""
                    INSERT OR IGNORE INTO teams ({', '.join(db_columns)})
                    VALUES ({', '.join(['?'] * len(db_columns))})
                """
                cursor.executemany(sql, teams_to_insert)
                conn.commit()
                logging.info(f"Processed {cursor.rowcount} records into the teams table.")
            except sqlite3.Error as e:
                logging.error(f"A database error occurred during teams insertion: {e}")
                conn.rollback()
                raise BuildError(e)

class VenuesLoader:
    """
    Handles loading venue data into the database.

    For ease of initial startup, csv files for both venues and venue_aliases tables are provided that include
    the venue_id. This prevents the user from having to do any work to link unknown stadiums to their canonical venue.
    However, the functionality to add new venues is also built in to be used.

    """
    def __init__(self, db_name: str):
        self.db_name = db_name

    def _validate_venues_data(self, df: pd.DataFrame) -> bool:
        required_columns = ['venue_id', 'venue_name', 'city', 'nation', 'continent', 'nation_code']
        if not all(column in df.columns for column in required_columns):
            logging.error(f"Venues CSV is missing required columns. Found: {list(df.columns)}, Required: {required_columns}")
            return False
        for col in required_columns:
            if df[col].isnull().any() or (df[col] == '').any():
                logging.error(f"Venue CSV has missing or empty values in required NOT NULL column: '{col}'")
                return False

        return True

    def load_venues_from_csv(self, csv_path: str):
        logging.info("Starting to load venue data...")
        try:
            df = pd.read_csv(csv_path, na_filter=False, dtype=str)

            nullable_string_cols = [
                'admin_area_1', 'admin_area_2', 'hemisphere', 'home_team_id_1', 'home_team_id_2', 'latitude',
                'longitude', 'elevation', 'timezone', 'utc_offset_str'
            ]

            for col in nullable_string_cols:
                if col in df.columns:
                    df[col] = df[col].replace(['', 'NaN'], None)

        except FileNotFoundError as e:
            logging.critical(f"Failed to read {csv_path}: {e}")
            raise BuildError(e)

        if not self._validate_venues_data(df):
            raise BuildError("Venues CSV validation failed.")

        db_columns = [
            'venue_id', 'venue_name', 'city', 'admin_area_1', 'admin_area_2', 'nation', 'nation_code', 'continent',
            'hemisphere', 'home_team_id_1', 'home_team_id_2', 'latitude', 'longitude', 'elevation', 'timezone',
            'utc_offset_str'
        ]

        venues_to_insert: List[Tuple] = []

        for index, row in df.iterrows():
            venue_tuple = tuple(row.get(col, None) for col in db_columns)
            venues_to_insert.append(venue_tuple)

        if not venues_to_insert:
            logging.warning("No valid data found to insert into the venues table.")
            return

        with db_connection(self.db_name) as conn:
            cursor = conn.cursor()
            try:
                sql = f"""
                    INSERT OR IGNORE INTO venues ({', '.join(db_columns)})
                    VALUES ({', '.join(['?'] * len(db_columns))})
                """
                cursor.executemany(sql, venues_to_insert)
                conn.commit()
                logging.info(f"Processed {cursor.rowcount} records into the venues table.")
            except sqlite3.Error as e:
                logging.error(f"A database error occurred during venues insertion: {e}")
                conn.rollback()
                raise BuildError(e)

class VenueAliasesLoader:
    """Handles loading venue alias data into the database."""

    def __init__(self, db_name: str):
        self.db_name = db_name

    def _validate_venue_aliases_data(self, df: pd.DataFrame) -> bool:
        required_columns = ['alias_name', 'alias_city', 'venue_id']
        if not all(column in df.columns for column in required_columns):
            logging.error(f"Venue Aliases CSV is missing required columns. Found: {list(df.columns)}, Required: {required_columns}")
            return False
        for col in required_columns:
            if df[col].isnull().any() or (df[col] == '').any():
                logging.error(f"Venue Aliases CSV has missing or empty values in required NOT NULL column: '{col}'")
                return False
        return True

    def load_venue_aliases_from_csv(self, csv_path: str):
        logging.info("Starting to load venue alias data...")
        try:
            df = pd.read_csv(csv_path, na_filter=False, dtype=str)
        except FileNotFoundError as e:
            logging.critical(f"Failed to read {csv_path}: {e}")
            raise BuildError(e)

        if not self._validate_venue_aliases_data(df):
            raise BuildError("Venue alias CSV validation failed.")

        db_columns = [
            'alias_name', 'alias_city', 'venue_id',
        ]

        venue_aliases_to_insert: List[Tuple] = []

        for index, row in df.iterrows():
            alias_tuple = tuple(row.get(col, None) for col in db_columns)
            venue_aliases_to_insert.append(alias_tuple)

        if not venue_aliases_to_insert:
            logging.warning("No valid data found to insert as a venue alias.")
            return

        with db_connection(self.db_name) as conn:
            cursor = conn.cursor()
            try:
                sql = f"""
                    INSERT OR IGNORE INTO venue_aliases ({', '.join(db_columns)})
                    VALUES ({', '.join(['?'] * len(db_columns))})
                """
                cursor.executemany(sql, venue_aliases_to_insert)
                conn.commit()
                logging.info(f"Processed {cursor.rowcount} records into the venue aliases table.")
            except sqlite3.Error as e:
                logging.error(f"A database error occurred during venue alias insertion: {e}")
                conn.rollback()
                raise BuildError(e)

class PlayersLoader:
    """
    Handles loading player data into the database.

    The usual process involves this table being populated with players from match_players and then
    the user being prompted for the player's information (e.g. birth_date etc.) for new players.
    To speed up the replication process, that data is provided in a CSV file for initial database creation.
    The functionality to add new players is included elsewhere (all full-member nation T20 players for 2024
    are already in the database).
    """
    def __init__(self, db_name: str):
        self.db_name = db_name

    def _validate_player_data(self, df: pd.DataFrame) -> bool:
        required_columns = ['key_cricsheet','unique_name', 'full_name', 'sex', 'current_nation']
        if not all(column in df.columns for column in required_columns):
            logging.error(f"Players CSV is missing required columns. Required: {required_columns}")
            return False

        for col in required_columns:
            if df[col].isnull().any() or (df[col] == '').any():
                logging.error(f"Players CSV has missing or empty values in required NOT NULL column: '{col}'")
                problematic_rows = df[df[col].isnull() | (df[col] == '')]
                logging.error(f"Problematic rows:\n{problematic_rows}")
                return False

        valid_sex = {'male', 'female'}
        if not set(df['sex'].unique()).issubset(valid_sex):
            logging.error("Invalid value found in 'sex' column. Must be 'male' or 'female'.")
            return False

        valid_bat_hands = {'R', 'L'}
        mask = df['bat_hand'].notna() & (df['bat_hand'] != '')
        bat_hand_values = df.loc[mask, 'bat_hand'].unique()
        if not set(bat_hand_values).issubset(valid_bat_hands):
            logging.error(f"Invalid value found in 'bat_hand' column. Found: {set(bat_hand_values) - valid_bat_hands}")
            return False

        return True

    def load_players_from_csv(self, csv_path: str):
        logging.info("Starting to load the players...")
        try:
            df = pd.read_csv(csv_path)

            for col in ['birth_date', 'death_date']:
                temp_dates = pd.to_datetime(df[col], errors='coerce')
                df[col] = temp_dates.apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)

            nullable_string_cols = [
                'birth_place', 'birth_nation', 'bat_hand', 'bowl_hand', 'bowl_style',
                'previous_nation_1', 'previous_nation_2'
            ]

            for col in nullable_string_cols:
                if col in df.columns:
                    df[col] = df[col].replace(['', 'NaN'], None)

            df['wicketkeeper'] = pd.to_numeric(df['wicketkeeper'], errors='coerce')

        except FileNotFoundError as e:
            logging.critical(f"Failed to read {csv_path}: {e}")
            raise BuildError(e)

        if not self._validate_player_data(df):
            raise BuildError("Players CSV validation failed.")

        df.rename(columns={'key_cricsheet': 'identifier'}, inplace=True)

        db_columns = [
            'identifier', 'unique_name', 'full_name',
            'display_name', 'sex', 'birth_date', 'birth_place', 'birth_nation', 'bat_hand',
            'bowl_hand', 'bowl_style', 'current_nation', 'previous_nation_1', 'previous_nation_2',
            'wicketkeeper', 'death_date'
        ]

        df_for_db = df.astype(object).where(pd.notna(df), None)

        players_to_insert = [tuple(row) for row in df_for_db[db_columns].itertuples(index=False)]

        if not players_to_insert:
            logging.warning("No valid data found to insert into the players table.")
            return

        with db_connection(self.db_name) as conn:
            cursor = conn.cursor()
            try:
                sql = f"""
                    INSERT OR IGNORE INTO players ({', '.join(db_columns)})
                    VALUES ({', '.join(['?'] * len(db_columns))})
                """
                cursor.executemany(sql, players_to_insert)
                conn.commit()
                logging.info(f"Processed {cursor.rowcount} records into the players table.")
            except sqlite3.Error as e:
                logging.error(f"A database error occurred during player insertion: {e}")
                conn.rollback()
                raise BuildError(e)

class OfficialsLoader:
    """
    Handles loading officials data into the database.

    The usual process involves this table being populated with officials from matches table and then
    the user being prompted for the official's information (e.g. birth_date etc.) for new officials.
    To speed up the replication process, that data is provided in a CSV file for initial database creation.
    The functionality to add new officials is also built in to be used.
    """

    def __init__(self, db_name: str):
        self.db_name = db_name

    def _validate_officials_data(self, df: pd.DataFrame) -> bool:
        required_columns = ['key_cricsheet', 'unique_name', 'full_name', 'sex']
        if not all(column in df.columns for column in required_columns):
            logging.error(f"Officials CSV is missing required columns. Required: {required_columns}")
            return False

        for col in required_columns:
            if df[col].isnull().any() or (df[col] == '').any():
                logging.error(f"Officials CSV has missing or empty values in required NOT NULL column: '{col}'")
                return False

        valid_sex = {'male', 'female'}
        if not set(df['sex'].unique()).issubset(valid_sex):
            logging.error("Invalid value found in 'sex' column. Must be 'male' or 'female'.")
            return False

        return True

    def load_officials_from_csv(self, csv_path: str):
        logging.info("Starting to load the officials...")
        try:
            df = pd.read_csv(csv_path)

            for col in ['birth_date', 'death_date']:
                temp_dates = pd.to_datetime(df[col], errors='coerce')
                df[col] = temp_dates.apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else None)

            for col in ['birth_date', 'death_date']:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].dt.strftime('%Y-%m-%d')

        except FileNotFoundError as e:
            logging.critical(f"Failed to read {csv_path}: {e}")
            raise BuildError(e)

        if not self._validate_officials_data(df):
            raise BuildError("Officials CSV validation failed.")

        df.rename(columns={'key_cricsheet': 'identifier'}, inplace=True)

        db_columns = [
            'identifier', 'unique_name', 'full_name', 'display_name', 'sex', 'birth_date',
            'birth_place', 'birth_nation', 'death_date'
        ]

        df_for_db = df.astype(object).where(pd.notna(df), None)

        officials_to_insert = [tuple(row) for row in df_for_db[db_columns].itertuples(index=False)]

        if not officials_to_insert:
            logging.warning("No valid data found to insert into the officials table.")
            return

        with db_connection(self.db_name) as conn:
            cursor = conn.cursor()
            try:
                sql = f"""
                    INSERT OR IGNORE INTO officials ({', '.join(db_columns)})
                    VALUES ({', '.join(['?'] * len(db_columns))})
                """
                cursor.executemany(sql, officials_to_insert)
                conn.commit()
                logging.info(f"Processed {cursor.rowcount} records into the officials table.")
            except sqlite3.Error as e:
                logging.error(f"A database error occurred during officials insertion: {e}")
                conn.rollback()
                raise BuildError(e)

def load_all_static_data(config: Config):
    """Orchestrates the loading of all static CSV files in the correct order."""
    logging.info("--- Starting Static Data Loading Step ---")
    try:
        # The order is important due to foreign key dependencies
        RegistryLoader(config.DB_NAME).load_registry_from_csv(config.REGISTRY_CSV_PATH)
        TeamsLoader(config.DB_NAME).load_teams_from_csv(config.TEAMS_CSV_PATH)
        VenuesLoader(config.DB_NAME).load_venues_from_csv(config.VENUES_CSV_PATH)
        VenueAliasesLoader(config.DB_NAME).load_venue_aliases_from_csv(config.VENUE_ALIASES_CSV_PATH)
        PlayersLoader(config.DB_NAME).load_players_from_csv(config.PLAYERS_CSV_PATH)
        OfficialsLoader(config.DB_NAME).load_officials_from_csv(config.OFFICIALS_CSV_PATH)
        logging.info("--- Static Data Loading Step Complete ---")
    except (FileNotFoundError, ValueError) as e:
        raise BuildError(f"Failed to load static data: {e}")

if __name__ == '__main__':
    logging.info("Running static data loading script directly...")
    cfg = Config()
    load_all_static_data(cfg)
