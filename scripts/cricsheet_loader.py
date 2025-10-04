import os
import json
import sqlite3
import logging
import pandas as pd
from typing import Dict, List, Tuple
from utils import BuildError, db_connection
from cricsheet_extract_transform import MatchesExtractor, MatchPlayersExtractor, DeliveriesExtractor

class BaseLoader:
    """A base class for loaders to share common functionality."""

    def __init__(self, db_name: str):
        self.db_name = db_name

    def _execute_many(self, sql: str, data: List[Tuple], table_name: str):
        if not data:
            logging.warning(f"No data provided to load into '{table_name}'.")
            return
        with db_connection(self.db_name) as conn:
            cursor = conn.cursor()
            try:
                cursor.executemany(sql, data)
                conn.commit()
                logging.info(f"Processed {cursor.rowcount} records into the '{table_name}' table.")
            except sqlite3.Error as e:
                logging.error(f"DB error during bulk insert into '{table_name}': {e}")
                conn.rollback()
                raise BuildError(e)

class MatchesLoader(BaseLoader):
    """Loads data for a single match into the matches table."""

    def load_match(self, df: pd.DataFrame, maps: Dict):
        if df.empty:
            raise BuildError("Empty DataFrame provided to load_match")

        active_team_map = maps['teams_women'] if df.loc[0, 'sex'] == 'female' else maps['teams_men']
        df['team1_id'] = df['team1'].map(active_team_map)
        df['team2_id'] = df['team2'].map(active_team_map)
        df['toss_winner_id'] = df['toss_winner'].map(active_team_map)
        df['winner_id'] = df['winner'].map(active_team_map)
        df['venue'] = df['venue'].str.split(',').str[0].str.strip()
        df['venue_lookup_key'] = df['venue'] + " | " + df['city'].fillna('')
        df['venue_id'] = df['venue_lookup_key'].map(maps['venues'])

        required_id_columns = ['team1_id', 'team2_id', 'venue_id']
        if df[required_id_columns].isnull().values.any():
            if df['team1_id'].isnull().any():
                failed_team = df[df['team1_id'].isnull()]['team1'].iloc[0]
                raise BuildError(
                    f"Mapping failed! Team name '{failed_team}' not found in your teams.csv data.")
            if df['team2_id'].isnull().any():
                failed_team = df[df['team2_id'].isnull()]['team2'].iloc[0]
                raise BuildError(
                    f"Mapping failed! Team name '{failed_team}' not found in your teams.csv data.")
            if df['venue_id'].isnull().any():
                failed_venue = df[df['venue_id'].isnull()]['venue'].iloc[0]
                failed_city = df[df['venue_id'].isnull()]['city'].iloc[0]
                raise BuildError(
                    f"Mapping failed! Venue '{failed_venue}' in city '{failed_city}' not found in your venues.csv data.")

        numeric_cols = ['overs', 'balls_per_over', 'team1_prepostpens', 'team2_prepostpens', 'by_runs',
                        'by_wickets', 'by_other', 'no_result', 'tie', 'super_over', 'bowl_out', 'DLS',
                        'event_match_number']
        existing_numeric_cols = [col for col in numeric_cols if col in df.columns]
        for col in existing_numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
        df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')

        df['start_date'] = df['start_date'].dt.strftime('%Y-%m-%d').where(df['start_date'].notna(), None)
        df['end_date'] = df['end_date'].dt.strftime('%Y-%m-%d').where(df['end_date'].notna(), None)

        db_columns = [
            'match_id', 'match_type', 'match_type_number', 'overs', 'balls_per_over', 'powerplay_starti1',
            'powerplay_endi1', 'powerplay_starti2', 'powerplay_endi2', 'team_type', 'sex', 'start_date',
            'end_date', 'season', 'team1_id', 'team2_id', 'umpire1_id', 'umpire2_id', 'tv_umpire_id',
            'match_referee_id', 'reserve_umpire_id', 'toss_winner_id', 'toss_decision', 'team1_prepostpens',
            'team2_prepostpens', 'winner_id', 'by_runs', 'victory_margin_runs', 'by_wickets',
            'victory_margin_wickets', 'by_other', 'victory_margin_other', 'no_result', 'tie', 'super_over',
            'bowl_out', 'DLS', 'player_of_match_id', 'event_name', 'event_match_number', 'venue_id'
            ]

        def convert_to_native_type(value):
            if pd.isna(value):
                return None
            if hasattr(value, 'item'):
                return value.item()
            return value

        match_tuple = tuple(convert_to_native_type(df.loc[0].get(col)) for col in db_columns)
        sql = f"INSERT INTO matches ({', '.join(db_columns)}) VALUES ({', '.join(['?'] * len(db_columns))})"
        self._execute_many(sql, [match_tuple], 'matches')


class MatchPlayersLoader(BaseLoader):
    """Loads data for the match_players table."""

    def load_players(self, df: pd.DataFrame, maps: Dict):
        active_team_map = maps['teams_women'] if df.loc[0, 'sex'] == 'female' else maps['teams_men']

        df['team_id'] = df['team_name'].map(active_team_map)

        db_columns = ['match_id', 'identifier', 'team_id']
        players_to_insert = [tuple(row.get(col) for col in db_columns) for _, row in df.iterrows()]

        sql = f"INSERT INTO match_players ({', '.join(db_columns)}) VALUES ({', '.join(['?'] * len(db_columns))})"
        self._execute_many(sql, players_to_insert, 'match_players')


class DeliveriesLoader(BaseLoader):
    """Loads ball-by-ball data for a match into the deliveries table."""

    def load_deliveries(self, df: pd.DataFrame, maps: Dict):
        active_team_map = maps['teams_women'] if df.loc[0, 'sex'] == 'female' else maps['teams_men']
        df['review_by_id'] = df['review_by'].map(active_team_map)
        df.drop('sex', axis=1, inplace=True)

        db_columns = [
            'match_id', 'innings', 'overs', 'balls', 'batter_id', 'bowler_id', 'non_striker_id',
            'runs_batter', 'runs_extras', 'runs_total', 'runs_batter_non_boundary', 'wickets',
            'player_out_id', 'how_out', 'fielder1_id', 'fielder2_id', 'fielder3_id', 'wickets2',
            'player_out2_id', 'how_out2', 'extras_byes', 'extras_legbyes', 'extras_noballs',
            'extras_penalty', 'extras_wides', 'review', 'ump_decision', 'review_by_id',
            'review_ump_id', 'review_batter_id', 'review_result', 'umpires_call'
        ]

        numeric_cols = ['innings', 'overs', 'balls', 'runs_batter', 'runs_extras', 'runs_total',
                        'wickets', 'wickets2', 'extras_byes', 'extras_legbyes', 'extras_noballs',
                        'extras_penalty', 'extras_wides']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        df['runs_batter_non_boundary'] = pd.to_numeric(df['runs_batter_non_boundary'], errors='coerce')
        df['review'] = pd.to_numeric(df['review'], errors='coerce')
        df['umpires_call'] = pd.to_numeric(df['umpires_call'], errors='coerce')

        df_for_db = df.where(pd.notna(df), None)

        deliveries_to_insert = [tuple(row.get(col) for col in db_columns) for _, row in df_for_db.iterrows()]

        sql = f"INSERT INTO deliveries ({', '.join(db_columns)}) VALUES ({', '.join(['?'] * len(db_columns))})"
        self._execute_many(sql, deliveries_to_insert, 'deliveries')

def load_all_cricsheet_data(db_name: str, cricsheet_dir: str):
    """
    Orchestrates the loading process for a directory of Cricsheet files.
    """
    logging.info("Starting Cricsheet data loading process...")

    maps = {}
    with db_connection(db_name) as conn:
        maps['teams_men'] = dict(conn.cursor().execute("SELECT nation, team_id FROM teams WHERE sex = 'male'").fetchall())
        maps['teams_women'] = dict(
            conn.cursor().execute("SELECT nation, team_id FROM teams WHERE sex = 'female'").fetchall())
        venue_data = conn.cursor().execute("SELECT alias_name, alias_city, venue_id FROM venue_aliases").fetchall()
        maps['venues'] = {f"{alias_name} | {alias_city or ''}": vid for alias_name, alias_city, vid in venue_data}
    logging.info("Successfully pre-loaded mapping tables from database.")

    matches_ext = MatchesExtractor()
    players_ext = MatchPlayersExtractor()
    deliveries_ext = DeliveriesExtractor()

    matches_loader = MatchesLoader(db_name)
    players_loader = MatchPlayersLoader(db_name)
    deliveries_loader = DeliveriesLoader(db_name)

    json_files = [f for f in os.listdir(cricsheet_dir) if f.endswith('.json')]
    logging.info(f"Found {len(json_files)} JSON files to process.")

    for filename in json_files:
        match_id = os.path.splitext(filename)[0]
        file_path = os.path.join(cricsheet_dir, filename)

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                match_data = json.load(f)

            logging.info(f"--- Processing Match ID: {match_id} ---")

            # 1. Load Match Info
            matches_df = matches_ext.generate_df(match_data, match_id)
            matches_loader.load_match(matches_df, maps)

            # 2. Load Match Players
            players_df = players_ext.generate_df(match_data, match_id)
            players_loader.load_players(players_df, maps)

            # 3. Load Deliveries
            deliveries_df = deliveries_ext.generate_df(match_data, match_id)
            deliveries_loader.load_deliveries(deliveries_df, maps)

        except (json.JSONDecodeError, BuildError) as e:
            logging.error(f"Failed to process {filename}. Error: {e}. Skipping file.")
            continue

    logging.info("All Cricsheet data loading tasks complete.")
