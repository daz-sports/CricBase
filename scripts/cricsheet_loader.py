import os
import json
import sqlite3
import logging
import pandas as pd
from typing import Dict, List, Tuple
from utils import BuildError, db_connection, open_icc_url, get_files_to_process
from cricsheet_extract_transform import MatchesExtractor, MetadataExtractor, MatchPlayersExtractor, DeliveriesExtractor
from scraper import ICCScraper
from interactive_utils import InputManager

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

    def __init__(self, db_name: str, config):
        super().__init__(db_name)
        self.input_manager = InputManager(db_name, config)

    def load_match(self, df: pd.DataFrame, maps: Dict):
        if df.empty:
            raise BuildError("Empty DataFrame provided to load_match")

        sex = str(df.loc[0, 'sex'])
        active_team_map = maps['teams_women'] if sex == 'female' else maps['teams_men']

        for col in ['team1', 'team2']:
            team_name = df.loc[0, col]
            if team_name not in active_team_map:
                new_id = self.input_manager.resolve_missing_team(team_name, sex)
                active_team_map[team_name] = new_id

            df.loc[0, f'{col}_id'] = active_team_map[team_name]

        raw_venue = df.loc[0, 'venue'].split(',')[0].strip()
        city = df.loc[0, 'city']
        lookup_key = f"{raw_venue} | {city or ''}"

        if lookup_key not in maps['venues']:
            venue_id = self.input_manager.resolve_missing_venue(raw_venue, city)
            maps['venues'][lookup_key] = venue_id

        df.loc[0, 'venue_id'] = maps['venues'][lookup_key]

        df['toss_winner_id'] = df['toss_winner'].map(active_team_map)
        df['winner_id'] = df['winner'].map(active_team_map)

        for col in ['umpire1_id', 'umpire2_id', 'tv_umpire_id', 'match_referee_id', 'reserve_umpire_id']:
            if col in df.columns:
                self.input_manager.verifying_official(df.loc[0, col])

        numeric_cols = ['overs', 'balls_per_over', 'team1_prepostpens', 'team2_prepostpens', 'by_runs',
                        'by_wickets', 'by_other', 'no_result', 'tie', 'super_over_pld', 'bowl_out', 'DLS',
                        'event_match_number']
        existing_numeric_cols = [col for col in numeric_cols if col in df.columns]
        for col in existing_numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
        df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')

        df['start_date'] = df['start_date'].dt.strftime('%Y-%m-%d').where(df['start_date'].notna(), None)
        df['end_date'] = df['end_date'].dt.strftime('%Y-%m-%d').where(df['end_date'].notna(), None)

        db_columns = [
            'match_id', 'match_type', 'overs', 'balls_per_over', 'powerplay_starti1',
            'powerplay_endi1', 'powerplay_starti2', 'powerplay_endi2', 'team_type', 'sex', 'start_date',
            'end_date', 'season', 'team1_id', 'team2_id', 'umpire1_id', 'umpire2_id', 'tv_umpire_id',
            'match_referee_id', 'reserve_umpire_id', 'toss_winner_id', 'toss_decision', 'team1_prepostpens',
            'team2_prepostpens', 'winner_id', 'by_runs', 'victory_margin_runs', 'by_wickets',
            'victory_margin_wickets', 'by_other', 'victory_margin_other', 'no_result', 'tie', 'super_over_pld',
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

class MetadataLoader(BaseLoader):
    """Loads data for the metadata table."""

    def load_metadata(self, df: pd.DataFrame):
        db_columns = ['match_id', 'data_version', 'cricsheet_created', 'revision']
        md_to_insert = [tuple(row.get(col) for col in db_columns) for _, row in df.iterrows()]

        sql = f"INSERT INTO match_metadata ({', '.join(db_columns)}) VALUES ({', '.join(['?'] * len(db_columns))})"
        self._execute_many(sql, md_to_insert, 'match_metadata')

class MatchPlayersLoader(BaseLoader):
    """Loads data for the match_players table."""

    def __init__(self, db_name: str, config):
        super().__init__(db_name)
        self.input_manager = InputManager(db_name, config)

    def load_players(self, df: pd.DataFrame, maps: Dict):

        if df.empty:
            logging.info("No players recorded for this match. Skipping player load.")
            return

        for identifier in df['identifier'].tolist():
            self.input_manager.verifying_player(identifier)

        active_team_map = maps['teams_women'] if df.loc[0, 'sex'] == 'female' else maps['teams_men']

        df['team_id'] = df['team_name'].map(active_team_map)

        db_columns = ['match_id', 'identifier', 'team_id']
        players_to_insert = [tuple(row.get(col) for col in db_columns) for _, row in df.iterrows()]

        sql = f"INSERT INTO match_players ({', '.join(db_columns)}) VALUES ({', '.join(['?'] * len(db_columns))})"
        self._execute_many(sql, players_to_insert, 'match_players')


class DeliveriesLoader(BaseLoader):
    """Loads ball-by-ball data for a match into the deliveries table."""

    def load_deliveries(self, df: pd.DataFrame, maps: Dict):

        if df.empty:
            logging.info("No deliveries recorded for this match. Skipping delivery load.")
            return

        active_team_map = maps['teams_women'] if df.loc[0, 'sex'] == 'female' else maps['teams_men']
        df['review_by_id'] = df['review_by'].map(active_team_map)
        df.drop('sex', axis=1, inplace=True)

        db_columns = [
            'match_id', 'innings', 'overs', 'balls', 'batter_id', 'bowler_id', 'non_striker_id',
            'runs_batter', 'runs_extras', 'runs_total', 'runs_batter_non_boundary', 'wickets',
            'player_out_id', 'how_out', 'fielder1_id', 'fielder2_id', 'fielder3_id', 'wickets2',
            'player_out2_id', 'how_out2', 'extras_byes', 'extras_legbyes', 'extras_noballs',
            'extras_penalty', 'extras_wides', 'review', 'ump_decision', 'review_by_id',
            'review_ump_id', 'review_batter_id', 'review_result', 'umpires_call', 'powerplay',
            'super_over'
        ]

        numeric_cols = ['innings', 'overs', 'balls', 'runs_batter', 'runs_extras', 'runs_total',
                        'wickets', 'wickets2', 'extras_byes', 'extras_legbyes', 'extras_noballs',
                        'extras_penalty', 'extras_wides']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        df['runs_batter_non_boundary'] = pd.to_numeric(df['runs_batter_non_boundary'], errors='coerce')
        df['review'] = pd.to_numeric(df['review'], errors='coerce')
        df['umpires_call'] = pd.to_numeric(df['umpires_call'], errors='coerce')
        df['powerplay'] = pd.to_numeric(df['powerplay'], errors='coerce')
        df['super_over'] = pd.to_numeric(df['super_over'], errors='coerce')

        df_for_db = df.where(pd.notna(df), None)

        deliveries_to_insert = [tuple(row.get(col) for col in db_columns) for _, row in df_for_db.iterrows()]

        sql = f"INSERT INTO deliveries ({', '.join(db_columns)}) VALUES ({', '.join(['?'] * len(db_columns))})"
        self._execute_many(sql, deliveries_to_insert, 'deliveries')

class MissingMatchesLoader(BaseLoader):
    """Loads missing matches into the database."""

    def __init__(self, db_name: str, user_agent: str):
        super().__init__(db_name)
        self.scraper = ICCScraper(user_agent)

    def update_missing_matches(self, start_y: int, start_m: int, end_y: int, end_m: int):
        logging.info("--- Starting Missing Matches Check ---")

        icc_df = self.scraper.scrape_period(start_y, start_m, end_y, end_m)
        logging.info(f"Total ICC Matches Scraped: {len(icc_df)}")

        if icc_df.empty:
            logging.warning("No data found from ICC. Skipping comparison.")
            return

        logging.info("Checking for missing venue_nation...")

        icc_df = self._handle_icc_duplicates(icc_df)
        icc_df['match_teams_key'] = icc_df.apply(lambda x: tuple(sorted([str(x['team1']), str(x['team2'])])), axis=1)

        db_df = self._fetch_existing_matches()
        logging.info(f"Total Matches in DB: {len(db_df)}")

        missing_matches = self._identify_missing_matches(icc_df, db_df)

        if missing_matches.empty:
            logging.info("No missing matches found. Database is fully up-to-date with the ICC.")
            return

        self._insert_missing_matches(missing_matches)

    def _handle_icc_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handles multi-month game duplication due to the ICC use of IST."""

        if not df.empty and 'icc_id' in df.columns:
            duplicated_mask = df.duplicated(subset=['icc_id'], keep=False)
            duplicates = df[duplicated_mask].sort_values(by='icc_id')

            if not duplicates.empty:
                logging.warning(f"Found {len(duplicates)} rows with duplicate ICC IDs due to month crossover (Gibraltar vs Serbia on 2024-09-30 should appear).")
                for icc_id, group in duplicates.groupby('icc_id'):
                    logging.info(
                        f"   [DUPLICATE DETECTED] ID: {icc_id} | {group.iloc[0]['team1']} vs {group.iloc[0]['team2']} on {group.iloc[0]['start_date']}")
                    for _, row in group.iterrows():
                        logging.info(
                            f"      Row detail: Month Scraped context - Result: {row['match_result']} | Toss result: {row['toss_result']} | Venue: {row['venue_name']}")
                        open_icc_url(row)
                    logging.warning(f"   >> Action: Dropping extra instance of ICC ID {icc_id}")

                return df.drop_duplicates(subset=['icc_id'], keep='first')
        return df

    def _fetch_existing_matches(self) -> pd.DataFrame:
        """Retrieves existing matches from the database."""

        with db_connection(self.db_name) as conn:
            query = "SELECT start_date, team1, team2, toss_result, match_result, venue_nation FROM match_summary"
            df = pd.read_sql(query, conn)

        if not df.empty:
            df['match_teams_key'] = df.apply(lambda x: tuple(sorted([str(x['team1']), str(x['team2'])])), axis=1)
        return df

    def _identify_missing_matches(self, icc_df: pd.DataFrame, db_df: pd.DataFrame) -> pd.DataFrame:
        """Compares ICC data against DB data and performs diagnostic logging."""

        if db_df.empty:
            logging.info("DB is empty. All scraped matches are considered missing.")
            return icc_df.drop(columns=['match_teams_key'])

        join_keys = ['start_date', 'match_teams_key', 'match_result', 'toss_result', 'venue_nation']

        merged_diag = pd.merge(icc_df, db_df, on=join_keys, how='right', indicator=True)
        unmatched_db = merged_diag[merged_diag['_merge'] == 'right_only'].copy()

        if not unmatched_db.empty:
            self._log_diagnostics(unmatched_db, icc_df)

        merged = pd.merge(icc_df, db_df[join_keys], how='left', on=join_keys, indicator=True)

        exact_matches = merged[merged['_merge'] == 'both']
        logging.info(f"Exact Matches Found (Present in both ICC & DB): {len(exact_matches)}")

        missing_matches = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])

        self._check_nation_mismatches(missing_matches, db_df)

        return missing_matches.drop(columns=['match_teams_key'])

    def _log_diagnostics(self, unmatched_db: pd.DataFrame, icc_df: pd.DataFrame):
        """Runs the 3 specific mismatch tests for logging purposes."""

        logging.info(f"DIAGNOSTIC: Analyzing {len(unmatched_db)} DB matches that failed exact match...")

        # Test 1: Result Mismatch
        res_mismatch = pd.merge(unmatched_db, icc_df, on=['start_date', 'match_teams_key', 'venue_nation'],
                                suffixes=('_db', '_icc'))
        for _, row in res_mismatch.iterrows():
            logging.warning(
                f"   [RESULT MISMATCH]: {row['match_teams_key']} on {row['start_date']}. DB Result: '{row['match_result_db']}' | ICC Result: '{row['match_result_icc']}'")

        # Test 2: Date Mismatch
        date_mismatch = pd.merge(unmatched_db, icc_df, on=['match_teams_key', 'match_result'], suffixes=('_db', '_icc'))
        for _, row in date_mismatch.iterrows():
            if row['start_date_db'] != row['start_date_icc']:
                logging.warning(
                    f"   [DATE MISMATCH]: {row['match_teams_key']}. DB Date: {row['start_date_db']} | ICC Date: {row['start_date_icc']}")

        # Test 3: Toss Mismatch
        toss_mismatch = pd.merge(unmatched_db, icc_df, on=['start_date', 'match_teams_key', 'match_result'],
                                 suffixes=('_db', '_icc'))
        for _, row in toss_mismatch.iterrows():
            if row['toss_result_db'] != row['toss_result_icc']:
                logging.warning(
                    f"   [TOSS MISMATCH]: {row['match_teams_key']} on {row['start_date']}. DB Toss: '{row['toss_result_db']}' | ICC Toss: '{row['toss_result_icc']}'")

    def _check_nation_mismatches(self, missing_matches: pd.DataFrame, db_df: pd.DataFrame):
        """Test 4: Specifically logs matches where only the Venue Nation differs."""
        partial_check_keys = ['start_date', 'match_teams_key', 'match_result', 'toss_result']
        partial_merged = pd.merge(missing_matches, db_df, how='inner', on=partial_check_keys, suffixes=('_icc', '_db'))

        if not partial_merged.empty:
            logging.warning(
                f"Found {len(partial_merged)} matches with matching Date/Teams/Result/Toss but DIFFERENT Venue Nation.")
            for _, row in partial_merged.iterrows():
                logging.warning(
                    f"   >> Mismatch: {row['team1_icc']} vs {row['team2_icc']} on {row['start_date']}. ICC Nation: '{row['venue_nation_icc']}' | DB Nation: '{row['venue_nation_db']}'")

    def _insert_missing_matches(self, missing_matches: pd.DataFrame):
        """Final DB insertion logic."""
        logging.info(f"Found {len(missing_matches)} truly missing matches. Inserting into database...")
        db_columns = ['icc_id', 'start_date', 'team1', 'team2', 'venue_name', 'city', 'venue_nation', 'match_result',
                      'toss_result']
        data_to_insert = [tuple(row) for row in missing_matches[db_columns].itertuples(index=False)]

        sql = f"INSERT INTO missing_matches ({', '.join(db_columns)}) VALUES ({', '.join(['?'] * len(db_columns))})"
        self._execute_many(sql, data_to_insert, 'missing_matches')

def load_all_cricsheet_data(config, db_name: str, cricsheet_dir: str, additional_dir: str, start_y: int, start_m: int, end_y: int, end_m: int, user_agent: str):
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
    md_ext = MetadataExtractor()
    players_ext = MatchPlayersExtractor()
    deliveries_ext = DeliveriesExtractor()

    matches_loader = MatchesLoader(db_name, config)
    metadata_loader = MetadataLoader(db_name)
    players_loader = MatchPlayersLoader(db_name)
    deliveries_loader = DeliveriesLoader(db_name)

    all_files = []
    for directory in [cricsheet_dir, additional_dir]:
        if os.path.exists(directory):
            all_files.extend([(f, os.path.join(directory, f))
                              for f in os.listdir(directory) if f.endswith('.json')])

    logging.info(f"Found {len(all_files)} total JSON files in directories.")

    all_files = get_files_to_process(db_name, all_files)

    if not all_files:
        logging.info("No new matches to process.")
    else:
        for filename, file_path in all_files:
            match_id = os.path.splitext(filename)[0]

            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    match_data = json.load(f)

                logging.info(f"--- Processing Match ID: {match_id} ---")

                # 1. Load Match Info
                matches_df = matches_ext.generate_df(match_data, match_id)
                matches_loader.load_match(matches_df, maps)

                # 2. Load Match Metadata
                metadata_df = md_ext.generate_df(match_data, match_id)
                metadata_loader.load_metadata(metadata_df)

                # 3. Load Match Players
                players_df = players_ext.generate_df(match_data, match_id)
                players_loader.load_players(players_df, maps)

                # 4. Load Deliveries
                deliveries_df = deliveries_ext.generate_df(match_data, match_id)
                deliveries_loader.load_deliveries(deliveries_df, maps)

            except (json.JSONDecodeError, BuildError) as e:
                logging.error(f"Failed to process {filename}. Error: {e}. Skipping file.")
                continue

        logging.info("All Cricsheet data loading tasks complete.")

        # Run the Missing Matches Check
        missing_loader = MissingMatchesLoader(db_name, user_agent)
        missing_loader.update_missing_matches(start_y, start_m, end_y, end_m)