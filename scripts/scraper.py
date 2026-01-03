import requests
import pandas as pd
import calendar
import logging
import time
import random
from typing import Dict, List, Tuple

class ICCScraper:
    """Handles scraping the ICC website for T20I matches."""

    def __init__(self, user_agent: str = None):
        self.team_map = {}
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": user_agent
        })

    def _create_months_list(self, start_year, start_month, end_year, end_month) -> List[Tuple[int, int]]:
        months = []
        curr_year, curr_month = start_year, start_month
        while (curr_year * 12 + curr_month) <= (end_year * 12 + end_month):
            months.append((curr_year, curr_month))
            if curr_month == 12:
                curr_month = 1
                curr_year += 1
            else:
                curr_month += 1
        return months

    def scrape_period(self, start_year: int, start_month: int, end_year: int, end_month: int) -> pd.DataFrame:
        year_months = self._create_months_list(start_year, start_month, end_year, end_month)
        all_data = []

        logging.info(f"Starting ICC scrape: {start_year}-{start_month} to {end_year}-{end_month}")

        for y, m in year_months:
            first_day, last_day = self._get_month_range(y, m)
            try:
                raw_df = self._fetch_json_as_df(first_day, last_day)
                if not raw_df.empty:
                    all_data.append(self._transform_data(raw_df))
                    logging.info(f"Fetched {len(all_data[-1])} matches for {y}-{m:02d}")
            except Exception as e:
                logging.error(f"Error scraping {y}-{m:02d}: {e}")

        return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

    def _get_month_range(self, year: int, month: int) -> Tuple[str, str]:
        first = f"{year}{month:02d}01"
        _, last_num = calendar.monthrange(year, month)
        return first, f"{year}{month:02d}{last_num}"

    def _fetch_json_as_df(self, from_date: str, to_date: str) -> pd.DataFrame:
        url = (f"https://assets-icc.sportz.io/cricket/v1/schedule?client_id=tPZJbRgIub3Vua93%2FDWtyQ%3D%3D"
               f"&feed_format=json&lang=en&from_date={from_date}&to_date={to_date}"
               f"&is_deleted=false&pagination=true&page_number=1&page_size=400"
               f"&is_upcoming=false&is_live=false&is_recent=true")

        response = self.session.get(url, timeout=15)

        # Ethical delay:
        time.sleep(random.uniform(1.0, 3.0))
        data = response.json()
        return pd.DataFrame(data.get('data', {}).get('matches', []))

    def _clean_venue_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enrich and normalize venue information."""
        place_map = {
            'Bridgetown': 'Barbados', 'Gros Islet': 'Saint Lucia', 'Kingston': 'Jamaica',
            'Port Of Spain': 'Trinidad and Tobago', 'Tarouba': 'Trinidad and Tobago',
            'North Sound': 'Antigua and Barbuda', 'Coolidge': 'Antigua and Barbuda',
            'Kingstown': 'Saint Vincent and the Grenadines', 'Providence': 'Guyana',
            'Guyana': 'Guyana', 'Saint Peters': 'Antigua and Barbuda', 'Cardiff': 'Wales',
            'Episkopi': 'Cyprus', 'Oslo': 'Norway'
        }

        # Cleanup column names and strings
        df.rename(columns={'country': 'venue_nation', 'match_id': 'icc_id'}, inplace=True)
        df['city'] = df['city'].str.title().str.strip()
        df['venue_nation'] = df['venue_nation'].str.strip().replace({'USA': 'United States of America'})

        # Map West Indies cities
        wi_mask = df['venue_nation'] == 'West Indies'
        df.loc[wi_mask, 'venue_nation'] = df.loc[wi_mask, 'city'].map(place_map).fillna('West Indies')

        # General city overrides (Wales, etc)
        df.loc[df['city'].isin(place_map), 'venue_nation'] = df['city'].map(place_map).fillna(df['venue_nation'])

        return df

    def _transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Structural transformation of the raw API response."""
        # Filter for T20Is (3) and Women's T20Is (13)
        df = df[df['comp_type_id'].isin(['3', '13'])].copy()

        # Handle Dates
        df['start_date'] = pd.to_datetime(df['match_date_local']).dt.strftime('%Y-%m-%d')
        df['sex'] = df['comp_type'].str.contains('w', case=False).map({True: 'female', False: 'male'})

        # Map Teams
        self.team_map = self._build_team_map(df)
        df['team1'] = df['teama_id'].map(self.team_map)
        df['team2'] = df['teamb_id'].map(self.team_map)

        # Parse Venues
        venue_parts = df['venue'].str.split(',', n=1, expand=True)
        df['venue_name'] = venue_parts[0].str.strip()
        df['city'] = venue_parts[1].str.strip().fillna('')

        df = self._clean_venue_data(df)

        # Generate Results
        df['toss_result'] = df.apply(lambda r: self._generate_toss_result(r), axis=1)
        df['match_result'] = df.apply(lambda r: self._generate_match_result(r), axis=1)
        df['match_result'] = df['match_result'].replace('Match Abandoned', 'No Result')

        return df[['icc_id', 'start_date', 'team1', 'team2', 'toss_result', 'match_result', 'venue_name', 'city',
                   'venue_nation']]

    def _build_team_map(self, df: pd.DataFrame) -> Dict:
        suffix = {'male': ' Men', 'female': ' Women'}
        a = df[['teama_id', 'teama', 'sex']].rename(columns={'teama_id': 'id', 'teama': 'n'})
        b = df[['teamb_id', 'teamb', 'sex']].rename(columns={'teamb_id': 'id', 'teamb': 'n'})
        combined = pd.concat([a, b]).drop_duplicates('id')
        return combined.set_index('id').apply(lambda x: x['n'] + suffix.get(x['sex'], ''), axis=1).to_dict()

    def _generate_toss_result(self, row: pd.Series) -> str:
        winner = self.team_map.get(row['toss_won_by'])
        if not winner: return "Toss Info Missing/No Toss"
        decision = 'bat' if 'bat' in str(row['toss_elected_to']).lower() else 'field'
        return f"{winner} won the toss and chose to {decision}"

    def _generate_match_result(self, row: pd.Series) -> str:
        if row['match_status'] in ['No Result', 'Abandoned']: return "No Result"

        winner = self.team_map.get(row['winning_team_id'])
        if row['match_result'] == 'Tie' or 'super over' in str(row['match_result']).lower():
            return f"Tie ({winner} won the Super Over)" if winner else "Match Tied"

        margin = str(row['winning_margin']).replace('(DLS method)', '').replace('(D/L method)', '').strip()
        if winner and margin:
            is_dls = 'DLS' in row['match_result'] or 'D/L' in row['match_result']
            suffix = (" (DLS method)" if row['start_date'] >= '2014-11-01' else " (D/L method)") if is_dls else ""
            return f"{winner} won by {margin}{suffix}"

        return str(row['match_result'])

