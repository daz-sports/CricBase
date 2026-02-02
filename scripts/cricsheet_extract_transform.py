import pandas as pd
from typing import Dict, Optional
from utils import get_nested_value

class MatchesExtractor:
    """Extracts and transforms the Cricsheet json files for the matches table."""

    def __init__(self):
        self.column_map = {
            'info.match_type': 'match_type',
            'info.balls_per_over': 'balls_per_over',
            'info.team_type': 'team_type',
            'info.gender': 'sex',
            'info.season': 'season',
            'info.teams.0': 'team1',
            'info.teams.1': 'team2',
            'info.toss.winner': 'toss_winner',
            'info.toss.decision': 'toss_decision',
            'info.outcome.by.runs': 'victory_margin_runs',
            'info.outcome.by.wickets': 'victory_margin_wickets',
            'info.event.name': 'event_name',
            'info.event.match_number': 'event_match_number',
            'info.venue': 'venue',
            'info.city': 'city'
        }

    def _extract_basic_info(self, data: Dict, df: pd.DataFrame):
        for json_path, col_name in self.column_map.items():
            df.loc[0, col_name] = get_nested_value(data, json_path)
        dates = get_nested_value(data, 'info.dates', [])
        if dates:
            df.loc[0, 'start_date'] = dates[0]
            df.loc[0, 'end_date'] = dates[-1]
        overs = get_nested_value(data, 'info.overs', [])
        # Cricsheet has some spurious matches with overs > 20 (even though they were actually T20s, e.g. 1430821)
        df.loc[0, 'overs'] = 20 if overs > 20 else overs

    def _extract_official_id(self, data: Dict, official_name: str) -> Optional[str]:
        registry = get_nested_value(data, 'info.registry.people', {})
        return registry.get(official_name)

    def _extract_officials(self, data: Dict, df: pd.DataFrame):
        umpires = get_nested_value(data, 'info.officials.umpires', [])
        if len(umpires) > 0:
            df.loc[0, 'umpire1_id'] = self._extract_official_id(data, umpires[0])
        if len(umpires) > 1:
            df.loc[0, 'umpire2_id'] = self._extract_official_id(data, umpires[1])

        tv_umpire = get_nested_value(data, 'info.officials.tv_umpires', [])
        if tv_umpire:
            df.loc[0, 'tv_umpire_id'] = self._extract_official_id(data, tv_umpire[0])

        referee = get_nested_value(data, 'info.officials.match_referees', [])
        if referee:
            df.loc[0, 'match_referee_id'] = self._extract_official_id(data, referee[0])

        reserve_ump = get_nested_value(data, 'info.officials.reserve_umpires', [])
        if reserve_ump:
            df.loc[0, 'reserve_umpire_id'] = self._extract_official_id(data, reserve_ump[0])

    def _extract_overs(self, data: Dict, df: pd.DataFrame):
        overs = get_nested_value(data, 'info.overs', [])
        df.loc[0, 'overs_count'] = len(overs)

    def _extract_outcome(self, data: Dict, df: pd.DataFrame):
        outcome = get_nested_value(data, 'info.outcome', {})
        df.loc[0, 'winner'] = outcome.get('eliminator') or outcome.get('bowl_out') or outcome.get('winner')
        df.loc[0, 'by_runs'] = 1 if 'runs' in outcome.get('by', {}) else 0
        df.loc[0, 'by_wickets'] = 1 if 'wickets' in outcome.get('by', {}) else 0
        df.loc[0, 'no_result'] = 1 if outcome.get('result') == 'no result' else 0
        df.loc[0, 'tie'] = 1 if outcome.get('result') == 'tie' else 0
        df.loc[0, 'super_over_pld'] = 1 if 'eliminator' in outcome else 0
        df.loc[0, 'bowl_out'] = 1 if 'bowl_out' in outcome else 0
        df.loc[0, 'DLS'] = 1 if outcome.get('method') == 'D/L' else 0
        df.loc[0, 'by_other'] = 1 if outcome.get('method') not in [None, 'D/L'] else 0
        df.loc[0, 'victory_margin_other'] = outcome['method'] if df.loc[0, 'by_other'] == 1 else None

        player_of_match_name = get_nested_value(data, 'info.player_of_match.0')
        if player_of_match_name:
            df.loc[0, 'player_of_match_id'] = self._extract_official_id(data, player_of_match_name)

    def _extract_other(self, data: Dict, df: pd.DataFrame):
        team1_name = get_nested_value(data, 'info.teams.0')

        team1_pens = 0
        team2_pens = 0

        innings1_team = get_nested_value(data, 'innings.0.team')
        if innings1_team:
            pre_pens = get_nested_value(data, 'innings.0.penalty_runs.pre', 0)
            post_pens = get_nested_value(data, 'innings.0.penalty_runs.post', 0)
            if innings1_team == team1_name:
                team1_pens += pre_pens + post_pens
            else:
                team2_pens += pre_pens + post_pens

        innings2_team = get_nested_value(data, 'innings.1.team')
        if innings2_team:
            pre_pens = get_nested_value(data, 'innings.1.penalty_runs.pre', 0)
            post_pens = get_nested_value(data, 'innings.1.penalty_runs.post', 0)
            if innings2_team == team1_name:
                team1_pens += pre_pens + post_pens
            else:
                team2_pens += pre_pens + post_pens

        df.loc[0, 'team1_prepostpens'] = team1_pens
        df.loc[0, 'team2_prepostpens'] = team2_pens
        df.loc[0, 'powerplay_starti1'] = get_nested_value(data, 'innings.0.powerplays.0.from')
        df.loc[0, 'powerplay_endi1'] = get_nested_value(data, 'innings.0.powerplays.0.to')
        df.loc[0, 'powerplay_starti2'] = get_nested_value(data, 'innings.1.powerplays.0.from')
        df.loc[0, 'powerplay_endi2'] = get_nested_value(data, 'innings.1.powerplays.0.to')

    def generate_df(self, match_data: Dict, match_id: str) -> pd.DataFrame:
        df = pd.DataFrame(index=[0])
        df.loc[0, 'match_id'] = match_id
        self._extract_basic_info(match_data, df)
        self._extract_officials(match_data, df)
        self._extract_outcome(match_data, df)
        self._extract_other(match_data, df)
        return df

class MetadataExtractor:
    """Extracts and transforms the Cricsheet json files for the metadata table."""

    def __init__(self):
        self.column_map = {
            'meta.data_version': 'data_version',
            'meta.created': 'cricsheet_created',
            'meta.revision': 'revision'
        }

    def generate_df(self, match_data: Dict, match_id: str) -> pd.DataFrame:
        df = pd.DataFrame(index=[0])
        df.loc[0, 'match_id'] = match_id
        for json_path, col_name in self.column_map.items():
            df.loc[0, col_name] = get_nested_value(match_data, json_path)
        return df

class MatchPlayersExtractor:
    """Extracts and transforms the Cricsheet json files for the match_players table."""

    def generate_df(self, match_data: Dict, match_id: str) -> pd.DataFrame:
        players_data = []
        teams = get_nested_value(match_data, 'info.teams', [])
        sex = get_nested_value(match_data, 'info.gender')

        for team_name in teams:
            player_names = get_nested_value(match_data, f"info.players.{team_name}", [])
            for player_name in player_names:
                player_id = get_nested_value(match_data, f"info.registry.people.{player_name}")
                if player_id:
                    players_data.append({
                        'match_id': match_id,
                        'identifier': player_id,
                        'team_name': team_name,
                        'sex': sex
                    })
        return pd.DataFrame(players_data)

class DeliveriesExtractor:
    """Extracts and transforms the Cricsheet json files for the deliveries table."""

    def _get_fielder_id(self, fielder_data: Dict, registry: Dict) -> Optional[str]:
        """Determines if the fielder is a substitute or a participating player."""
        if get_nested_value(fielder_data, 'substitute'):
            return 'substitute'
        fielder_name = get_nested_value(fielder_data, 'name')
        return registry.get(fielder_name) if fielder_name else None

    def _extract_review_info(self, review: Dict, delivery: Dict, registry: Dict, batting_team: str) -> Dict:
        """Helper to process the review logic."""
        review_info = {}
        umpire_name = review.get('umpire')
        if umpire_name: review_info['review_ump_id'] = registry.get(umpire_name)

        review_by_batting_team = review.get('by') == batting_team
        review_info['review_by'] = review.get('by')
        review_info['ump_decision'] = 'out' if review_by_batting_team else 'not out'

        if review.get('decision') == 'struck down': # Cricsheet 'struck down' means unsuccessful review
            review_info['review_result'] = review_info['ump_decision']
        else:
            review_info['review_result'] = 'not out' if review_by_batting_team else 'out'

        if review_by_batting_team:
            review_info['review_batter_id'] = registry.get(delivery.get('batter'))

        review_info['umpires_call'] = 1 if review.get('umpires_call') else 0
        return review_info

    def _powerplay(self, inning: Dict, innings_idx: int, match_id: str) -> tuple:
        """Helper to determine if the delivery is a powerplay."""
        if innings_idx >= 2:
            return 0, 0

        raw_val = get_nested_value(inning, 'powerplays.0.to')
        if raw_val is None:
            raise KeyError(f"Powerplay data missing for Inning {innings_idx + 1} in match {match_id}")

        pp_end_val = str(raw_val)
        if '.' not in pp_end_val:
            raise ValueError(f"Invalid PP format '{pp_end_val}' in match {match_id}")

        pp_end_over, pp_end_ball = map(int, pp_end_val.split('.'))
        return pp_end_over, pp_end_ball

    def generate_df(self, match_data: Dict, match_id: str) -> pd.DataFrame:
        deliveries_list = []
        registry = get_nested_value(match_data, 'info.registry.people', {})

        for i, inning in enumerate(get_nested_value(match_data, 'innings', [])):
            batting_team = inning.get('team')

            # Powerplay start/end, done by innings for efficiency
            pp_end_over, pp_end_ball = self._powerplay(inning, i, match_id)

            for j, over in enumerate(get_nested_value(inning, 'overs', [])):
                for k, delivery in enumerate(get_nested_value(over, 'deliveries', [])):
                    runs_batter = get_nested_value(delivery, 'runs.batter', 0)
                    if runs_batter in [4, 6]:
                        runs_non_boundary_value = 1 if get_nested_value(delivery, 'runs.non_boundary') else 0
                    else:
                        runs_non_boundary_value = None
                    delivery_dict = {
                        'match_id': match_id, 'innings': i + 1, 'overs': j + 1, 'balls': k + 1,
                        'batter_id': registry.get(delivery.get('batter')),
                        'bowler_id': registry.get(delivery.get('bowler')),
                        'non_striker_id': registry.get(delivery.get('non_striker')),
                        'runs_batter': runs_batter,
                        'runs_extras': get_nested_value(delivery, 'runs.extras', 0),
                        'runs_total': get_nested_value(delivery, 'runs.total', 0),
                        'runs_batter_non_boundary': runs_non_boundary_value,
                        'wickets': 0,
                        'player_out': None,
                        'how_out': None,
                        'fielder1_id': None,
                        'fielder2_id': None,
                        'fielder3_id': None,
                        'fielder_missing': 0,
                        'wickets2': 0,
                        'player_out2': None,
                        'how_out2': None,
                        'extras_byes': get_nested_value(delivery, 'extras.byes', 0),
                        'extras_legbyes': get_nested_value(delivery, 'extras.legbyes', 0),
                        'extras_noballs': get_nested_value(delivery, 'extras.noballs', 0),
                        'extras_penalty': get_nested_value(delivery, 'extras.penalty', 0),
                        'extras_wides': get_nested_value(delivery, 'extras.wides', 0),
                        'review': 0,
                        'ump_decision': None,
                        'review_by': None,
                        'review_ump': None,
                        'review_batter': None,
                        'review_result': None,
                        'umpires_call': None,
                        'powerplay': 1 if (j < pp_end_over or (j == pp_end_over and k + 1 <= pp_end_ball)) else 0,
                        'super_over': 1 if i >= 2 else 0,
                        'sex': get_nested_value(match_data, 'info.gender')
                    }

                    # Wicket 1
                    wicket1 = get_nested_value(delivery, 'wickets.0')
                    if wicket1:
                        delivery_dict['wickets'] = 1
                        delivery_dict['player_out_id'] = registry.get(wicket1.get('player_out'))
                        delivery_dict['how_out'] = wicket1.get('kind')
                        fielders = get_nested_value(wicket1, 'fielders', [])
                        if len(fielders) > 0: delivery_dict['fielder1_id'] = self._get_fielder_id(fielders[0], registry)
                        if delivery_dict['how_out'] == 'caught and bowled' and delivery_dict['fielder1_id'] is None:
                            delivery_dict['fielder1_id'] = delivery_dict['bowler_id']
                        if len(fielders) > 1: delivery_dict['fielder2_id'] = self._get_fielder_id(fielders[1], registry)
                        if len(fielders) > 2: delivery_dict['fielder3_id'] = self._get_fielder_id(fielders[2], registry)
                        if delivery_dict['how_out'] in ('caught', 'caught and bowled', 'stumped', 'run out') and \
                                delivery_dict['fielder1_id'] is None:
                            delivery_dict['fielder_missing'] = 1
                    else:
                        delivery_dict['wickets'] = 0

                    # Wicket 2 (rare, e.g., retired out and run out non-striker)
                    wicket2 = get_nested_value(delivery, 'wickets.1')
                    if wicket2:
                        delivery_dict['wickets2'] = 1
                        delivery_dict['player_out2_id'] = registry.get(wicket2.get('player_out'))
                        delivery_dict['how_out2'] = wicket2.get('kind')
                    else:
                        delivery_dict['wickets2'] = 0

                    # Reviews
                    review = get_nested_value(delivery, 'review')
                    if review:
                        delivery_dict['review'] = 1
                        review_details = self._extract_review_info(review, delivery, registry, batting_team)
                        delivery_dict.update(review_details)
                    else:
                        delivery_dict['review'] = 0

                    deliveries_list.append(delivery_dict)

        return pd.DataFrame(deliveries_list)
