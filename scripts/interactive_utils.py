from datetime import datetime
import logging
import sqlite3
import urllib.parse
import webbrowser
import requests
import country_converter as coco
import reverse_geocoder as rg
import numpy as np
import xarray as xr
import time
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from geopy.distance import geodesic
from typing import Optional, Dict, Any, Tuple
from utils import db_connection
from config import Config


class InputManager:
    """
        Handles interactive user prompts for missing data.
    """

    def __init__(self, db_name: str, config: Config):
        self.db_name = db_name
        self.geolocator = Nominatim(user_agent=config.USER_AGENT)
        self.dist2sea_path = config.NASA_DIST2COAST_PATH
        self.user_agent = config.USER_AGENT
        self.session = requests.Session()
        self.cc = coco.CountryConverter()

    def _get_input(self, prompt_text: str, required: bool = True, default: str = None) -> Optional[str]:
        """Helper to get and validate CLI input."""

        while True:
            full_prompt = f"{prompt_text}"
            if default:
                full_prompt += f" [{default}]"
            full_prompt += ": "

            value = input(full_prompt).strip()

            if not value and default:
                return default

            if required and not value:
                print("  ! This field is required.")
                continue

            return value if value else None

    def _check_valid_date(self, date_str) -> bool:
        """Parses a date pair (start/end) from a string."""
        if date_str is None: return True
        try:
            datetime.strptime(date_str, '%Y-%m-%d').date()
            return True
        except ValueError:
            return False

    def _confirm_entry(self, details: Dict) -> bool:
        """Shows the user what they entered and asks for confirmation."""

        print("\n  --- Review Entry ---")
        for k, v in details.items():
            if isinstance(v, float):
                val_str = f"{v:.4f}"
            else:
                val_str = v
            print(f"  {k}: {val_str}")

        choice = input("\n  Save this entry? (y/n/retry): ").lower()
        return choice == 'y'

    def _get_lat_long(self, venue: str, city: str, nation: str) -> Tuple[Optional[float], Optional[float]]:
        """
        Interactive loop to determine lat/long coordinates for a venue using Nominatim or manual input.
        """
        query_text = f"{venue}, {city}, {nation}"
        print(f"\n [GIS] Searching coordinates for: {query_text}...")

        lat, lon = None, None
        api_match = False

        try:
            location = self.geolocator.geocode(query_text, timeout=10)
            if location:
                print(f" > API Found: {location.address}")
                lat, lon = location.latitude, location.longitude
                api_match = True
                url = f"https://www.google.com/maps/search/?api=1&query={lat},{lon}"
            else:
                print(" > API Failed. No direct match found.")
                url = f"https://www.google.com/maps/search/?api=1&query={urllib.parse.quote(query_text)}"

        except (GeocoderTimedOut, GeocoderServiceError) as e:
            logging.error(f"Geocoding API error: {e}")
            print(" > API Error. Opening manual search.")
            url = f"https://www.google.com/maps/search/?api=1&query={urllib.parse.quote(query_text)}"

        print(f" > Verifying on Google Maps: {url}")
        webbrowser.open(url)

        print("\n Coordinate Options:")
        if api_match:
            print(f" [ENTER] Accept API Result ({lat}, {lon})")
        print(" [lat, long] Type coordinates manually to override")
        print(" [skip] Leave coordinates blank")

        while True:
            user_input = input("  >> ").strip().lower()

            if not user_input and api_match:
                logging.info("  [GIS] User accepted API coordinates.")
                return lat, lon

            if user_input == 'skip' or (not user_input and not api_match):
                logging.warning(f"  [GIS] Coordinates skipped for {venue}.")
                return None, None

            if ',' in user_input:
                try:
                    parts = user_input.split(',')
                    manual_lat = float(parts[0].strip())
                    manual_lon = float(parts[1].strip())
                    logging.info("  [GIS] User provided manual coordinates.")
                    return manual_lat, manual_lon
                except ValueError:
                    print("  [!] Invalid format. Example: 51.380338, -2.353638")
                    continue

            print("  [!] Invalid input.")

    def _get_gis_data(self, lat: float, lon: float) -> Dict[str, Any]:
        """
        Automated gis based on Lat/Long (Continent, Timezone, Elevation, Administrative Divisions, Distance from Coast).
        """
        if lat is None or lon is None: return {}

        gis = {}

        gis['hemisphere'] = 'N' if lat >= 0 else 'S'

        try:
            res = rg.search((lat, lon), verbose=False)[0]
            gis['continent'] = self.cc.convert(res['cc'], to='continent')
            gis['admin_area_1'] = res.get('admin1', 'Unknown')
            gis['admin_area_2'] = res.get('admin2', 'Unknown')
        except Exception as e:
            logging.warning(f"GIS Mapping Error: {e}")
            gis['continent'] = 'Unknown'
            gis['admin_area_1'] = 'Unknown'
            gis['admin_area_2'] = 'Unknown'

        try:
            url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current_weather=true&timezone=auto"
            response = self.session.get(url, timeout=5).json()

            gis['timezone'] = response.get('timezone')

            offset_sec = response.get('utc_offset_seconds', 0)
            hours = int(offset_sec // 3600)
            minutes = int((abs(offset_sec) % 3600) / 60)
            sign = "+" if offset_sec >= 0 else "-"
            gis['utc_offset_str'] = f"{sign}{abs(hours):02}:{minutes:02}"

        except Exception as e:
            logging.error(f"GIS Timezone Error: {e}")
            gis['timezone'] = None
            gis['utc_offset_str'] = None

        try:
            url = f"https://api.opentopodata.org/v1/srtm30m?locations={lat},{lon}"
            response = self.session.get(url, timeout=5).json()
            if 'results' in response and response['results']:
                gis['elevation'] = response['results'][0]['elevation']
        except Exception as e:
            logging.warning(f"GIS Elevation Error: {e}")
            gis['elevation'] = None

        gis['dist2coast_coarse'] = self._get_coarse_dist2sea(lat, lon)
        gis['dist2coast_fine'] = self._get_precise_dist2sea(lat, lon, gis['dist2coast_coarse'])

        logging.info(f"  [GIS] Enriched: {gis.get('continent')}, {gis.get('timezone')}")
        return gis

    def _get_coarse_dist2sea(self, lat, lon):
        """
        Uses NASA 0.01 degree dist2coast map to get approximate distance to coast.
        Used to inform the search radius for Overpass API calls.
        """

        dataset = xr.open_dataset(self.dist2sea_path)
        try:
            val = dataset['dist'].sel(
                latitude=lat,
                longitude=lon,
                method='nearest',
                tolerance=0.1
            ).values.item()
        except KeyError as e:
            logging.warning(f"Error lookup up {lat}, {lon}: {e}")
            return np.nan

        if np.isnan(val):
            return 0.0
        return val

    def _get_precise_dist2sea(self, lat, lon, rough_dist_km):
        """Uses Overpass API to get the precise distance to coast. Searches for a maximum radius of 300km."""

        search_radius_km = min(max(rough_dist_km + 50, 50), 300)
        search_radius = search_radius_km * 1000

        overpass_url = "http://overpass-api.de/api/interpreter"
        overpass_query = f"""
        [out:json][timeout:90];
        (
          way["natural"="coastline"](around:{search_radius},{lat},{lon});
          relation["natural"="coastline"](around:{search_radius},{lat},{lon});
        );
        out geom;
        """

        headers = {
            'User-Agent': self.user_agent,
            'Accept-Encoding': 'gzip, deflate'
        }

        try:
            logging.info(f"  [GIS] Calculating precise distance to coast (Radius: {search_radius_km}km)...")
            response = requests.get(overpass_url, params={'data': overpass_query}, headers=headers)

            retries = 0
            while response.status_code in [429, 503, 504] and retries < 3:
                logging.info(f"  [GIS] Server busy (Status {response.status_code}). Waiting 30s... (Attempt {retries + 1})")
                time.sleep(30)
                response = requests.get(overpass_url, params={'data': overpass_query}, headers=headers)
                retries += 1

            if response.status_code != 200:
                logging.info(f"  [GIS] Failed to get data for {lat}, {lon} after retries. Code: {response.status_code}")
                return None

            data = response.json()

            if not data.get('elements'):
                return None

            min_dist = float('inf')
            for element in data['elements']:
                if 'geometry' in element:
                    for point in element['geometry']:
                        dist = geodesic((lat, lon), (point['lat'], point['lon'])).km
                        if dist < min_dist:
                            min_dist = dist

            return min_dist

        except Exception as e:
            print(f"Error fetching data for {lat}, {lon}: {e}")
            return rough_dist_km

    def resolve_missing_team(self, team_name: str, sex: str) -> str | None:
        """Prompts the user to create a new team entry."""

        print(f"\n" + "=" * 60)
        print(f"[MISSING TEAM DETECTED]")
        print(f"Name in JSON: {team_name} ({sex})")
        print("=" * 60)

        count = 0

        while True:
            print("\n  Please provide details to add this team.")

            full_name = self._get_input("Full Team Name (Country + Sex)", required=True)
            short_name = self._get_input("Short Team Name (Country)", required=False, default=None)
            nickname = self._get_input("Team Nickname", required=False, default=None)
            nation = self._get_input("Nation", required=True)
            if count == 0:
                webbrowser.open_new(f"https://www.iban.com/country-codes")
            iso_code = self._get_input("Nation ISO Alpha-3 Code", required=True).upper()

            abbreviation = f"{iso_code}-{'M' if sex.lower() == 'male' else 'F'}"
            team_id = f"{iso_code.lower()}{'M' if sex.lower() == 'male' else 'F'}T20"

            details = {
                'team_id': team_id,
                'format': 'T20',
                'full_name': full_name,
                'short_name': short_name,
                'abbreviation': abbreviation,
                'nickname': nickname,
                'sex': sex,
                'nation': nation
            }

            if self._confirm_entry(details):
                with db_connection(self.db_name) as conn:
                    try:
                        conn.execute("""
                                     INSERT INTO teams (team_id, format, full_name, short_name, abbreviation, nickname,
                                                        sex, nation)
                                     VALUES (:team_id, :format, :full_name, :short_name, :abbreviation, :nickname, :sex,
                                             :nation)
                                     """, details)
                        conn.commit()
                        logging.info(f"Successfully created team '{full_name}' ({team_id}).")
                        return team_id
                    except sqlite3.IntegrityError:
                        print(f"\n[!] ERROR: The ID '{team_id}' already exists in the database.")
                        logging.warning(f"Manual team creation collision: '{team_id}' already exists.")
                        continue

            count += 1

    def resolve_missing_venue(self, venue_name: str, city: str) -> str | None | Any:
        """Prompts the user to select a venue from the list of aliases."""

        print(f"\n" + "=" * 60)
        print(f"[UNKNOWN VENUE DETECTED]")
        print(f"Name in JSON: {venue_name}")
        print(f"City in JSON: {city}")
        print("=" * 60)

        count = 0

        while True:
            print("\nOptions:")
            print("  1. LINK to an existing venue (Search DB)")
            print("  2. CREATE a new venue entry")
            choice = input("Select option (1/2): ").strip()

            if choice == '1':
                search_term = input("  Search for the venue (search by name, city, or nation - search nation if unsure): ")
                with db_connection(self.db_name) as conn:
                    sql = """
                        SELECT venue_id, venue_name, city, nation
                        FROM venues
                        WHERE venue_name LIKE ? OR city LIKE ? OR nation LIKE ?
                    """
                    wildcard = f"%{search_term}%"
                    results = conn.execute(sql, (wildcard, wildcard, wildcard)).fetchall()

                if not results:
                    print("  [!] No matches found. Search again or create new.")
                    continue

                print(f"\n  Found {len(results)} matches:")
                for i, res in enumerate(results):
                    print(f"    {i+1}. {res[1]} ({res[2]}, {res[3]}) [ID: {res[0]}]")

                sel = input("\n  Select number to link (or 0 to cancel): ")
                try:
                    idx = int(sel) - 1
                    if idx == -1: continue

                    selected_id = results[idx][0]
                    nation = results[idx][3]
                    self._add_venue_alias(venue_name, city, nation, selected_id)
                    return selected_id

                except (ValueError, IndexError):
                    print("  [!] Invalid selection.")

            elif choice == '2':
                print("\n --- New Venue Details ---")
                webbrowser.open_new(f"https://www.google.com/search?q={urllib.parse.quote_plus(venue_name)}")
                canonical_name = self._get_input("Canonical Venue Name", default=venue_name, required=True)
                new_nation = self._get_input("Nation", required=True)
                if count == 0:
                    webbrowser.open_new(f"https://www.iban.com/country-codes")
                iso_code = self._get_input("Nation ISO Alpha-3 Code", required=True).upper()
                new_city = self._get_input("City", required=True)

                lat, lon = self._get_lat_long(venue_name, city, new_nation)
                gis_data = self._get_gis_data(lat, lon)

                with db_connection(self.db_name) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM venues WHERE nation_code = ?", (iso_code,))
                    count_nation = cursor.fetchone()[0]

                    cursor.execute("SELECT COUNT(*) FROM venues")
                    count_total = cursor.fetchone()[0]

                    cursor.execute("SELECT team_id FROM teams WHERE nation = ?", (new_nation,))
                    teams = cursor.fetchall()
                    home_team_id_1 = teams[0][0] if len(teams) > 0 else None
                    home_team_id_2 = teams[1][0] if len(teams) > 1 else None

                next_nation_rank = count_nation + 1
                next_total_rank = count_total + 1
                venue_id = f"{next_nation_rank}{iso_code.lower()}{next_total_rank}"

                details = {
                    'venue_id': venue_id,
                    'venue_name': canonical_name,
                    'city': new_city,
                    'admin_area_1': gis_data.get('admin_area_1', 'Unknown'),
                    'admin_area_2': gis_data.get('admin_area_2', 'Unknown'),
                    'nation': new_nation,
                    'nation_code': iso_code,
                    'continent': gis_data.get('continent', 'Unknown'),
                    'hemisphere': gis_data.get('hemisphere'),
                    'home_team_id_1': home_team_id_1,
                    'home_team_id_2': home_team_id_2,
                    'latitude': lat,
                    'longitude': lon,
                    'elevation': gis_data.get('elevation'),
                    'dist2coast_coarse': gis_data.get('dist2coast_coarse'),
                    'dist2coast_fine': gis_data.get('dist2coast_fine'),
                    'timezone': gis_data.get('timezone'),
                    'utc_offset_str': gis_data.get('utc_offset_str')
                }

                if self._confirm_entry(details):
                    with db_connection(self.db_name) as conn:
                        conn.execute("""
                                     INSERT INTO venues (venue_id, venue_name, city, admin_area_1, admin_area_2, 
                                                         nation, nation_code, continent, hemisphere, home_team_id_1, 
                                                         home_team_id_2, latitude, longitude, elevation, 
                                                         dist2coast_coarse, dist2coast_fine, timezone, utc_offset_str
                                                        )
                                     VALUES (:venue_id, :venue_name, :city, :admin_area_1, :admin_area_2, :nation, 
                                             :nation_code, :continent, :hemisphere, :home_team_id_1, :home_team_id_2,
                                             :latitude, :longitude, :elevation, :dist2coast_coarse, :dist2coast_fine,
                                             :timezone, :utc_offset_str
                                            )
                                     """, details)
                        conn.commit()
                        self._add_venue_alias(venue_name, city, new_nation, venue_id)
                        logging.info(f"Successfully created venue '{venue_name}' ({venue_id}).")
                        return venue_id

                count += 1

    def _add_venue_alias(self, name: str, city: str, nation: str, venue_id: str):
        with db_connection(self.db_name) as conn:
            try:
                conn.execute("""
                            INSERT INTO venue_aliases (alias_name, alias_city, alias_nation, venue_id)
                            VALUES (?, ?, ?, ?)
                """, (name, city or '', nation, venue_id))
                conn.commit()
                print(f"  [SUCCESS] Alias saved.")
            except sqlite3.Error as e:
                print(f"  [ERROR] Could not save alias: {e}")

    def verifying_official(self, identifier: str):
        """
        Checks if an official exists in the officials table. If not, prompts user.
        Uses registry keys to open Cricinfo pages.
        """

        if not identifier: return

        with db_connection(self.db_name) as conn:
            exists = conn.execute("SELECT 1 FROM officials WHERE identifier = ?", (identifier,)).fetchone()
            if exists:
                return

            reg_row = conn.execute(
                "SELECT unique_name, key_cricinfo, key_cricinfo_2 FROM registry WHERE identifier = ?",
                (identifier,)
            ).fetchone()

        unique_name = reg_row[0]

        name_parts = unique_name.split(" ")
        f_name = name_parts[0]
        l_name = name_parts[-1] if len(name_parts) > 1 else ""
        slug = f"{f_name}-{l_name}".strip("-")

        key_cricinfo = reg_row[1] if reg_row else None
        key_cricinfo_2 = reg_row[2] if reg_row else None

        print(f"\n" + "=" * 60)
        print(f"[MISSING OFFICIAL BIO DETECTED]")
        print(f"Name: {unique_name}")
        print(f"ID: {identifier}")
        print("=" * 60)

        for key in [key_cricinfo, key_cricinfo_2]:
            if key:
                url = f"https://www.espncricinfo.com/cricketers/{slug}-{key}"
                print(f"  > Opening: {url}")
                webbrowser.open(url)

                if key == key_cricinfo_2:
                    keep = input(f"  Is the second Cricinfo key ({key}) correct? (y/n/skip): ").lower()
                    if keep == 'n':
                        with db_connection(self.db_name) as conn:
                            conn.execute("UPDATE registry SET key_cricinfo_2 = NULL WHERE identifier = ?",
                                         (identifier,))
                            conn.commit()

        while True:
            print("\n  --- Enter Official Details ---")
            full_name = self._get_input("Full Name", default=unique_name, required=True)
            display_name = self._get_input("Display Name", default=full_name, required=True)

            sex = self._get_input("Sex (male/female)", required=True).lower()
            while sex not in ['male', 'female']:
                print("  [!] Sex must be 'male' or 'female'")
                sex = self._get_input("Sex (male/female)", required=True).lower()

            birth_date = self._get_input("Birth Date (YYYY-MM-DD)", required=False)
            while not self._check_valid_date(birth_date):
                print("  [!] Invalid date format. Use YYYY-MM-DD.")
                birth_date = self._get_input("Birth Date (YYYY-MM-DD)", required=False)

            death_date = self._get_input("Death Date (YYYY-MM-DD)", required=False)
            while not self._check_valid_date(death_date):
                print("  [!] Invalid date format. Use YYYY-MM-DD.")
                death_date = self._get_input("Death Date (YYYY-MM-DD)", required=False)

            if birth_date and death_date:
                while birth_date > death_date:
                    print(f"  [!] Woah. {full_name} died before they were born. Try again.")
                    birth_date = self._get_input("Birth Date (YYYY-MM-DD)", required=False)
                    while not self._check_valid_date(birth_date):
                        print("  [!] Invalid date format. Use YYYY-MM-DD.")
                        birth_date = self._get_input("Birth Date (YYYY-MM-DD)", required=False)

                    death_date = self._get_input("Death Date (YYYY-MM-DD)", required=False)
                    while not self._check_valid_date(death_date):
                        print("  [!] Invalid date format. Use YYYY-MM-DD.")
                        death_date = self._get_input("Death Date (YYYY-MM-DD)", required=False)

            birth_place = self._get_input("Birth Place", required=False)
            birth_nation = self._get_input("Birth Nation", required=False)

            details = {
                'identifier': identifier,
                'unique_name': unique_name,
                'full_name': full_name,
                'display_name': display_name,
                'sex': sex,
                'birth_date': birth_date,
                'birth_place': birth_place,
                'birth_nation': birth_nation,
                'death_date': death_date
            }

            if self._confirm_entry(details):
                with db_connection(self.db_name) as conn:
                    try:
                        conn.execute("""
                                     INSERT INTO officials (identifier, unique_name, full_name, display_name, sex, 
                                                            birth_date, birth_place, birth_nation, death_date)
                                     VALUES (:identifier, :unique_name, :full_name, :display_name, :sex, :birth_date, 
                                             :birth_place, :birth_nation, :death_date)
                                     """, details)
                        conn.commit()
                        logging.info(f"Successfully created official {display_name} ({identifier}).")
                        return
                    except sqlite3.IntegrityError as e:
                        print(f"  [!] Database Error: {e}")

    def verifying_player(self, identifier: str):
        """
        Checks if a player exists in the players table. If not, prompts user.
        Uses registry keys to open Cricinfo pages.
        """

        if not identifier: return

        with db_connection(self.db_name) as conn:
            exists = conn.execute("SELECT 1 FROM players WHERE identifier = ?", (identifier,)).fetchone()
            if exists:
                return

            reg_row = conn.execute(
                "SELECT unique_name, key_cricinfo, key_cricinfo_2 FROM registry WHERE identifier = ?",
                (identifier,)
            ).fetchone()

        unique_name = reg_row[0]

        name_parts = unique_name.split(" ")
        f_name = name_parts[0]
        l_name = name_parts[-1] if len(name_parts) > 1 else ""
        slug = f"{f_name}-{l_name}".strip("-")

        key_cricinfo = reg_row[1] if reg_row else None
        key_cricinfo_2 = reg_row[2] if reg_row else None

        print(f"\n" + "=" * 60)
        print(f"[MISSING PLAYER BIO DETECTED]")
        print(f"Name: {unique_name}")
        print(f"ID: {identifier}")
        print("=" * 60)

        for key in [key_cricinfo, key_cricinfo_2]:
            if key:
                url = f"https://www.espncricinfo.com/cricketers/{slug}-{key}"
                print(f"  > Opening: {url}")
                webbrowser.open(url)

                if key == key_cricinfo_2:
                    keep = input(f"  Is the second Cricinfo key ({key}) correct? (y/n/skip): ").lower()
                    if keep == 'n':
                        with db_connection(self.db_name) as conn:
                            conn.execute("UPDATE registry SET key_cricinfo_2 = NULL WHERE identifier = ?",
                                         (identifier,))
                            conn.commit()

        while True:
            print("\n  --- Enter Player Details ---")
            full_name = self._get_input("Full Name", default=unique_name, required=True)
            display_name = self._get_input("Display Name", default=full_name, required=True)

            sex = self._get_input("Sex (male/female)", required=True).lower()
            while sex not in ['male', 'female']:
                print("  [!] Sex must be 'male' or 'female'")
                sex = self._get_input("Sex (male/female)", required=True).lower()

            birth_date = self._get_input("Birth Date (YYYY-MM-DD)", required=False)
            while not self._check_valid_date(birth_date):
                print("  [!] Invalid date format. Use YYYY-MM-DD.")
                birth_date = self._get_input("Birth Date (YYYY-MM-DD)", required=False)

            death_date = self._get_input("Death Date (YYYY-MM-DD)", required=False)
            while not self._check_valid_date(death_date):
                print("  [!] Invalid date format. Use YYYY-MM-DD.")
                death_date = self._get_input("Death Date (YYYY-MM-DD)", required=False)

            if birth_date and death_date:
                while birth_date > death_date:
                    print(f"  [!] Woah. {full_name} died before they were born. Try again.")
                    birth_date = self._get_input("Birth Date (YYYY-MM-DD)", required=False)
                    while not self._check_valid_date(birth_date):
                        print("  [!] Invalid date format. Use YYYY-MM-DD.")
                        birth_date = self._get_input("Birth Date (YYYY-MM-DD)", required=False)

                    death_date = self._get_input("Death Date (YYYY-MM-DD)", required=False)
                    while not self._check_valid_date(death_date):
                        print("  [!] Invalid date format. Use YYYY-MM-DD.")
                        death_date = self._get_input("Death Date (YYYY-MM-DD)", required=False)

            birth_place = self._get_input("Birth Place", required=False)
            birth_nation = self._get_input("Birth Nation", required=False)

            bat_hand = self._get_input("Batting Hand (R/L)", required=False)
            while bat_hand is not None and bat_hand.upper() not in ['R', 'L']:
                print("  [!] Batting hand must be 'R' or 'L'")
                bat_hand = self._get_input("Batting Hand (R/L)", required=False)

            bowl_hand = self._get_input("Bowling Hand (R/L). Separate ambidextrous bowlers with a comma.", required=False)

            while bowl_hand is not None and not set(bowl_hand.upper()).issubset(" ,RL"):
                print(f"  [!] Invalid bowling hand. Should be 'R', 'L', or a comma-separated list of both.")
                bowl_hand = self._get_input("Bowling Hand (R/L). Separate ambidextrous bowlers with a comma", required=False)

            styles = ['Seam', 'Offbreak', 'Legbreak', 'Orthodox', 'Unorthodox']
            bowl_style = self._get_input("Bowling Style. Separate multiple styles with a comma", required=False)

            bowl_style = [s.strip() for s in bowl_style.split(',')] if bowl_style else None

            while bowl_style is not None and not set(bowl_style).issubset(styles):
                print(f"  [!] Invalid bowling style. Should be one of {', '.join(styles)}.")
                raw_input = self._get_input("Bowling Style. Separate multiple styles with a comma", required=False)
                bowl_style = [s.strip() for s in raw_input.split(',')] if raw_input else None

            while not bowl_hand and bowl_style:
                print(f"  [!] You've got to use a hand to bowl {bowl_style}.")
                bowl_hand = self._get_input("Bowling Hand (R/L). Separate ambidextrous bowlers with a comma", required=False)

                while bowl_hand is not None and not set(bowl_hand.upper()).issubset(" ,RL"):
                    print(f"  [!] Invalid bowling hand. Should be 'R', 'L', or a comma-separated list of both.")
                    bowl_hand = self._get_input("Bowling Hand (R/L). Separate ambidextrous bowlers with a comma", required=False)

                bowl_style = [s.strip() for s in bowl_style.split(',')] if bowl_style else None

                while bowl_style is not None and not set(bowl_style).issubset(styles):
                    print(f"  [!] Invalid bowling style. Should be one of {', '.join(styles)}.")
                    raw_input = self._get_input("Bowling Style. Separate multiple styles with a comma", required=False)
                    bowl_style = [s.strip() for s in raw_input.split(',')] if raw_input else None

            bat_hand = bat_hand.upper() if bat_hand else None
            bowl_hand = bowl_hand.upper() if bowl_hand else None
            bowl_style = ','.join(bowl_style) if bowl_style else None

            wk = self._get_input("Wicketkeeper (1=Yes, Enter=No)", required=False)
            while wk is not None and wk != '1':
                print("  [!] Wicketkeeper must be '1' for Yes or 'Enter' for No.")
                wk = self._get_input("Wicketkeeper (1=Yes, Enter=No)", required=False)


            details = {
                'identifier': identifier,
                'unique_name': unique_name,
                'full_name': full_name,
                'display_name': display_name,
                'sex': sex,
                'birth_date': birth_date,
                'death_date': death_date,
                'birth_place': birth_place,
                'birth_nation': birth_nation,
                'bat_hand': bat_hand,
                'bowl_hand': bowl_hand,
                'bowl_style': bowl_style,
                'wicketkeeper': wk
            }

            if self._confirm_entry(details):
                with db_connection(self.db_name) as conn:
                    try:
                        conn.execute("""
                                     INSERT INTO players (identifier, unique_name, full_name, display_name, sex, 
                                                          birth_date, death_date, birth_place, birth_nation, bat_hand, 
                                                          bowl_hand, bowl_style, wicketkeeper)
                                     VALUES (:identifier, :unique_name, :full_name, :display_name, :sex, :birth_date,
                                            :death_date, :birth_place, :birth_nation, :bat_hand, :bowl_hand, :bowl_style,
                                            :wicketkeeper)""", details)
                        conn.commit()
                        logging.info(f"Successfully created player {display_name} ({identifier}).")
                        return
                    except sqlite3.IntegrityError as e:
                        print(f"  [!] Database Error: {e}")