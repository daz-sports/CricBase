import logging
import sqlite3
import urllib.parse
import webbrowser
import requests
import country_converter as coco
import reverse_geocoder as rg
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
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
        Automated gis based on Lat/Long (Continent, Timezone, Elevation, Administrative Divisions).
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

        logging.info(f"  [GIS] Enriched: {gis.get('continent')}, {gis.get('timezone')}")
        return gis

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
                search_term = input("  Enter search term (part of venue name or city): ")
                with db_connection(self.db_name) as conn:
                    sql = """
                        SELECT venue_id, venue_name, city, nation
                        FROM venues
                        WHERE venue_name LIKE ? OR city LIKE ?
                    """
                    wildcard = f"%{search_term}%"
                    results = conn.execute(sql, (wildcard, wildcard)).fetchall()

                if not results:
                    print("  [!] No matches found. Try again or create new.")
                    continue

                print(f"\n  Found {len(results)} matches:")
                for i, res in enumerate(results):
                    print(f"    {i+1}. {res[1]} ({res[2]}, {res[3]}) [ID: {res[0]}]")

                sel = input("\n  Select number to link (or 0 to cancel): ")
                try:
                    idx = int(sel) - 1
                    if idx == -1: continue

                    selected_id = results[idx][0]
                    self._add_venue_alias(venue_name, city, "Unknown", selected_id)
                    return selected_id

                except (ValueError, IndexError):
                    print("  [!] Invalid selection.")

            elif choice == '2':
                print("\n --- New Venue Details ---")
                webbrowser.open_new(f"https://www.google.com/search?q={urllib.parse.quote_plus(venue_name)}")
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
                    'venue_name': venue_name,
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
                    'timezone': gis_data.get('timezone'),
                    'utc_offset_str': gis_data.get('utc_offset_str')
                }

                if self._confirm_entry(details):
                    with db_connection(self.db_name) as conn:
                        conn.execute("""
                                     INSERT INTO venues (venue_id, venue_name, city, admin_area_1, admin_area_2, 
                                                         nation, nation_code, continent, hemisphere, home_team_id_1, 
                                                         home_team_id_2, latitude, longitude, elevation, timezone, 
                                                         utc_offset_str
                                                        )
                                     VALUES (:venue_id, :venue_name, :city, :admin_area_1, :admin_area_2, :nation, 
                                             :nation_code, :continent, :hemisphere, :home_team_id_1, :home_team_id_2,
                                             :latitude, :longitude, :elevation, :timezone, :utc_offset_str)
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

    def resolve_missing_official(self, identifier: str, name_in_json: str):
        """
        Checks if an official exists in the officials table. If not, prompts user.
        Uses registry keys to open Cricinfo pages.
        """

        with db_connection(self.db_name) as conn:
            exists = conn.execute("SELECT 1 FROM officials WHERE identifier = ?", (identifier,)).fetchone()
            if exists:
                return

            reg_row = conn.execute(
                "SELECT unique_name, key_cricinfo, key_cricinfo_2 FROM registry WHERE identifier = ?",
                (identifier,)
            ).fetchone()

        unique_name = reg_row[0] if reg_row else name_in_json

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

            sex = self._get_input("Sex (male/female)", required=True, default="male").lower()
            while sex not in ['male', 'female']:
                print("  [!] Sex must be 'male' or 'female'")
                sex = self._get_input("Sex (male/female)", required=True, default="male").lower()

            birth_date = self._get_input("Birth Date (YYYY-MM-DD)", required=False)
            birth_place = self._get_input("Birth Place", required=False)
            birth_nation = self._get_input("Birth Nation", required=False)
            death_date = self._get_input("Death Date (YYYY-MM-DD)", required=False)

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