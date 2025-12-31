import logging
import sqlite3
import urllib.parse
import webbrowser
from typing import Optional, Dict, Any
from utils import db_connection

class InputManager:
    """
        Handles interactive user prompts for missing data.
    """

    def __init__(self, db_name: str):
        self.db_name = db_name

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
            print(f"  {k}: {v}")
        choice = input("\n  Save this entry? (y/n/retry): ").lower()
        return choice == 'y'

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
                webbrowser.open_new(f"https://www.google.com/search?q={urllib.parse.urlencode(venue_name)}")
                new_nation = self._get_input("Nation", required=True)
                if count == 0:
                    webbrowser.open_new(f"https://www.iban.com/country-codes")
                iso_code = self._get_input("Nation ISO Alpha-3 Code", required=True).upper()
                new_city = self._get_input("City", required=True)

                with db_connection(self.db_name) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM venues WHERE nation_code = ?", (iso_code,))
                    count_nation = cursor.fetchone()[0]

                    cursor.execute("SELECT COUNT(*) FROM venues")
                    count_total = cursor.fetchone()[0]

                    cursor.execute("SELECT team_id FROM teams WHERE nation = ?", (new_nation,))
                    home_team_id_1 = cursor.fetchone()[0]
                    home_team_id_2 = cursor.fetchone()[1]

                next_nation_rank = count_nation + 1
                next_total_rank = count_total + 1
                venue_id = f"{next_nation_rank}{iso_code.lower()}{next_total_rank}"

                details = {
                    'venue_id': venue_id,
                    'venue_name': venue_name,
                    'city': new_city,
                    'nation': new_nation,
                    'nation_code': iso_code,
                    'home_team_id_1': home_team_id_1,
                    'home_team_id_2': home_team_id_2
                }

                if self._confirm_entry(details):
                    with db_connection(self.db_name) as conn:
                        conn.execute("""
                                     INSERT INTO venues (venue_id, venue_name, city, nation, nation_code, home_team_id_1, home_team_id_2)
                                     VALUES (:venue_id, :venue_name, :city, :nation, :nation_code, :home_team_id_1, :home_team_id_2)
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