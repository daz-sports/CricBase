import sqlite3
import logging
import os
from datetime import datetime
from config import Config
from scripts.utils import BuildError
from utils import db_connection

class CricketDatabase:
    def __init__(self, config: Config):
        self.config = config
        self.db_name = config.DB_NAME
        self._init_database()

    SCHEMA_SQL = {
        "schema_version": """
                          CREATE TABLE IF NOT EXISTS schema_version
                          (
                              version    TEXT NOT NULL,
                              applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                          )
                          """,
        "registry": """
                       CREATE TABLE IF NOT EXISTS registry
                       (
                           identifier           TEXT PRIMARY KEY,
                           name                 TEXT NOT NULL,
                           unique_name          TEXT NOT NULL UNIQUE,
                           key_cricinfo         TEXT NOT NULL,
                           key_cricinfo_2       TEXT,
                           key_bcci             TEXT,
                           key_bcci_2           TEXT,
                           key_bigbash          TEXT,
                           key_cricbuzz         TEXT,
                           key_cricheroes       TEXT,
                           key_crichq           TEXT,
                           key_cricingif        TEXT,
                           key_cricketarchive   TEXT,
                           key_cricketarchive_2 TEXT,
                           key_cricketworld     TEXT,
                           key_nvplay           TEXT,
                           key_nvplay_2         TEXT,
                           key_opta             TEXT,
                           key_opta_2           TEXT,
                           key_pulse            TEXT,
                           key_pulse_2          TEXT,
                           created_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                           updated_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                           CHECK (length(identifier) > 0),
                           CHECK (length(unique_name) > 0)
                       )
                       """,
        "teams": """
                 CREATE TABLE IF NOT EXISTS teams
                 (
                     team_id               TEXT PRIMARY KEY,
                     format                TEXT NOT NULL,
                     full_name             TEXT NOT NULL,
                     short_name            TEXT,
                     abbreviation          TEXT NOT NULL,
                     nickname              TEXT,
                     sex                   TEXT NOT NULL CHECK (sex IN ('male', 'female')),
                     nation                TEXT NOT NULL,
                     created_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                     updated_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                 )
        """,
        "venues": """
                    CREATE TABLE IF NOT EXISTS venues
                    (
                        venue_id           TEXT PRIMARY KEY,
                        venue_name         TEXT NOT NULL,
                        city               TEXT NOT NULL,
                        nation             TEXT NOT NULL,
                        nation_code        TEXT NOT NULL,
                        home_team_id_1     TEXT REFERENCES teams (team_id),
                        home_team_id_2     TEXT REFERENCES teams (team_id),
                        created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE (venue_name, city, nation)
                    )
                  """,
        "venue_aliases": """
                         CREATE TABLE IF NOT EXISTS venue_aliases 
                         (
                             alias_name      TEXT NOT NULL,
                             alias_city      TEXT NOT NULL,
                             alias_nation    TEXT NOT NULL,
                             venue_id        TEXT NOT NULL REFERENCES venues (venue_id),
                             created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                             updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                             PRIMARY KEY (alias_name, alias_city, alias_nation)
                         )
                         """,
        "matches": """
                       CREATE TABLE IF NOT EXISTS matches
                       (
                           match_id                  TEXT PRIMARY KEY,
                           match_type                TEXT NOT NULL CHECK (match_type IN ('T20')),
                           overs                     INTEGER,
                           balls_per_over            INTEGER,
                           powerplay_starti1         TEXT,
                           powerplay_endi1           TEXT,
                           powerplay_starti2         TEXT,
                           powerplay_endi2           TEXT,
                           team_type                 TEXT CHECK (team_type IN ('international')),
                           sex                       TEXT NOT NULL CHECK (sex IN ('male', 'female')),
                           start_date                DATE NOT NULL,
                           end_date                  DATE NOT NULL,
                           season                    TEXT,
                           team1_id                  TEXT NOT NULL REFERENCES teams (team_id),
                           team2_id                  TEXT NOT NULL REFERENCES teams (team_id),
                           umpire1_id                TEXT REFERENCES registry (identifier),
                           umpire2_id                TEXT REFERENCES registry (identifier),
                           tv_umpire_id              TEXT REFERENCES registry (identifier),
                           match_referee_id          TEXT REFERENCES registry (identifier),
                           reserve_umpire_id         TEXT REFERENCES registry (identifier),
                           toss_winner_id            TEXT REFERENCES teams (team_id),
                           toss_decision             TEXT,
                           team1_prepostpens         INTEGER   DEFAULT 0,
                           team2_prepostpens         INTEGER   DEFAULT 0,
                           winner_id                 TEXT REFERENCES teams(team_id),
                           by_runs                   INTEGER CHECK (by_runs IN (0, 1)),
                           victory_margin_runs       INTEGER CHECK ((victory_margin_runs >= 0) OR victory_margin_runs IS NULL),
                           by_wickets                INTEGER CHECK (by_wickets IN (0, 1)),
                           victory_margin_wickets    INTEGER CHECK ((victory_margin_wickets >= 1 AND victory_margin_wickets <= 10) OR victory_margin_wickets IS NULL),
                           by_other                  INTEGER CHECK (by_other IN (0, 1)),
                           victory_margin_other      TEXT,
                           no_result                 INTEGER   DEFAULT 0 CHECK (no_result IN (0, 1)),
                           tie                       INTEGER   DEFAULT 0 CHECK (tie IN (0, 1)),
                           super_over_pld            INTEGER   DEFAULT 0 CHECK (super_over_pld IN (0, 1)),
                           bowl_out                  INTEGER   DEFAULT 0 CHECK (bowl_out IN (0, 1)),
                           DLS                       INTEGER   DEFAULT 0 CHECK (DLS IN (0, 1)),
                           player_of_match_id        TEXT REFERENCES registry (identifier),
                           event_name                TEXT,
                           event_match_number        INTEGER,
                           venue_id                  TEXT REFERENCES venues (venue_id),
                           created_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                           updated_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                           CHECK (start_date <= end_date),
                           CHECK (winner_id IS NULL OR winner_id = team1_id OR winner_id = team2_id),
                           CHECK (team1_id != team2_id)
                       )
                       """,
        "match_metadata": """
                          CREATE TABLE IF NOT EXISTS match_metadata
                          (
                              match_id          TEXT PRIMARY KEY,
                              data_version      TEXT    NOT NULL,
                              cricsheet_created DATE    NOT NULL,
                              revision          INTEGER NOT NULL,
                              created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                              updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                              FOREIGN KEY (match_id) REFERENCES matches (match_id)
                          )
                          """,
        "match_players": """
                       CREATE TABLE IF NOT EXISTS match_players
                       (
                           match_id         TEXT NOT NULL REFERENCES matches (match_id),
                           identifier       TEXT NOT NULL REFERENCES registry (identifier),
                           team_id          TEXT NOT NULL REFERENCES teams (team_id),
                           created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                           updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                           PRIMARY KEY (match_id, identifier)
                       )
        """,
        "players": """
                   CREATE TABLE IF NOT EXISTS players
                       (
                           identifier     TEXT PRIMARY KEY REFERENCES registry (identifier),
                           unique_name    TEXT NOT NULL UNIQUE,
                           full_name      TEXT NOT NULL,
                           display_name   TEXT,
                           sex            TEXT NOT NULL CHECK (sex IN ('male', 'female')),
                           birth_date     DATE,
                           birth_place    TEXT,
                           birth_nation   TEXT,
                           bat_hand       TEXT CHECK (bat_hand IN ('R', 'L') OR bat_hand IS NULL),
                           bowl_hand      TEXT,
                           bowl_style     TEXT,
                           current_nation   TEXT NOT NULL,
                           previous_nation_1  TEXT,
                           previous_nation_2   TEXT,
                           wicketkeeper     INTEGER CHECK (wicketkeeper IS NULL OR wicketkeeper = 1),
                           death_date     DATE,
                           created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                           updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                           CHECK (previous_nation_1 IS NULL OR previous_nation_1 != current_nation),
                           CHECK (previous_nation_2 IS NULL OR (previous_nation_2 != previous_nation_1 AND previous_nation_2 != current_nation))
                       )
                   """,
        "officials": """
                     CREATE TABLE IF NOT EXISTS officials 
                     (
                         identifier     TEXT PRIMARY KEY REFERENCES registry (identifier),
                         unique_name    TEXT NOT NULL UNIQUE,
                         full_name      TEXT NOT NULL,
                         display_name   TEXT,
                         sex            TEXT NOT NULL CHECK (sex IN ('male', 'female')),
                         birth_date     DATE,
                         birth_place    TEXT,
                         birth_nation   TEXT,
                         death_date     DATE,
                         created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                         updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                     )
        """,
        "deliveries": """
                       CREATE TABLE IF NOT EXISTS deliveries
                       (
                           match_id                 TEXT NOT NULL,
                           innings                  INTEGER,
                           overs                    INTEGER,
                           balls                    INTEGER,
                           batter_id                TEXT REFERENCES registry (identifier),
                           bowler_id                TEXT REFERENCES registry (identifier),
                           non_striker_id           TEXT REFERENCES registry (identifier),
                           runs_batter              INTEGER CHECK (runs_batter >= 0),
                           runs_extras              INTEGER CHECK (runs_extras >= 0),
                           runs_total               INTEGER CHECK (runs_total >= 0),
                           runs_batter_non_boundary INTEGER CHECK (
                               runs_batter_non_boundary IS NULL OR
                               (runs_batter_non_boundary IN (0, 1) AND runs_batter IN (4, 6))
                               ),
                           wickets                  INTEGER CHECK (wickets IN (0, 1)),
                           player_out_id            TEXT REFERENCES registry (identifier),
                           how_out                  TEXT CHECK (how_out IN (
                                                                            'bowled', 'caught', 'caught and bowled',
                                                                            'lbw', 'stumped',
                                                                            'run out', 'hit wicket',
                                                                            'obstructing the field',
                                                                            'hit the ball twice',
                                                                            'handled the ball', 'timed out',
                                                                            'retired hurt', 'retired out',
                                                                            'retired not out'
                               )),
                           fielder1_id              TEXT REFERENCES registry (identifier),
                           fielder2_id              TEXT REFERENCES registry (identifier),
                           fielder3_id              TEXT REFERENCES registry (identifier),
                           wickets2                 INTEGER CHECK (wickets2 = 0 OR (wickets2 = 1 AND wickets = 1)),
                           player_out2_id           TEXT REFERENCES registry (identifier),
                           how_out2                 TEXT CHECK (how_out2 IN (
                                                                             'timed out',
                                                                             'retired hurt', 'retired out',
                                                                             'retired not out', 'run out'
                               )),
                           extras_byes              INTEGER   DEFAULT 0 CHECK (extras_byes = 0 OR (extras_byes >= 1 AND runs_extras >= 1)),
                           extras_legbyes           INTEGER   DEFAULT 0 CHECK (extras_legbyes = 0 OR (extras_legbyes >= 1 AND runs_extras >= 1)),
                           extras_noballs           INTEGER   DEFAULT 0 CHECK (extras_noballs = 0 OR (extras_noballs >= 1 AND runs_extras >= 1)),
                           extras_penalty           INTEGER   DEFAULT 0 CHECK (extras_penalty = 0 OR (extras_penalty >= 1 AND runs_extras >= 1)),
                           extras_wides             INTEGER   DEFAULT 0 CHECK (extras_wides = 0 OR (extras_wides >= 1 AND runs_extras >= 1)),
                           review                   INTEGER   DEFAULT 0 CHECK (review IN (0, 1)),
                           ump_decision             TEXT CHECK (
                               ump_decision IS NULL OR 
                               (ump_decision IN ('out', 'not out') AND review = 1)
                               ), -- As umpire reviews are not considered by Cricsheet
                           review_by_id             TEXT REFERENCES teams (team_id),
                           review_ump_id            TEXT REFERENCES registry (identifier),
                           review_batter_id         TEXT REFERENCES registry (identifier),
                           review_result            TEXT CHECK (review_result IN ('out', 'not out')),
                           umpires_call             INTEGER   DEFAULT 0 CHECK (umpires_call IS NULL OR (umpires_call IN (0, 1) AND review = 1)),
                           powerplay                INTEGER   DEFAULT 0 CHECK (powerplay IN (0, 1)),
                           super_over               INTEGER   DEFAULT 0 CHECK (super_over IN (0, 1)),
                           created_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                           updated_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                           FOREIGN KEY (match_id) REFERENCES matches (match_id),
                           PRIMARY KEY (match_id, innings, overs, balls),
                           CHECK (
                               (player_out_id IS NULL AND wickets = 0) OR 
                               (player_out_id IS NOT NULL AND wickets = 1)
                               ),
                           CHECK (
                               fielder1_id IS NULL AND (
                                   wickets = 0 OR 
                                   how_out IN ('bowled', 'lbw', 'hit wicket', 'obstructing the field',
                                               'hit the ball twice', 'handled the ball', 'timed out',
                                               'retired hurt', 'retired out', 'retired not out')
                                   ) OR
                               fielder1_id IS NOT NULL AND how_out IN ('caught', 'caught and bowled', 'stumped', 'run out')
                               ),
                           CHECK (
                               fielder2_id IS NULL OR
                               (fielder2_id IS NOT NULL AND 
                                fielder1_id IS NOT NULL AND 
                                how_out = 'run out')
                               ),
                           CHECK (
                               fielder3_id IS NULL OR
                               (fielder3_id IS NOT NULL AND 
                                fielder2_id IS NOT NULL AND
                                fielder1_id IS NOT NULL AND how_out = 'run out')),
                           CHECK (player_out2_id IS NULL OR (player_out2_id IS NOT NULL AND wickets2 = 1)),
                           CHECK (review_batter_id IS NULL OR review_batter_id = batter_id),
                           CHECK (extras_wides = 0 OR extras_noballs = 0), -- Cannot have both wide and no-ball
                           CHECK (runs_total = runs_batter + runs_extras),
                           CHECK ((wickets = 1 AND player_out_id IS NOT NULL) OR (wickets = 0 AND player_out_id IS NULL)),
                           CHECK ((review = 1 AND review_by_id IS NOT NULL AND review_ump_id IS NOT NULL AND
                                   review_result IS NOT NULL) OR 
                                  (review = 0 AND review_by_id IS NULL AND review_ump_id IS NULL AND 
                                   review_result IS NULL)),
                           CHECK ((ump_decision IS NULL AND review = 0) OR 
                                  (ump_decision IS NOT NULL AND review = 1)),
                           CHECK ((wickets2 = 1 AND player_out2_id IS NOT NULL AND how_out2 IS NOT NULL) OR 
                                  (wickets2 = 0 AND player_out2_id IS NULL AND how_out2 IS NULL))
                       )
                       """,
        "missing_matches": """
                           CREATE TABLE IF NOT EXISTS missing_matches
                           (
                               icc_id       INTEGER PRIMARY KEY,
                               start_date   DATE NOT NULL,
                               team1        TEXT NOT NULL,
                               team2        TEXT NOT NULL,
                               venue_name   TEXT NOT NULL,
                               city         TEXT,
                               venue_nation TEXT NOT NULL,
                               match_result TEXT NOT NULL,
                               toss_result  TEXT NOT NULL,
                               created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                               updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                           )
                           """,
        "check_correct_team_players": """
                                      CREATE TRIGGER IF NOT EXISTS check_correct_team_players
                                          BEFORE INSERT
                                          ON match_players
                                          FOR EACH ROW
                                      BEGIN
                                          SELECT CASE
                                                     WHEN NEW.team_id NOT IN (SELECT team1_id
                                                                           FROM matches
                                                                           WHERE match_id = NEW.match_id
                                                                           UNION
                                                                           SELECT team2_id
                                                                           FROM matches
                                                                           WHERE match_id = NEW.match_id) THEN
                                                         RAISE(ABORT, 'Team must be either team1 or team2 from the match')
                                                     END;
                                      END;
                                      """,
        "check_review_by_team": """
                       CREATE TRIGGER IF NOT EXISTS check_review_by_team
                           BEFORE INSERT
                           ON deliveries
                           FOR EACH ROW
                       BEGIN
                           SELECT CASE
                                      WHEN NEW.review_by_id IS NOT NULL
                                          AND NEW.review_by_id NOT IN (SELECT team1_id
                                                                    FROM matches
                                                                    WHERE match_id = NEW.match_id
                                                                    UNION
                                                                    SELECT team2_id
                                                                    FROM matches
                                                                    WHERE match_id = NEW.match_id)
                                          THEN RAISE(ABORT, 'review_by must match either team1 or team2 from the match')
                                      END;
                       END;
                       """,
        "check_review_ump_official": """
                       CREATE TRIGGER IF NOT EXISTS check_review_ump_official
                           BEFORE INSERT
                           ON deliveries
                           FOR EACH ROW
                           WHEN NEW.review_ump_id IS NOT NULL
                       BEGIN
                           SELECT CASE
                                      WHEN NEW.review = 0 THEN
                                          RAISE(ABORT, 'review_ump cannot be set when review is 0')
                                      WHEN NEW.review_ump_id NOT IN (SELECT umpire1_id
                                                                  FROM matches
                                                                  WHERE match_id = NEW.match_id
                                                                  UNION
                                                                  SELECT umpire2_id
                                                                  FROM matches
                                                                  WHERE match_id = NEW.match_id
                                                                  UNION
                                                                  SELECT reserve_umpire_id
                                                                  FROM matches
                                                                  WHERE match_id = NEW.match_id
                                                                  UNION
                                                                  SELECT tv_umpire_id
                                                                  FROM matches
                                                                  WHERE match_id = NEW.match_id
                                                                  UNION
                                                                  SELECT match_referee_id
                                                                  FROM matches
                                                                  WHERE match_id = NEW.match_id) THEN
                                          RAISE(ABORT, 'review_ump must be a match official')
                                      END;
                       END;
                       """,
        "idx_matches_date": """
                            CREATE INDEX IF NOT EXISTS idx_matches_date ON matches (start_date);
                            """,
        "idx_matches_team": """
                            CREATE INDEX IF NOT EXISTS idx_matches_teams ON matches (team1_id, team2_id);
                            """,
        "idx_deliveries_match_innings": """
                                        CREATE INDEX IF NOT EXISTS idx_deliveries_match_innings ON deliveries (match_id, innings);
                                        """,
        "idx_deliveries_batter": """
                                 CREATE INDEX IF NOT EXISTS idx_deliveries_batter ON deliveries (batter_id);
                                 """,
        "idx_deliveries_bowler": """
                                 CREATE INDEX IF NOT EXISTS idx_deliveries_bowler ON deliveries (bowler_id);
                                 """,
        "idx_deliveries_dismissal": """
                                    CREATE INDEX IF NOT EXISTS idx_deliveries_dismissal ON deliveries (how_out);
                                    """,
        "idx_registry_unique_name": """
                                    CREATE INDEX IF NOT EXISTS idx_registry_unique_name ON registry (unique_name); 
                                    """,
        "idx_matches_venue": """
                             CREATE INDEX IF NOT EXISTS idx_matches_venue ON matches (venue_id);
                             """,
        "idx_match_players_match": """
                                   CREATE INDEX IF NOT EXISTS idx_match_players_match ON match_players (match_id);
                                   """,
        "idx_match_players_identifier": """
                                        CREATE INDEX IF NOT EXISTS idx_match_players_identifier ON match_players (identifier);
                                        """,
        "idx_deliveries_match_batter": """
                                       CREATE INDEX IF NOT EXISTS idx_deliveries_match_batter ON deliveries (match_id, batter_id);
                                       """,
        "idx_deliveries_match_bowler": """
                                       CREATE INDEX IF NOT EXISTS idx_deliveries_match_bowler ON deliveries (match_id, bowler_id);
                                       """,
        "idx_deliveries_fielder1": """
                                   CREATE INDEX IF NOT EXISTS idx_deliveries_fielder1 ON deliveries (fielder1_id);
                                   """,
        "update_registry_timestamp": """
                       CREATE TRIGGER IF NOT EXISTS update_registry_timestamp
                           AFTER UPDATE
                           ON registry
                       BEGIN
                           UPDATE registry
                           SET updated_at = CURRENT_TIMESTAMP
                           WHERE identifier = NEW.identifier;
                       END;
                       """,
        "update_teams_timestamp": """
                       CREATE TRIGGER IF NOT EXISTS update_teams_timestamp
                           AFTER UPDATE
                           ON teams
                       BEGIN
                           UPDATE teams
                           SET updated_at = CURRENT_TIMESTAMP
                           WHERE team_id = NEW.team_id; 
                       END;
                                  """,
        "update_venues_timestamp": """
                                   CREATE TRIGGER IF NOT EXISTS update_venues_timestamp 
                                       AFTER UPDATE 
                                       on venues
                                   BEGIN
                                       UPDATE venues
                                       SET updated_at = CURRENT_TIMESTAMP
                                       WHERE venue_id = NEW.venue_id; 
                                   END;
                                   """,
        "update_venue_aliases_timestamp": """
                                          CREATE TRIGGER IF NOT EXISTS update_venue_aliases_timestamp 
                                              AFTER UPDATE 
                                              ON venue_aliases 
                                          BEGIN 
                                              UPDATE venue_aliases 
                                              SET updated_at = CURRENT_TIMESTAMP 
                                              WHERE alias_name = NEW.alias_name 
                                                AND alias_city = NEW.alias_city 
                                                AND alias_nation = NEW.alias_nation; 
                                          END;
                                          """,
        "update_matches_timestamp": """
                       CREATE TRIGGER IF NOT EXISTS update_matches_timestamp
                           AFTER UPDATE
                           ON matches
                       BEGIN
                           UPDATE matches
                           SET updated_at = CURRENT_TIMESTAMP
                           WHERE match_id = NEW.match_id;
                       END;
                       """,
        "update_match_metadata_timestamp": """
                                           CREATE TRIGGER IF NOT EXISTS update_match_metadata_timestamp
                                               AFTER UPDATE
                                               ON match_metadata
                                           BEGIN
                                               UPDATE match_metadata
                                               SET updated_at = CURRENT_TIMESTAMP
                                               WHERE match_id = NEW.match_id;
                                           END;
                                           """,
        "update_match_players_timestamp": """
                       CREATE TRIGGER IF NOT EXISTS update_match_players_timestamp
                           AFTER UPDATE
                           ON match_players
                       BEGIN
                           UPDATE match_players
                           SET updated_at = CURRENT_TIMESTAMP
                           WHERE match_id = NEW.match_id
                             AND identifier = NEW.identifier;
                       END;
                       """,
        "update_players_timestamp": """
                                    CREATE TRIGGER IF NOT EXISTS update_players_timestamp 
                                        AFTER UPDATE 
                                        ON players 
                                    BEGIN 
                                        UPDATE players 
                                        SET updated_at = CURRENT_TIMESTAMP 
                                        WHERE identifier = NEW.identifier; 
                                    END;
                                    """,
        "update_officials_timestamp": """
                                    CREATE TRIGGER IF NOT EXISTS update_officials_timestamp
                                        AFTER UPDATE
                                        ON officials
                                    BEGIN 
                                        UPDATE officials
                                        SET updated_at = CURRENT_TIMESTAMP
                                        WHERE identifier = NEW.identifier;
                                    END;
                                    """,
        "update_deliveries_timestamp": """
                       CREATE TRIGGER IF NOT EXISTS update_deliveries_timestamp
                           AFTER UPDATE
                           ON deliveries
                       BEGIN
                           UPDATE deliveries
                           SET updated_at = CURRENT_TIMESTAMP
                           WHERE match_id = NEW.match_id
                             AND innings = NEW.innings
                             AND overs = NEW.overs
                             AND balls = NEW.balls;
                       END;
                       """,
        "update_missing_matches_timestamp": """
                                            CREATE TRIGGER IF NOT EXISTS update_missing_matches_timestamp
                                                AFTER UPDATE
                                                ON missing_matches
                                            BEGIN
                                                UPDATE missing_matches
                                                SET updated_at = CURRENT_TIMESTAMP
                                                WHERE icc_id = NEW.icc_id;
                                            END;
                                            """,
        "match_summary": """
                       CREATE VIEW IF NOT EXISTS match_summary AS
                       WITH delivery_summary AS ( 
                           SELECT
                               match_id,
                               SUM(CASE WHEN innings = 1 THEN runs_total ELSE 0 END) AS runs_1st_raw,
                               SUM(CASE WHEN innings = 2 THEN runs_total ELSE 0 END) AS runs_2nd_raw,
                               SUM(CASE WHEN innings = 1 THEN wickets ELSE 0 END)    AS wickets_1st,
                               SUM(CASE WHEN innings = 2 THEN wickets ELSE 0 END)    AS wickets_2nd,
                               
                               SUM(CASE WHEN innings = 1 AND powerplay = 1 THEN runs_total ELSE 0 END) AS runs_1st_pp,
                               SUM(CASE WHEN innings = 1 AND powerplay = 1 THEN wickets ELSE 0 END)    AS wickets_1st_pp,
                               SUM(CASE WHEN innings = 2 AND powerplay = 1 THEN runs_total ELSE 0 END) AS runs_2nd_pp,
                               SUM(CASE WHEN innings = 2 AND powerplay = 1 THEN wickets ELSE 0 END)    AS wickets_2nd_pp
                           FROM deliveries
                           GROUP BY match_id
                       )
                       SELECT
                           m.match_id,
                           m.start_date,
                           m.season,
                           m.event_name,
                           t1.full_name AS team1,
                           t2.full_name AS team2,
                           -- Innings scores including penalties
                           COALESCE(ds.runs_1st_raw, 0) + m.team1_prepostpens AS runs_1st_innings,
                           COALESCE(ds.wickets_1st, 0) AS wickets_1st_innings,
                           COALESCE(ds.runs_2nd_raw, 0) + m.team2_prepostpens AS runs_2nd_innings,
                           COALESCE(ds.wickets_2nd, 0) AS wickets_2nd_innings,
                           -- Powerplay runs/wickets (doesn't include any pre-innings penalties)
                           COALESCE(ds.runs_1st_pp, 0) || '-' || COALESCE(ds.wickets_1st_pp, 0) AS score_1st_pp,
                           COALESCE(ds.runs_2nd_pp, 0) || '-' || COALESCE(ds.wickets_2nd_pp, 0) AS score_2nd_pp,
                           -- Toss Information
                           COALESCE(toss_winner.full_name || ' won the toss and chose to ' || m.toss_decision, 'Toss Info Missing/No Toss') AS toss_result,                           
                           CASE 
                               WHEN m.no_result = 1 THEN 'No Result'
                               WHEN m.tie = 1 AND m.super_over_pld = 1 THEN 'Tie (' || winner.full_name || ' won the Super Over)'
                               WHEN m.tie = 1 THEN 'Match Tied'
                               ELSE 
                                   winner.full_name || ' won' ||
                                   CASE 
                                       WHEN m.by_runs = 1 AND m.victory_margin_runs > 1 THEN ' by ' || m.victory_margin_runs || ' runs' 
                                       WHEN m.by_runs = 1 AND m.victory_margin_runs = 1 THEN ' by ' || m.victory_margin_runs || ' run' 
                                       WHEN m.by_wickets = 1 AND m.victory_margin_wickets > 1 THEN ' by ' || m.victory_margin_wickets || ' wickets'
                                       WHEN m.by_wickets = 1 AND m.victory_margin_wickets = 1 THEN ' by ' || m.victory_margin_wickets || ' wicket'
                                       ELSE '' 
                                   END ||
                                   CASE 
                                       WHEN m.DLS = 1 AND m.start_date >= '2014-11-01' THEN ' (DLS method)' 
                                       WHEN m.DLS = 1 AND m.start_date < '2014-11-01' THEN ' (D/L method)'
                                       ELSE '' 
                                   END
                           END AS match_result,
                           v.venue_name,
                           v.city,
                           v.nation AS venue_nation,
                           -- Officials and Player of the Match
                           pom.name AS player_of_match,
                           ump1.name AS umpire_1,
                           ump2.name AS umpire_2,
                           tv_ump.name AS tv_umpire,
                           ref.name AS match_referee
                       FROM
                           matches m
                       LEFT JOIN
                           delivery_summary ds ON m.match_id = ds.match_id
                       LEFT JOIN
                           teams t1 ON m.team1_id = t1.team_id
                       LEFT JOIN
                           teams t2 ON m.team2_id = t2.team_id
                       LEFT JOIN
                           teams winner ON m.winner_id = winner.team_id
                       LEFT JOIN
                           teams toss_winner ON m.toss_winner_id = toss_winner.team_id
                       LEFT JOIN
                           venues v ON m.venue_id = v.venue_id
                       LEFT JOIN
                           registry pom ON m.player_of_match_id = pom.identifier
                       LEFT JOIN
                           registry ump1 ON m.umpire1_id = ump1.identifier
                       LEFT JOIN
                           registry ump2 ON m.umpire2_id = ump2.identifier
                       LEFT JOIN
                           registry tv_ump ON m.tv_umpire_id = tv_ump.identifier
                       LEFT JOIN
                           registry ref ON m.match_referee_id = ref.identifier;
                         """,
        "batting_stats": """
                        CREATE VIEW IF NOT EXISTS batting_stats AS 
                        WITH PlayerMatchParticipation AS (
                            SELECT DISTINCT
                                match_id,
                                batter_id AS player_id
                            FROM deliveries
                            WHERE batter_id IS NOT NULL AND super_over = 0
                            UNION
                            SELECT DISTINCT
                                match_id,
                                non_striker_id AS player_id
                            FROM deliveries
                            WHERE non_striker_id IS NOT NULL AND super_over = 0
                        ),
                             PlayerInningsOutcomes AS (
                                 SELECT 
                                     pmp.match_id,
                                     pmp.player_id,
                                     MAX(CASE
                                             WHEN d.player_out_id = pmp.player_id AND d.how_out NOT IN ('retired hurt', 'retired not out') AND d.super_over = 0 THEN 1
                                             WHEN d.player_out2_id = pmp.player_id AND d.how_out2 NOT IN ('retired hurt', 'retired not out') AND d.super_over = 0 THEN 1
                                             ELSE 0 
                                         END) AS was_out
                                 FROM PlayerMatchParticipation pmp
                                          LEFT JOIN deliveries d ON pmp.match_id = d.match_id AND (d.player_out_id = pmp.player_id OR d.player_out2_id = pmp.player_id)
                                 GROUP BY pmp.match_id, pmp.player_id
                             ),
                            WicketsTotals AS (
                                SELECT player_id, SUM(was_out) as total_outs, COUNT(match_id) as total_innings
                                FROM PlayerInningsOutcomes
                                GROUP BY player_id
                            )
                        SELECT
                            p.unique_name AS uniqueName,
                            p.identifier AS playerId,
                            p.bat_hand AS batHand,
                            -- Innings and Games Calculations
                            wt.total_innings AS batGames,
                            wt.total_outs AS batWickets,
                            (wt.total_innings - wt.total_outs) AS batNotOuts,
                            -- Runs and Balls Calculations
                            SUM(CASE WHEN d.batter_id = p.identifier AND d.super_over = 0 THEN d.runs_batter ELSE 0 END) AS batRuns,
                            SUM(CASE WHEN d.batter_id = p.identifier AND d.extras_wides = 0 AND d.super_over = 0 THEN 1 ELSE 0 END) AS ballsFaced,
                            SUM(CASE WHEN d.batter_id = p.identifier AND d.super_over = 0 THEN 1 ELSE 0 END) AS ballsOnStrike,
                            SUM(CASE WHEN (d.batter_id = p.identifier OR d.non_striker_id = p.identifier) AND d.super_over = 0 THEN 1 ELSE 0 END) AS ballsSeen,
                            -- Scoring Shot Calculations
                            SUM(CASE WHEN d.batter_id = p.identifier AND d.runs_batter = 0 AND d.extras_wides = 0 AND d.extras_noballs = 0 AND d.super_over = 0 THEN 1 ELSE 0 END) AS zeros,
                            SUM(CASE WHEN d.batter_id = p.identifier AND d.runs_batter = 1 AND d.super_over = 0 THEN 1 ELSE 0 END) AS singles,
                            SUM(CASE WHEN d.batter_id = p.identifier AND d.runs_batter = 2 AND d.super_over = 0 THEN 1 ELSE 0 END) AS doubles,
                            SUM(CASE WHEN d.batter_id = p.identifier AND d.runs_batter = 3 AND d.super_over = 0 THEN 1 ELSE 0 END) AS triples,
                            SUM(CASE WHEN d.batter_id = p.identifier AND d.runs_batter = 4 AND d.runs_batter_non_boundary = 1 AND d.super_over = 0 THEN 1 ELSE 0 END) AS quadruples,
                            SUM(CASE WHEN d.batter_id = p.identifier AND d.runs_batter = 5 AND d.super_over = 0 THEN 1 ELSE 0 END) AS quintuples,
                            SUM(CASE WHEN d.batter_id = p.identifier AND d.runs_batter = 6 AND d.runs_batter_non_boundary = 1 AND d.super_over = 0 THEN 1 ELSE 0 END) AS sextuples,
                            SUM(CASE WHEN d.batter_id = p.identifier AND d.runs_batter = 7 AND d.super_over = 0 THEN 1 ELSE 0 END) AS septuples,
                            SUM(CASE WHEN d.batter_id = p.identifier AND d.runs_batter = 4 AND d.runs_batter_non_boundary = 0 AND d.super_over = 0 THEN 1 ELSE 0 END) AS fours,
                            SUM(CASE WHEN d.batter_id = p.identifier AND d.runs_batter = 6 AND d.runs_batter_non_boundary = 0 AND d.super_over = 0 THEN 1 ELSE 0 END) AS sixes,
                            -- Extras Faced
                            SUM(CASE WHEN d.batter_id = p.identifier AND d.extras_wides > 0 AND d.super_over = 0 THEN 1 ELSE 0 END) AS widesFaced,
                            SUM(CASE WHEN d.batter_id = p.identifier AND d.extras_noballs > 0 AND d.super_over = 0 THEN 1 ELSE 0 END) AS noBallsFaced,
                            -- Dismissal Calculations
                            SUM(CASE WHEN p.identifier = d.player_out_id AND d.how_out = 'bowled' AND d.super_over = 0 THEN 1 ELSE 0 END) AS bowled,
                            SUM(CASE WHEN p.identifier = d.player_out_id AND d.how_out = 'caught' AND d.super_over = 0 THEN 1 ELSE 0 END) AS caught,
                            SUM(CASE WHEN p.identifier = d.player_out_id AND d.how_out = 'caught and bowled' AND d.super_over = 0 THEN 1 ELSE 0 END) AS caughtAndBowled,
                            SUM(CASE WHEN p.identifier = d.player_out_id AND d.how_out = 'lbw' AND d.super_over = 0 THEN 1 ELSE 0 END) AS lbw,
                            SUM(CASE WHEN p.identifier = d.player_out_id AND d.how_out = 'stumped' AND d.super_over = 0 THEN 1 ELSE 0 END) AS stumped,
                            SUM(CASE WHEN p.identifier IN (d.player_out_id, d.player_out2_id) AND (d.how_out = 'run out' OR d.how_out2 = 'run out') AND d.super_over = 0 THEN 1 ELSE 0 END) AS runOut,
                            SUM(CASE WHEN p.identifier = d.player_out_id AND d.how_out = 'hit wicket' AND d.super_over = 0 THEN 1 ELSE 0 END) AS hitWicket,
                            SUM(CASE WHEN p.identifier = d.player_out_id AND d.how_out = 'obstructing the field' AND d.super_over = 0 THEN 1 ELSE 0 END) AS obstructingTheField,
                            SUM(CASE WHEN p.identifier = d.player_out_id AND d.how_out = 'hit the ball twice' AND d.super_over = 0 THEN 1 ELSE 0 END) AS hitTheBallTwice,
                            SUM(CASE WHEN p.identifier = d.player_out_id AND d.how_out = 'handled the ball' AND d.super_over = 0 THEN 1 ELSE 0 END) AS handledTheBall,
                            SUM(CASE WHEN p.identifier IN (d.player_out_id, d.player_out2_id) AND (d.how_out = 'timed out' OR d.how_out2 = 'timed out') AND d.super_over = 0 THEN 1 ELSE 0 END) AS timedOut,
                            SUM(CASE WHEN p.identifier IN (d.player_out_id, d.player_out2_id) AND (d.how_out = 'retired out' OR d.how_out2 = 'retired out') AND d.super_over = 0 THEN 1 ELSE 0 END) AS retiredOut,
                            -- Powerplay Specific Stats
                            SUM(CASE WHEN d.batter_id = p.identifier AND d.powerplay = 1 THEN d.runs_batter ELSE 0 END) AS ppRuns,
                            SUM(CASE WHEN d.batter_id = p.identifier AND d.extras_wides = 0 AND d.powerplay = 1 THEN 1 ELSE 0 END) AS ppBallsFaced,
                            SUM(CASE WHEN (d.player_out_id = p.identifier OR d.player_out2_id = p.identifier) AND d.powerplay = 1 THEN 1 ELSE 0 END) AS ppOuts,
                            -- Super Over Career Stats (Separated)
                            SUM(CASE WHEN d.batter_id = p.identifier AND d.super_over = 1 THEN d.runs_batter ELSE 0 END) AS superOverRuns,
                            SUM(CASE WHEN d.batter_id = p.identifier AND d.extras_wides = 0 AND d.super_over = 1 THEN 1 ELSE 0 END) AS superOverBallsFaced,
                            SUM(CASE WHEN (d.player_out_id = p.identifier OR d.player_out2_id = p.identifier) AND d.super_over = 1 THEN 1 ELSE 0 END) AS superOverOuts
                        FROM players p
                                 JOIN WicketsTotals wt ON p.identifier = wt.player_id
                                 LEFT JOIN deliveries d ON (p.identifier = d.batter_id OR p.identifier = d.non_striker_id)
                            GROUP BY p.identifier;
                        """,
        "bowling_stats": """
                         CREATE VIEW IF NOT EXISTS bowling_stats AS
                         SELECT
                             p.unique_name AS uniqueName,
                             p.identifier AS playerId,
                             p.bowl_hand AS bowlHand,
                             p.bowl_style AS bowlType,
                             -- Games Bowled
                             COUNT(DISTINCT CASE WHEN d.super_over = 0 THEN d.match_id END) AS bowlGames,
                             -- Wickets (only those attributable to the bowler)
                             SUM(CASE WHEN d.super_over = 0  AND d.how_out IN ('bowled', 'caught', 'caught and bowled', 'lbw', 'stumped', 'hit wicket') THEN 1 ELSE 0 END) AS bowlWickets,
                             -- Runs Conceded
                             SUM(CASE WHEN d.super_over = 0 THEN (d.runs_batter + d.extras_wides + d.extras_noballs) ELSE 0 END) AS bowlRuns,                            
                             SUM(CASE WHEN d.super_over = 0 THEN d.runs_batter ELSE 0 END) AS bowlRunsBat,
                             -- Balls and Overs
                             COUNT(CASE WHEN d.super_over = 0 THEN d.balls ELSE 0 END) AS ballsBowled,
                             SUM(CASE WHEN d.super_over = 0 AND d.extras_wides = 0 AND d.extras_noballs = 0 THEN 1 ELSE 0 END) AS ballsBowledLegal,
                             CAST(SUM(CASE WHEN d.super_over = 0 AND d.extras_wides = 0 AND d.extras_noballs = 0 THEN 1 ELSE 0 END) / 6 AS TEXT) || '.' || CAST(SUM(CASE WHEN d.super_over = 0 AND d.extras_wides = 0 AND d.extras_noballs = 0 THEN 1 ELSE 0 END) % 6 AS TEXT) AS overs,
                             -- Runs conceded breakdown
                             SUM(CASE WHEN d.super_over = 0 AND d.runs_batter = 0 AND d.extras_wides = 0 AND d.extras_noballs = 0 THEN 1 ELSE 0 END) AS zeros,
                             SUM(CASE WHEN d.super_over = 0 AND d.runs_batter = 1 THEN 1 ELSE 0 END) AS singles,
                             SUM(CASE WHEN d.super_over = 0 AND d.runs_batter = 2 THEN 1 ELSE 0 END) AS doubles,
                             SUM(CASE WHEN d.super_over = 0 AND d.runs_batter = 3 THEN 1 ELSE 0 END) AS triples,
                             SUM(CASE WHEN d.super_over = 0 AND d.runs_batter = 4 AND d.runs_batter_non_boundary = 1 THEN 1 ELSE 0 END) AS quadruples,
                             SUM(CASE WHEN d.super_over = 0 AND d.runs_batter = 5 THEN 1 ELSE 0 END) AS quintuples,
                             SUM(CASE WHEN d.super_over = 0 AND d.runs_batter = 6 AND d.runs_batter_non_boundary = 1 THEN 1 ELSE 0 END) AS sextuples,
                             SUM(CASE WHEN d.super_over = 0 AND d.runs_batter = 7 THEN 1 ELSE 0 END) AS septuples,
                             SUM(CASE WHEN d.super_over = 0 AND d.runs_batter = 4 AND d.runs_batter_non_boundary = 0 THEN 1 ELSE 0 END) AS fours,
                             SUM(CASE WHEN d.super_over = 0 AND d.runs_batter = 6 AND d.runs_batter_non_boundary = 0 THEN 1 ELSE 0 END) AS sixes,
                             -- Extras Bowled
                             SUM(CASE WHEN d.super_over = 0 THEN d.extras_wides ELSE 0 END) AS widesBowled,
                             SUM(CASE WHEN d.super_over = 0 THEN d.extras_noballs ELSE 0 END) AS noBallsBowled,
                             SUM(CASE WHEN d.super_over = 0 AND d.extras_legbyes > 0 THEN 1 ELSE 0 END) AS legByesCount,
                             SUM(CASE WHEN d.super_over = 0 THEN d.extras_legbyes ELSE 0 END) AS legByesRuns,
                             -- Dismissal types for wickets taken
                             SUM(CASE WHEN d.super_over = 0 AND d.how_out = 'bowled' THEN 1 ELSE 0 END) AS bowled,
                             SUM(CASE WHEN d.super_over = 0 AND d.how_out = 'caught' THEN 1 ELSE 0 END) AS caught,
                             SUM(CASE WHEN d.super_over = 0 AND d.how_out = 'caught and bowled' THEN 1 ELSE 0 END) AS caughtAndBowled,
                             SUM(CASE WHEN d.super_over = 0 AND d.how_out = 'lbw' THEN 1 ELSE 0 END) AS lbw,
                             SUM(CASE WHEN d.super_over = 0 AND d.how_out = 'stumped' THEN 1 ELSE 0 END) AS stumped,
                             SUM(CASE WHEN d.super_over = 0 AND d.how_out = 'hit wicket' THEN 1 ELSE 0 END) AS hitWicket,
                             -- Powerplay Specific Stats
                             CAST(SUM(CASE WHEN d.powerplay = 1 AND d.extras_wides = 0 AND d.extras_noballs = 0 THEN 1 ELSE 0 END) / 6 AS TEXT) || '.' || CAST(SUM(CASE WHEN d.powerplay = 1 AND d.extras_wides = 0 AND d.extras_noballs = 0 THEN 1 ELSE 0 END) % 6 AS TEXT) AS ppOvers,
                             SUM(CASE WHEN d.powerplay = 1 AND d.extras_wides = 0 AND d.extras_noballs = 0 THEN 1 ELSE 0 END) AS ppBallsBowledLegal,
                             SUM(CASE WHEN d.powerplay = 1 AND d.how_out IN ('bowled', 'caught', 'caught and bowled', 'lbw', 'stumped', 'hit wicket') THEN 1 ELSE 0 END) AS ppWickets,
                             SUM(CASE WHEN d.powerplay = 1 THEN (d.runs_batter + d.extras_wides + d.extras_noballs) ELSE 0 END) AS ppRunsConceded,
                             -- Super Over Career Stats
                             CAST(SUM(CASE WHEN d.super_over = 1 AND d.extras_wides = 0 AND d.extras_noballs = 0 THEN 1 ELSE 0 END) / 6 AS TEXT) || '.' || CAST(SUM(CASE WHEN d.super_over = 1 AND d.extras_wides = 0 AND d.extras_noballs = 0 THEN 1 ELSE 0 END) % 6 AS TEXT) AS superOvers,
                             SUM(CASE WHEN d.super_over = 1 THEN (d.runs_batter + d.extras_wides + d.extras_noballs) ELSE 0 END) AS superOverRunsConceded,
                             SUM(CASE WHEN d.super_over = 1 AND d.extras_wides = 0 AND d.extras_noballs = 0 THEN 1 ELSE 0 END) AS superOverBallsBowledLegal,
                             SUM(CASE WHEN d.super_over = 1 AND d.how_out IN ('bowled', 'caught', 'caught and bowled', 'lbw', 'stumped', 'hit wicket') THEN 1 ELSE 0 END) AS superOverWickets
                         FROM players p
                                  JOIN deliveries d ON p.identifier = d.bowler_id
                         GROUP BY p.identifier;
                         """
    }

    def _init_database(self):
        """Initialize database with schema version tracking"""
        with db_connection(self.db_name) as conn:
            cursor = conn.cursor()

            # Create schema version table
            cursor.execute("""
                           CREATE TABLE IF NOT EXISTS schema_version
                           (
                               version    TEXT NOT NULL,
                               applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                           )
                           """)

            # Check if the schema needs to be initialised
            cursor.execute("SELECT version FROM schema_version ORDER BY applied_at DESC LIMIT 1")
            result = cursor.fetchone()

            if not result:
                self._create_schema(conn)
                cursor.execute(
                    "INSERT INTO schema_version (version) VALUES (?)",
                    (self.config.SCHEMA_VERSION,)
                )
                conn.commit()

    def _create_schema(self, conn: sqlite3.Connection):
        cursor = conn.cursor()
        for table_name, sql in self.SCHEMA_SQL.items():
            cursor.execute(sql)
        conn.commit()

    def backup_database(self):
        """Create a timestamped backup of the database"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = os.path.join(
            self.config.BACKUP_DIR,
            f"CricBase_backup_{timestamp}.db"
        )

        try:
            with db_connection(self.db_name) as conn:
                with sqlite3.connect(backup_path) as backup_conn:
                    conn.backup(backup_conn)
            logging.info(f"Database backed up successfully to {backup_path}")
        except Exception as e:
            logging.error(f"Backup failed: {e}")
            raise BuildError(f"Failed to backup database: {e}")

    def reset_database(self):
        objects_to_drop = {
            "view": ["batting_stats", "bowling_stats", "match_summary"],
            "trigger": [
                "update_registry_timestamp", "update_teams_timestamp", "update_venues_timestamp",
                "update_venue_aliases_timestamp",
                "update_matches_timestamp", "update_match_metadata_timestamp", "update_match_players_timestamp",
                "update_players_timestamp", "update_officials_timestamp",
                "update_deliveries_timestamp", "update_missing_matches_timestamp", "check_review_by_team",
                "check_correct_team_players", "check_review_ump_official"
            ],
            "table": [
                "missing_matches", "deliveries", "match_players", "match_metadata", "matches", "venue_aliases",
                "venues", "players", "officials", "teams", "registry", "schema_version"
            ],
            "index": [
                "idx_matches_date", "idx_matches_team", "idx_deliveries_match_innings", "idx_deliveries_batter",
                "idx_deliveries_bowler",
                "idx_deliveries_dismissal", "idx_registry_unique_name", "idx_matches_venue", "idx_match_players_match",
                "idx_match_players_identifier",
                "idx_deliveries_match_batter", "idx_deliveries_match_bowler", "idx_deliveries_fielder1"
            ]
        }
        with db_connection(self.db_name) as conn:
            cursor = conn.cursor()
            logging.info("Starting database reset...")
            for obj_type, obj_names in objects_to_drop.items():
                for name in obj_names:
                    try:
                        cursor.execute(f"DROP {obj_type.upper()} IF EXISTS {name}")
                        logging.info(f"Dropped {obj_type}: {name}")
                    except sqlite3.Error as e:
                        logging.warning(f"Could not drop {obj_type} {name}: {e}")
            conn.commit()
            logging.info("Database reset complete. Re-initializing schema...")
            self._init_database()
            logging.info("Database schema re-initialized successfully.")

    def prepare_for_update(self):
        with db_connection(self.db_name) as conn:
            cursor = conn.cursor()
            logging.info("Preparing for database reset...")
            cursor.execute("BEGIN TRANSACTION")
            try:
                cursor.execute(f"DELETE FROM missing_matches")
                conn.commit()
                logging.info(f"missing_matches table cleared.")
            except sqlite3.Error as e:
                conn.rollback()
                logging.error(f"Could not clear missing_matches: {e}")

    def verify_data_integrity(self):
        with db_connection(self.db_name) as conn:
            cursor = conn.cursor()
            integrity_issues = []

            # Check for orphaned records in deliveries
            cursor.execute("""
                           SELECT d.match_id, d.batter_id, d.bowler_id, d.non_striker_id
                           FROM deliveries d
                                    LEFT JOIN registry r1 ON d.batter_id = r1.identifier
                                    LEFT JOIN registry r2 ON d.bowler_id = r2.identifier
                                    LEFT JOIN registry r3 ON d.non_striker_id = r3.identifier
                           WHERE r1.identifier IS NULL
                              OR r2.identifier IS NULL
                              OR r3.identifier IS NULL
                           """)
            orphaned_deliveries = cursor.fetchall()
            if orphaned_deliveries:
                integrity_issues.append(f"Found {len(orphaned_deliveries)} deliveries with missing player references")

            # Check for deliveries with no associated match
            cursor.execute("""
                           SELECT d.match_id
                           FROM deliveries d
                                    LEFT JOIN matches m ON d.match_id = m.match_id
                           WHERE m.match_id IS NULL
                           """)
            orphaned_match_deliveries = cursor.fetchall()
            if orphaned_match_deliveries:
                unique_ids = len(set([row[0] for row in orphaned_match_deliveries]))
                integrity_issues.append(
                    f"Found {unique_ids} unique match_ids in deliveries with no associated matches entry.")

            # Check for match_metadata with no associated match
            cursor.execute("""
                           SELECT mm.match_id
                           FROM match_metadata mm
                                    LEFT JOIN matches m ON mm.match_id = m.match_id
                           WHERE m.match_id IS NULL
                           """)
            orphaned_metadata = cursor.fetchall()
            if orphaned_metadata:
                unique_ids = len(set([row[0] for row in orphaned_metadata]))
                integrity_issues.append(
                    f"Found {unique_ids} unique match_ids in match_metadata with no associated matches entry."
                )

            # Check for match_players with no associated match
            cursor.execute("""
                           SELECT mp.match_id, mp.identifier
                           FROM match_players mp
                                    LEFT JOIN matches m ON mp.match_id = m.match_id
                           WHERE m.match_id IS NULL
                           """)
            orphaned_match_players = cursor.fetchall()
            if orphaned_match_players:
                unique_ids = len(set([row[0] for row in orphaned_match_players]))
                integrity_issues.append(
                    f"Found {unique_ids} unique match_ids in match_players with no associated matches entry.")

            # Check for matches with invalid dates
            cursor.execute("""
                           SELECT match_id, start_date, end_date
                           FROM matches
                           WHERE start_date > end_date
                              OR start_date IS NULL
                              OR end_date IS NULL
                           """)
            invalid_dates = cursor.fetchall()
            if invalid_dates:
                integrity_issues.append(f"Found {len(invalid_dates)} matches with invalid dates")

            # Verify match result consistency
            cursor.execute("""
                           SELECT match_id
                           FROM matches
                           WHERE (by_runs > 0 AND by_wickets > 0)
                              OR (winner_id IS NOT NULL AND no_result = 1)
                              OR (winner_id IS NOT NULL AND tie = 1 AND super_over = 0)
                           """)
            inconsistent_results = cursor.fetchall()
            if inconsistent_results:
                integrity_issues.append(f"Found {len(inconsistent_results)} matches with inconsistent results")

            # Verify match player entries
            cursor.execute("""
                           SELECT mp.match_id, mp.identifier
                           FROM match_players mp
                                    LEFT JOIN matches m ON mp.match_id = m.match_id
                           WHERE m.match_id IS NULL
                           """)
            invalid_match_players = cursor.fetchall()
            if invalid_match_players:
                integrity_issues.append(f"Found {len(invalid_match_players)} invalid match-player associations")

            # Check for missing team references
            cursor.execute("""
                           SELECT DISTINCT match_id
                           FROM match_players mp
                           WHERE team_id NOT IN (SELECT team1_id
                                                 FROM matches m
                                                 WHERE m.match_id = mp.match_id
                                                 UNION
                                                 SELECT team2_id
                                                 FROM matches m
                                                 WHERE m.match_id = mp.match_id)
                           """)
            invalid_teams = cursor.fetchall()
            if invalid_teams:
                integrity_issues.append(f"Found {len(invalid_teams)} matches with invalid team references")

            if integrity_issues:
                for issue in integrity_issues:
                    logging.warning(issue)
                raise BuildError("Database integrity checks failed. See logs for details.")
            else:
                logging.info("All database integrity checks passed successfully")

            return len(integrity_issues) == 0


