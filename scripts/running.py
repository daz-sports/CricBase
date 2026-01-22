import os
from config import Config
from utils import BuildError, logging
from create_schema import CricketDatabase
from csv_loader import load_all_static_data
from cricsheet_loader import load_all_cricsheet_data

def main(config: Config, full_reset: bool = False):
    """
    Runs the database build process.
    :param config: The build configuration object.
    :param full_reset: If True, wipes the DB and starts fresh (or builds it if it doesn't exist).
    If False, updates existing DB.
    """
    mode_text = "FULL RE-INITIALIZATION" if full_reset else "INCREMENTAL UPDATE"

    logging.info("=" * 50)
    logging.info("STARTING CRICKET DATABASE BUILD PROCESS")
    logging.info("=" * 50)

    try:
        if not os.path.exists(config.BACKUP_DIR):
            os.makedirs(config.BACKUP_DIR)

        db_manager = CricketDatabase(config)

        db_manager.backup_database()

        if full_reset:
            logging.info(f"[STEP 1/4] Initializing database reset...")
            db_manager.reset_database()
            logging.info("Database schema reset successfully.")
        else:
            logging.info(f"[STEP 1/4] Maintaining existing database...")
            db_manager.prepare_for_update()

        logging.info("[STEP 2/4] Loading static data from CSV files...")
        load_all_static_data(config)
        logging.info("Static data loaded successfully.")

        logging.info("[STEP 3/4] Loading match data from Cricsheet JSON files...")
        if not os.path.isdir(config.CRICSHEET_JSON_DIR):
            raise BuildError(f"Cricsheet directory not found at: {config.CRICSHEET_JSON_DIR}")

        load_all_cricsheet_data(
            config,
            config.DB_NAME,
            config.CRICSHEET_JSON_DIR,
            config.ADDITIONAL_JSON_DIR,
            config.SCRAPE_START_YEAR,
            config.SCRAPE_START_MONTH,
            config.SCRAPE_END_YEAR,
            config.SCRAPE_END_MONTH,
            config.USER_AGENT
        )

        logging.info("Cricsheet data loaded successfully.")
        db_manager.post_update_queries()
        logging.info("[STEP 4/4] Verifying database integrity...")
        db_manager.verify_data_integrity()
        logging.info("Database integrity verified.")

    except (BuildError, FileNotFoundError, ValueError) as e:
        logging.critical(f"\nBUILD FAILED: A critical error occurred: {e}")
        return

    logging.info("=" * 50)
    logging.info(f"DATABASE {mode_text} COMPLETED SUCCESSFULLY")
    logging.info("=" * 50)


if __name__ == '__main__':
    build_config = Config()

    logging.info("--- Cricket Database Manager ---")
    logging.info("1. Full Re-initialization (Wipe and rebuild)")
    logging.info("2. Update (Add new data to existing database)")
    logging.info("Select an option (1 or 2): ")

    choice = input().strip()

    if choice == '1':
        logging.info("Are you sure you want to proceed? (y/n):")
        confirm = input().lower()
        if confirm != 'y':
            logging.info("Invalid selection. Exiting.")
        else:
            main(build_config, full_reset=True)
    elif choice == '2':
        main(build_config, full_reset=False)
    else:
        logging.info("Invalid selection. Exiting.")