import os
from config import Config
from utils import BuildError, logging
from create_schema import CricketDatabase
from csv_loader import load_all_static_data
from cricsheet_loader import load_all_cricsheet_data

def main(config: Config):
    """
    This runs the full database build process in the correct sequence.
    1. Resets and initializes the database schema.
    2. Loads all static data from CSV files.
    3. Extracts, transforms, and loads all match data from Cricsheet JSONs.
    4. Verifies the final database integrity.
    """
    logging.info("=" * 50)
    logging.info("STARTING CRICKET DATABASE BUILD PROCESS")
    logging.info("=" * 50)

    try:
        logging.info("[STEP 1/4] Initializing database schema...")
        if not os.path.exists(config.BACKUP_DIR):
            os.makedirs(config.BACKUP_DIR)

        db_manager = CricketDatabase(config)
        db_manager.backup_database()
        db_manager.reset_database()
        logging.info("Database schema initialized successfully.")

        logging.info("\n[STEP 2/4] Loading static data from CSV files...")
        load_all_static_data(config)
        logging.info("Static data loaded successfully.")

        logging.info("\n[STEP 3/4] Loading match data from Cricsheet JSON files...")
        if not os.path.isdir(config.CRICSHEET_JSON_DIR):
            raise BuildError(f"Cricsheet directory not found at: {config.CRICSHEET_JSON_DIR}")
        load_all_cricsheet_data(config.DB_NAME, config.CRICSHEET_JSON_DIR)
        logging.info("Cricsheet data loaded successfully.")

        logging.info("\n[STEP 4/4] Verifying database integrity...")
        db_manager.verify_data_integrity()
        logging.info("Database integrity verified.")

    except (BuildError, FileNotFoundError, ValueError) as e:
        logging.critical(f"\nBUILD FAILED: A critical error occurred: {e}")
        return

    logging.info("=" * 50)
    logging.info("DATABASE BUILD PROCESS COMPLETED SUCCESSFULLY")
    logging.info(f"Database created at: {config.DB_NAME}")
    logging.info("=" * 50)


if __name__ == '__main__':
    build_config = Config()
    main(build_config)