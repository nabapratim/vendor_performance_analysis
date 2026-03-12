import logging
from pathlib import Path
from ingestion import ingestion_csv
from run_pipeline import run_pipeline
from db import get_engine

engine = get_engine()

#-----------------
# Logging Setup
#-----------------

LOG_DIR = Path("logs")

LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR/"pipeline.log"

logging.basicConfig(
    level= logging.INFO,
    format="%(asctime)s|%(name)s|%(levelname)s|%(message)s",
    handlers=[ 
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
        ]
    )

logger = logging.getLogger(__name__)

DATA_DIR = Path("../00_data/raw")
OUTPUT_DIR = Path("../00_data/outputs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


#------------------------------
# CSV Ingestion
#------------------------------

def run_csv_ingestion():
    csv_files = list(DATA_DIR.glob("*.CSV"))

    if not csv_files:
        logger.warning("No csv files found")
        return
    for file in csv_files:
        table = 'stg_'+ file.stem.lower()

        logger.info(f"Discovered {file.name}--->{table}")

        ingestion_csv(file, table)


#------------------------------
# MAIN ORCHESTRATOR
#------------------------------

def main():

    try:
        logger.info("Pipeline started")
        # Ingest CSV files into the database
        run_csv_ingestion()
        # Run the stored procedure to refresh the data warehouse
        run_pipeline(engine, "refresh_warehouse")
        logger.info("Pipeline completed successfully")
    except Exception:
        logger.exception("Pipeline failed")

if __name__ == "__main__":
    main()




