import logging
from pathlib import Path
from ingestion import ingestion_csv
from extraction import load_base_data, load_all_kpis


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
# KPI Extraction
#------------------------------

def run_kpi_pipeline():
    kpis = load_all_kpis()

    for name, df in kpis.items():
        
        # save internal format
        parquet_path = OUTPUT_DIR/f"{name}.parquet"
        df.to_parquet(parquet_path, index=False)

        # save presentation-friendly format
        csv_path = OUTPUT_DIR/f"{name}.csv"
        df.to_csv(csv_path, index=False)

        logger.info(f"Saved KPI: {name}")


#------------------------------
# Base Processing
#------------------------------

def run_base_processing():
    
    total_rows = 0

    for chunk in load_base_data():
        total_rows += len(chunk)

        if "order_value" in chunk.columns:
            chunk['order_values'] = chunk["order_values"].clip(lower=0)
    
    logger.info(f"Processed {total_rows} base rows")

#------------------------------
# MAIN ORCHESTRATOR
#------------------------------

def main():

    try:
        logger.info("Pipeline started")

        run_csv_ingestion()
        #run_kpi_pipeline()
        #run_base_processing()

        logger.info("Pipeline completed successfully")
    except Exception:
        logger.exception("Pipeline failed")

if __name__ == "__main__":
    main()




