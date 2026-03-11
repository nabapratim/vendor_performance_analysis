import pandas as pd
import logging
from db import get_engine
from pathlib import Path

logger = logging.getLogger(__name__)

DATA_DIR = Path("../00_data/raw")
BRONZE_SCHEMA = 'bronze'

def ingestion_csv(file_path:Path, table_name: str=None, chunksize:int= 50_000):
    
    file_path = Path(file_path)
    
    if file_path.suffix.lower() !=".csv":
        raise ValueError(f"{file_path.name} is not a csv file")
   
    if not file_path.exists():
        raise FileNotFoundError(file_path)
   
    if table_name is None:
        table_name = "stg_" + file_path.stem.lower()
    
    engine = get_engine()

    
    logger.info(f"ingesting {file_path}--> {table_name}")

    try:
        for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunksize)):
            logger.info(f"{file_path}| chunk{i+1}")
            chunk.to_sql(table_name, engine, schema = BRONZE_SCHEMA, if_exists='append', index = False, method = 'multi')
            logger.info(f"{file_path.name} ingestion complete")
    
    except:
        logger.exception(f"ingestion failed for {file_path.name}")
        raise