import pandas as pd
import logging
from pathlib import Path
from db import get_engine

logger = logging.getLogger(__name__)

SQL_DIR = Path("../01_sql")

KPI_DIR = SQL_DIR/"KPI"

def _read_sql(path: Path):
    if not path.exists():
        raise FileNotFoundError(path)
    return path.read_text()

#----------------------------
# Base query (large- chunked)
#----------------------------

def load_base_data(chunksize: int=100_000):

    query_path = SQL_DIR/'base_query.sql'

    query = _read_sql(query_path)

    engine = get_engine()

    logger.info("Running base query (chunked)")

    for i, chunk in enumerate(pd.read_sql(query, engine, chunksize=chunksize)):
        logger.info(f"Base chunk {i+1}")
        yield chunk
    #---------------------------
    # single kpi
    #---------------------------
def load_kpi(sql_file: Path):
        engine = get_engine()
        query = _read_sql(sql_file)
        logger.info(f"Running KPI: {sql_file.name}") 
        df = pd.read_sql(query, engine)
        logger.info(f"{sql_file.name} rows: {len(df)}")
        return df
    

def load_all_kpis():
     kpis = {}
     for sql_file in KPI_DIR.glob("*.sql"):
          df = load_kpi(sql_file)
          kpis[sql_file.stem] = df
     return kpis  





























