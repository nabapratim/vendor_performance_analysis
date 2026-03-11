import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / "config"/"db_config.env"

print("Loading environment variables from:", ENV_PATH)
print("File Exists:", ENV_PATH.exists())


load_dotenv(dotenv_path=ENV_PATH)

def get_engine():

    url = os.getenv("DB_URL")
    if not url:
        logger.error("url not set")
        raise ValueError("url missing")
    engine = create_engine(url,pool_size=5, max_overflow=10, pool_pre_ping=True)
    logger.info("Database Engine Created")
    return engine

    