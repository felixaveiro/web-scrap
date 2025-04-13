# src/database.py

from sqlalchemy import create_engine
import pandas as pd
from typing import Optional
import logging
from config.settings import CONFIG

logger = logging.getLogger(__name__)

def load_to_mysql(df: Optional[pd.DataFrame]) -> None:
    """Load DataFrame to MySQL."""
    if df is None or df.empty:
        logger.warning("No data to load to MySQL")
        return

    try:
        engine = create_engine(
            f"mysql+mysqlconnector://DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root123',
    'database': 'commodities_db'
}@"
            f"{CONFIG['db']['host']}/{CONFIG['db']['database']}"
        )
        with engine.connect() as conn:
            df.to_sql(CONFIG['table_name'], conn, if_exists='replace', index=False)
        logger.info(f"Data loaded to MySQL, rows: {len(df)}")
    except Exception as e:
        logger.error(f"Error loading to MySQL: {e}")