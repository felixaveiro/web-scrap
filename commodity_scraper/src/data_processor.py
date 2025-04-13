# src/data_processor.py

import pandas as pd
from typing import Optional
import os
import logging
from config.settings import CONFIG

logger = logging.getLogger(__name__)

def save_to_csv(df: Optional[pd.DataFrame]) -> None:
    """Save DataFrame to CSV."""
    if df is not None and not df.empty:
        df.to_csv(CONFIG['csv_file'], index=False)
        logger.info(f"Data saved to {CONFIG['csv_file']}, rows: {len(df)}")
    else:
        logger.warning("No data to save to CSV")

def read_csv() -> Optional[pd.DataFrame]:
    """Read data from CSV."""
    try:
        return pd.read_csv(CONFIG['csv_file'])
    except FileNotFoundError:
        logger.warning("CSV file not found")
        return None

def clean_data(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """Clean and transform DataFrame."""
    if df is None or df.empty:
        logger.warning("No data to clean")
        return None

    try:
        logger.debug(f"Raw DataFrame shape: {df.shape}")
        logger.debug(f"Raw columns: {df.columns.tolist()}")
        logger.debug(f"Raw sample:\n{df.head(2).to_string()}")
        
        # Standardize column names
        df.columns = [str(col).strip().replace(' ', '_').replace('%', 'percent').lower() for col in df.columns]
        logger.debug(f"Standardized columns: {df.columns.tolist()}")
        
        # Dynamic column mapping
        col_map = {}
        for col in df.columns:
            col_lower = col.lower()
            if any(k in col_lower for k in ['commodity', 'name', 'item']):
                col_map[col] = 'commodity'
            elif 'price' in col_lower:
                col_map[col] = 'price'
            elif any(k in col_lower for k in ['change', 'chg', 'percent']):
                col_map[col] = 'change'
        
        # Ensure commodity column
        if not col_map.get('commodity') and len(df.columns) > 0:
            col_map[df.columns[0]] = 'commodity'
        df = df.rename(columns=col_map)
        logger.debug(f"Renamed columns: {df.columns.tolist()}")
        
        # Initialize missing columns
        for col in ['commodity', 'price', 'change']:
            if col not in df.columns:
                df[col] = pd.NA
                logger.warning(f"Added missing column: {col}")
        
        # Drop rows with missing commodity
        if 'commodity' in df.columns:
            df = df.dropna(subset=['commodity'])
            logger.debug(f"After dropping missing commodities, shape: {df.shape}")
        if df.empty:
            logger.warning("No valid data after dropping missing commodities")
            return None
        
        # Clean price
        if 'price' in df.columns:
            try:
                df['price'] = (
                    df['price'].astype(str)
                    .str.replace(',', '', regex=False)
                    .str.replace('$', '', regex=False)
                    .str.replace('[^0-9.-]', '', regex=True)
                    .str.strip()
                    .pipe(pd.to_numeric, errors='coerce')
                )
                # Instead of dropping, fill invalid prices with NaN
                invalid_prices = df['price'].isna() | (df['price'] <= 0)
                if invalid_prices.any():
                    logger.warning(f"Found {invalid_prices.sum()} invalid prices, filling with NaN")
                    df.loc[invalid_prices, 'price'] = pd.NA
                logger.debug(f"Price column stats: {df['price'].describe().to_dict()}")
            except Exception as e:
                logger.error(f"Error cleaning price column: {e}")
                df['price'] = pd.NA
        
        # Clean change
        if 'change' in df.columns:
            try:
                df['change'] = (
                    df['change'].astype(str)
                    .str.replace('%', '', regex=False)
                    .str.replace(',', '', regex=False)
                    .str.strip()
                    .pipe(pd.to_numeric, errors='coerce')
                    .div(100)
                    .clip(-1, 1)
                )
                # Fill missing changes with 0
                df['change'] = df['change'].fillna(0)
                logger.debug(f"Change column stats: {df['change'].describe().to_dict()}")
            except Exception as e:
                logger.error(f"Error cleaning change column: {e}")
                df['change'] = 0
        
        # Clean timestamp
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df['timestamp'] = df['timestamp'].fillna(pd.Timestamp.now())
        
        # Drop duplicates
        df = df.drop_duplicates()
        logger.debug(f"After dropping duplicates, shape: {df.shape}")
        
        # Validate agricultural commodities
        if 'commodity' in df.columns:
            df['commodity_lower'] = df['commodity'].str.lower()
            agri_count = df['commodity_lower'].isin(CONFIG['agri_terms']).sum()
            df = df.drop(columns=['commodity_lower'])
            logger.info(f"Found {agri_count} agricultural commodities")
            # Relaxed constraint: warn but proceed with data
            if agri_count < 1:
                logger.warning(f"Low agricultural commodity count ({agri_count}), proceeding anyway")
        
        if df.empty:
            logger.warning("DataFrame empty after cleaning")
            return None
        
        logger.info(f"Cleaned DataFrame shape: {df.shape}")
        logger.info(f"Cleaned columns: {df.columns.tolist()}")
        logger.debug(f"Cleaned sample:\n{df.head().to_string()}")
        return df
    except Exception as e:
        logger.error(f"Error cleaning data: {e}")
        return None