# src/scraper.py

import requests
from bs4 import BeautifulSoup
import pandas as pd
from typing import Optional
import logging
from config.settings import CONFIG

logger = logging.getLogger(__name__)

def scrape_commodities() -> Optional[pd.DataFrame]:
    """Scrape the agricultural commodity table from the target website."""
    try:
        response = requests.get(
            CONFIG['https://tradingeconomics.com/commodities'],
            headers={'User-Agent': CONFIG['user_agent']},
            timeout=10
        )
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        tables = soup.find_all('table')
        logger.debug(f"Found {len(tables)} tables")
        
        # Find agricultural table
        best_table = None
        max_agri_matches = 0
        for i, table in enumerate(tables):
            # Check for preceding heading
            prev_sibling = table.find_previous(['h2', 'h3'])
            heading = prev_sibling.text.strip().lower() if prev_sibling else ''
            is_agri_heading = 'agricult' in heading or 'soft' in heading
            logger.debug(f"Table {i}: Heading='{heading}', Is_agri_heading={is_agri_heading}")
            
            # Count agricultural term matches
            rows = table.find_all('tr')
            agri_matches = sum(
                1 for row in rows
                for term in CONFIG['agri_terms']
                if term in str(row).lower()
            )
            logger.debug(f"Table {i}: Agri_matches={agri_matches}")
            
            # Prioritize table with heading or most matches
            if (is_agri_heading and agri_matches > 0) or agri_matches > max_agri_matches:
                max_agri_matches = agri_matches
                best_table = table
                logger.debug(f"Table {i} selected as best so far")
        
        if best_table and max_agri_matches >= 2:
            logger.info(f"Selected table with {max_agri_matches} agricultural term matches")
            df = _parse_table(best_table)
            
            # Validate agricultural commodities
            if not df.empty and 'commodity' in df.columns:
                df['commodity_lower'] = df['commodity'].str.lower()
                agri_count = df['commodity_lower'].isin(CONFIG['agri_terms']).sum()
                logger.info(f"Validation: Found {agri_count} agricultural commodities")
                df = df.drop(columns=['commodity_lower'])
                if agri_count >= 2:
                    return df
                else:
                    logger.warning(f"Validation failed: Only {agri_count} agricultural commodities")
                    return None
            else:
                logger.error("Invalid DataFrame: Missing 'commodity' column or empty")
                return None
        else:
            logger.error("No agricultural table found with sufficient matches")
            return None
            
    except requests.RequestException as e:
        logger.error(f"Error scraping data: {e}")
        return None

def _parse_table(table: BeautifulSoup) -> pd.DataFrame:
    """Parse HTML table into DataFrame."""
    headers = [th.text.strip() for th in table.find_all('th')]
    logger.debug(f"Raw headers: {headers}")
    
    rows = [
        [td.text.strip() for td in tr.find_all('td')]
        for tr in table.find_all('tr')[1:] if tr.find_all('td')
    ]
    logger.debug(f"Sample row: {rows[0] if rows else 'No rows'}")
    
    # Determine max columns from data rows
    max_cols = max(len(row) for row in rows) if rows else len(headers)
    logger.debug(f"Max columns in data: {max_cols}, Headers count: {len(headers)}")
    
    # Create DataFrame
    df = pd.DataFrame(rows)
    logger.debug(f"Initial DataFrame shape: {df.shape}")
    
    # Handle header-data mismatch
    if headers and len(headers) > 0:
        if len(headers) < max_cols:
            headers = headers + [f"col_{i}" for i in range(len(headers), max_cols)]
            logger.warning(f"Padded headers to match {max_cols} columns: {headers}")
        elif len(headers) > max_cols:
            headers = headers[:max_cols]
            logger.warning(f"Truncated headers to match {max_cols} columns: {headers}")
    else:
        headers = [f"col_{i}" for i in range(max_cols)]
        logger.warning(f"No headers found, using generic: {headers}")
    
    # Assign columns
    df.columns = headers
    df['timestamp'] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Rename first column to 'commodity' if appropriate
    if not any('commodity' in h.lower() or 'name' in h.lower() for h in headers):
        new_columns = ['commodity'] + headers[1:] if len(headers) > 1 else ['commodity']
        if len(new_columns) == df.shape[1]:
            df.columns = new_columns
        else:
            logger.warning(f"Column rename skipped: {len(new_columns)} vs {df.shape[1]} columns")
    
    logger.info(f"Scraped DataFrame shape: {df.shape}")
    logger.info(f"Final columns: {df.columns.tolist()}")
    logger.debug(f"Scraped sample:\n{df.head().to_string()}")
    return df