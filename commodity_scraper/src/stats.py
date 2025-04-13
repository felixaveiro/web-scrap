# src/stats.py

import pandas as pd
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)

def generate_statistics(df: Optional[pd.DataFrame]) -> Optional[Dict[str, Any]]:
    """Generate basic statistics from DataFrame."""
    if df is None or df.empty:
        logger.warning("No data for statistics")
        return None

    stats = {
        'total_commodities': len(df),
        'average_price': df['price'].mean() if 'price' in df.columns else 0,
        'max_price': df['price'].max() if 'price' in df.columns else 0,
        'min_price': df['price'].min() if 'price' in df.columns else 0,
        'positive_changes': len(df[df['change'] > 0]) if 'change' in df.columns else 0,
        'negative_changes': len(df[df['change'] < 0]) if 'change' in df.columns else 0
    }
    logger.info("Generated statistics: %s", stats)
    return stats