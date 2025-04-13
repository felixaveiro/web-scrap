# config/settings.py

CONFIG = {
    'csv_file': 'agri_commodities.csv',
    'db': {
        'host': 'localhost',
        'user': 'root',
        'password': 'root123',
        'database': 'commodities_db'
    },
    'table_name': 'agri_commodities',
    'scrape_url': 'https://tradingeconomics.com/commodities',
    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'agri_terms': {'corn', 'wheat', 'soybeans', 'sugar', 'coffee', 'cocoa', 'rice'}
}