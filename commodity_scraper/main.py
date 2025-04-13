# main.py

import schedule
import time
import threading
import logging
from src.scraper import scrape_commodities
from src.data_processor import clean_data, save_to_csv
from src.database import load_to_mysql
from src.stats import generate_statistics
from src.dashboard import create_dashboard

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def job():
    logger.info("Starting job")
    df = scrape_commodities()
    if df is not None:
        df = clean_data(df)
        if df is not None:
            save_to_csv(df)
            load_to_mysql(df)
            generate_statistics(df)
    else:
        logger.warning("No data scraped")

def main():
    job()
    schedule.every(30).minutes.do(job)
    app = create_dashboard()
    
    def run_scheduler():
        while True:
            schedule.run_pending()
            time.sleep(60)
    
    threading.Thread(target=run_scheduler, daemon=True).start()
    app.run(debug=False, host='0.0.0.0', port=8050)

if __name__ == "__main__":
    main()