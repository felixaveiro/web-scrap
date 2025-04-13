import requests
from bs4 import BeautifulSoup
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import schedule
import time
from datetime import datetime
import os
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px

# Configuration
CSV_FILE = 'agri_commodities.csv'
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root123',
    'database': 'commodities_db'
}
TABLE_NAME = 'agri_commodities'

# 1. Scrape Data
def scrape_commodities():
    try:
        url = 'https://tradingeconomics.com/commodities'
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        tables = soup.find_all('table')
        target_table = None
        agri_terms = ['corn', 'wheat', 'soybeans', 'sugar', 'coffee', 'cocoa', 'rice']
        
        # First, try to find agricultural table
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = [td.text.strip().lower() for td in row.find_all('td')]
                if any(term in cell for term in agri_terms for cell in cells):
                    target_table = table
                    print("Found agricultural table")
                    break
            if target_table:
                break
        
        # Fallback: Use first table with 'price' in headers if no agri table found
        if not target_table:
            for table in tables:
                headers = [th.text.strip().lower() for th in table.find_all('th')]
                if 'price' in headers:
                    target_table = table
                    print("No agricultural table found, using fallback table")
                    break
        
        if not target_table:
            print("No commodity table found")
            return None
            
        headers = [th.text.strip() for th in target_table.find_all('th')]
        rows = []
        for tr in target_table.find_all('tr')[1:]:
            cells = [td.text.strip() for td in tr.find_all('td')]
            if cells:
                rows.append(cells)
                
        df = pd.DataFrame(rows, columns=headers)
        df['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("Scraped DataFrame shape:", df.shape)
        return df
    except Exception as e:
        print(f"Error scraping data: {e}")
        return None

# 2. Write to CSV
def save_to_csv(df):
    if df is not None:
        df.to_csv(CSV_FILE, index=False)
        print(f"Data saved to {CSV_FILE}, rows: {len(df)}")
    else:
        print("No data to save")

# 3. Read CSV
def read_csv():
    if os.path.exists(CSV_FILE):
        df = pd.read_csv(CSV_FILE)
        print("CSV read, rows:", len(df))
        return df
    else:
        print("CSV file not found")
        return None

# 4. Clean and Transform Data
def clean_data(df):
    if df is None:
        print("Clean_data: Received None")
        return None
        
    print("Raw columns:", df.columns.tolist())
    df = df.fillna(0)
    
    # Standardize column names
    df.columns = [col.strip().replace(' ', '_').lower() for col in df.columns]
    
    # Identify columns
    commodity_col = None
    price_col = None
    change_col = None
    for col in df.columns:
        col_lower = col.lower()
        if 'commodity' in col_lower or 'name' in col_lower or 'energy' in col_lower or col_lower in ['item', 'product']:
            commodity_col = col
        if 'price' in col_lower:
            price_col = col
        if 'change' in col_lower or 'chg' in col_lower or col_lower == '%':
            change_col = col
    
    # Rename columns
    rename_dict = {}
    if commodity_col:
        rename_dict[commodity_col] = 'commodity'
    else:
        # Use first column as commodity if none found
        rename_dict[df.columns[0]] = 'commodity'
    if price_col:
        rename_dict[price_col] = 'price'
    if change_col:
        rename_dict[change_col] = 'change'
    
    df = df.rename(columns=rename_dict)
    
    # Clean price
    if 'price' in df.columns:
        try:
            df['price'] = df['price'].astype(str).str.replace(',', '').str.strip()
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
        except Exception as e:
            print(f"Error cleaning price column: {e}")
            df['price'] = 0
    else:
        df['price'] = 0
    
    # Clean change
    if 'change' in df.columns:
        try:
            df['change'] = df['change'].astype(str).str.replace('%', '').str.strip()
            df['change'] = pd.to_numeric(df['change'], errors='coerce') / 100
        except Exception as e:
            print(f"Error cleaning change column: {e}")
            df['change'] = 0
    else:
        df['change'] = 0
    
    # Clean timestamp
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    
    # Drop duplicates and invalid rows
    df = df.drop_duplicates()
    df = df.dropna(subset=['commodity'])
    
    print("Cleaned columns:", df.columns.tolist())
    print("Cleaned DataFrame shape:", df.shape)
    print("Cleaned sample:\n", df.head(2))
    return df

# 5. Load to MySQL
def load_to_mysql(df):
    if df is None:
        print("Load_to_mysql: Received None")
        return
        
    try:
        conn = mysql.connector.connect(
            host=DB_CONFIG['host'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
        cursor.close()
        conn.close()
        
        engine = create_engine(
            f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
        )
        df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
        print("Data loaded to MySQL, rows:", len(df))
    except Exception as e:
        print(f"Error loading to MySQL: {e}")

# 6. Generate Statistics
def generate_statistics(df):
    if df is None:
        print("Generate_statistics: Received None")
        return None
        
    stats = {
        'Total Commodities': len(df),
        'Average Price': df['price'].mean() if 'price' in df.columns else 0,
        'Max Price': df['price'].max() if 'price' in df.columns else 0,
        'Min Price': df['price'].min() if 'price' in df.columns else 0,
        'Positive Changes': len(df[df['change'] > 0]) if 'change' in df.columns else 0,
        'Negative Changes': len(df[df['change'] < 0]) if 'change' in df.columns else 0
    }
    print("Basic Statistics:")
    for key, value in stats.items():
        print(f"{key}: {value}")
    return stats

# 7. Main Job
def job():
    print(f"Running job at {datetime.now()}")
    df = scrape_commodities()
    if df is not None:
        print("Scraped columns:", df.columns.tolist())
        print("Scraped sample:\n", df.head(2))
        df = clean_data(df)
        save_to_csv(df)
        load_to_mysql(df)
        generate_statistics(df)
    else:
        print("No data scraped. Job skipped.")

# 8. Dashboard Setup
def create_dashboard():
    app = dash.Dash(__name__)
    
    app.layout = html.Div([
        html.H1("Agricultural Commodities Dashboard"),
        html.Div([
            html.H3("Data Last Updated: "),
            html.Div(id='last-update-time')
        ]),
        dcc.Graph(id='price-chart'),
        dcc.Graph(id='change-chart'),
        dcc.Interval(id='interval-component', interval=30*60*1000, n_intervals=0)
    ])
    
    @app.callback(
        [Output('price-chart', 'figure'),
         Output('change-chart', 'figure'),
         Output('last-update-time', 'children')],
        [Input('interval-component', 'n_intervals')]
    )
    def update_charts(n):
        df = read_csv()
        if df is None or df.empty:
            print("Dashboard: No data in CSV")
            return px.bar(title="No data available"), px.bar(title="No data available"), "No data"
        
        df = clean_data(df)
        if df is None or df.empty:
            print("Dashboard: No data after cleaning")
            return px.bar(title="No data after cleaning"), px.bar(title="No data after cleaning"), "No data"
        
        if 'commodity' not in df.columns or 'price' not in df.columns:
            print("Dashboard: Missing critical columns")
            return px.bar(title="Data error - missing columns"), px.bar(title="Data error"), "Error"
        
        # Try agricultural filter
        agri_terms = ['corn', 'wheat', 'soybeans', 'sugar', 'coffee', 'cocoa', 'rice']
        agri_df = df[df['commodity'].str.lower().isin(agri_terms)]
        
        if not agri_df.empty:
            df = agri_df
            title_prefix = "Agricultural"
        else:
            print("Dashboard: No agricultural commodities found, showing all commodities")
            title_prefix = "All"
        
        print("Dashboard columns:", df.columns.tolist())
        print("Dashboard sample:\n", df.head(2))
        
        price_fig = px.bar(
            df, 
            x='commodity', 
            y='price',
            title=f'{title_prefix} Commodity Prices',
            labels={'commodity': 'Commodity', 'price': 'Price'}
        )
        
        change_fig = px.bar(
            df, 
            x='commodity', 
            y='change',
            title=f'{title_prefix} Price Changes',
            labels={'commodity': 'Commodity', 'change': 'Change'}
        )
        change_fig.update_layout(yaxis_tickformat='.2%')
        change_fig.update_traces(marker_color=[
            'green' if x > 0 else 'red' for x in df['change']
        ])
        
        last_update = df['timestamp'].max() if 'timestamp' in df.columns else "Unknown"
        update_text = f"Last Updated: {last_update}"
        
        return price_fig, change_fig, update_text
    
    return app

# Schedule and Run
if __name__ == "__main__":
    job()
    schedule.every(30).minutes.do(job)
    app = create_dashboard()
    
    def run_scheduler():
        while True:
            schedule.run_pending()
            time.sleep(60)
    
    import threading
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.daemon = True
    scheduler_thread.start()
    
    app.run(debug=True)