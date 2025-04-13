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


CSV_FILE = 'agri_commodities.csv'
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root123',
    'database': 'commodities_db'
}
TABLE_NAME = 'agri_commodities'

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
    
    # Define professional color scheme
    colors = {
        'background': '#f8f9fa',
        'text': '#2d3748',
        'primary': '#2b6cb0',
        'secondary': '#718096',
        'accent': '#48bb78'
    }
    
    # Apply global styles
    app.layout = html.Div([
        html.Div([
            html.H1(
                "Agricultural Commodities Dashboard",
                style={
                    'textAlign': 'center',
                    'color': colors['text'],
                    'fontSize': '2.5em',
                    'fontWeight': 'bold',
                    'marginBottom': '20px',
                    'fontFamily': '"Roboto", sans-serif'
                }
            ),
            html.Div([
                html.H3(
                    "Data Last Updated: ",
                    style={
                        'display': 'inline-block',
                        'color': colors['secondary'],
                        'fontSize': '1.2em',
                        'fontWeight': 'normal',
                        'marginRight': '10px'
                    }
                ),
                html.Span(
                    id='last-update-time',
                    style={
                        'color': colors['primary'],
                        'fontSize': '1.2em',
                        'fontWeight': 'bold'
                    }
                )
            ], style={
                'textAlign': 'center',
                'marginBottom': '30px',
                'backgroundColor': '#ffffff',
                'padding': '15px',
                'borderRadius': '8px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.1)'
            }),
            html.Div([
                dcc.Graph(id='price-chart'),
                dcc.Graph(id='change-chart')
            ], style={
                'backgroundColor': '#ffffff',
                'padding': '20px',
                'borderRadius': '8px',
                'boxShadow': '0 2px 4px rgba(0,0,0,0.1)',
                'marginBottom': '20px'
            }),
            dcc.Interval(id='interval-component', interval=30*60*1000, n_intervals=0)
        ], style={
            'maxWidth': '1200px',
            'margin': '0 auto',
            'padding': '20px'
        })
    ], style={
        'backgroundColor': colors['background'],
        'minHeight': '100vh',
        'fontFamily': '"Roboto", sans-serif'
    })
    
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
            labels={'commodity': 'Commodity', 'price': 'Price'},
            color_discrete_sequence=[colors['primary']]
        )
        price_fig.update_layout(
            title={'x': 0.5, 'xanchor': 'center', 'font': {'size': 20, 'color': colors['text']}},
            xaxis_title="Commodity",
            yaxis_title="Price (USD)",
            plot_bgcolor='#ffffff',
            paper_bgcolor='#ffffff',
            font={'family': '"Roboto", sans-serif', 'color': colors['text']},
            margin={'l': 50, 'r': 50, 't': 50, 'b': 50}
        )
        
        change_fig = px.bar(
            df, 
            x='commodity', 
            y='change',
            title=f'{title_prefix} Price Changes',
            labels={'commodity': 'Commodity', 'change': 'Change'}
        )
        change_fig.update_layout(
            title={'x': 0.5, 'xanchor': 'center', 'font': {'size': 20, 'color': colors['text']}},
            xaxis_title="Commodity",
            yaxis_title="Change (%)",
            yaxis_tickformat='.2%',
            plot_bgcolor='#ffffff',
            paper_bgcolor='#ffffff',
            font={'family': '"Roboto", sans-serif', 'color': colors['text']},
            margin={'l': 50, 'r': 50, 't': 50, 'b': 50}
        )
        change_fig.update_traces(marker_color=[
            colors['accent'] if x > 0 else '#ef4444' for x in df['change']
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