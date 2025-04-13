# src/dashboard.py

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
from typing import Tuple
import logging
from config.settings import CONFIG
from src.data_processor import read_csv, clean_data

logger = logging.getLogger(__name__)

def create_dashboard() -> dash.Dash:
    """Create and configure Dash dashboard."""
    app = dash.Dash(__name__, external_stylesheets=[
        'https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css'
    ])

    app.layout = html.Div([
        html.H1("Agricultural Commodities Dashboard", className="text-center mb-4"),
        html.Div(
            id='error-message',
            className="alert alert-danger text-center",
            style={'display': 'none'}
        ),
        html.Div([
            html.Button("Refresh Data", id="refresh-button", className="btn btn-primary mb-3"),
            html.Span(id='last-update-time', className="ms-3")
        ], className="d-flex justify-content-center align-items-center"),
        dcc.Graph(id='price-chart'),
        dcc.Graph(id='change-chart'),
        html.H3("Commodity Data", className="mt-4"),
        dash_table.DataTable(
            id='data-table',
            columns=[],
            data=[],
            style_table={'overflowX': 'auto'},
            style_cell={'padding': '5px', 'textAlign': 'left'},
            style_header={'fontWeight': 'bold'},
            page_size=10
        ),
        dcc.Interval(id='interval-component', interval=30*60*1000, n_intervals=0)
    ], className="container p-4")

    @app.callback(
        [
            Output('price-chart', 'figure'),
            Output('change-chart', 'figure'),
            Output('last-update-time', 'children'),
            Output('data-table', 'columns'),
            Output('data-table', 'data'),
            Output('error-message', 'children'),
            Output('error-message', 'style')
        ],
        [
            Input('interval-component', 'n_intervals'),
            Input('refresh-button', 'n_clicks')
        ]
    )
    def update_dashboard(n_intervals: int, n_clicks: int) -> Tuple:
        df = read_csv()
        if df is None or df.empty:
            logger.warning("No data for dashboard")
            return (
                px.bar(title="No data"),
                px.bar(title="No data"),
                "No data",
                [],
                [],
                "No commodity data found in CSV.",
                {'display': 'block'}
            )
        
        df = clean_data(df)
        if df is None or df.empty:
            logger.warning("No cleaned data for dashboard")
            return (
                px.bar(title="No data"),
                px.bar(title="No data"),
                "No data",
                [],
                [],
                "Failed to process commodity data. Check logs for details.",
                {'display': 'block'}
            )
        
        if 'commodity' not in df.columns:
            logger.error("Missing commodity column")
            return (
                px.bar(title="Data error"),
                px.bar(title="Data error"),
                "Error",
                [],
                [],
                "Data error: missing commodity column.",
                {'display': 'block'}
            )
        
        # Filter agricultural commodities
        if 'commodity' in df.columns:
            agri_df = df[df['commodity'].str.lower().isin(CONFIG['agri_terms'])]
            if agri_df.empty:
                logger.warning("No agricultural commodities found")
                # Still display data to avoid blank dashboard
                error_msg = "No agricultural commodities found, showing all available data."
            else:
                df = agri_df
                error_msg = ""
        else:
            error_msg = "Commodity column missing."
        
        price_fig = px.bar(
            df, x='commodity', y='price',
            title="Agricultural Commodity Prices",
            labels={'commodity': 'Commodity', 'price': 'Price'},
            template='plotly_white'
        ).update_traces(marker_color='#1f77b4')
        
        change_fig = px.bar(
            df, x='commodity', y='change',
            title="Agricultural Price Changes",
            labels={'commodity': 'Commodity', 'change': 'Change'}
        ).update_layout(yaxis_tickformat='.2%').update_traces(
            marker_color=['green' if x > 0 else 'red' for x in df['change']] if 'change' in df.columns else '#1f77b4'
        )
        
        last_update = (
            df['timestamp'].max().strftime("%Y-%m-%d %H:%M:%S")
            if 'timestamp' in df.columns and not df['timestamp'].isna().all()
            else "Unknown"
        )
        
        table_columns = [{'name': col.title(), 'id': col} for col in df.columns]
        table_data = df.fillna('-').to_dict('records')  # Replace NaN for display
        
        return (
            price_fig,
            change_fig,
            f"Last Updated: {last_update}",
            table_columns,
            table_data,
            error_msg,
            {'display': 'block' if error_msg else 'none'}
        )
    
    return app