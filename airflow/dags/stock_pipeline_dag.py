from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_pipeline():
    """
    Main pipeline function that fetches current stock prices and updates the database.
    This function is called by the Airflow DAG.
    """
    # Ensure our scripts directory is importable inside the container
    import sys
    if "/opt/airflow/scripts" not in sys.path:
        sys.path.append("/opt/airflow/scripts")

    # Lazy import to ensure dependencies are available inside container
    from fetch_and_update import fetch_and_update_all

    # Get configuration from environment variables
    symbols_csv = os.environ.get("STOCK_SYMBOLS", "MSFT,AAPL,GOOGL")
    symbols = [s.strip().upper() for s in symbols_csv.split(",") if s.strip()]

    db_uri = os.environ.get("STOCKS_DB_URI")
    if not db_uri:
        raise RuntimeError("STOCKS_DB_URI environment variable is required")

    api_key = os.environ.get("ALPHA_VANTAGE_API_KEY")
    if not api_key:
        raise RuntimeError("ALPHA_VANTAGE_API_KEY environment variable is required")

    print(f"Starting stock price pipeline for symbols: {symbols}")
    print(f"Database URI: {db_uri}")
    print(f"API Key configured: {'Yes' if api_key else 'No'}")

    # Execute the main pipeline
    fetch_and_update_all(symbols=symbols, db_uri=db_uri)
    
    print("Stock price pipeline completed successfully")


# DAG configuration
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="stock_market_pipeline",
    default_args=default_args,
    description="Fetch current stock prices from Alpha Vantage and store in PostgreSQL",
    schedule="@hourly",  # Run daily at midnight
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["stock-market", "alpha-vantage", "postgresql"],
) as dag:

    # Main task that fetches and stores stock data
    fetch_and_store_task = PythonOperator(
        task_id="fetch_and_store_stock_prices",
        python_callable=run_pipeline,
        doc_md="""
        ## Fetch and Store Stock Prices
        
        This task:
        1. Fetches current stock prices from Alpha Vantage API
        2. Parses the JSON response and extracts relevant data
        3. Updates the PostgreSQL database with current prices
        4. Handles errors gracefully and provides comprehensive logging
        
        **Inputs:**
        - STOCK_SYMBOLS: Comma-separated list of stock symbols
        - ALPHA_VANTAGE_API_KEY: API key for Alpha Vantage
        - STOCKS_DB_URI: PostgreSQL connection string
        
        **Outputs:**
        - Updated stock_prices table in PostgreSQL
        - Comprehensive logs for monitoring and debugging
        """,
    )

    # Set task dependencies (in this case, just one task)
    fetch_and_store_task