import logging
import os
import time
import requests
from typing import Iterable, List, Dict, Optional
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.create import create_engine


def _get_logger() -> logging.Logger:
    """Initialize and configure logger for the stock pipeline."""
    logger = logging.getLogger("stock_pipeline")
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


logger = _get_logger()


def ensure_schema(engine: Engine) -> None:
    """Create or migrate the stock_prices table as needed.

    - Creates table if it does not exist
    - Adds new columns if they are missing
    - Creates indexes if missing
    """
    create_table_sql = text(
        """
        CREATE TABLE IF NOT EXISTS stock_prices (
            symbol TEXT NOT NULL,
            trading_day DATE NOT NULL,
            open NUMERIC,
            high NUMERIC,
            low NUMERIC,
            close NUMERIC,
            adjusted_close NUMERIC,
            volume BIGINT,
            PRIMARY KEY (symbol, trading_day)
        );
        """
    )

    add_columns_sql = text(
        """
        ALTER TABLE stock_prices
            ADD COLUMN IF NOT EXISTS current_price NUMERIC;
        ALTER TABLE stock_prices
            ADD COLUMN IF NOT EXISTS last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
        """
    )

    create_indexes_sql = text(
        """
        CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol ON stock_prices(symbol);
        CREATE INDEX IF NOT EXISTS idx_stock_prices_last_updated ON stock_prices(last_updated);
        """
    )

    try:
        with engine.begin() as conn:
            # Ensure base table exists
            conn.execute(create_table_sql)
            # Ensure new columns exist for older installs
            conn.execute(add_columns_sql)
            # Ensure indexes exist (after columns definitely exist)
            conn.execute(create_indexes_sql)
        logger.info("Database schema ensured successfully")
    except Exception as e:
        logger.error(f"Failed to ensure database schema: {e}")
        raise


def fetch_current_price(symbol: str, api_key: str) -> Optional[Dict]:
    """
    Fetch current stock price from Alpha Vantage using GLOBAL_QUOTE endpoint.
    
    Args:
        symbol: Stock symbol (e.g., 'MSFT')
        api_key: Alpha Vantage API key
        
    Returns:
        Dictionary containing current price data or None if failed
    """
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "apikey": api_key
    }
    
    logger.info(f"Fetching current price for {symbol}...")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Check for API errors
        if "Error Message" in data:
            logger.error(f"API Error for {symbol}: {data['Error Message']}")
            return None
        
        if "Note" in data:
            logger.warning(f"API Notice for {symbol}: {data['Note']}")
            logger.warning("This usually means you've hit the rate limit (5 requests per minute for free tier)")
            return None
        
        # Check if Global Quote exists
        if "Global Quote" not in data:
            logger.warning(f"No Global Quote data found for {symbol}")
            return None
        
        quote_data = data["Global Quote"]
        
        # Extract relevant data with error handling
        try:
            current_price = float(quote_data.get("05. price", 0))
            if current_price == 0:
                logger.warning(f"Invalid price (0) for {symbol}")
                return None
                
            return {
                "symbol": symbol,
                "current_price": current_price,
                "open": _safe_float(quote_data.get("02. open")),
                "high": _safe_float(quote_data.get("03. high")),
                "low": _safe_float(quote_data.get("04. low")),
                "close": _safe_float(quote_data.get("05. price")),
                "volume": _safe_int(quote_data.get("06. volume")),
                "previous_close": _safe_float(quote_data.get("08. previous close")),
                "change": _safe_float(quote_data.get("09. change")),
                "change_percent": quote_data.get("10. change percent", "").replace("%", ""),
                "last_updated": datetime.now()
            }
        except (ValueError, TypeError) as e:
            logger.error(f"Failed to parse price data for {symbol}: {e}")
            return None
            
    except requests.RequestException as e:
        logger.error(f"Network error fetching {symbol}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching {symbol}: {e}")
        return None


def upsert_current_price(engine: Engine, price_data: Dict) -> bool:
    """
    Upsert current price data into the database.
    
    Args:
        engine: SQLAlchemy engine
        price_data: Dictionary containing price data
        
    Returns:
        True if successful, False otherwise
    """
    if not price_data:
        return False
        
    upsert_sql = text(
        """
        INSERT INTO stock_prices (
            symbol, trading_day, open, high, low, close, 
            adjusted_close, volume, current_price, last_updated
        )
        VALUES (
            :symbol, :trading_day, :open, :high, :low, :close,
            :close, :volume, :current_price, :last_updated
        )
        ON CONFLICT (symbol, trading_day)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            adjusted_close = EXCLUDED.adjusted_close,
            volume = EXCLUDED.volume,
            current_price = EXCLUDED.current_price,
            last_updated = EXCLUDED.last_updated;
        """
    )
    
    try:
        with engine.begin() as conn:
            conn.execute(
                upsert_sql,
                {
                    "symbol": price_data["symbol"],
                    "trading_day": datetime.now().date(),
                    "open": price_data.get("open"),
                    "high": price_data.get("high"),
                    "low": price_data.get("low"),
                    "close": price_data.get("close"),
                    "volume": price_data.get("volume"),
                    "current_price": price_data.get("current_price"),
                    "last_updated": price_data.get("last_updated")
                }
            )
        logger.info(f"Successfully upserted current price for {price_data['symbol']}: ${price_data['current_price']}")
        return True
    except Exception as e:
        logger.error(f"Failed to upsert price data for {price_data.get('symbol', 'unknown')}: {e}")
        return False


def _safe_float(value) -> Optional[float]:
    """Safely convert value to float, return None if conversion fails."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _safe_int(value) -> Optional[int]:
    """Safely convert value to int, return None if conversion fails."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def fetch_and_update_all(symbols: Iterable[str], db_uri: str) -> None:
    """
    Main function to fetch current prices for all symbols and update the database.
    
    Args:
        symbols: Iterable of stock symbols
        db_uri: Database connection URI
    """
    if not db_uri:
        raise ValueError("Database URI is required")

    api_key = os.environ.get("ALPHA_VANTAGE_API_KEY")
    if not api_key:
        raise ValueError("ALPHA_VANTAGE_API_KEY environment variable is required")

    # Create database engine
    try:
        engine: Engine = create_engine(db_uri, pool_pre_ping=True)
        logger.info("Database engine created successfully")
    except Exception as e:
        logger.error(f"Failed to create database engine: {e}")
        raise

    # Ensure schema exists
    ensure_schema(engine)

    # Get configuration
    throttle_seconds = int(os.environ.get("API_THROTTLE_SECONDS", "15"))  # Increased for Alpha Vantage
    symbols_list: List[str] = [s.strip().upper() for s in symbols if s.strip()]
    total_symbols = len(symbols_list)
    
    if total_symbols == 0:
        logger.warning("No valid symbols provided")
        return

    logger.info(f"Starting to process {total_symbols} symbols with {throttle_seconds}s throttle")

    successful_updates = 0
    failed_updates = 0

    for idx, symbol in enumerate(symbols_list, start=1):
        logger.info(f"Processing symbol {symbol} ({idx}/{total_symbols})")
        
        try:
            # Fetch current price
            price_data = fetch_current_price(symbol, api_key)
            
            if price_data:
                # Update database
                if upsert_current_price(engine, price_data):
                    successful_updates += 1
                else:
                    failed_updates += 1
            else:
                failed_updates += 1
                logger.warning(f"No price data retrieved for {symbol}")

        except Exception as e:
            failed_updates += 1
            logger.exception(f"Unexpected error while processing {symbol}: {e}")

        # Throttle between requests to respect API limits
        if idx < total_symbols:  # Don't sleep after the last request
            logger.info(f"Waiting {throttle_seconds} seconds before next request...")
            time.sleep(throttle_seconds)

    logger.info(f"Pipeline completed. Successful: {successful_updates}, Failed: {failed_updates}")


__all__: List[str] = [
    "fetch_and_update_all",
]


