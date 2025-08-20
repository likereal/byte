#!/usr/bin/env python3
"""
Test script for the Alpha Vantage stock price fetching functionality.
This script tests the fetch_and_update module to ensure it works correctly.
"""

import os
import sys
import requests
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load .env from repo root
load_dotenv(dotenv_path=Path(__file__).resolve().parents[2] / ".env")

def test_alpha_vantage_api(symbol="MSFT", api_key=None):
    """Test Alpha Vantage API directly to ensure it's working."""
    
    if not api_key:
        api_key = os.environ.get("ALPHA_VANTAGE_API_KEY")
        if not api_key:
            print("❌ No Alpha Vantage API key found!")
            print("Please get a free API key from: https://www.alphavantage.co/")
            print("Then set it as ALPHA_VANTAGE_API_KEY environment variable")
            return False
    
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "apikey": api_key
    }
    
    print(f"📡 Testing Alpha Vantage API for {symbol}...")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Check for API errors
        if "Error Message" in data:
            print(f"❌ API Error: {data['Error Message']}")
            return False
        
        if "Note" in data:
            print(f"⚠️ API Notice: {data['Note']}")
            print("This usually means you've hit the rate limit (5 requests per minute for free tier)")
            return False
        
        # Check if Global Quote exists
        if "Global Quote" not in data:
            print(f"❌ No Global Quote data found for {symbol}")
            print(f"🔍 Available keys: {list(data.keys())}")
            return False
        
        quote_data = data["Global Quote"]
        current_price = quote_data.get("05. price", "N/A")
        
        print(f"✅ Successfully fetched current price for {symbol}: ${current_price}")
        print(f"📊 Additional data:")
        print(f"   Open: ${quote_data.get('02. open', 'N/A')}")
        print(f"   High: ${quote_data.get('03. high', 'N/A')}")
        print(f"   Low: ${quote_data.get('04. low', 'N/A')}")
        print(f"   Volume: {quote_data.get('06. volume', 'N/A')}")
        print(f"   Change: {quote_data.get('09. change', 'N/A')} ({quote_data.get('10. change percent', 'N/A')})")
        
        return True
        
    except requests.RequestException as e:
        print(f"❌ Network error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def test_fetch_and_update_module():
    """Test the fetch_and_update module."""
    print("\n🔧 Testing fetch_and_update module...")
    
    try:
        # Add the scripts directory to the path
        scripts_dir = os.path.join(os.path.dirname(__file__))
        if scripts_dir not in sys.path:
            sys.path.insert(0, scripts_dir)
        
        from fetch_and_update import fetch_current_price, _safe_float, _safe_int
        
        # Test helper functions
        print("Testing helper functions...")
        assert _safe_float("123.45") == 123.45
        assert _safe_float("") is None
        assert _safe_int("123") == 123
        assert _safe_int("") is None
        print("✅ Helper functions working correctly")
        
        # Test fetch_current_price function
        api_key = os.environ.get("ALPHA_VANTAGE_API_KEY")
        if api_key:
            price_data = fetch_current_price("MSFT", api_key)
            if price_data:
                print(f"✅ fetch_current_price working correctly")
                print(f"   Symbol: {price_data['symbol']}")
                print(f"   Current Price: ${price_data['current_price']}")
                print(f"   Last Updated: {price_data['last_updated']}")
            else:
                print("❌ fetch_current_price returned None")
        else:
            print("⚠️ Skipping fetch_current_price test - no API key")
        
        return True
        
    except ImportError as e:
        print(f"❌ Failed to import fetch_and_update module: {e}")
        return False
    except Exception as e:
        print(f"❌ Error testing fetch_and_update module: {e}")
        return False

def main():
    """Main test function."""
    print("🚀 Starting comprehensive test of Alpha Vantage stock price pipeline...")
    print(f"⏰ Test started at: {datetime.now()}")
    
    # Test 1: Direct API call
    print("\n" + "="*50)
    print("TEST 1: Direct Alpha Vantage API Call")
    print("="*50)
    api_test_success = test_alpha_vantage_api("MSFT")
    
    # Test 2: Module functionality
    print("\n" + "="*50)
    print("TEST 2: Fetch and Update Module")
    print("="*50)
    module_test_success = test_fetch_and_update_module()
    
    # Summary
    print("\n" + "="*50)
    print("TEST SUMMARY")
    print("="*50)
    print(f"API Test: {'✅ PASSED' if api_test_success else '❌ FAILED'}")
    print(f"Module Test: {'✅ PASSED' if module_test_success else '❌ FAILED'}")
    
    if api_test_success and module_test_success:
        print("\n🎉 All tests passed! Your pipeline should work correctly.")
        print("\n💡 Next steps:")
        print("1. Set up your .env file with your Alpha Vantage API key")
        print("2. Run 'docker compose up -d' to start the pipeline")
        print("3. Access Airflow UI at http://localhost:8080")
        print("4. Enable and trigger the 'stock_market_pipeline' DAG")
    else:
        print("\n⚠️ Some tests failed. Please check the errors above.")
        print("\n💡 Troubleshooting:")
        print("1. Ensure you have a valid Alpha Vantage API key")
        print("2. Check your internet connection")
        print("3. Verify the API key is set correctly in environment variables")

if __name__ == "__main__":
    main()