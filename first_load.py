# main.py

from src.fetcher import Fetcher
from src.loader import Loader
from src.index import IndexBuilder
from src.config import DB_FOLDER, TABLE_NAME
import os
from datetime import date, timedelta

def main():
    # 1. Get S&P 500 tickers
    tickers = Fetcher.get_sp500_tickers()
    # tickers=['AAPL', 'MSFT']
    print(f"Fetched {len(tickers)} S&P 500 tickers.")

    os.makedirs(DB_FOLDER, exist_ok=True)
    # 2. Download historical price data for tickers
    loader = Loader()
    loader.init_schema()
    # 2. Find which tickers are already in DB
    existing_tickers = set([])
    # loader.get_existing_tickers(TABLE_NAME)
    missing_tickers = list(set(tickers) - existing_tickers)
    print(f"{len(missing_tickers)} tickers missing from DB.")

    # 3. Get date range for t-30 to t-1
    today = date.today()
    start_date = '2025-05-01'
    # today - timedelta(days=30)
    end_date = today  # yf.download's 'end' is exclusive, so this gets up to t-1

    if missing_tickers:
        print(f"Missing Tickers {missing_tickers}")
        print("Downloading missing ticker data...")
        df = Fetcher.download_yahoo_data(missing_tickers, start_date, end_date)
        print("Downloaded new price data.")
        loader.save_prices(df)
        print("Saved new data to SQLITE.")
    else:
        exit()
        print("All tickers already present. No new download required.")
    

    # 3. Initialize SQLITE and save data
    
    loader.save_prices(df)
    print("Saved data to SQLITE.")

    # 4. Example: Get top 100 by market cap for a date
    index_builder = IndexBuilder()
    example_date = df['Date'].max()  # Most recent date
    top_100 = index_builder.get_top_n_by_marketcap(example_date)
    print(f"Top 100 on {example_date}:\n", top_100.head())

if __name__ == "__main__":
    main()
