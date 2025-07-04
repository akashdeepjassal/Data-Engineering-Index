from src.fetcher import Fetcher
from src.loader import Loader
from src.index import IndexBuilder
from src.config import DB_FOLDER, TABLE_NAME
import os
from datetime import date, timedelta

def main():
    # 1. Get S&P 500 tickers
    tickers = Fetcher.get_sp500_tickers()
    print(f"Fetched {len(tickers)} S&P 500 tickers.")

    os.makedirs(DB_FOLDER, exist_ok=True)
    loader = Loader()
    loader.init_schema()

    # 2. Determine target date (yesterday)
    today_date = date.today()
    target_date = (today_date - timedelta(days=1)).isoformat()

    # 3. Find which tickers are missing for that date
    existing_tickers = set([])
    # loader.get_existing_tickers(TABLE_NAME)
    missing_tickers = list(set(tickers) - existing_tickers)

    print(f"{len(missing_tickers)} tickers missing data for {target_date}.")

    if missing_tickers:
        print("Downloading missing ticker data...")
        df = Fetcher.download_yahoo_data(missing_tickers, target_date, today_date)
        print("Downloaded new price data.")
        
        if not df.empty:
            loader.upsert_prices(df)
            print("Saved new data to DuckDB.")
        else:
            print("No new price data to save!")
    else:
        print(f"All tickers for {target_date} already present. No new download required.")
        return

    # 4. Example: Get top 100 by market cap for a date
    index_builder = IndexBuilder()
    example_date = df['Date'].max()  # Most recent date
    top_100 = index_builder.get_top_n_by_marketcap(example_date)
    print(f"Top 100 on {example_date}:\n", top_100.head())

if __name__ == "__main__":
    main()