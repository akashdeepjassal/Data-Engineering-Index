# src/main.py

from   fetcher import Fetcher
from loader import Loader
from utils import get_past_month_dates
from config import START_DATE, END_DATE

import pandas as pd
from tqdm import tqdm

def main():
    fetcher = Fetcher()
    loader = Loader()
    loader.create_tables()
    # tickers = fetcher.get_sp500_tickers()

    data=fetcher.fetch_all_ndaq_stat()

    print(data)
    exit()

    all_data = []
    print("Fetching data for tickers...")
    for ticker in tqdm(tickers):
        data = fetcher.get_daily_market_cap(ticker, START_DATE, END_DATE)
        if data is not None:
            all_data.append(data)
    if not all_data:
        print("No data fetched!")
        return
    df = pd.concat(all_data, ignore_index=True)
    loader.insert_data(df)
    print("Data loaded into DuckDB.")

    # Example: fetch top 100 by mcap for a given date
    sample_date = df['date'].max()
    top100 = loader.fetch_top_100_by_marketcap(sample_date)
    print(top100)
    loader.close()

if __name__ == "__main__":
    main()
