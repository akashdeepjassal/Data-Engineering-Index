# src/fetcher.py

import pandas as pd
import requests
import yfinance as yf
from .config import SP500_WIKI_URL, YF_PERIOD, YF_INTERVAL, BATCH_SIZE
from .utils import batch_list, retry_with_backoff

class Fetcher:
    @staticmethod
    def get_sp500_tickers() -> list:
        """
        Scrape S&P 500 tickers from Wikipedia.
        """
        resp = requests.get(SP500_WIKI_URL)
        tables = pd.read_html(resp.text)
        tickers = tables[0]['Symbol'].tolist()
        # Some tickers have '.' instead of '-', yfinance uses '-'
        tickers = [ticker.replace('.', '-') for ticker in tickers]
        return tickers

    @staticmethod
    def download_yahoo_data(tickers: list, start_date, end_date) -> pd.DataFrame:
        """
        Download last 30 days daily OHLCV data for all tickers (batched).
        Returns: DataFrame with columns: Date, Ticker, Open, High, Low, Close, Adj Close, Volume
        """
        all_data = []
        for batch in batch_list(tickers, BATCH_SIZE):
            df = retry_with_backoff(
                yf.download,
                batch,
                group_by='Ticker',
                start=start_date,
                end=end_date,
                # period=YF_PERIOD,
                # interval=YF_INTERVAL,
                auto_adjust=False
                # threads=True
            )
            # If only one ticker, no MultiIndex, so convert it
            if isinstance(df.columns, pd.MultiIndex):
                stacked = df.stack(level=0).rename_axis(['Date', 'Ticker']).reset_index()
            else:
                df['Ticker'] = batch[0]
                df = df.reset_index()
                stacked = df.rename(columns={'Adj Close': 'Adj Close'})  # keep column for merging
            all_data.append(stacked)
            # break
        # Merge all batches
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.head(10)
        print(final_df.columns)
        # exit()
        # Standardize column names
        # final_df = final_df.rename(columns={'Adj Close': 'Adj_Close'})
        final_df=final_df[['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']]
        final_df = final_df.drop_duplicates(subset=['Date', 'Ticker'], keep='last')
        final_df['Date'] = pd.to_datetime(final_df['Date']).dt.date
        final_df.to_csv(f"db/prices_{end_date}.csv", index=False)
        return final_df
