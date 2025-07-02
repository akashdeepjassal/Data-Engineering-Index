import requests
import pandas as pd
import nasdaqdatalink
import pandas as pd
from config import NASDAQ_DATA_LINK_API_KEY

class Fetcher:
    def __init__(self):
        nasdaqdatalink.ApiConfig.api_key = NASDAQ_DATA_LINK_API_KEY
        self.api_key=NASDAQ_DATA_LINK_API_KEY

    def get_sp500_tickers(self):
        """Fetch list of S&P 500 tickers (can use other Nasdaq source if you have permission)."""
        # Nasdaq Data Link has some premium endpoints; fallback to static S&P500 list if needed
        # Example for demonstration (for prod, fetch dynamically if possible):
        # url = 'https://data.nasdaq.com/api/v3/datatables/WIKI/PRICES?ticker=&qopts.columns=ticker&date=2018-03-27&api_key=' + NASDAQ_DATA_LINK_API_KEY
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        tables = pd.read_html(requests.get(url, headers={"User-Agent": "Mozilla/5.0"}).text)
        df = tables[0]  # First table is the constituent table
        symbols = df['Symbol'].tolist()
        return symbols

    def get_daily_market_cap(self, ticker, start_date, end_date):
        """
        Fetch daily price and market cap for a single ticker from NDAQ/STAT (Nasdaq Data Link).
        """
        try:
            data = nasdaqdatalink.get(
                "NDAQ/STAT",
                params={
                    "symbol": ticker,
                    "start_date": start_date,
                    "end_date": end_date
                }
            )
            # Reset index if date is index
            data = data.reset_index()
            data['ticker'] = ticker
            
            # The marketcap column is already present, so you can use it directly
            # Optionally rename columns for consistency
            if 'marketcap' in data.columns:
                data.rename(columns={'marketcap': 'market_cap'}, inplace=True)
            else:
                data['market_cap'] = None  # Or handle error
            
            # For price, you might want to use closing price from another table if needed
            # Or keep only market_cap, as per requirement
            return data
        except Exception as e:
            print(f"Failed to fetch for {ticker}: {e}")
            return None
    
    def fetch_all_ndaq_stat(self, start_date=None, end_date=None):
        """Fetch the entire NDAQ/STAT table (optionally filter by date)."""
        nasdaqdatalink.ApiConfig.api_key = self.api_key
        params = {}
        if start_date:
            params['start_date'] = start_date
        if end_date:
            params['end_date'] = end_date
        # data = nasdaqdatalink.get("NDAQ/STAT", params=params)
        data = nasdaqdatalink.get('NSE/OIL')
        print("start")
        # data = data.reset_index()
        print(data.head())
        print("end")
        print(data['symbol'])
        return data

