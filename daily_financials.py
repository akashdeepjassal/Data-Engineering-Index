import pandas as pd
import yfinance as yf
from src.fetcher import Fetcher
from src.loader import Loader
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor
from src.config import DB_FOLDER, TABLE_NAME, FINANCIAL_TABLE_NAME
import time

tickers = Fetcher.get_sp500_tickers()
tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', "MMM", "A", "TSLA", "AMD", "PLTR"] 
loader = Loader()
loader.init_schema_sp500_financials()
existing_tickers = loader.get_existing_tickers(FINANCIAL_TABLE_NAME)
tickers_list=missing_tickers = list(set(tickers) - existing_tickers)
today = date.today()-timedelta(1)
print(f"{len(missing_tickers)} tickers missing data for {today}.")
tickerSymbol = " ".join(tickers_list)
tickers = yf.Tickers(tickerSymbol)
print(tickers)
data = []

print(f"Running for {today}")


def fetch_data(symbol):
    print(f"Start {symbol}")
    print("Sleeping now for 15s")
    time.sleep(15)
    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info
        income_stmt = ticker.quarterly_income_stmt
        all_dates = income_stmt.columns
        if not all_dates.empty:
            latest_date = max(all_dates)
            total_shares = income_stmt[latest_date].get('Basic Average Shares', None)
        else:
            latest_date = None
            total_shares = None
        shares_outstanding = info.get('sharesOutstanding', None)
        previous_close = info.get('previousClose', None)
        mcap = shares_outstanding * previous_close if shares_outstanding and previous_close else None

        return ({
            "Date": today,
            "Report_Date": latest_date,
            "Ticker": symbol,
            "Total_Shares": total_shares,
            "Total_Shares_Outstanding": shares_outstanding,
            "Previous_Close": previous_close,
            "Market_Cap": mcap
        })
        
        
    except Exception as e:
        print(f"Error processing {symbol}: {e}")
        return None

# Convert to DataFrame


with ThreadPoolExecutor(max_workers=4) as executor:
    results = list(executor.map(fetch_data, tickers_list))

# df = pd.DataFrame(data)
# Clean up and convert
print(results)
df = pd.DataFrame([r for r in results if r is not None])
print(df)
print(df.columns)
# Convert date columns properly if present
if 'Date' in df.columns:
    df['Date'] = pd.to_datetime(df['Date'])
if 'Report_Date' in df.columns:
    df['Report_Date'] = pd.to_datetime(df['Report_Date'])

loader.upsert_financials(df)
