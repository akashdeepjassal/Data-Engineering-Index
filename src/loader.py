# src/loader.py

import duckdb
import pandas as pd
from src.config import DB_PATH, TABLE_NAME, FINANCIAL_TABLE_NAME
import os

class Loader:
    def __init__(self, db_path=DB_PATH):
        self.table_name=TABLE_NAME
        self.financials_table_name = FINANCIAL_TABLE_NAME
        self.conn = duckdb.connect(db_path)
        
    def get_existing_tickers(self, tables):
        try:
            result = self.conn.execute(f"SELECT DISTINCT Ticker FROM {tables}").fetchdf()
            return set(result['Ticker'].tolist())
        except Exception:
            return set()
        
    def drop_table(self, table_name):
        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    def init_schema(self):
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                Date DATE,
                Ticker VARCHAR,
                Open DOUBLE,
                High DOUBLE,
                Low DOUBLE,
                Close DOUBLE,
                Adj_Close DOUBLE,
                Volume BIGINT,
                UNIQUE (Date, Ticker)
            )
        """)
    def init_schema_sp500_financials(self):
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.financials_table_name} (
            Date DATE,
            Report_Date DATE,
            Ticker VARCHAR,
            Total_Shares DOUBLE,
            Total_Shares_Outstanding DOUBLE,
            Previous_Close DOUBLE,
            Market_Cap DOUBLE,
            UNIQUE (Date, Ticker)
            )
            """
        )

    def save_prices(self, df: pd.DataFrame):
        self.drop_table(self.table_name)
        self.init_schema()
        # Clean column names if needed
        df = df.rename(columns={
            'Open': 'Open',
            'High': 'High',
            'Low': 'Low',
            'Close': 'Close',
            'Adj Close': 'Adj_Close',
            'Volume': 'Volume'
        })
        self.conn.execute(f"INSERT INTO {self.table_name} SELECT * FROM df")

    def upsert_prices(self, df: pd.DataFrame):
        # Ensure columns are correctly named
        df = df.rename(columns={
            'Open': 'Open',
            'High': 'High',
            'Low': 'Low',
            'Close': 'Close',
            'Adj Close': 'Adj_Close',
            'Volume': 'Volume'
        })

        # Prepare an upsert statement
        # DuckDB supports parameterized executemany for upserts
        query = f"""
            INSERT INTO {self.table_name} (Date, Ticker, Open, High, Low, Close, Adj_Close, Volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (Date, Ticker) DO UPDATE SET
                Open = excluded.Open,
                High = excluded.High,
                Low = excluded.Low,
                Close = excluded.Close,
                Adj_Close = excluded.Adj_Close,
                Volume = excluded.Volume;
        """

        values = df[['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Adj_Close', 'Volume']].values.tolist()
        self.conn.executemany(query, values)
    
    def exists_for_date(self, table_name, ticker, date_str):
        query = f"SELECT 1 FROM {table_name} WHERE Ticker = ? AND Date = ? LIMIT 1"
        result = self.conn.execute(query, [ticker, date_str]).fetchone()
        return result is not None
    
    
    
    def upsert_financials(self, df: pd.DataFrame):
        # Ensure columns are correctly named
        df = df.rename(columns={
            'Date': 'Date',
            'Report_Date': 'Report_Date',
            'Ticker': 'Ticker',
            'Total_Shares': 'Total_Shares',
            'Total_Shares_Outstanding': 'Total_Shares_Outstanding',
            'Previous_Close': 'Previous_Close',
            'Market_Cap': 'Market_Cap'
        })

        # Prepare an upsert statement
        # DuckDB supports parameterized executemany for upserts
        query = f"""
            INSERT INTO {self.financials_table_name} (Date, Report_Date, Ticker, Total_Shares, Total_Shares_Outstanding, Previous_Close, Market_Cap)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (Date, Ticker) DO UPDATE SET
                Report_Date = excluded.Report_Date,
                Total_Shares = excluded.Total_Shares,
                Total_Shares_Outstanding = excluded.Total_Shares_Outstanding,
                Previous_Close = excluded.Previous_Close,
                Market_Cap = excluded.Market_Cap;
        """
        values = df[['Date', 'Report_Date', 'Ticker', 'Total_Shares', 'Total_Shares_Outstanding', 'Previous_Close', 'Market_Cap']].values.tolist()
        self.conn.executemany(query, values)


    

    
