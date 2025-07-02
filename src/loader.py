# src/loader.py

import duckdb
import pandas as pd
from config import DATABASE_PATH

class Loader:
    def __init__(self, db_path=DATABASE_PATH):
        self.conn = duckdb.connect(db_path)

    def create_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS stock_prices (
                date DATE,
                ticker VARCHAR,
                adj_close DOUBLE,
                shares_outstanding DOUBLE,
                market_cap DOUBLE
            );
        """)

    def insert_data(self, df: pd.DataFrame):
        # Only insert relevant columns
        self.conn.execute("""
            INSERT INTO stock_prices
            SELECT * FROM df
        """)
    
    def fetch_top_100_by_marketcap(self, date):
        return self.conn.execute(f"""
            SELECT ticker, market_cap FROM stock_prices
            WHERE date='{date}'
            ORDER BY market_cap DESC
            LIMIT 100
        """).fetchdf()

    def close(self):
        self.conn.close()
