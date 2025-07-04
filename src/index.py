# src/index.py

import duckdb
import pandas as pd
from .config import DB_PATH

class IndexBuilder:
    def __init__(self, db_path=DB_PATH):
        self.conn = duckdb.connect(db_path)

    def get_top_n_by_marketcap(self, date, n=100):
        # Calculate Market Cap = Close * Shares Outstanding (if available)
        # For now, just pick the top 100 by Volume as a placeholder (improve if shares out is available)
        q = f"""
        SELECT Ticker, Close*Volume AS marketcap
        FROM prices
        WHERE Date = '{date}'
        ORDER BY marketcap DESC
        LIMIT {n}
        """
        return self.conn.execute(q).df()

    # Add more index logic as needed...
