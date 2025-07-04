# src/loader.py

import duckdb
import pandas as pd
from src.config import DB_PATH, TABLE_NAME, FINANCIAL_TABLE_NAME
import os
from pyspark.sql import SparkSession
# from duckdb.experimental.spark.sql import SparkSession
import sqlite3
import urllib.request

SQLITE_JDBC_URL = "https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.45.1.0/sqlite-jdbc-3.45.1.0.jar"
JAR_LOCAL_PATH = os.path.join(os.getcwd(), "sqlite-jdbc-3.45.1.0.jar")



class SparkLoader:
    def __init__(self, db_path):
        self.jdbc_jar_path=self.ensure_sqlite_jdbc(JAR_LOCAL_PATH, SQLITE_JDBC_URL)
        self.spark = SparkSession.builder \
            .appName("IndexConstruction") \
            .config("spark.jars", self.jdbc_jar_path) \
            .getOrCreate()
        self.db_path = db_path
        self.url = f"jdbc:sqlite:{self.db_path}"
        self.driver = "org.sqlite.JDBC"
    
    def ensure_sqlite_jdbc(self, jar_path=JAR_LOCAL_PATH, jar_url=SQLITE_JDBC_URL):
        if not os.path.exists(jar_path):
            print(f"Downloading SQLite JDBC jar to {jar_path} ...")
            urllib.request.urlretrieve(jar_url, jar_path)
            print("Download complete.")
        else:
            print(f"Found existing SQLite JDBC jar at {jar_path}.")
        return jar_path

    def load_prices(self):
        return self.spark.read.format("jdbc") \
            .option("url", self.url) \
            .option("dbtable", "prices") \
            .option("driver", self.driver) \
            .load()

    def load_financials(self):
        return self.spark.read.format("jdbc") \
            .option("url", self.url) \
            .option("dbtable", "sp500_financials") \
            .option("driver", self.driver) \
            .load()


class Loader:
    def __init__(self, db_path=DB_PATH):
        self.table_name = TABLE_NAME
        self.financials_table_name = FINANCIAL_TABLE_NAME
        self.conn = sqlite3.connect(db_path)
        self.conn.execute('PRAGMA foreign_keys = ON;')  # Optional

    def get_existing_tickers(self, table):
        try:
            result = pd.read_sql_query(f"SELECT DISTINCT Ticker FROM {table}", self.conn)
            return set(result['Ticker'].tolist())
        except Exception:
            return set()

    def drop_table(self, table_name):
        self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

    def init_schema(self):
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                Date TEXT,
                Ticker TEXT,
                Open REAL,
                High REAL,
                Low REAL,
                Close REAL,
                Adj_Close REAL,
                Volume INTEGER,
                UNIQUE (Date, Ticker)
            )
        """)

    def init_schema_sp500_financials(self):
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.financials_table_name} (
                Date TEXT,
                Report_Date TEXT,
                Ticker TEXT,
                Total_Shares REAL,
                Total_Shares_Outstanding REAL,
                Previous_Close REAL,
                Market_Cap REAL,
                UNIQUE (Date, Ticker)
            )
        """)

    def save_prices(self, df: pd.DataFrame):
        self.drop_table(self.table_name)
        self.init_schema()
        df = df.rename(columns={
            'Adj Close': 'Adj_Close',
        })
        df = df[['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Adj_Close', 'Volume']]
        df.to_sql(self.table_name, self.conn, if_exists='append', index=False)

    def upsert_prices(self, df: pd.DataFrame):
        df = df.rename(columns={'Adj Close': 'Adj_Close'})
        values = df[['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Adj_Close', 'Volume']].values.tolist()
        query = f"""
            INSERT INTO {self.table_name}
                (Date, Ticker, Open, High, Low, Close, Adj_Close, Volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(Date, Ticker) DO UPDATE SET
                Open=excluded.Open,
                High=excluded.High,
                Low=excluded.Low,
                Close=excluded.Close,
                Adj_Close=excluded.Adj_Close,
                Volume=excluded.Volume
        """
        self.conn.executemany(query, values)
        self.conn.commit()

    def exists_for_date(self, table_name, ticker, date_str):
        query = f"SELECT 1 FROM {table_name} WHERE Ticker = ? AND Date = ? LIMIT 1"
        cur = self.conn.execute(query, [ticker, date_str])
        return cur.fetchone() is not None

    def upsert_financials(self, df: pd.DataFrame):
        print("DF columns:", df.columns.tolist())
        df = df.astype({
        'Date': str,
        'Report_Date': str,
        'Ticker': str,
        'Total_Shares': float,
        'Total_Shares_Outstanding': float,
        'Previous_Close': float,
        'Market_Cap': float,
        })
        # Replace NaNs with None so they map to NULL in SQLite
        df = df.where(pd.notnull(df), None)
        values = df[['Date', 'Report_Date', 'Ticker', 'Total_Shares', 'Total_Shares_Outstanding', 'Previous_Close', 'Market_Cap']].values.tolist()
        query = f"""
            INSERT INTO {self.financials_table_name}
                (Date, Report_Date, Ticker, Total_Shares, Total_Shares_Outstanding, Previous_Close, Market_Cap)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(Date, Ticker) DO UPDATE SET
                Report_Date=excluded.Report_Date,
                Total_Shares=excluded.Total_Shares,
                Total_Shares_Outstanding=excluded.Total_Shares_Outstanding,
                Previous_Close=excluded.Previous_Close,
                Market_Cap=excluded.Market_Cap
        """
        self.conn.executemany(query, values)
        self.conn.commit()
    

    
