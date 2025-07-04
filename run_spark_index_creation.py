# main.py

from src.loader import SparkLoader
from src.index import IndexBuilderSpark
from src.exporter import Exporter
from src.config import DB_PATH, TABLE_NAME, FINANCIAL_TABLE_NAME
import datetime

if __name__ == "__main__":
    OUTPUT_XLSX = "index_output.xlsx"
    END_DATE = datetime.date.today().strftime("%Y-%m-%d")
    START_DATE = (datetime.date.today() - datetime.timedelta(days=30)).strftime("%Y-%m-%d")

    loader = SparkLoader(DB_PATH)
    builder = IndexBuilderSpark(loader)
    tracking = builder.build_equal_weighted_index(START_DATE, END_DATE)
    summary = builder.summarize(tracking)
    Exporter.export(tracking, summary, OUTPUT_XLSX)
    print("Index calculation and export completed!")
