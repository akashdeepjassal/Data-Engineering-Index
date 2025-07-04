# src/loader.py

from pyspark.sql import SparkSession

class Loader:
    def __init__(self, db_path):
        self.spark = SparkSession.builder \
            .appName("IndexConstruction") \
            .config("spark.sql.execution.arrow.enabled", "true") \
            .getOrCreate()
        self.db_path = db_path

    def load_prices(self):
        return self.spark.read.format("duckdb") \
            .option("path", self.db_path) \
            .option("table", "Prices") \
            .load()

    def load_financials(self):
        return self.spark.read.format("duckdb") \
            .option("path", self.db_path) \
            .option("table", "sp500_financials") \
            .load()
