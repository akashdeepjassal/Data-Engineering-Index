# src/index.py

import duckdb
import sqlite3
import pandas as pd
from .config import DB_PATH
from pyspark.sql.window import Window
import pyspark.sql.functions as F

class IndexBuilderSpark:
    def __init__(self, loader):
        self.loader = loader

    def __init__(self, loader):
        self.loader = loader

    def build_equal_weighted_index(self, start_date, end_date):
        prices = self.loader.load_prices()
        financials = self.loader.load_financials()

        prices = prices.withColumn("Date", F.to_date(F.col("Date")))
        financials = financials.withColumn("Date", F.to_date(F.col("Date")))

        print("Loaded prices and financials data.")
        print(f"Prices sample:")
        prices.show(5)
        print(f"Financials sample:")
        financials.show(5)

        joined = prices.join(
            financials,
            (prices.Ticker == financials.Ticker),
            # & (financials.Date <= prices.Date),
            "left"
        ).select(
            prices["Date"].alias("Price_Date"),
            prices["Ticker"],
            prices["Close"],
            prices["Adj_Close"],
            prices["Volume"],
            financials["Date"].alias("Fund_Date"),
            # financials["Market_Cap"]
            financials["Total_Shares_Outstanding"]
        )

        
        print("After as-of join, sample:")
        joined.show(5)
        window = Window.partitionBy("Price_Date", "Ticker").orderBy(F.col("Fund_Date").desc())
        joined = joined.withColumn("rank", F.row_number().over(window)).filter(F.col("rank") == 1).drop("rank")

        joined = joined.withColumn(
            "Market_Cap",
            F.col("Total_Shares_Outstanding") * F.col("Adj_Close")
        )

        print("After picking latest Fund_Date for each (Price_Date, Ticker), sample:")
        joined.show(5)

        daily = joined.filter((joined.Price_Date >= start_date) & (joined.Price_Date <= end_date)).cache()
        all_days = [row["Price_Date"] for row in daily.select("Price_Date").distinct().orderBy("Price_Date").collect()]

        print(f"Total trading days to process: {len(all_days)}")
        print(f"First 5 days: {all_days[:5]}")

        index_tracking = []
        previous_constituents = set()
        index_value = 100.0
        prev_closes = {}

        for day in all_days:
            print(f"\nProcessing {day}...")
            day_df = daily.filter(F.col("Price_Date") == day)

            # Filter out invalid rows
            day_df_valid = day_df.filter(
                F.col("Market_Cap").isNotNull() & (F.col("Market_Cap") > 0) &
                F.col("Close").isNotNull() & (F.col("Close") > 0)
            )

            valid_count = day_df_valid.count()
            print(f"  Valid tickers with Market_Cap & Close: {valid_count}")

            if valid_count < 100:
                print(f"  WARNING: Only {valid_count} valid stocks found on {day}")

            # Show a few suspicious/edge cases if any
            invalid = day_df.filter(
                (F.col("Market_Cap").isNull() | (F.col("Market_Cap") <= 0)) |
                (F.col("Close").isNull() | (F.col("Close") <= 0))
            )
            inv_count = invalid.count()
            if inv_count > 0:
                print(f"  {inv_count} rows with missing/zero Market_Cap or Close on {day}")
                invalid.show(5)

            # Use Market_Cap to select top 100
            top100 = day_df_valid.orderBy(F.col("Market_Cap").desc_nulls_last()).limit(100).cache()
            constituents = set([row["Ticker"] for row in top100.select("Ticker").collect()])
            print(f"  Top100 tickers: {list(constituents)[:5]} ... ({len(constituents)} total)")

            if len(constituents) == 0:
                print(f"  ERROR: No valid constituents for {day}, skipping calculation for this day.")
                continue

            # Print stats for debug
            price_stats = top100.agg(
                F.mean("Close").alias("mean_close"),
                F.min("Close").alias("min_close"),
                F.max("Close").alias("max_close"),
            ).collect()[0]
            print(f"  Top100 Close stats: mean={price_stats['mean_close']}, min={price_stats['min_close']}, max={price_stats['max_close']}")

            avg_price = price_stats['mean_close']
            prev_avg = (
                sum([prev_closes[t] for t in constituents if t in prev_closes]) / 100
                if prev_closes and len(constituents) == 100 else avg_price
            )
            daily_return = (avg_price - prev_avg) / prev_avg if prev_avg else 0.0

            print(f"  Previous average: {prev_avg}")
            print(f"  Index value before: {index_value}")
            print(f"  Calculated daily return: {daily_return}")

            if abs(daily_return) > 0.2:
                print(f"  WARNING: Unusually large daily return: {daily_return} on {day}")

            index_value = index_value * (1 + daily_return)

            added = constituents - previous_constituents
            removed = previous_constituents - constituents
            print(f"  {len(added)} added, {len(removed)} removed tickers.")

            for row in top100.select("Ticker", "Close").collect():
                prev_closes[row["Ticker"]] = row["Close"]

            index_tracking.append({
                "Date": day,
                "Index Value": round(index_value, 4),
                "Daily Return": round(daily_return, 6),
                "Constituents": list(constituents),
                "Added": list(added),
                "Removed": list(removed)
            })
            previous_constituents = constituents

        print("\nIndex construction finished. First 3 results:")
        for i, entry in enumerate(index_tracking[:3]):
            print(f"  {entry}")

        return index_tracking

        
    def summarize(self, tracking):
        df = pd.DataFrame(tracking)
        df["Cumulative Return"] = (df["Index Value"] / df["Index Value"].iloc[0]) - 1
        best = df.loc[df["Daily Return"].idxmax()]
        worst = df.loc[df["Daily Return"].idxmin()]
        summary = {
            "Total Composition Changes": df["Added"].apply(len).sum() + df["Removed"].apply(len).sum(),
            "Best Performing Day": best["Date"],
            "Best Return": best["Daily Return"],
            "Worst Performing Day": worst["Date"],
            "Worst Return": worst["Daily Return"],
            "Aggregate Return": df["Cumulative Return"].iloc[-1]
        }
        return summary

class IndexBuilder:
    def __init__(self, db_path=DB_PATH):
        # self.conn = duckdb.connect(db_path)
        self.conn = sqlite3.connect(db_path)

    def get_top_n_by_marketcap(self, date, n=100):
        # Calculate Market Cap = Close * Shares Outstanding (if available)
        # For now, just pick the top 100 by Volume as a placeholder (improve if shares out is available)
        q = f"""
        SELECT Ticker, Close * Volume AS marketcap
        FROM prices
        WHERE Date = ?
        ORDER BY marketcap DESC
        LIMIT ?
        """
        # Use Pandas for convenience
        return pd.read_sql_query(q, self.conn, params=(date, n))

    # Add more index logic as needed...
