from src.loader import Loader
from src.index import IndexBuilder
from src.config import DB_PATH, DB_FOLDER, TABLE_NAME
index_builder = IndexBuilder(DB_PATH)
df=index_builder.get_top_n_by_marketcap("2025-07-03")
print(df)
# example_date = df['Date'].max()  # Most recent date
# top_100 = index_builder.get_top_n_by_marketcap(example_date)
print(f"Top 100 on {example_date}:\n", top_100.head())