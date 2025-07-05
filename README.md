# Data-Engineering-Index

## Objective

Construct and track an **equal-weighted custom index** comprising the top 100 US stocks by market cap, updated daily and monitored over the past month. The output is a well-structured Excel file with all relevant tracking and insights, plus clean, modular Python code.

---

## Table of Contents

- [Instructions for Reviewers](#instructions-for-reviewers)
- [Project Structure](#project-structure)
- [Data Sources & Acquisition](#data-sources--acquisition)
- [Data Storage](#data-storage)
- [Index Construction Logic](#index-construction-logic)
- [Code Overview](#code-overview)
- [How to Run](#how-to-run)
- [Excel Output](#excel-output)
- [Design Decisions & Challenges](#design-decisions--challenges)
- [Scalability & Maintenance](#scalability--maintenance)


---

##### Instructions for Reviewers
```
All code is modular, commented, and ready for inspection.

Use Airflow as compared to runnign from scratch.
The DB files ahve been populated already, please delete dbs flder and then build docker image

To rerun from scratch, use the data loaders to populate the database, then run the index script.

The Excel output is fully self-contained and interpretable.

See comments in src/index.py and src/exporter.py for additional documentation.
```

---

## Project Structure
```tree
.
├── create_index.py
├── dags
│   ├── daily_and_index_dag.py
│   └── historical_and_financials_dag.py
├── daily_financials.py
├── daily_load.py
├── db
│   ├── index_data.duckdb
│   └── prices_2025-07-04.csv
├── dbs
│   ├── index_data.sqlite
│   ├── prices_2025-07-04.csv
│   └── prices_2025-07-05.csv
├── Dockerfile
├── historical_load.py
├── index_output_2025-07-04.xlsx
├── index_output_2025-07-05.xlsx
├── java_opts.txt
├── README.md
├── requirements.txt
├── sqlite-jdbc-3.45.1.0.jar
└── src
    ├── __init__.py
    ├── __pycache__
    ├── config.py
    ├── exporter.py
    ├── fetcher.py
    ├── index.py
    ├── loader.py
    ├── spark_loader.py
    └── utils.py
```


---

## Data Sources & Acquisition

### Sources Evaluated

- **Yahoo Finance**:  
  - **Chosen.** Reliable, broad US market coverage, free, provides daily prices and fundamental data via [yfinance](https://pypi.org/project/yfinance/).
- **Alpha Vantage, IEX Cloud, Quandl, Polygon**:  
  - Evaluated but limited by API keys, quotas, or coverage.

### Why Yahoo Finance?

- **Historical & daily data:** Consistent daily OHLCV prices and fundamental data (e.g., shares outstanding).
- **Coverage:** All S&P 500 stocks, up-to-date.
- **API:** Python library, well-documented, robust.

### How Data is Fetched

- **S&P 500 tickers:** Scraped from Wikipedia for accuracy and timeliness.
- **Price and fundamental data:** Pulled via yfinance, batch processed, and saved into DuckDB/SQLite for fast analytics.

---

**Equal Weighted Index Formula**

$$
\text{Index Return} = \frac{1}{n} \sum_{i=1}^{n} R_i
$$

---

## Data Storage

- **SQLITE3** is used as the primary analytical storage engine.
  - Fast, in-memory, SQL-compliant, no external dependencies.
  - Note:
    DuckDB was considered for analytics, but has limited compatibility with PySpark for direct integration and parallel processing.
    SQLite was chosen for its universal support, reliability in containerized environments, and seamless integration with both pandas and Spark workflows (using JDBC or CSV intermediates).
    SQLite also makes the solution portable, easy to inspect, and compatible with most data engineering tools.
- **Tables:**
  - `Prices`: Daily prices (`Date`, `Ticker`, `Open`, `High`, `Low`, `Close`, `Adj_Close`, `Volume`)
  - `sp500_financials`: Fundamental data (`Date`, `Report_Date`, `Ticker`, `Total_Shares_Outstanding`, etc.)

---

## Index Construction Logic

### Overview

- For each trading day in the past month:
  - **Join daily price data with the most recent available outstanding shares** for each ticker.
  - **Calculate daily market cap:**  
    `Market Cap = Total Shares Outstanding × Adj_Close`
  - **Select top 100 stocks** by daily market cap.
  - **Rebalance index** if composition changes.
  - **Equal-weighted index:** Index value is an average of price returns across the 100 constituents.
  - **Track:** Index value, daily return, cumulative return, constituents, and composition changes.

### Business Rules & Edge Cases

- If fundamental data is only available as the latest snapshot, it is used for all prior dates (limitation noted).
- If fewer than 100 stocks have valid data, the day is flagged and skipped or logged.
- All data processing leverages Spark for scalable, robust computation.

---

## Code Overview

**Key modules:**
- `src/loader.py`: Loads price and fundamental data from DuckDB/SQLite into Spark DataFrames.
- `src/index.py`: Constructs and tracks the index, including joins, filtering, and all index calculations.
  - If only one financials row per ticker, joins on Ticker only.
  - Computes daily and cumulative returns, and index composition changes.
- `src/exporter.py`: Exports the final results into a structured Excel workbook.
- Other scripts (`first_load.py`, `daily_load.py`, etc.) handle data loading and updates.

---

## How to Run

```
Note: reccomd to use docker as the standalone solution may not work on some OS and is tailor made for MAC and may fail with some mac os versions
```

### 1. Setup Environment

#### Run Locally

##### Install SPark on MAC
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
brew install --cask miniconda
conda create -n pyspark_env python=3.11
conda activate pyspark_env
brew install openjdk@11
brew install apache-spark
export JAVA_HOME=$(/usr/libexec/java_home)
export SPARK_HOME="/opt/homebrew/Cellar/apache-spark/3.5.0/libexec"
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
```

##### Test Spark Installation

```bash
pip install findspark jupyter
```
```python
import findspark
findspark.init("/opt/homebrew/Cellar/apache-spark/3.5.0/libexec")

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()

data = [("xyz","400"),("abc","450"),("qwe","678")]
df = spark.createDataFrame(data)
df.show()

```

##### Install dependencies:

```bash
pip install -r requirements.txt &&
python3 first_load.py && 
python3 daily_financials.py &&
python3 daily_load.py &&
python3 run_spark_index_creation.py
```

### 2. Prepare Data

Ensure db/index_data.duckdb (or the relevant SQLite DB) exists and is populated with both Prices and sp500_financials tables.

The latest daily prices and financials should be present.

See first_load.py or daily_load.py for sample ETL routines.
Run first_load.py for historic load and daily_load.py  for Day on day load.

### 3. Run Index Construction
```bash
python run_spark_index_creation.py
```
Generates index_output.xlsx in the project root with all required sheets.

### 4. Excel Output
The Excel file (index_output.xlsx) contains:

```index_performance:

Columns: Date, Index Value, Daily Return, Cumulative Return
```
```
daily_composition:

Date, 100 constituent tickers for each day
```
```
composition_changes:

Date, Added, Removed tickers (shows all dates, blanks if no change)
```
```
summary_metrics:

Total composition changes, best/worst days, aggregate return, and other insights.
```

No charts or visualizations are included, as per requirements.

#### Easier Solution Run with Docker 

```bash
docker build -t spark-py-duckdb-airflow .
docker run -p 8080:8080 spark-py-duckdb-airflow
# docker run --rm -v "$PWD":/app spark-py-duckdb:latest
```

##### Access Airflow UI:

```
Go to http://localhost:8080 in your browser.
```
##### Login

```
Username: admin
Password: admin
```

##### Trigger a DAG:

```
Two DAGs are available under the Airflow UI:

historical_and_financials — runs historical_load.py then daily_financials.py

daily_and_index — runs daily_load.py, daily_financials.py, then create_index.py

Trigger as needed, monitor logs and task status in real time.
```

#### Design Decisions & Challenges

# Data Acquisition Tools Comparison

| Feature                | Yahoo Finance (yfinance) | Nasdaq Data Link (Quandl) | Alpha Vantage | Polygon.io   |
|------------------------|:------------------------:|:-------------------------:|:-------------:|:------------:|
| **Free Tier**          | ✅                       | Limited/Some Paid         | ✅            | No           |
| **Data Coverage**      | Wide (US + Intl. Stocks) | US Focused                | US + Intl.    | US, Crypto   |
| **Real-time Data**     | Delayed                  | Delayed/Historical        | Delayed       | Real-time    |
| **Ease of Use**        | Very Easy (yfinance API) | Moderate (API Key, EOD)   | Moderate      | API Only     |
| **Historical Depth**   | Excellent                | Good                      | Good          | Good         |
| **Documentation**      | Good                     | Good                      | Moderate      | Good         |
| **Community Support**  | High (open source)       | Moderate                  | Moderate      | Low          |
| **Rate Limits**        | Moderate                 | Varies (plan based)       | Strict        | Strict       |
| **Direct Python Lib**  | ✅ yfinance              | quandl                    | alpha_vantage | ❌           |

#### Why Yahoo Finance (yfinance)?
- **No API Key required:** Fast to get started for prototyping and student projects.
- **Free & open-source:** No upfront costs.
- **Comprehensive US and international coverage:** Ideal for S&P 500 or global equity projects.
- **All core daily price fields:** Open, High, Low, Close, Volume, Adj Close, etc.
- **Good community & support:** Problems are well-documented on forums and GitHub.
- **Flexible DataFrames:** Well-integrated with pandas, easy for further manipulation.
- **Drawback:** Rate limits can be hit with very high-frequency downloads, but this can be handled via batching or pauses.
- **Usage:** Use a VPN with Japan/US location for low rate limits

---

### S&P 500 Ticker Data

```
Despite the variety of free and paid financial data providers, 
no major public API provides a current, authoritative, and programmatically 
accessible list of S&P 500 constituent tickers as part of their free tier
```
- **Yahoo Finance (yfinance):**
    ```
    Does not provide an endpoint or API method to fetch the latest S&P 500 constituent tickers. Its ^GSPC symbol tracks the index price, not its members.
    ```
- **Nasdaq Data Link (Quandl):**
    ```
    Offers historical and alternative data, but S&P 500 constituent lists are not available in the free tier, and most index constituent feeds are behind a paywall or require special licensing.
    Sharadar and Equities360 product give a comprehensive data but are for paid usera only.
    ```

- **Alpha Vantage:**
    ```
    Does not provide S&P 500 constituents via their free API. Ticker metadata endpoints only return individual security info, not index membership.
    ```
- **Polygon.io:**
    ```
    Requires a paid subscription for access to index constituents or full ticker universes.
    ```
- **Wikipedia:**
    ```
    Wikipedia’s S&P 500 page is maintained by the community, reflecting index rebalances quickly and accurately.

    It is open, free, and requires no API key or paid subscription.

    The page is reliably structured for scraping with tools like pandas, allowing for easy programmatic extraction.

    This ensures our project always uses the most current set of S&P 500 tickers, while avoiding vendor lock-in and paywalls.
    ```

---

## Step 2: Solution Plan

### Data Source Chosen:

```
Yahoo Finance (via yfinance). Best trade-off of reliability, coverage, and accessibility.
```

#### Market Cap Calculation: 
```
Calculated as Total_Shares_Outstanding × Adj_Close for each ticker and day.
```

#### Financials Join:
```
If only one snapshot available, join on Ticker only.

In production, an as-of join with quarterly reporting dates would be used to avoid lookahead bias.
```

#### Edge Cases:
```
Days with incomplete data are flagged.

Index not calculated if fewer than 100 valid stocks.
```

#### Testing & Debug:
```
Extensive logging and debugging statements added for transparency, especially around data joins and index jumps.
```

#### Excel Export:

All output requirements (including cumulative return) fully satisfied.

Every trading day included in composition_changes, even with no changes.

#### Scalability & Maintenance

##### Automation:

All scripts modular; can be scheduled for daily/weekly runs.

Data fetching and ETL separated for easier orchestration (e.g., with Airflow or cron).

##### Scaling:

Spark supports scaling to larger universes, more metrics, or more granular data (minute, not just daily).

To add more data sources, implement additional fetchers/loaders and unify via a staging layer.


##### Advanced Analytics:

Architecture easily supports additional index methodologies, attribution analysis, or custom metrics.

##### Real-Time Updates:

Adapt ETL for streaming updates or intraday refresh with minimal code changes.

