# Data-Engineering-Index

Step 1: Tool Comparison and Why Nasdaq Data Link (README section in Markdown)
Here’s a concise but thorough markdown section for your README, comparing common data sources and justifying your selection of Nasdaq Data Link (formerly Quandl):

Data Source Comparison
Source	Historical Data	US Market Coverage	Reliability	Ease of Use	Cost
Yahoo Finance	Good	Good	Moderate*	Easy	Free
Alpha Vantage	Moderate	Good	Good	Simple API	Free/Ltd
IEX Cloud	Good	Excellent	Very Good	API-based	Paid/Ltd
Polygon	Excellent	Excellent	Excellent	Modern API	Paid
Nasdaq Data Link	Excellent	Excellent	Excellent	Easy API	Free/Ltd

*Yahoo Finance often has rate limits and can be less reliable for large data pulls or automation.

Rationale for Choosing Nasdaq Data Link
For this assignment, Nasdaq Data Link was chosen due to:

Comprehensive and reliable historical US stock data: Provides well-maintained, clean, and complete datasets for US equities, including prices and market caps.

Ease of use: A modern API with Python SDK support, clear documentation, and consistent data schemas.

Reliability for production workflows: Maintained by Nasdaq, ensuring greater data accuracy and long-term availability compared to crowdsourced APIs.

Scalability: Designed for automated data ingestion and scalable queries, with both free and paid tiers suitable for prototyping and production.