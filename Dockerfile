FROM apache/spark-py:latest

USER root

# Install pip and upgrade it (if needed)
RUN apt-get update 
# RUN apt-get install -y python3-pip && \
#     pip3 install --upgrade pip

# Install Python libraries
RUN pip3 install duckdb openpyxl yfinance

# (OPTIONAL) Download DuckDB Spark connector JAR
# You only need this if you want to use Spark <-> DuckDB integration
ENV DUCKDB_SPARK_VERSION=0.9.2
# RUN wget -O /opt/spark/jars/duckdb_spark_connector_2.12-${DUCKDB_SPARK_VERSION}.jar \
#     https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_SPARK_VERSION}/duckdb_spark_connector_2.12-${DUCKDB_SPARK_VERSION}.jar

USER 185  
# revert to the non-root user used by the image

