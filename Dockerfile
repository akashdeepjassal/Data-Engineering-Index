FROM apache/spark-py:latest

USER root

# Install pip and upgrade it (if needed)
RUN apt-get update 
# RUN apt-get install -y python3-pip && \
#     pip3 install --upgrade pip

# Copy your project files into the container
WORKDIR /app
COPY . /app

# Install Python dependencies
RUN pip install -r requirements.txt

# (Optional) Download DuckDB Spark connector JAR if needed
# ENV DUCKDB_SPARK_VERSION=0.9.2
# RUN wget -O /opt/spark/jars/duckdb_spark_connector_2.12-${DUCKDB_SPARK_VERSION}.jar \
#     https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_SPARK_VERSION}/duckdb_spark_connector_2.12-${DUCKDB_SPARK_VERSION}.jar
ENV PYSPARK_PYTHON=python3

USER 185

# Default command to run all ETL steps in order
CMD python3 first_load.py && \
    python3 daily_financials.py && \
    python3 daily_load.py && \
    python3 run_spark_index_creation.py

