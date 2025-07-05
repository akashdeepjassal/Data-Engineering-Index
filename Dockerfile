FROM apache/spark-py:latest

USER root

RUN apt-get clean && rm -rf /var/lib/apt/lists/*
# Install pip and upgrade it (if needed)
RUN apt-get update 
RUN apt-get install -y python3-pip && \
    pip3 install --upgrade pip

# # Copy your project files into the container
WORKDIR /app
COPY . /app

# # Install Python dependencies
# RUN pip install -r requirements.txt

# # (Optional) Download DuckDB Spark connector JAR if needed
# # ENV DUCKDB_SPARK_VERSION=0.9.2
# # RUN wget -O /opt/spark/jars/duckdb_spark_connector_2.12-${DUCKDB_SPARK_VERSION}.jar \
# #     https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_SPARK_VERSION}/duckdb_spark_connector_2.12-${DUCKDB_SPARK_VERSION}.jar
# ENV PYSPARK_PYTHON=python3

# USER 185

# # Default command to run all ETL steps in order
# CMD python3 historical_load.py && \
#     python3 daily_financials.py && \
#     python3 daily_load.py && \
#     python3 create_index.py

# Set Airflow home and working directory
ENV AIRFLOW_HOME=/app/airflow

# Install system dependencies (for Python, Airflow, and MySQL if needed)
RUN apt-get install -y --no-install-recommends \
        build-essential \
        libmysqlclient-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Install Airflow (using constraints file as best practice)
ARG AIRFLOW_VERSION=2.8.4
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.10.txt"
RUN pip install --upgrade pip \
    && pip install "apache-airflow[sqlite]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# (Optional) Install Python dependencies for your project
# COPY requirements.txt /app/
COPY dags/ $AIRFLOW_HOME/dags/

RUN pip install --no-cache-dir -r requirements.txt


# Ensure necessary Airflow folders exist
RUN mkdir -p $AIRFLOW_HOME/dags $AIRFLOW_HOME/logs $AIRFLOW_HOME/plugins
RUN mkdir -p /app/db && chmod 777 /app/db

# Airflow: disable loading example DAGs
ENV AIRFLOW__CORE__LOAD_EXAMPLES=false

# Initialize Airflow DB and create an admin user with fixed password
RUN airflow db init && \
    airflow users create \
      --username admin \
      --password admin \
      --firstname Airflow \
      --lastname Admin \
      --role Admin \
      --email admin@example.com

ENV PYSPARK_PYTHON=python3

# USER 185

# Expose Airflow webserver port
EXPOSE 8080

# Entrypoint for Airflow webserver and scheduler (Airflow 2.2+ supports 'standalone')
CMD ["airflow", "standalone"]