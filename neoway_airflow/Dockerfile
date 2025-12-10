FROM apache/airflow:2.9.1

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    vim \
    curl \
    unzip \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ARG DUCKDB_VERSION=v1.1.3
RUN curl -L https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/duckdb_cli-linux-amd64.zip -o /tmp/duckdb.zip \
    && unzip -p /tmp/duckdb.zip > /usr/local/bin/duckdb \
    && chmod +x /usr/local/bin/duckdb \
    && rm /tmp/duckdb.zip

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
