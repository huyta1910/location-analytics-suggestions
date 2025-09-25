FROM apache/airflow:2.8.1

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends nodejs npm && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /
# Install dbt and the clickhouse adapter
RUN pip install --no-cache-dir -r /requirements.txt dbt-core dbt-clickhouse

COPY crawler/ /opt/airflow/crawler/
RUN cd /opt/airflow/crawler && npm install