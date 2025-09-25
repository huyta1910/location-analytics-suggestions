# Use the official Airflow image as a base
FROM apache/airflow:2.8.1

# Switch to root user to install system packages
USER root

# Install Node.js and npm
RUN apt-get update && \
    apt-get install -y --no-install-recommends nodejs npm && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user
USER airflow

# Copy and install Python dependencies
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# Copy the Node.js crawler script and install its dependencies
COPY crawler/ /opt/airflow/crawler/
RUN cd /opt/airflow/crawler && npm install

# Copy the Python data pipeline scripts
COPY datapipeline/ /opt/airflow/datapipeline/