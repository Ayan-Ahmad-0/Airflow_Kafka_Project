FROM apache/airflow:2.5.1

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3-dev \
        wget \
        curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Ensure python points to python3
RUN ln -sf /usr/bin/python3 /usr/bin/python

# Upgrade pip and install specific Python packages
RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir \
        psycopg2-binary==2.9.6 \
        kafka-python==2.0.2

# Copy your scripts into the container
COPY --chown=airflow:root ./scripts /opt/airflow/scripts
RUN chmod +x /opt/airflow/scripts/*.py

# Switch back to airflow user for safety
USER airflow
