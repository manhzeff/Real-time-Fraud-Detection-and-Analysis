# Use the official Airflow image as the base
FROM apache/airflow:2.10.5

# Switch to the root user
USER root

# Create the AIRFLOW_HOME directory and change its ownership to the airflow user
RUN mkdir -p /app/models

USER airflow
COPY requirements.txt /tmp/requirements.txt

RUN  pip install -r /tmp/requirements.txt

# Switch back to the airflow user
USER airflow


