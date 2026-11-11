FROM apache/airflow:3.4.0

USER root

RUN python -m venv /opt/data-venv \
    && chown -R airflow:root /opt/data-venv

USER airflow

COPY requirements-data.txt /tmp/requirements-data.txt

RUN /opt/data-venv/bin/pip install --no-cache-dir -r /tmp/requirements-data.txt
