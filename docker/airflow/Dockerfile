FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends build-essential curl \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    pandas==2.1.4 \
    ta==0.11.0 \
    elasticsearch==8.12.0 \
    psycopg2-binary==2.9.9 \
    requests==2.31.0
