FROM apache/airflow:3.3.2-python3.12
USER root
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /mlartifacts && chown airflow:root /mlartifacts
USER airflow
RUN pip install --no-cache-dir "apache-airflow==3.3.2" "mlflow==3.10.1" "scikit-learn==1.7.2" "psycopg2-binary==2.9.11"
ENV PYTHONPATH=/opt/demo
WORKDIR /opt/demo
