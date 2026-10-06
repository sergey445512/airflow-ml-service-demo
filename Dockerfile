FROM apache/airflow:3.3.2-python3.12
USER root
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /mlartifacts && chown airflow:root /mlartifacts
USER airflow
RUN pip install --no-cache-dir "apache-airflow==3.3.2" "mlflow==3.10.1" "scikit-learn==1.7.2" "psycopg2-binary==2.9.11"
RUN pip install --no-cache-dir "pyopenssl==25.3.0" "asyncssh==2.21.1" && pip check
RUN pip install --no-cache-dir "prometheus-client==0.26.0" && pip check
RUN pip install --no-cache-dir "redis==6.4.0" && pip check
ENV PYTHONPATH=/opt/demo
WORKDIR /opt/demo
COPY --chown=airflow:root . /opt/demo
ARG SOURCE_REVISION=local
ENV DEMO_CODE_REVISION=${SOURCE_REVISION}
LABEL org.opencontainers.image.revision=${SOURCE_REVISION}
