# Custom Airflow image. Pin everything, install deps at BUILD time, never at container start.
ARG AIRFLOW_VERSION=3.3.1
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

# Install extra providers / libs against the official constraints file so pip
# cannot silently upgrade Airflow's own dependencies.
COPY --chown=airflow:root requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -r /requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
