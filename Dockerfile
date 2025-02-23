FROM apache/airflow:2.7.1-python3.11

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64

USER airflow

# Remover versão antiga do OpenLineage antes de instalar a correta
RUN pip uninstall -y apache-airflow-providers-openlineage && \
    pip install --no-cache-dir apache-airflow-providers-openlineage>=1.8.0
