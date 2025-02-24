FROM apache/airflow:2.7.1-python3.11

USER root

# Atualizar pacotes e instalar dependências necessárias
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-11-jdk curl && \
    apt-get clean

# Configurar variável de ambiente do Java
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Baixar e instalar o Apache Spark 3.4.0 com Hadoop 3.3
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz | tar -xz -C /opt/ \
    && mv /opt/spark-3.4.0-bin-hadoop3 /opt/spark

# Definir variáveis de ambiente do Spark
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"

# Baixar e instalar Delta Lake 2.4.0
RUN mkdir -p /opt/spark/jars && \
    curl -fsSL https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar -o /opt/spark/jars/delta-core_2.12-2.4.0.jar && \
    curl -fsSL https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar -o /opt/spark/jars/delta-storage-2.4.0.jar

# Garantir que Airflow continue funcionando normalmente
USER airflow

# Remover versão antiga do OpenLineage antes de instalar a correta
RUN pip uninstall -y apache-airflow-providers-openlineage && \
    pip install --no-cache-dir apache-airflow-providers-openlineage>=1.8.0
