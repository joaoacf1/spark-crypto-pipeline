FROM apache/airflow:3.0.0

USER root

# Install OpenJDK-17 and other dependencies
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get install -y ant && \
    apt-get install -y procps && \
    apt-get clean

# Set JAVA_HOME and other environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH

USER airflow

# Install PySpark
RUN pip install --no-cache-dir pyspark==3.5.0
