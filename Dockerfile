# --- Shared JAR logic ---
FROM alpine:3.23.5 AS base
USER root

# Install curl
RUN apk add --no-cache curl

# Create /opt/spark/jars directory
RUN mkdir -p /opt/spark/jars

# Install required JAR connectors in /opt/spark/jars
RUN curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.13-4.0.3.jar \
  https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.3/spark-sql-kafka-0-10_2.13-4.0.3.jar && \
  curl -L -o /opt/spark/jars/hadoop-aws-3.4.0.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.0/hadoop-aws-3.4.0.jar && \
  curl -L -o /opt/spark/jars/aws-sdk-bundle-2.25.60.jar \
  https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.25.60/bundle-2.25.60.jar



# --- Spark ---
FROM apache/spark:4.0.3 AS spark
USER root
COPY --from=base /opt/spark/jars/ /opt/spark/jars/

# Install Python requirements
COPY requirements/spark.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt



# --- Airflow ---
FROM apache/airflow:3.2.2 AS airflow
USER root
COPY --from=spark /opt/spark /opt/spark

RUN chown -R airflow:root /opt/spark && chmod -R 755 /opt/spark

# Install OpenJDK 17 and clean apt cache
RUN apt-get update && \
  apt-get install -y openjdk-17-jre-headless && \
  apt-get clean

# Install Python requirements
COPY requirements/airflow.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir -r /tmp/requirements.txt

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
ENV PATH=$SPARK_HOME/bin:$PATH

USER airflow