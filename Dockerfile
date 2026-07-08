# --- Shared JAR logic ---
FROM alpine:3.23.5 AS base
USER root

# Install curl
RUN apk add --no-cache curl

# Create /opt/spark/jars directory
RUN mkdir -p /opt/spark/jars

# Install required JAR connectors in /opt/spark/jars
# 1. Spark SQL Kafka 4.0.3
RUN curl -L -o /opt/spark/jars/spark-sql-kafka-0-10_2.13-4.0.3.jar \
  https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.3/spark-sql-kafka-0-10_2.13-4.0.3.jar && \
  # 2. Spark Token Provider Kafka 4.0.3
  curl -L -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.13-4.0.3.jar \
  https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.3/spark-token-provider-kafka-0-10_2.13-4.0.3.jar && \
  # 3. Kafka Clients 3.9.1
  curl -L -o /opt/spark/jars/kafka-clients-3.9.1.jar \
  https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar && \
  # 4. Commons Pool 2.12.0
  curl -L -o /opt/spark/jars/commons-pool2-2.12.0.jar \
  https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar && \
  # 5. Hadoop AWS 3.4.0
  curl -L -o /opt/spark/jars/hadoop-aws-3.4.0.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.0/hadoop-aws-3.4.0.jar && \
  # 6. AWS SDK Bundle 2.25.60
  curl -L -o /opt/spark/jars/aws-sdk-bundle-2.25.60.jar \
  https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.25.60/bundle-2.25.60.jar && \
  # 7. Delta Spark 4.0.1
  curl -L -o /opt/spark/jars/delta-spark_2.13-4.0.1.jar \
  https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/4.0.1/delta-spark_2.13-4.0.1.jar && \
  # 8. Delta Storage 4.0.1
  curl -L -o /opt/spark/jars/delta-storage-4.0.1.jar \
  https://repo1.maven.org/maven2/io/delta/delta-storage/4.0.1/delta-storage-4.0.1.jar && \
  # 9. ClickHouse Spark Connector 4.0.2
  curl -L -o /opt/spark/jars/clickhouse-spark-runtime-4.0_2.13-0.10.0.jar \
  https://repo1.maven.org/maven2/com/clickhouse/spark/clickhouse-spark-runtime-4.0_2.13/0.10.0/clickhouse-spark-runtime-4.0_2.13-0.10.0.jar && \
  # 10. ClickHouse JDBC 0.9.5 ('all' for transitive dependencies)
  curl -L -o /opt/spark/jars/clickhouse-jdbc-0.9.5-all.jar \
  https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.9.5/clickhouse-jdbc-0.9.5-all.jar


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