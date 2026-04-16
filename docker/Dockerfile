FROM apache/spark:4.0.0-python3

USER root

# Install delta-spark for Python
RUN pip3 install delta-spark==4.0.0

# Copy JARs from Mac Spark
COPY spark-jars/hadoop-aws-3.4.1.jar          /opt/spark/jars/
COPY spark-jars/aws-java-sdk-v2-bundle-2.21.33.jar /opt/spark/jars/
COPY spark-jars/delta-spark_2.13-4.0.0.jar   /opt/spark/jars/
COPY spark-jars/delta-storage-4.0.0.jar      /opt/spark/jars/

# Create writable directories for Spark artifacts and temp files
RUN mkdir -p /tmp/spark-artifacts /tmp/spark-local && \
    chmod 777 /tmp/spark-artifacts /tmp/spark-local

# Copy pipeline code
COPY jobs/     /app/jobs/
COPY utils/    /app/utils/
COPY scripts/  /app/scripts/

WORKDIR /app

USER spark
