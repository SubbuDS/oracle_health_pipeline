FROM apache/spark:4.0.0-python3

USER root

# Install delta-spark for Python
RUN pip3 install delta-spark==4.0.0

# Download required JARs for MinIO/S3A and Delta Lake
RUN curl -o /opt/spark/jars/hadoop-aws-3.4.1.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar && \
    curl -o /opt/spark/jars/aws-java-sdk-v2-bundle-2.21.33.jar \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.21.33/bundle-2.21.33.jar && \
    curl -o /opt/spark/jars/delta-spark_2.13-4.0.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/4.0.0/delta-spark_2.13-4.0.0.jar && \
    curl -o /opt/spark/jars/delta-storage-4.0.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/4.0.0/delta-storage-4.0.0.jar

# Create writable dirs for Spark
RUN mkdir -p /tmp/spark-artifacts /tmp/spark-local && \
    chmod 777 /tmp/spark-artifacts /tmp/spark-local

# Copy pipeline code
COPY jobs/     /app/jobs/
COPY utils/    /app/utils/
COPY scripts/  /app/scripts/

WORKDIR /app

USER spark
