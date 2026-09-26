# Flink 1.20 with Python 3.11 + PyFlink, and the Kafka SQL connector on the classpath.
FROM flink:1.20-scala_2.12-java17

USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-pip curl \
 && ln -sf /usr/bin/python3 /usr/bin/python \
 && pip3 install --no-cache-dir --break-system-packages apache-flink==1.20.2 \
 && curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-3.4.0-1.20.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar \
 && rm -rf /var/lib/apt/lists/*
USER flink
