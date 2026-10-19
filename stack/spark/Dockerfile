# Spark Connect server with the Iceberg runtime baked in (no downloads at start).
FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu
ARG ICEBERG=1.12.0
ARG MAVEN=https://repo1.maven.org/maven2/org/apache/iceberg
USER root
RUN curl -fsSL -o /opt/spark/jars/iceberg-spark-runtime.jar \
      $MAVEN/iceberg-spark-runtime-4.1_2.13/$ICEBERG/iceberg-spark-runtime-4.1_2.13-$ICEBERG.jar \
 && curl -fsSL -o /opt/spark/jars/iceberg-aws-bundle.jar \
      $MAVEN/iceberg-aws-bundle/$ICEBERG/iceberg-aws-bundle-$ICEBERG.jar
COPY start.sh /opt/start.sh
USER spark
ENTRYPOINT ["/bin/bash", "/opt/start.sh"]
