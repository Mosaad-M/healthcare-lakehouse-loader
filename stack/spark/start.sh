#!/bin/bash
# Spark Connect in the foreground. Catalog settings, including the maintenance principal's
# credential, come from the properties file the bootstrap writes to the state volume.
set -euo pipefail
export SPARK_NO_DAEMONIZE=true
exec /opt/spark/sbin/start-connect-server.sh \
  --properties-file /state/spark-maintenance.conf \
  --conf spark.driver.memory="${SPARK_DRIVER_MEMORY:-1g}"
