#!/bin/bash
set -e

# Cleanup of all jobs in case of bronze crashing or stop signal
cleanup() {
  echo "[!] Shutting down all jobs..."
  kill $BRONZE_PID 2>/dev/null
  for pid in "${SILVER_PIDS[@]}"; do
    kill $pid 2>/dev/null
  done
  exit 1
}
trap cleanup SIGINT SIGTERM

SPARK_MASTER="spark://spark-master:7077"
JOBS_DIR="/app/jobs"

echo "[*] Starting bronze ingestion..."
/opt/spark/bin/spark-submit --master $SPARK_MASTER --conf spark.cores.max=${SPARK_APP_CORES} --driver-memory 512m --executor-memory 512m $JOBS_DIR/consumer_bronze.py &
BRONZE_PID=$!

SILVER_JOBS=(
  "silver/enriched_streams.py"
  "silver/tags.py"
  "silver/transitions.py"
)

for job in "${SILVER_JOBS[@]}"; do
  echo "[*] Starting silver job $job..."
  (
  until /opt/spark/bin/spark-submit --master "$SPARK_MASTER" --conf spark.cores.max="${SPARK_APP_CORES}" --driver-memory 512m --executor-memory 512m --py-files "$JOBS_DIR/common.py" "$JOBS_DIR/$job"; do
    echo "[X] $job crashed! Waiting 60 seconds before attempting restart..."
    sleep 60
  done
  ) &
  SILVER_PIDS+=($!)
done

# If the bronze job crashes, the container cleans all the processes and exits
wait $BRONZE_PID
echo "[-] Bronze job terminated!"

cleanup