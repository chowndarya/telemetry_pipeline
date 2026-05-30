#!/bin/sh
set -e

INFLUX_HOST="${INFLUX_HOST:-http://influxdb:8181}"
DB_NAME="${DB_NAME:-tel_db}"
TABLE_NAME="${TABLE_NAME:-gpu_metrics}"

echo "Waiting for InfluxDB at $INFLUX_HOST ..."
# InfluxDB 3 health endpoint returns HTTP 200 when ready
until curl -fsS "$INFLUX_HOST/health" >/dev/null 2>&1; do
    echo "  ...still waiting"
    sleep 3
done
echo "InfluxDB is up."

echo "Creating database '$DB_NAME' (idempotent)..."
influxdb3 create database "$DB_NAME" --host "$INFLUX_HOST" 2>&1 | grep -v "already exists" || true

echo "Creating table '$TABLE_NAME' in database '$DB_NAME' (idempotent)..."
influxdb3 create table "$TABLE_NAME" \
    --database "$DB_NAME" \
    2>&1 | grep -v "already exists" || true

echo "✅ Initialization complete."