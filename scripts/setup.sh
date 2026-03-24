#!/bin/bash
# ================================================================
#  Oracle Health Pipeline — Setup
#  Run after: docker compose up -d (your existing stack)
#  Does: creates EHR tables + registers Debezium connector
# ================================================================

set -e
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'

log_ok()   { echo -e "${GREEN}[✓]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[!]${NC} $1"; }
log_err()  { echo -e "${RED}[✗]${NC} $1"; }

echo ""
echo "================================================"
echo "  Oracle Health Pipeline — Setup"
echo "================================================"
echo ""

# ── 1. Create EHR tables in Postgres ────────────────────────
echo "Creating EHR tables in demo_db..."
PGPASSWORD=demo psql -h localhost -U demo -d demo_db -f sql/init.sql

if [ $? -eq 0 ]; then
    log_ok "EHR tables created"
else
    log_err "Failed to create tables. Is Postgres running?"
    exit 1
fi

# ── 2. Wait for Debezium Connect ─────────────────────────────
echo ""
echo "Waiting for Debezium Connect on :8083..."
until curl -s http://localhost:8083/connectors > /dev/null 2>&1; do
    echo -n "."
    sleep 3
done
log_ok "Debezium Connect is ready"

# ── 3. Register connector ─────────────────────────────────────
echo ""
echo "Registering EHR CDC connector..."

HTTP=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST http://localhost:8083/connectors \
    -H "Content-Type: application/json" \
    -d @connectors/ehr_connector.json)

if [ "$HTTP" = "201" ]; then
    log_ok "Connector registered (HTTP 201)"
elif [ "$HTTP" = "409" ]; then
    log_warn "Connector already exists — skipping"
else
    log_err "Failed to register connector (HTTP $HTTP)"
    echo "Check: curl http://localhost:8083/connectors"
    exit 1
fi

# ── 4. Check connector status ─────────────────────────────────
sleep 5
STATE=$(curl -s http://localhost:8083/connectors/ehr-connector/status \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'])" 2>/dev/null || echo "UNKNOWN")

if [ "$STATE" = "RUNNING" ]; then
    log_ok "Connector state: RUNNING"
else
    log_warn "Connector state: $STATE"
    echo "Check logs: docker logs connect"
fi

# ── 5. Verify Kafka topics ────────────────────────────────────
echo ""
echo "Kafka topics created by Debezium:"
sleep 5
docker exec kafka kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --list 2>/dev/null | grep "^ehr\." | sed 's/^/  /' || \
docker exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --list 2>/dev/null | grep "^ehr\." | sed 's/^/  /'

# ── Done ──────────────────────────────────────────────────────
echo ""
echo "================================================"
echo "  Setup complete! Next step:"
echo ""
echo "  spark-submit \\"
echo "    --master local[4] \\"
echo "    --packages io.delta:delta-core_2.12:2.4.0,\\"
echo "  org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,\\"
echo "  org.apache.hadoop:hadoop-aws:3.3.4 \\"
echo "    spark/streaming_bronze.py"
echo ""
echo "  Then check MinIO: http://localhost:9000"
echo "  Bucket: comet → ehr/bronze/"
echo "================================================"