# Oracle Health EHR Data Pipeline

A production-grade EHR data pipeline built on a medallion architecture (Bronze / Silver / Gold), 
using CDC from PostgreSQL through to queryable Delta Lake tables on MinIO.

## Architecture

```
PostgreSQL → Debezium CDC → Kafka → Spark Streaming → Bronze (Delta/MinIO)
                                                            ↓
                                                    Silver (Delta/MinIO)
                                                            ↓
                                                     Gold (Delta/MinIO)
                                                            ↓
                                                   Text-to-SQL / RAG API
```

## Stack

| Layer | Technology |
|-------|-----------|
| Source Database | PostgreSQL 15 (Docker) |
| CDC | Debezium 2.x (Kafka Connect) |
| Message Broker | Apache Kafka (Docker) |
| Stream Processing | Apache Spark 4.0 (PySpark) |
| Table Format | Delta Lake 4.0 |
| Object Storage | MinIO (local S3-compatible) |
| Orchestration | Apache Airflow |
| Monitoring | Grafana + ClickHouse |
| API | FastAPI (Text-to-SQL + RAG) |

## Pipeline Layers

### Bronze
- Raw CDC events from Kafka, written as Delta Lake to MinIO
- Append-only — no transforms, no deletions
- PII masked at ingestion (SSN, phone, email, name — SHA-256 hashed)
- Preserves `__op` field (c/u/d) from Debezium envelope
- Tables: `patients`, `adt_events`, `lab_results`, `vitals`

### Silver
- Cleaned, deduplicated, type-cast data
- MERGE operation handles inserts, updates, and deletes from CDC
- Clinical enrichments:
  - `date_of_birth` converted from epoch-days to proper date
  - NEWS2 score calculated from vitals (SpO2, heart rate)
  - Abnormal lab flag classification (HIGH/LOW/NORMAL)
  - ICU flag derived from ADT event type + ward
- Tables: `patients`, `adt_events`, `lab_results`, `vitals`

### Gold
- Aggregated, analytics-ready tables
- Tables: `patient_360`, `daily_census`, `critical_alerts`, `lab_turnaround_kpi`

## Key Design Decisions

**Why CDC over batch extraction?**  
Debezium tails the PostgreSQL WAL (Write-Ahead Log), registering as a logical replication slot. 
This captures every INSERT, UPDATE, and DELETE in real time without polling.

**Why Delta Lake over plain Parquet?**  
Delta provides ACID transactions, schema enforcement, and MERGE support — essential for 
applying CDC updates and deletes to Silver without duplicating records.

**Why Bronze is append-only?**  
Bronze is the audit trail. Every CDC event is preserved exactly as received. 
Silver applies business logic on top — Bronze never gets modified.

**MERGE key per table:**
- patients: `patient_id`
- adt_events: `event_id`
- lab_results: `result_id`
- vitals: `vital_id`

## Project Structure

```
oracle_health_pipeline/
├── spark/
│   └── streaming_bronze.py     # Kafka → Bronze streaming job
├── jobs/
│   ├── silver_batch.py         # Bronze → Silver batch MERGE job
│   ├── gold_batch.py           # Silver → Gold aggregations
│   ├── query_gold.py           # Query Gold tables
│   └── metrics_collector.py    # Pipeline metrics for Grafana
├── utils/
│   ├── spark_session.py        # Spark + MinIO session factory
│   └── minio_setup.py          # MinIO bucket setup
├── connectors/
│   └── ehr_connector.json      # Debezium connector config
├── config/
│   └── semantic_layer.yml      # Text-to-SQL semantic layer
├── scripts/                    # Setup and utility scripts
└── artifacts/                  # Grafana dashboards, SQL queries
```

## Infrastructure

All services run locally via Docker:

```bash
docker-compose up -d
```

Services:
- PostgreSQL (port 5432) — source EHR database
- Zookeeper (port 2181)
- Kafka (port 29092)
- Kafka Connect + Debezium (port 8083)
- MinIO (port 9000) — S3-compatible object storage

## Running the Pipeline

**1. Start Bronze streaming (Kafka → MinIO):**
```bash
python spark/streaming_bronze.py
```

**2. Run Silver batch (Bronze → Silver MERGE):**
```bash
python jobs/silver_batch.py
```

**3. Run Gold batch (Silver → Gold aggregations):**
```bash
python jobs/gold_batch.py
```

**4. Query Gold tables:**
```bash
python jobs/query_gold.py
```

## Monitoring

Pipeline metrics are collected and visualized in Grafana:
- CDC offset lag per Kafka topic
- Spark job status (Bronze streaming, Silver batch)
- Record counts per layer per table
- Data freshness per table

## Configuration

| Service | Credentials |
|---------|------------|
| PostgreSQL | demo / demo / ehr_db |
| MinIO | minioadmin / minioadmin |
| Kafka | localhost:29092 |
| MinIO bucket | spark-data |

## Known Issues / In Progress

- `temperature` field arrives as base64 from Debezium (NUMERIC type serialization) — fix pending in Debezium connector config
- Silver vitals: first run creates table, subsequent runs MERGE correctly
# CI/CD enabled
# webhook test
