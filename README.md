# Real-Time Crypto Data Lakehouse

An end-to-end streaming and batch data engineering platform built on the Medallion Architecture.

**Platform Stack:** Python 3.10+ | Apache Kafka 7.5.0 | Apache Spark 3.5.8 | Delta Lake 3.x | Google Cloud Storage | Apache Airflow 2.8+ | Trino 432 | Docker Compose

Ingests the top 50 Binance USDT trading pairs in real-time via WebSocket, stores historical OHLCV klines from REST API, processes everything through a Bronze-Silver-Gold Delta Lake pipeline on Google Cloud Storage, orchestrates via Apache Airflow, and serves analytics via Trino and Power BI.

**Repository:** https://github.com/Quan-min211/Crypto-DataLakehouse-Project

License: MIT

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Flow](#data-flow)
- [Infrastructure Services](#infrastructure-services)
- [Quick Start](#quick-start)
  - [Prerequisites](#prerequisites)
  - [Phase 1 Start Infrastructure](#phase-1--start-infrastructure)
  - [Phase 2 Run Ingestion](#phase-2--run-ingestion)
  - [Phase 3 Spark Processing](#phase-3--spark-processing)
  - [Phase 4 Query with Trino](#phase-4--query-with-trino)
- [Configuration](#configuration)
- [Roadmap](#roadmap)
- [Author](#author)

---

## Overview

This project implements a production-grade data lakehouse for real-time cryptocurrency market analysis. It combines two ingestion strategies into a unified pipeline:

| Ingestion Type | Source | Target | Frequency |
|---|---|---|---|
| Streaming | Binance WebSocket (@trade) | Kafka -> Bronze (Delta Lake) | Real-time ticks |
| Batch | Binance REST API (/klines) | Google Cloud Storage raw-batch | Daily scheduled |

Data processing follows the three-tier Medallion Architecture:

**Bronze Layer:** Raw, immutable, partitioned Delta Lake tables. No transformations applied.

**Silver Layer:** Deduplicated, type-validated records with schema enforcement. Data quality rules applied.

**Gold Layer:** Business-ready OHLCV aggregates with moving averages (1m and 5m windows), optimized for analytics and BI consumption.

---

## Architecture

```
Real-Time Crypto Data Lakehouse Architecture

[EXTERNAL SOURCES]                     [INGESTION LAYER]
       |
 Binance WS                            Python Stream Producer
 Top-50 Trades          +------------->  (WebSocket, Auto-Retry)
       |                |
       |                v
       |          Apache Kafka
       |          Topic: crypto_trades_raw
       |
 Binance REST           |
 Historical Klines      |
       +----------------+

============================================================================

[PROCESSING LAYER - APACHE SPARK CLUSTER]
       |
       +-- Core Streaming Job (Kafka -> GCS)  --> BRONZE LAYER
       |                                          (Raw Append-Only)
       |
       +-- Micro-Batch Upsert (DQ Rules)      --> SILVER LAYER
       |                                          (Deduplicated)
       |
       +-- Batch Aggregation (OHLCV)          --> GOLD LAYER
                                                  (Business Ready)

============================================================================

[STORAGE LAYER]
       |
Google Cloud Storage:
  - gs://crypto-lakehouse-group8/bronze
  - gs://crypto-lakehouse-group8/silver
  - gs://crypto-lakehouse-group8/gold
       |
       v
Hive Metastore <-> PostgreSQL

============================================================================

[SERVING LAYER]
       |
       +-- Trino (Distributed SQL Query Engine)
       |
       +-- Power BI (Live Dashboards)
       |
       +-- Grafana (Monitoring)

============================================================================

[ORCHESTRATION]
       |
Apache Airflow DAGs
  - bronze_streaming_dag
  - silver_dag
  - gold_dag
  - maintenance_dag
```

---

## Tech Stack

| Layer | Technology | Version | Purpose |
|---|---|---|---|
| Message Broker | Apache Kafka (Confluent) | 7.5.0 | Real-time trade streaming |
| Coordination | Apache ZooKeeper | 7.5.0 | Kafka cluster management |
| Object Storage | Google Cloud Storage | Current | Cloud data lake with ADC authentication |
| Processing | Apache Spark | 3.5.8 | Streaming and batch ETL engine |
| Table Format | Delta Lake | 3.x | ACID transactions on object storage |
| Metadata Store | Hive Metastore (Starburst) | 3.1.2-e.18 | Schema and metadata catalog |
| Database | PostgreSQL | 15-alpine | Hive Metastore backend database |
| Query Engine | Trino | 432 | Federated SQL over Delta Lake |
| Ingestion | Python | 3.10+ | WebSocket and REST API producers |
| Orchestration | Apache Airflow | 2.8+ | DAG scheduling and data-aware workflows |
| Visualization | Power BI | Current | Business intelligence dashboards |
| Containerization | Docker Compose | v3.8 | Local deployment and infrastructure management

---

## Project Structure

```
Project Root/

├── docker-compose.yml               Full stack configuration
├── .env.example                     Environment variables template
├── .gitignore                       Git ignore rules
│
├── ingestion/                       Data ingestion layer
│   ├── producer_stream.py           WebSocket producer for real-time trades
│   ├── producer_batch.py            REST API producer for historical data
│   └── requirements.txt
│
├── spark/                           Spark cluster configuration
│   ├── Dockerfile                   Custom Spark image (3.5.8 + Delta + GCS)
│   └── start-spark.sh               Entrypoint script for master/worker
│
├── hive/                            Hive Metastore configuration
│   └── hive-site.xml                HMS JDBC configuration
│
├── trino/                           Trino query engine configuration
│   └── catalog/
│       └── delta.properties         Delta Lake connector config
│
├── processing/                      Spark ETL jobs
│   ├── bronze_streaming.py          Kafka to Bronze layer (streaming)
│   ├── bronze_to_silver.py          Bronze to Silver transformation (batch)
│   ├── silver_to_gold.py            Silver to Gold aggregation (batch)
│   └── requirements.txt
│
├── dags/                            Apache Airflow DAGs
│   ├── 01_ingestion_dag.py          REST API ingestion orchestration
│   ├── 02_bronze_streaming_dag.py   Bronze streaming job scheduler
│   ├── 03_silver_dag.py             Silver transformation scheduler
│   ├── 04_gold_dag.py               Gold aggregation scheduler
│   └── 05_maintenance_dag.py        Delta Lake optimization
│
├── dbt/                             Data transformation and validation
│   ├── dbt_project.yml              dbt configuration
│   ├── models/                      SQL models and tests
│   └── tests/                       Data quality tests
│
├── cloud/                           Cloud infrastructure scripts
│   └── gcs_setup.ps1                GCS bucket provisioning
│
├── tests/                           Integration tests
│   └── validate_pipeline.py         Pipeline validation suite
│
└── docs/                            Documentation
```

---

## Data Flow

### Streaming Path (Real-Time)

```
Binance WebSocket Feed
    |
    +- Validate required fields
    +- Enrich with ingested_at timestamp
    |
    +-- Valid   -> Kafka Topic: crypto_trades_raw
    +-- Invalid -> Kafka Topic: crypto_trades_dlq (dead-letter queue)

Spark Structured Streaming
    |
    +-- Append to Bronze Delta Lake (raw, immutable)
    |
    +-- Micro-batch transformation to Silver
        (deduplicate, validate, schema enforcement)
    |
    +-- Batch aggregation to Gold
        (1-minute and 5-minute OHLCV candles)
```

### Batch Path (Historical)

```
Binance REST API (/klines endpoint)
    |
    +- Fetch Top-50 USDT pairs
    +- 1000 candles per pair (1-minute interval)
    +- Rate-limit aware (1100/1200 weight budget)
    |
    v
Google Cloud Storage (raw-batch/)
    |
    +-- Spark ingests CSV files
    +-- Creates Bronze Delta tables
    +-- Cascades through Silver to Gold
```

---

## Infrastructure Services

| Service | Container Name | Port(s) | Memory | Role |
|---|---|---|---|---|
| ZooKeeper | zookeeper | 2181 | 512 MB | Kafka coordination |
| Apache Kafka | kafka | 9092, 29092 | 1 GB | Message broker |
| Kafka Connect | kafka-connect | 8083 | 1 GB | Connectors and plugins |
| PostgreSQL | postgres | 5432 | 512 MB | Hive Metastore database |
| Hive Metastore | hive-metastore | 9083 | 512 MB | Schema and metadata catalog |
| Trino | trino | 8080 | 2 GB | SQL query engine |
| Spark Master | spark-master | 7077, 8082 | 1 GB | Cluster manager and UI |
| Spark Worker | spark-worker | Dynamic | 2 GB | Compute workers |

### Google Cloud Storage Buckets

| Bucket Path | Purpose |
|---|---|
| gs://crypto-lakehouse-group8/bronze | Raw data (streaming append-only) |
| gs://crypto-lakehouse-group8/silver | Cleaned and deduplicated data |
| gs://crypto-lakehouse-group8/gold | Aggregated business metrics |
| gs://crypto-lakehouse-group8/checkpoints | Spark Structured Streaming state |
| gs://crypto-lakehouse-group8/raw-batch | Historical batch ingestion staging

---

## Quick Start

### Prerequisites

- Docker Desktop ≥ 24.0 (WSL2 backend on Windows)
- Python ≥ 3.10
- Minimum 10 GB RAM available for Docker
- Minimum 20 GB disk space (Spark image is large)

### Phase 1 - Start Infrastructure

```bash
# Clone the repository
git clone https://github.com/Quan-min211/Crypto-DataLakehouse-Project.git
cd Crypto-DataLakehouse-Project

# Copy environment configuration
cp .env.example .env

# Build and launch all services
docker-compose up -d --build

# Wait 60-90 seconds for services to initialize
# Verify all containers are running
docker ps
```

#### Verification Checklist

| Service | URL / Command | Expected Result |
|---|---|---|
| Trino | http://localhost:8080 | Query editor accessible |
| Spark Master | http://localhost:8082 | Dashboard with 1 worker ALIVE |
| Kafka | `docker logs kafka \| tail -5` | Server started message |
| Hive Metastore | `docker logs hive-metastore \| tail -5` | Server started message |
| PostgreSQL | `docker exec postgres pg_isready -U hive` | Accepting connections |

---

### Phase 2 - Run Ingestion

#### Setup Python Environment

```bash
cd ingestion
python -m venv .venv

# Windows PowerShell
.venv\Scripts\Activate.ps1

# Linux / macOS
source .venv/bin/activate

pip install -r requirements.txt
```

#### Configure Environment Variables

```powershell
# Windows PowerShell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:MINIO_ENDPOINT = "http://localhost:9000"
$env:MINIO_ACCESS_KEY = "admin"
$env:MINIO_SECRET_KEY = "admin123"
$env:BINANCE_REST_URL = "https://api.binance.com"
$env:BINANCE_WS_URL = "wss://stream.binance.com:9443/stream"
$env:TOP_N_COINS = "50"
```

#### Run Batch Producer

```bash
python ingestion/producer_batch.py
```

Fetches historical 1-minute OHLCV data for top-50 USDT pairs and stores in Google Cloud Storage.

#### Run Stream Producer

```bash
python ingestion/producer_stream.py
```

Starts continuous real-time trade tick stream to Kafka with automatic reconnection and error handling.

#### Verify Kafka Messages

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic crypto_trades_raw \
  --from-beginning \
  --max-messages 5
```

---

### Phase 3 - Spark Processing

Bronze and Silver transformations are deployed on Spark 3.5.8 with Delta Lake support.

```bash
docker run --rm --network finalproject_lakehouse-net \
  -v "${PWD}/processing:/processing" \
  -v "${env:APPDATA}\gcloud:/home/spark/.config/gcloud:ro" \
  -e GOOGLE_APPLICATION_CREDENTIALS=/home/spark/.config/gcloud/application_default_credentials.json \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  finalproject-spark-master:latest \
  spark-submit \
    --packages io.delta:delta-spark_2.12:3.2.1 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    /processing/bronze_streaming.py
```

---

### Phase 4 - Query with Trino

Open http://localhost:8080 in your browser and execute SQL queries:

```sql
-- List all available Delta Lake tables
SHOW TABLES FROM delta.default;

-- Query Gold layer OHLCV data
SELECT
    symbol,
    window_start,
    open, high, low, close,
    volume,
    ma_5m, ma_15m
FROM delta.default.gold_ohlcv
WHERE symbol = 'BTCUSDT'
ORDER BY window_start DESC
LIMIT 100;
```

---

## Configuration

Environment variables are managed in `.env` file (copy from `.env.example`):

| Variable | Default | Description |
|---|---|---|
| KAFKA_BOOTSTRAP_SERVERS | localhost:9092 | Kafka broker address |
| KAFKA_TOPIC_RAW | crypto_trades_raw | Main ingestion topic |
| KAFKA_TOPIC_DLQ | crypto_trades_dlq | Dead-letter queue topic |
| MINIO_ENDPOINT | http://localhost:9000 | Object storage endpoint |
| MINIO_ACCESS_KEY | admin | Object storage access key |
| MINIO_SECRET_KEY | admin123 | Object storage secret key |
| BINANCE_REST_URL | https://api.binance.com | Binance REST API base URL |
| BINANCE_WS_URL | wss://stream.binance.com:9443/stream | Binance WebSocket endpoint |
| TOP_N_COINS | 50 | Number of top trading pairs to track |

---

## Roadmap

- [x] Phase 1 - Dockerized infrastructure with Kafka, Spark, Trino, HMS, PostgreSQL
- [x] Phase 2 - Real-time WebSocket and REST API ingestion producers
- [x] Phase 3 - Spark 3.5.8 streaming to Bronze layer
- [x] Phase 4 - Silver layer transformation with data quality rules
- [x] Phase 5 - Gold layer OHLCV aggregation (1m and 5m intervals)
- [x] Phase 6 - Apache Airflow DAG orchestration
- [x] Phase 7 - Infrastructure memory optimization and tuning
- [ ] Phase 8 - Power BI dashboards and visualization
- [ ] Phase 9 - dbt data quality models on Gold layer

---

## Author

Minh Quan (Quan-min211)
- GitHub: https://github.com/Quan-min211
- Email: minhquan021105@gmail.com
- Project: https://github.com/Quan-min211/Crypto-DataLakehouse-Project

Built with Apache Kafka | Apache Spark | Delta Lake | Trino | Google Cloud Storage | Docker

License: MIT
 
 