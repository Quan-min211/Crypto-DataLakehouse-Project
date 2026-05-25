# Real-Time Crypto Data Lakehouse

**A production-grade streaming and batch data engineering platform**

Built on the Medallion Architecture for cryptocurrency market analysis at scale.

---

## Quick Facts

| Aspect | Details |
|---|---|
| **Purpose** | Real-time and historical crypto data pipeline |
| **Data Source** | Binance (Top 50 USDT pairs) |
| **Architecture** | Medallion (Bronze → Silver → Gold) |
| **Storage** | Google Cloud Storage (Delta Lake format) |
| **Scale** | 50 trading pairs, tick-level granularity |
| **Repository** | https://github.com/Quan-min211/Crypto-DataLakehouse-Project |
| **License** | MIT |

---

## Table of Contents

1. [Overview](#overview)
2. [Core Concepts](#core-concepts)
3. [Technology Stack](#technology-stack)
4. [Architecture](#architecture)
5. [Project Structure](#project-structure)
6. [Data Flow](#data-flow)
7. [Infrastructure Services](#infrastructure-services)
8. [Quick Start](#quick-start)
9. [Configuration](#configuration)
10. [Roadmap](#roadmap)
11. [Author](#author)

---

## Overview

This project demonstrates an enterprise-grade data lakehouse architecture for cryptocurrency market analysis. 

**Key capabilities:**

- **Real-time streaming** of 50 Binance trading pairs via WebSocket
- **Historical batch ingestion** of 1000 candles per pair via REST API
- **Three-layer data lake** with Bronze (raw) → Silver (cleaned) → Gold (aggregated) pipeline
- **Automated orchestration** via Apache Airflow with data-aware scheduling
- **SQL analytics** through Trino on Delta Lake tables
- **Cloud-native deployment** on Google Cloud Storage with Infrastructure as Code

---

## Core Concepts

### Medallion Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA LAKEHOUSE LAYERS                    │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  GOLD LAYER                                                 │
│  └─ Business-ready metrics                                 │
│  └─ OHLCV candles (1m, 5m)                                 │
│  └─ Moving averages                                        │
│  └─ Optimized for analytics & BI                           │
│                                                             │
│  ↑                                                          │
│  │  Aggregation + Window Functions                         │
│  │                                                          │
│  SILVER LAYER                                               │
│  └─ Deduplicated & validated                              │
│  └─ Type-enforced schemas                                 │
│  └─ Data quality applied                                  │
│  └─ Ready for transformation                              │
│                                                             │
│  ↑                                                          │
│  │  Deduplication + Validation                             │
│  │                                                          │
│  BRONZE LAYER                                               │
│  └─ Raw, immutable data                                   │
│  └─ Append-only storage                                   │
│  └─ No transformations                                    │
│  └─ Single source of truth                                │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Two Ingestion Paths

**Path A: Real-Time Streaming**
```
Binance WebSocket
    ↓
Message Validation
    ↓
Apache Kafka Topic
    ↓
Spark Structured Streaming
    ↓
Bronze Layer (GCS)
```

**Path B: Historical Batch**
```
Binance REST API
    ↓
Rate-Limited Download
    ↓
Google Cloud Storage (raw-batch)
    ↓
Spark Batch Job
    ↓
Bronze → Silver → Gold
```

---

## Technology Stack

### System Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          EXTERNAL DATA SOURCES                            │
│  ┌─────────────────────┐                    ┌──────────────────────────┐ │
│  │  Binance WebSocket  │                    │  Binance REST API        │ │
│  │  (@trade stream)    │                    │  (/klines endpoint)      │ │
│  └──────────┬──────────┘                    └────────────┬─────────────┘ │
└─────────────┼────────────────────────────────────────────┼────────────────┘
              │                                            │
              │                                            │
              v                                            v
┌──────────────────────────────────────────────────────────────────────────┐
│                          INGESTION LAYER                                  │
│  ┌──────────────────────────────┐           ┌──────────────────────────┐ │
│  │  Stream Producer             │           │  Batch Producer          │ │
│  │  (WebSocket with retry)      │           │  (Rate-limited)          │ │
│  └────────────┬─────────────────┘           └────────────┬─────────────┘ │
│               │                                          │              │
│               v                                          v              │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │            Apache Kafka Topic: crypto_trades_raw                 │  │
│  │  (Partitioned by symbol, auto topic creation enabled)            │  │
│  └────────────┬─────────────────────────────────────────────────────┘  │
└─────────────────┼───────────────────────────────────────────────────────┘
                  │
                  │
                  v
┌──────────────────────────────────────────────────────────────────────────┐
│                   PROCESSING LAYER (Apache Spark)                         │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │  Structured Streaming  │  Micro-Batch Jobs  │  Batch Jobs        │   │
│  │                        │                     │                    │   │
│  │  Kafka → Bronze        │  Bronze → Silver   │  Silver → Gold     │   │
│  │  (Append-only)         │  (Dedup + Validate)│  (Aggregate OHLCV) │   │
│  └────┬─────────────────────┬──────────────────┬────────────────────┘   │
└───────┼─────────────────────┼──────────────────┼────────────────────────┘
        │                     │                  │
        v                     v                  v
┌──────────────────────────────────────────────────────────────────────────┐
│              STORAGE LAYER (Google Cloud Storage)                         │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐       │
│  │  Bronze Layer    │  │  Silver Layer    │  │  Gold Layer      │       │
│  │  (Raw)           │  │  (Cleaned)       │  │  (Aggregated)    │       │
│  │                  │  │                  │  │                  │       │
│  │  Delta Tables    │  │  Delta Tables    │  │  Delta Tables    │       │
│  │  Append-only     │  │  Upsert Merge    │  │  Overwrite       │       │
│  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘       │
│           │                     │                     │                 │
│           └─────────────────────┼─────────────────────┘                 │
│                                 │                                       │
│                    Metadata Sync to Hive Metastore                       │
└─────────────────────────────────┼───────────────────────────────────────┘
                                  │
                                  v
                    ┌──────────────────────────┐
                    │   Hive Metastore + PG    │
                    │   (Schema Catalog)       │
                    └──────────┬───────────────┘
                               │
                               v
┌──────────────────────────────────────────────────────────────────────────┐
│                        SERVING LAYER                                      │
│  ┌──────────────────────────┐           ┌──────────────────────────┐    │
│  │  Trino SQL Engine        │           │  Power BI Dashboards     │    │
│  │  (SQL over Delta Lake)   │           │  (Live Analytics)        │    │
│  │  Port: 8080              │           │                          │    │
│  └──────────────┬───────────┘           └──────────────────────────┘    │
│                 │                                                         │
│                 └──────────────────────────────────────────────────┐     │
│                                                                    │     │
│  ┌──────────────────────────────────────────────────────────────┐ │     │
│  │  Grafana / Prometheus (Monitoring & Observability)           │ │     │
│  └──────────────────────────────────────────────────────────────┘ │     │
└──────────────────────────────────────────────────────────────────────────┘
                                  │
                                  v
┌──────────────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION (Apache Airflow)                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │ Ingestion    │  │ Bronze       │  │ Silver       │  │ Gold + Maint │ │
│  │ DAG          │  │ Streaming DAG│  │ DAG          │  │ DAG          │ │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘ │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

### Messaging & Coordination

| Component | Technology | Version | Role |
|---|---|---|---|
| Message Broker | Apache Kafka | 7.5.0 | Reliable message queuing |
| Coordination | Apache ZooKeeper | 7.5.0 | Cluster management |

### Processing & Storage

| Component | Technology | Version | Role |
|---|---|---|---|
| Stream Processing | Apache Spark | 3.5.8 | ETL/ELT engine |
| Batch Processing | Apache Spark | 3.5.8 | Batch transformations |
| Table Format | Delta Lake | 3.x | ACID on cloud storage |
| Data Lake | Google Cloud Storage | Current | Managed object storage |
| Metadata Store | Hive Metastore | 3.1.2-e.18 | Schema catalog |
| Metastore DB | PostgreSQL | 15-alpine | Backend database |

### Query & Analytics

| Component | Technology | Version | Role |
|---|---|---|---|
| SQL Engine | Trino | 432 | Federated queries |
| Visualization | Power BI | Current | Business dashboards |

### Orchestration & Deployment

| Component | Technology | Version | Role |
|---|---|---|---|
| Workflow Orchestration | Apache Airflow | 2.8+ | DAG scheduling |
| Data Transformation | dbt Core | Latest | Analytics engineering |
| Containerization | Docker Compose | v3.8 | Infrastructure management |

### Client Integration

| Component | Technology | Version | Role |
|---|---|---|---|
| Ingestion Code | Python | 3.10+ | Producers & ETL scripts |

---

## Architecture

---

## Project Structure

```
Crypto-DataLakehouse-Project/
│
├── Core Configuration
│   ├── docker-compose.yml           Multi-service orchestration (v3.8)
│   ├── package.json                 npm dependencies (if applicable)
│   ├── requirements.txt             Python dependencies
│   ├── LICENSE                      MIT License
│   └── README.md                    Project documentation
│
├── ingestion/                       Data Ingestion Module
│   ├── producer_stream.py           Real-time WebSocket producer
│   ├── producer_batch.py            Batch REST API producer
│   └── requirements.txt             Ingestion dependencies
│
├── processing/                      Data Processing & Transformation
│   ├── bronze_streaming.py          Kafka → GCS Bronze (Streaming)
│   ├── bronze_to_silver.py          Bronze → Silver (Deduplication)
│   ├── silver_to_gold.py            Silver → Gold (OHLCV Aggregation)
│   ├── check_delta_paths.py         Delta Lake validation
│   ├── gcs_auth.py                  Google Cloud Storage authentication
│   └── requirements.txt             Processing dependencies
│
├── dags/                            Apache Airflow Orchestration
│   ├── 01_ingestion_dag.py          Binance data ingestion DAG
│   ├── 02_bronze_streaming_dag.py   Bronze streaming scheduler
│   ├── 03_silver_dag.py             Silver transformation scheduler
│   ├── 04_gold_dag.py               Gold aggregation scheduler
│   └── 05_maintenance_dag.py        Delta Lake maintenance DAG
│
├── dbt/                             Data Build Tool Transformations
│   ├── dbt_project.yml              dbt project configuration
│   ├── profiles_template.yml        dbt profiles template
│   ├── models/                      SQL transformation models
│   ├── tests/                       Data quality test definitions
│   ├── seeds/                       Reference data
│   └── scripts/                     Custom SQL utilities
│
├── spark/                           Custom Spark Docker Image
│   ├── Dockerfile                   Spark 3.5.8 + Delta + GCS
│   └── start-spark.sh               Container entrypoint
│
├── kafka-connect/                   Kafka Connect Connectors
│   └── Dockerfile                   Kafka Connect integration layer
│
├── airflow/                         Apache Airflow Configuration
│   └── Dockerfile                   Airflow 2.8+ with GCP support
│
├── ML/                              Machine Learning Module
│   ├── app.py                       Flask web application
│   ├── train_all.py                 Model training script
│   ├── requirements.txt             ML dependencies
│   ├── models/                      Trained model artifacts
│   ├── data/                        Training and inference data
│   ├── templates/                   HTML templates
│   └── static/                      Frontend assets (CSS, JS)
│
├── cloud/                           Cloud Infrastructure Setup
│   ├── gcs_setup.ps1                GCS bucket provisioning
│   └── docker_gcs_auth.md           GCS authentication guide
│
├── hive/                            Hive Metastore Configuration
│   └── hive-site.xml                Hive JDBC configuration
│
├── scripts/                         Database & Utility Scripts
│   └── init_postgres.sql            PostgreSQL initialization
│
├── tests/                           Integration Test Suite
│   ├── test_dags.py                 Airflow DAG validation
│   ├── validate_pipeline.py         End-to-end pipeline tests
│   └── run_task2_tests.ps1          PowerShell test runner
│
├── docs/                            Technical Documentation
│   ├── KIEN_TRUC_HE_THONG_DAY_DU.md System architecture overview
│   ├── TASK2_ORCHESTRATION.md       Orchestration guide
│   ├── Script_How_to_run_project.md Setup and execution guide
│   └── Additional technical docs
│
└── ActionPlan                       Project roadmap & milestones
```

**Directory Breakdown:**

| Directory | Purpose | Key Technologies |
|-----------|---------|------------------|
| `ingestion/` | Binance data collection | Python, WebSocket, REST API |
| `processing/` | ETL transformations | PySpark, Delta Lake, GCS |
| `dags/` | Workflow scheduling | Apache Airflow, DAG definitions |
| `dbt/` | Data quality & modeling | dbt, SQL, Data validation |
| `spark/` | Container runtime | Docker, Spark 3.5.8 |
| `ML/` | Predictive analytics | Flask, scikit-learn, ML models |
| `tests/` | Quality assurance | pytest, Airflow testing |
| `cloud/` | Infrastructure automation | PowerShell, GCP scripts |
| `docs/` | Technical guides | Markdown, Diagrams |

---

## Data Flow

### 1. Streaming Path (Real-Time Updates)

```
┌──────────────────────────────────────────────────────────────────┐
│ STEP 1: INGESTION                                                │
│                                                                  │
│  Binance WebSocket Feed (@trade stream)                         │
│  └── Continuous tick-level data for 50 USDT pairs               │
│                                                                  │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         v
┌──────────────────────────────────────────────────────────────────┐
│ STEP 2: VALIDATION & ENRICHMENT                                  │
│                                                                  │
│  ✓ Validate required fields (symbol, price, quantity, time)    │
│  ✓ Enrich with ingested_at timestamp                            │
│  ✓ Schema validation                                             │
│                                                                  │
│  Output: Valid records → Apache Kafka                            │
│          Invalid records → Dead-Letter Queue (DLQ)               │
│                                                                  │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         v
┌──────────────────────────────────────────────────────────────────┐
│ STEP 3: MESSAGE BROKER                                           │
│                                                                  │
│  Apache Kafka Topic: crypto_trades_raw                           │
│  ├── Partition Count: 10 (by symbol)                             │
│  ├── Replication Factor: 1                                       │
│  ├── Retention: 7 days                                           │
│  └── Format: JSON (symbol, price, qty, time, ingested_at)        │
│                                                                  │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         v
┌──────────────────────────────────────────────────────────────────┐
│ STEP 4: SPARK STRUCTURED STREAMING                               │
│                                                                  │
│  Micro-Batch Window: 30 seconds                                  │
│  ├── Read from Kafka                                             │
│  ├── Apply watermark (allowedLateness: 5 minutes)                │
│  ├── Append-only writes to Bronze                                │
│  └── Trigger: Processing time (30 sec)                           │
│                                                                  │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         v
┌──────────────────────────────────────────────────────────────────┐
│ STEP 5: BRONZE LAYER (Google Cloud Storage)                      │
│                                                                  │
│  gs://crypto-lakehouse-group8/bronze/                            │
│  ├── Immutable append-only data                                  │
│  ├── Delta Table Format (ACID)                                   │
│  ├── Partitioned by: symbol, date                                │
│  └── Retention: Full history (90+ days)                          │
│                                                                  │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         v
                  (SILVER LAYER)
                  Deduplication, validation, schema enforcement
                         │
                         v
                   (GOLD LAYER)
                   OHLCV aggregation (1m, 5m candles)
```

### 2. Batch Path (Historical Data)

```
┌──────────────────────────────────────────────────────────────────┐
│ STEP 1: REST API DOWNLOAD                                        │
│                                                                  │
│  Binance REST API: /api/v3/klines                                │
│  ├── Endpoint: https://api.binance.com/api/v3/klines             │
│  ├── Top 50 USDT trading pairs                                   │
│  ├── 1000 candles per pair (1-minute interval)                   │
│  ├── Rate-limited: 1100/1200 weight budget                       │
│  └── Retry strategy: Exponential backoff                         │
│                                                                  │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         v
┌──────────────────────────────────────────────────────────────────┐
│ STEP 2: GCS STAGING                                              │
│                                                                  │
│  gs://crypto-lakehouse-group8/raw-batch/                         │
│  ├── Store as CSV files                                          │
│  ├── Structure: symbol_date.csv                                  │
│  ├── Format: timestamp, open, high, low, close, volume           │
│  └── Ready for Spark ingestion                                   │
│                                                                  │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         v
┌──────────────────────────────────────────────────────────────────┐
│ STEP 3: SPARK BATCH INGESTION                                    │
│                                                                  │
│  Spark Batch Job: producer_batch.py                              │
│  ├── Read CSV files from raw-batch/                              │
│  ├── Schema inference and validation                             │
│  ├── Casting to correct types                                    │
│  └── Write to Bronze Delta tables                                │
│                                                                  │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         v
┌──────────────────────────────────────────────────────────────────┐
│ STEP 4: CASCADING TRANSFORMATIONS                                │
│                                                                  │
│  Bronze (Raw) ──→ Silver (Cleaned) ──→ Gold (Aggregated)         │
│                                                                  │
│  Bronze → Silver:                                                │
│  ├─ Deduplication (on symbol, timestamp)                         │
│  ├─ Data quality rules                                           │
│  ├─ Schema enforcement                                           │
│  └─ Merge (upsert) pattern                                       │
│                                                                  │
│  Silver → Gold:                                                  │
│  ├─ Window aggregation (1m, 5m)                                  │
│  ├─ OHLCV calculation                                            │
│  ├─ Moving averages (SMA, EMA)                                   │
│  └─ Overwrite pattern (daily refresh)                            │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

**Key Differences:**

| Aspect | Streaming | Batch |
|--------|-----------|-------|
| **Frequency** | Continuous (30s windows) | Scheduled (e.g., daily) |
| **Latency** | ~30-60 seconds | Hours/days |
| **Data Volume** | Incremental tick data | Full candles (1000/pair) |
| **Update Pattern** | Append-only | Merge/Upsert |
| **Use Case** | Real-time monitoring | Historical analysis |

---

## Infrastructure Services

### Running Services

| Service | Container | Port(s) | Memory | CPU | Role |
|---------|-----------|--------|--------|-----|------|
| **ZooKeeper** | zookeeper | 2181 | 512 MB | 0.5 | Kafka coordination & leadership |
| **Apache Kafka** | kafka | 9092, 29092 | 1 GB | 1 | Message broker, topic management |
| **Kafka Connect** | kafka-connect | 8083 | 1 GB | 1 | Connectors & streaming pipelines |
| **PostgreSQL** | postgres | 5432 | 512 MB | 1 | Hive Metastore backend database |
| **Hive Metastore** | hive-metastore | 9083 | 512 MB | 0.5 | Schema catalog, metadata |
| **Apache Trino** | trino-coordinator | 8080 | 2 GB | 2 | SQL query engine (federated) |
| **Spark Master** | spark-master | 7077, 8082 | 1 GB | 1 | Cluster manager, job orchestrator |
| **Spark Worker** | spark-worker-1,2,... | Dynamic | 2 GB | 2 | Compute executors |
| **Apache Airflow** | airflow-webserver | 8081 | 1 GB | 1 | DAG scheduling & monitoring |
| **MinIO** (Optional) | minio | 9000, 9001 | 512 MB | 1 | S3-compatible object storage |

---

### Cloud Storage Buckets

| Bucket Path | Format | Purpose | Retention |
|---|---|---|---|
| `gs://crypto-lakehouse-group8/bronze/` | Delta Lake | Raw, immutable append-only data | 90+ days |
| `gs://crypto-lakehouse-group8/silver/` | Delta Lake | Deduplicated, validated data | 90+ days |
| `gs://crypto-lakehouse-group8/gold/` | Delta Lake | Aggregated OHLCV metrics | 1+ year |
| `gs://crypto-lakehouse-group8/checkpoints/` | Parquet | Spark Streaming state management | Temporary |
| `gs://crypto-lakehouse-group8/raw-batch/` | CSV | Historical batch staging area | 7 days |

---

### Port Reference

```
INFRASTRUCTURE PORTS
├── Message Broker
│   ├── 2181  → ZooKeeper (client connections)
│   ├── 9092  → Kafka (broker, internal)
│   └── 29092 → Kafka (broker, container network)
│
├── Processing
│   ├── 7077  → Spark Master (cluster)
│   ├── 8082  → Spark Master UI
│   └── 4040  → Spark Driver UI (dynamic)
│
├── Metadata & Query
│   ├── 9083  → Hive Metastore (Thrift)
│   ├── 5432  → PostgreSQL (Hive backend)
│   ├── 8080  → Trino (SQL query UI)
│   └── 8888  → Jupyter (optional)
│
├── Orchestration
│   ├── 8081  → Airflow WebUI
│   └── 8793  → Airflow worker logs
│
└── Storage
    ├── 8083  → Kafka Connect REST API
    ├── 9000  → MinIO (S3 API)
    └── 9001  → MinIO UI (optional)
```

## Quick Start

### Prerequisites

Before starting, ensure you have:

| Requirement | Minimum Version | Notes |
|---|---|---|
| **Docker Desktop** | 24.0+ | WSL2 backend required on Windows |
| **Python** | 3.10+ | For producer scripts |
| **RAM** | 10 GB | Available for Docker containers |
| **Disk Space** | 20 GB | Spark images and data storage |
| **Git** | Any | For repository cloning |

---

### Phase 1: Start Infrastructure

**Objective:** Launch all backend services (Kafka, Spark, Trino, PostgreSQL, etc.)

```bash
# Step 1: Clone repository
git clone https://github.com/Quan-min211/Crypto-DataLakehouse-Project.git
cd Crypto-DataLakehouse-Project

# Step 2: Setup environment
cp .env.example .env
# Optional: Edit .env with custom values

# Step 3: Build and launch services
docker-compose up -d --build

# Step 4: Wait for initialization (60-90 seconds)
sleep 90

# Step 5: Verify services
docker ps
```

**Health Verification:**

```bash
# Check all services are healthy
docker-compose ps

# Expected output: All containers showing "Up" status
```

**Service Readiness Checklist:**

- [ ] ZooKeeper: `docker logs zookeeper | grep "binding"`
- [ ] Kafka: `docker logs kafka | grep "started"`
- [ ] Trino: `docker logs trino | grep "serving"`
- [ ] Spark Master: `docker logs spark-master | grep "Master has begun"` OR visit http://localhost:8082
- [ ] Hive Metastore: `docker logs hive-metastore | grep "started"`
- [ ] PostgreSQL: `docker exec postgres pg_isready -U hive` (should return "accepting connections")

**Access URLs:**

```
Spark Master UI     → http://localhost:8082
Trino Query UI      → http://localhost:8080
Kafka Broker        → localhost:9092
Hive Metastore      → localhost:9083
PostgreSQL          → localhost:5432
```

---

### Phase 2: Run Data Ingestion

**Objective:** Feed real-time and historical data into the pipeline

#### 2.1 Setup Python Environment

```bash
cd ingestion
python -m venv .venv

# Windows PowerShell
.venv\Scripts\Activate.ps1

# Linux / macOS / WSL
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

#### 2.2 Configure Environment

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

```bash
# Linux / macOS
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export MINIO_ENDPOINT="http://localhost:9000"
export MINIO_ACCESS_KEY="admin"
export MINIO_SECRET_KEY="admin123"
export BINANCE_REST_URL="https://api.binance.com"
export BINANCE_WS_URL="wss://stream.binance.com:9443/stream"
export TOP_N_COINS="50"
```

#### 2.3 Run Batch Producer (Historical Data)

```bash
python producer_batch.py
```

**What this does:**
- Fetches 1000 historical 1-minute candles per pair
- Covers top 50 Binance USDT trading pairs
- Stores data in Google Cloud Storage or MinIO
- Ingests into Bronze layer
- Takes ~5-15 minutes depending on network

**Monitoring:**
```bash
# In another terminal, watch Bronze layer growth
docker logs -f spark-master | grep "bronze"
```

#### 2.4 Run Stream Producer (Real-Time Data)

```bash
python producer_stream.py
```

**What this does:**
- Connects to Binance WebSocket @trade streams
- Streams real-time tick data for 50 pairs
- Auto-reconnects on failures
- Runs indefinitely until stopped (Ctrl+C)

**Monitoring:**
```bash
# Verify messages in Kafka
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic crypto_trades_raw \
  --from-beginning \
  --max-messages 10
```

**Expected Output:**
```json
{"symbol":"BTCUSDT","price":"45234.12","quantity":"0.5","timestamp":1234567890000}
{"symbol":"ETHUSDT","price":"2341.45","quantity":"1.2","timestamp":1234567891000}
...
```

---

### Phase 3: Spark ETL Processing

**Objective:** Transform Bronze data through Silver and Gold layers

#### 3.1 Bronze Streaming (Real-Time to Delta Lake)

```bash
docker run --rm --network crypto-lakehouse_default \
  -v "${PWD}/processing:/processing" \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  finalproject-spark-master:latest \
  spark-submit \
    --packages io.delta:delta-spark_2.12:3.2.1 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    /processing/bronze_streaming.py
```

#### 3.2 Silver Transformation (Deduplication & Validation)

```bash
docker run --rm --network crypto-lakehouse_default \
  -v "${PWD}/processing:/processing" \
  finalproject-spark-master:latest \
  spark-submit \
    --packages io.delta:delta-spark_2.12:3.2.1 \
    /processing/bronze_to_silver.py
```

#### 3.3 Gold Aggregation (OHLCV Candles)

```bash
docker run --rm --network crypto-lakehouse_default \
  -v "${PWD}/processing:/processing" \
  finalproject-spark-master:latest \
  spark-submit \
    --packages io.delta:delta-spark_2.12:3.2.1 \
    /processing/silver_to_gold.py
```

**Monitoring Spark Jobs:**
- Spark Master UI: http://localhost:8082 (watch Executors and Jobs tabs)
- Application logs: Check Docker logs for the Spark container

---

### Phase 4: Query with Trino

**Objective:** Execute SQL queries on Delta Lake tables

#### 4.1 Open Trino UI

Navigate to http://localhost:8080 in your web browser

#### 4.2 Example Queries

**List available tables:**
```sql
SHOW TABLES FROM delta.default;
```

**Query Gold layer OHLCV data:**
```sql
SELECT
    symbol,
    window_start,
    open, high, low, close,
    volume,
    ma_5m, ma_15m
FROM delta.default.gold_ohlcv
WHERE symbol = 'BTCUSDT'
    AND window_start >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
ORDER BY window_start DESC
LIMIT 100;
```

**Aggregated statistics by symbol:**
```sql
SELECT
    symbol,
    COUNT(*) as candle_count,
    AVG(close) as avg_price,
    MAX(high) as max_high,
    MIN(low) as min_low
FROM delta.default.gold_ohlcv
WHERE window_start >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
GROUP BY symbol
ORDER BY symbol;
```

**Performance tip:** Filter by specific symbols and time ranges for faster queries.

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|---|---|---|
| **KAFKA_BOOTSTRAP_SERVERS** | localhost:9092 | Kafka broker address |
| **KAFKA_TOPIC_RAW** | crypto_trades_raw | Main ingestion topic name |
| **KAFKA_TOPIC_DLQ** | crypto_trades_dlq | Dead-letter queue for errors |
| **MINIO_ENDPOINT** | http://localhost:9000 | Object storage endpoint |
| **MINIO_ACCESS_KEY** | admin | S3 API access key |
| **MINIO_SECRET_KEY** | admin123 | S3 API secret key |
| **BINANCE_REST_URL** | https://api.binance.com | Binance REST API base URL |
| **BINANCE_WS_URL** | wss://stream.binance.com:9443/stream | Binance WebSocket endpoint |
| **TOP_N_COINS** | 50 | Number of top trading pairs |
| **GCS_PROJECT_ID** | (required) | Google Cloud Project ID |
| **GCS_BUCKET_NAME** | crypto-lakehouse-group8 | GCS bucket name |

### File Locations

- **Configuration template:** `.env.example`
- **Docker services:** `docker-compose.yml`
- **Spark config:** `spark/Dockerfile` and `spark/start-spark.sh`
- **Airflow DAGs:** `dags/`
- **Processing scripts:** `processing/`

---

## Roadmap

**Completed:**
- [x] Dockerized infrastructure (Kafka, Spark, Trino, HMS, PostgreSQL)
- [x] Real-time WebSocket ingestion producer
- [x] Batch REST API ingestion producer
- [x] Spark 3.5.8 streaming to Bronze layer
- [x] Silver layer transformations with data quality rules
- [x] Gold layer OHLCV aggregation (1m and 5m intervals)
- [x] Apache Airflow DAG orchestration

**In Progress:**
- [ ] Power BI dashboards and real-time visualization
- [ ] Advanced data quality monitoring (Great Expectations)
- [ ] ML feature engineering and model training

**Planned:**
- [ ] Kubernetes deployment (from Docker Compose)
- [ ] Cost optimization & multi-region setup
- [ ] Advanced monitoring and alerting (Prometheus + Grafana)

---

## Author

**Minh Quan (Quan-min211)**

- GitHub: https://github.com/Quan-min211
- Email: minhquan021105@gmail.com
- Project Repository: https://github.com/Quan-min211/Crypto-DataLakehouse-Project

**Technology Stack:**
Apache Kafka | Apache Spark | Delta Lake | Trino | Google Cloud Storage | Apache Airflow | Docker | Python

**License:** MIT
 
 