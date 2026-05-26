<div align="center">

# 🪙 Real-Time Crypto Data Lakehouse

**A production-grade streaming & batch data engineering platform**

Built on the **Medallion Architecture** (Bronze → Silver → Gold) for cryptocurrency market analysis at scale.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose_v3.8-2496ED.svg)](https://docs.docker.com/compose/)
[![Spark](https://img.shields.io/badge/Apache_Spark-3.5.8-E25A1C.svg)](https://spark.apache.org/)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-7.5.0-231F20.svg)](https://kafka.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.x-003366.svg)](https://delta.io/)

---

[Quick Start](#-quick-start) •
[Architecture](#-architecture) •
[Tech Stack](#-technology-stack) •
[Documentation](#-documentation) •
[Contributing](#-contributing)

</div>

---

## 📋 Overview

This project implements an **enterprise-grade data lakehouse** for real-time and historical cryptocurrency market data, featuring:

- 🔴 **Real-time streaming** of 50 Binance trading pairs via WebSocket
- 📦 **Batch ingestion** of 1,000 historical candles per pair via REST API
- 🏗️ **Medallion architecture** with three-layer data transformation (Bronze → Silver → Gold)
- ⚙️ **Automated orchestration** via Apache Airflow with data-aware scheduling
- 🔍 **SQL analytics** through Trino on Delta Lake tables
- ☁️ **Cloud-native deployment** on Google Cloud Storage

---

## 🏛️ Architecture

```
Binance Data Sources
  ├─ WebSocket (real-time) ──→ Kafka ──→ Spark Streaming ──→ Bronze (GCS)
  └─ REST API (historical) ──→ Staging ──→ Spark Batch ────→ Bronze (GCS)
                                                                  │
                                                                  ▼
                                                     ┌────────────────────┐
                                                     │  SILVER LAYER      │
                                                     │  Dedup + Validate  │
                                                     └────────┬───────────┘
                                                              │
                                                              ▼
                                                     ┌────────────────────┐
                                                     │  GOLD LAYER        │
                                                     │  Aggregate OHLCV   │
                                                     └────────┬───────────┘
                                                              │
                                                              ▼
                                                     ┌────────────────────┐
                                                     │  Trino + BI        │
                                                     │  Query & Dashboards│
                                                     └────────────────────┘
```

### Medallion Layers

| Layer | Purpose | Pattern | Retention |
|-------|---------|---------|-----------|
| 🥉 **Bronze** | Raw, immutable data — single source of truth | Append-only | 90+ days |
| 🥈 **Silver** | Deduplicated, validated, schema-enforced | Upsert (merge) | 90+ days |
| 🥇 **Gold** | Business-ready OHLCV candles & moving averages | Overwrite (refresh) | 1+ year |

> 📖 **Full architecture details** → [ARCHITECTURE.md](ARCHITECTURE.md)

---

## 🚀 Quick Start

Get the platform running in **4 phases**:

### Phase 1 — Infrastructure

```bash
git clone https://github.com/Quan-min211/Crypto-DataLakehouse-Project.git
cd Crypto-DataLakehouse-Project
cp .env.example .env
docker-compose up -d --build
```

### Phase 2 — Data Ingestion

```bash
cd ingestion && pip install -r requirements.txt
python producer_batch.py     # Historical data (1000 candles × 50 pairs)
python producer_stream.py    # Real-time WebSocket stream
```

### Phase 3 — Spark Processing

```bash
# Bronze → Silver → Gold transformations
# Run via Airflow DAGs or manually with spark-submit
```

### Phase 4 — Analytics

```
Open http://localhost:8080 (Trino UI) → Query the Gold layer
```

> 📖 **Detailed setup with verification steps** → [QUICK_START.md](QUICK_START.md)

---

## 🛠️ Technology Stack

### Core Components

| Category | Technology | Version | Role |
|----------|-----------|---------|------|
| **Messaging** | Apache Kafka | 7.5.0 | Reliable message queuing |
| **Coordination** | Apache ZooKeeper | 7.5.0 | Cluster management |
| **Processing** | Apache Spark | 3.5.8 | Streaming & batch ETL |
| **Table Format** | Delta Lake | 3.x | ACID transactions on cloud storage |
| **Storage** | Google Cloud Storage | — | Scalable, durable data lake |
| **Metadata** | Hive Metastore | 3.1.2-e.18 | Schema catalog |
| **Query Engine** | Trino | 432 | Federated SQL queries |
| **Orchestration** | Apache Airflow | 2.8+ | DAG scheduling & monitoring |
| **Deployment** | Docker Compose | v3.8 | Container orchestration |
| **Language** | Python | 3.10+ | Producers & ETL scripts |

> 📖 **Full tech stack breakdown** → [ARCHITECTURE.md](ARCHITECTURE.md#technology-stack)

---

## 📊 Data Flow

### Streaming Path (Real-Time)

```
Binance WebSocket → Validation → Kafka → Spark Streaming (30s batches) → Bronze
```

- **Latency:** ~30–60 seconds
- **Pattern:** Append-only, continuous

### Batch Path (Historical)

```
Binance REST API → GCS Staging (CSV) → Spark Batch → Bronze → Silver → Gold
```

- **Latency:** Hours (scheduled)
- **Pattern:** Full reload, merge/upsert

| Aspect | Streaming | Batch |
|--------|-----------|-------|
| **Frequency** | Continuous (30s windows) | Scheduled (daily) |
| **Data Volume** | Incremental tick data | 1,000 candles/pair |
| **Update Pattern** | Append-only | Merge/Upsert |
| **Use Case** | Real-time monitoring | Historical analysis |

> 📖 **Full pipeline details** → [DATA_FLOW.md](DATA_FLOW.md)

---

## 🖥️ Infrastructure

### Services & Ports

| Service | Port | Purpose |
|---------|------|---------|
| **Kafka** | `9092` | Message broker |
| **Spark Master** | `8082` | Distributed computing UI |
| **Trino** | `8080` | SQL query engine |
| **Airflow** | `8081` | DAG orchestration |
| **Hive Metastore** | `9083` | Schema catalog |
| **PostgreSQL** | `5432` | Metadata store |
| **MinIO** *(optional)* | `9000` | S3-compatible storage |

**Minimum Requirements:** 10 GB RAM · 12 CPU cores · 20 GB disk

> 📖 **Full infrastructure details** → [INFRASTRUCTURE.md](INFRASTRUCTURE.md)

---

## ⚙️ Configuration

All settings are managed via `.env` file:

```bash
cp .env.example .env
```

**Key variables:**

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
GCS_PROJECT_ID=your-project
GCS_BUCKET_NAME=crypto-lakehouse-group8
BINANCE_REST_URL=https://api.binance.com
TOP_N_COINS=50
```

> 📖 **All configurable options** → [CONFIGURATION.md](CONFIGURATION.md)

---

## 📁 Project Structure

```
Crypto-DataLakehouse-Project/
├── ingestion/           # Data producers (WebSocket, REST API)
├── processing/          # Spark ETL jobs (Bronze, Silver, Gold)
├── dags/                # Apache Airflow DAGs
├── dbt/                 # Data transformations & tests
├── spark/               # Custom Spark Docker image
├── ML/                  # Machine learning module (Flask app)
├── airflow/             # Airflow Docker configuration
├── cloud/               # GCS setup scripts
├── hive/                # Hive Metastore config
├── scripts/             # Database initialization
├── tests/               # Integration test suite
├── docs/                # Technical documentation
├── docker-compose.yml   # Multi-service orchestration
└── README.md            # This file
```

> 📖 **Detailed file descriptions** → [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md)

---

## 📚 Documentation

| Document | Description |
|----------|-------------|
| 📐 [ARCHITECTURE.md](ARCHITECTURE.md) | System design, medallion layers, tech stack, ingestion paths |
| 🚀 [QUICK_START.md](QUICK_START.md) | 4-phase setup guide with verification steps |
| 📁 [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md) | Directory breakdown, file purposes |
| 📊 [DATA_FLOW.md](DATA_FLOW.md) | Streaming & batch pipelines, transformations |
| 🖥️ [INFRASTRUCTURE.md](INFRASTRUCTURE.md) | Services, deployment, scaling, backup & recovery |
| ⚙️ [CONFIGURATION.md](CONFIGURATION.md) | Environment variables, tuning, troubleshooting |
| 🔄 [FLOW_ORCHESTRATION.md](FLOW_ORCHESTRATION.md) | Airflow DAG orchestration details |

---

## 🗺️ Roadmap

### ✅ Completed

- [x] Dockerized infrastructure (Kafka, Spark, Trino, HMS, PostgreSQL)
- [x] Real-time WebSocket ingestion producer
- [x] Batch REST API ingestion producer
- [x] Spark Structured Streaming to Bronze layer
- [x] Silver layer transformations with data quality rules
- [x] Gold layer OHLCV aggregation (1m and 5m intervals)
- [x] Apache Airflow DAG orchestration

### 🔄 In Progress

- [ ] Power BI dashboards and real-time visualization
- [ ] Advanced data quality monitoring (Great Expectations)
- [ ] ML feature engineering and model training

### 📋 Planned

- [ ] Kubernetes deployment (from Docker Compose)
- [ ] Advanced monitoring and alerting (Prometheus + Grafana)
- [ ] Cost optimization & multi-region setup

---

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 👤 Author

**Minh Quan (Quan-min211)**

- 🔗 GitHub: [Quan-min211](https://github.com/Quan-min211)
- 📧 Email: minhquan021105@gmail.com

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.

---

<div align="center">

**Built With**

Apache Kafka · Apache Spark · Delta Lake · Trino · Google Cloud Storage · Apache Airflow · Docker · Python

</div>