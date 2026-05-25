# System Architecture

## Overview

The Crypto Data Lakehouse implements a **Medallion Architecture** (Bronze → Silver → Gold) for processing cryptocurrency market data at scale. This design pattern ensures data quality, auditability, and accessibility through layered transformations.

---

## Medallion Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA LAKEHOUSE LAYERS                    │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  GOLD LAYER                                                 │
│  └─ Business-ready metrics                                 │
│  └─ OHLCV candles (1m, 5m intervals)                       │
│  └─ Technical indicators (MA, EMA)                         │
│  └─ Optimized for BI & Analytics                           │
│  └─ User: Analysts, Dashboard builders                     │
│                                                             │
│  ↑                                                          │
│  │  Aggregation + Window Functions + Enrichment            │
│  │                                                          │
│  SILVER LAYER                                               │
│  └─ Deduplicated & validated data                          │
│  └─ Type-enforced schemas                                 │
│  └─ Data quality rules applied                             │
│  └─ Ready for transformation & aggregation                 │
│  └─ User: Data engineers, Analytics engineers              │
│                                                             │
│  ↑                                                          │
│  │  Deduplication + Validation + Schema Enforcement        │
│  │                                                          │
│  BRONZE LAYER                                               │
│  └─ Raw, immutable, append-only data                       │
│  └─ No transformations applied                            │
│  └─ Single source of truth (SSOT)                          │
│  └─ Full audit trail maintained                            │
│  └─ User: Data engineers, Data governance                  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Layer Characteristics

| Aspect | Bronze | Silver | Gold |
|--------|--------|--------|------|
| **Data Quality** | Raw, unvalidated | Validated, deduplicated | Business-ready |
| **Update Pattern** | Append-only | Upsert (merge) | Overwrite (refresh) |
| **Schema** | Flexible | Enforced | Denormalized |
| **Retention** | 90+ days | 90+ days | 1+ years |
| **Use Case** | Audit trail | Transformations | Analytics, BI |
| **Consumers** | Data engineers | Analysts | Business users |

---

## System Architecture Diagram

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          EXTERNAL DATA SOURCES                            │
│  ┌─────────────────────┐                    ┌──────────────────────────┐ │
│  │  Binance WebSocket  │                    │  Binance REST API        │ │
│  │  (@trade stream)    │                    │  (/klines endpoint)      │ │
│  │  Real-time ticks    │                    │  Historical candles      │ │
│  └──────────┬──────────┘                    └────────────┬─────────────┘ │
└─────────────┼────────────────────────────────────────────┼────────────────┘
              │                                            │
              │                                            │
              v                                            v
┌──────────────────────────────────────────────────────────────────────────┐
│                          INGESTION LAYER                                  │
│  ┌──────────────────────────────┐           ┌──────────────────────────┐ │
│  │  Stream Producer             │           │  Batch Producer          │ │
│  │  (Python + WebSocket)        │           │  (Python + REST)         │ │
│  │  • Auto-reconnect            │           │  • Rate-limited          │ │
│  │  • Error handling             │           │  • Exponential backoff   │ │
│  └────────────┬─────────────────┘           └────────────┬─────────────┘ │
│               │                                          │              │
│               v                                          v              │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │    Apache Kafka Topic: crypto_trades_raw                        │  │
│  │    • Partition: 10 (by symbol)                                  │  │
│  │    • Replication: 1                                             │  │
│  │    • Retention: 7 days                                          │  │
│  │    • Format: JSON (symbol, price, qty, timestamp)               │  │
│  └────────────┬─────────────────────────────────────────────────────┘  │
└─────────────────┼───────────────────────────────────────────────────────┘
                  │
                  │
                  v
┌──────────────────────────────────────────────────────────────────────────┐
│                   PROCESSING LAYER (Apache Spark)                         │
│  ┌──────────────────┬──────────────────┬──────────────────┐             │
│  │ Structured       │ Micro-Batch      │ Batch Jobs       │             │
│  │ Streaming        │ Jobs             │                  │             │
│  │                  │                  │                  │             │
│  │ Kafka → Bronze   │ Bronze → Silver  │ Silver → Gold    │             │
│  │ (30s windows)    │ (Hourly)         │ (Daily)          │             │
│  │ Append-only      │ Dedup + Validate │ Aggregate OHLCV  │             │
│  └────┬─────────────┬──────────────────┬────────────────┬─┘             │
└───────┼─────────────┼──────────────────┼────────────────┼────────────────┘
        │             │                  │                │
        v             v                  v                v
┌──────────────────────────────────────────────────────────────────────────┐
│              STORAGE LAYER (Google Cloud Storage)                         │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐       │
│  │  Bronze Layer    │  │  Silver Layer    │  │  Gold Layer      │       │
│  │  (Raw)           │  │  (Cleaned)       │  │  (Aggregated)    │       │
│  │                  │  │                  │  │                  │       │
│  │  Delta Tables    │  │  Delta Tables    │  │  Delta Tables    │       │
│  │  Append-only     │  │  Upsert Merge    │  │  Overwrite       │       │
│  │  Partitioned     │  │  Partitioned     │  │  Partitioned     │       │
│  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘       │
│           │                     │                     │                 │
│           └─────────────────────┼─────────────────────┘                 │
│                                 │                                       │
│            Metadata Sync to Hive Metastore (HMS)                         │
└─────────────────────────────────┼───────────────────────────────────────┘
                                  │
                                  v
                    ┌──────────────────────────┐
                    │   Hive Metastore + PG    │
                    │   (Schema Registry)      │
                    └──────────┬───────────────┘
                               │
                               v
┌──────────────────────────────────────────────────────────────────────────┐
│                        SERVING LAYER                                      │
│  ┌──────────────────────────┐           ┌──────────────────────────┐    │
│  │  Trino SQL Engine        │           │  Power BI Dashboards     │    │
│  │  (SQL over Delta Lake)   │           │  (Live Analytics)        │    │
│  │  Port: 8080              │           │  Real-time insights      │    │
│  │  Federated queries       │           │  Business dashboards     │    │
│  └──────────────┬───────────┘           └──────────────────────────┘    │
│                 │                                                         │
│                 └──────────────────────────────────────────────────┐     │
│                                                                    │     │
│  ┌──────────────────────────────────────────────────────────────┐ │     │
│  │  Observability: Grafana / Prometheus                         │ │     │
│  │  • Infrastructure monitoring                                 │ │     │
│  │  • Performance metrics                                       │ │     │
│  └──────────────────────────────────────────────────────────────┘ │     │
└──────────────────────────────────────────────────────────────────────────┘
                                  │
                                  v
┌──────────────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION (Apache Airflow)                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │ Ingestion    │  │ Bronze       │  │ Silver       │  │ Gold +       │ │
│  │ DAG          │  │ Streaming    │  │ Transform    │  │ Maintenance  │ │
│  │              │  │ DAG          │  │ DAG          │  │ DAG          │ │
│  │ Daily @ 00:00   │ Continuous      │ Hourly       │  │ Daily @       │ │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘ │
│                                                                          │
│  ✓ Data-aware scheduling                                                │
│  ✓ Dependency management                                                │
│  ✓ Monitoring & alerting                                                │
│  ✓ Retry & backfill capabilities                                        │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

### Messaging & Coordination

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Message Broker | Apache Kafka | 7.5.0 | Reliable, scalable message queuing |
| Cluster Manager | Apache ZooKeeper | 7.5.0 | Kafka coordination & leadership |

### Processing & Storage

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Stream Processing | Apache Spark | 3.5.8 | Real-time ETL engine |
| Batch Processing | Apache Spark | 3.5.8 | Historical data transformations |
| Table Format | Delta Lake | 3.x | ACID transactions on cloud storage |
| Data Lake | Google Cloud Storage | Current | Scalable, durable storage |
| Metadata Store | Hive Metastore | 3.1.2-e.18 | Schema catalog & versioning |
| Backend Database | PostgreSQL | 15-alpine | Hive metadata persistence |

### Query & Analytics

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Query Engine | Trino (PrestoSQL) | 432 | Federated SQL on multiple sources |
| Visualization | Power BI | Current | Business dashboards & insights |

### Orchestration & Deployment

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Workflow Scheduler | Apache Airflow | 2.8+ | DAG scheduling & monitoring |
| Data Modeling | dbt Core | Latest | SQL-based transformations |
| Infrastructure | Docker Compose | v3.8 | Container orchestration |

### Client Integration

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Data Producers | Python | 3.10+ | Ingestion & ETL scripts |

---

## Ingestion Paths

### Path A: Real-Time Streaming

```
Binance WebSocket (@trade stream)
    ↓ (continuous connection with auto-reconnect)
Python Stream Producer
    ↓ (validation + enrichment)
Apache Kafka Topic: crypto_trades_raw
    ↓ (message queue)
Spark Structured Streaming (30-second micro-batches)
    ↓ (immutable append)
Bronze Layer (GCS Delta Tables)
```

**Characteristics:**
- Latency: ~30-60 seconds
- Frequency: Continuous, 30-second windows
- Data: Tick-level trade information
- Volume: ~1-5 MB/minute across 50 pairs
- Watermarking: 5-minute allowed lateness

### Path B: Historical Batch

```
Binance REST API (/klines endpoint)
    ↓ (rate-limited downloads)
Batch Producer (Python)
    ↓ (CSV files)
Google Cloud Storage (raw-batch bucket)
    ↓ (Spark batch read)
Spark Batch Job
    ↓ (cascading transformations)
Bronze → Silver → Gold
```

**Characteristics:**
- Frequency: Daily or on-demand
- Latency: Hours
- Data: 1000 OHLCV candles per pair
- Volume: ~50-100 MB per run
- Pattern: Full reload

---

## Data Transformations

### Bronze → Silver

**Objective:** Data quality and deduplication

| Operation | Details |
|-----------|---------|
| **Deduplication** | Remove duplicates on (symbol, timestamp) |
| **Validation** | Check data types, non-null values |
| **Type Casting** | Enforce schemas (price as decimal, qty as double) |
| **Merge Pattern** | Upsert existing records (idempotent) |
| **Partition** | By symbol and date for performance |

```sql
MERGE INTO silver.trades t
USING bronze_dedup b
ON t.symbol = b.symbol 
   AND t.timestamp = b.timestamp
WHEN MATCHED THEN UPDATE SET 
  price = b.price, qty = b.qty
WHEN NOT MATCHED THEN INSERT 
  (symbol, timestamp, price, qty, ingested_at)
  VALUES (b.symbol, b.timestamp, b.price, b.qty, b.ingested_at)
```

### Silver → Gold

**Objective:** Business-ready aggregation

| Aggregation | Details |
|-------------|---------|
| **1-min OHLCV** | Aggregate trades into 1-minute candles |
| **5-min OHLCV** | Further aggregate to 5-minute candles |
| **SMA** | Simple Moving Average (5, 20, 50 periods) |
| **EMA** | Exponential Moving Average (12, 26 periods) |
| **Volume Stats** | Min, max, avg volume per period |

```sql
SELECT
  symbol,
  WINDOW(trade_time, '1 minute') as window_start,
  FIRST(price) as open,
  MAX(price) as high,
  MIN(price) as low,
  LAST(price) as close,
  SUM(quantity) as volume
FROM silver.trades
GROUP BY symbol, WINDOW(trade_time, '1 minute')
```

---

## Key Design Decisions

### 1. Delta Lake Format

**Why:** ACID transactions on cloud storage with time-travel capabilities

- Ensure data consistency despite failures
- Enable rollback and versioning
- Support concurrent reads/writes
- Integrate seamlessly with Spark & Trino

### 2. Medallion Architecture

**Why:** Separation of concerns with clear data quality boundaries

- **Bronze:** Audit trail and source of truth
- **Silver:** Cleaned, validated data for analysis
- **Gold:** Optimized for specific use cases

### 3. Kafka for Streaming

**Why:** Reliable message broker with replay capabilities

- Decouple producers from processors
- Enable multiple consumers (streaming jobs)
- Provide replay for data recovery
- Scale horizontally with partitions

### 4. Airflow for Orchestration

**Why:** Data-aware workflow scheduling

- Schedule tasks based on data availability (SLAs)
- Handle complex dependencies between jobs
- Monitor and retry failed tasks
- Maintain full audit trail

---

## Performance Considerations

### Partitioning Strategy

```
Bronze Layer:  symbol, date
Silver Layer:  symbol, date
Gold Layer:    symbol, date, interval (1m/5m)
```

**Benefit:** Partition pruning reduces data scanned

### Compaction

```bash
# Optimize Delta tables (combine small files)
OPTIMIZE delta.crypto_bronze
WHERE symbol = 'BTCUSDT'
ZORDER BY (symbol, timestamp)
```

### Caching Strategy

- Hot data: Last 7 days in memory
- Warm data: Compressed in GCS
- Cold data: Archived (S3 Glacier)

---

## Monitoring & Observability

### Metrics Tracked

| Metric | Purpose |
|--------|---------|
| **Ingestion Rate** | Messages/sec into Kafka |
| **Processing Latency** | Time from trade to availability |
| **Data Quality Score** | % of valid records |
| **Storage Growth** | GB/day per layer |
| **Query Performance** | P50, P99 latency on Trino |

### Alerting Rules

- Ingestion lag > 5 minutes
- Data quality score < 95%
- Query latency > 30 seconds
- Storage utilization > 80%

---

## Security & Access Control

### Data Governance

| Layer | Access | Users |
|-------|--------|-------|
| Bronze | Read-only | Data engineers, governance teams |
| Silver | Query via Trino | Analysts, BI teams |
| Gold | Query via Trino | Business users, dashboards |

### Authentication

- GCS: Service account with minimal permissions
- Trino: LDAP or basic auth
- Airflow: Role-based access control (RBAC)

---

## Disaster Recovery

### Backup Strategy

| Component | Frequency | Method |
|-----------|-----------|--------|
| **Delta Tables** | Continuous | GCS versioning |
| **Hive Metadata** | Daily | PostgreSQL backups |
| **Configurations** | On change | Git version control |

### Recovery Time Objectives (RTO)

- **Kafka restart:** < 2 minutes
- **Spark job restart:** < 5 minutes
- **Full system recovery:** < 30 minutes

