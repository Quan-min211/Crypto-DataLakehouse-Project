# Quick Start Guide

Get the Crypto Data Lakehouse running in 4 phases: infrastructure, ingestion, processing, and querying.

---

## Prerequisites

Ensure you have the following before starting:

| Requirement | Minimum | Notes |
|---|---|---|
| **Docker Desktop** | 24.0+ | WSL2 backend required on Windows |
| **Python** | 3.10+ | For producer scripts |
| **RAM** | 10 GB | Available for Docker containers |
| **Disk** | 20 GB | For Spark images and data |
| **Git** | Any | For repository cloning |
| **GCP Account** (Optional) | - | If using Google Cloud Storage |

---

## Phase 1: Start Infrastructure

### Objective
Launch all backend services: Kafka, Spark, Trino, PostgreSQL, Hive Metastore, and Airflow.

### Steps

#### 1.1 Clone Repository

```bash
git clone https://github.com/Quan-min211/Crypto-DataLakehouse-Project.git
cd Crypto-DataLakehouse-Project
```

#### 1.2 Setup Environment

```bash
# Copy environment template
cp .env.example .env

# Optional: Edit .env with custom values
nano .env  # or use your editor
```

**Key variables to configure:**
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
GCS_PROJECT_ID=<your-gcp-project>
GCS_BUCKET_NAME=crypto-lakehouse-group8
BINANCE_REST_URL=https://api.binance.com
TOP_N_COINS=50
```

#### 1.3 Build and Launch Services

```bash
# Build custom images and start all services
docker-compose up -d --build

# Expected output: "Creating network..." "Creating service..."
```

#### 1.4 Wait for Initialization

```bash
# Wait 60-90 seconds for services to initialize
sleep 90

# Verify services running
docker ps
```

**Expected output:**
```
CONTAINER ID   IMAGE                    STATUS              PORTS
abc123def      finalproject-kafka       Up 45 seconds       0.0.0.0:9092->9092
xyz789abc      finalproject-spark-master Up 40 seconds       0.0.0.0:8082->8081
...
```

### 1.5 Verify Services

#### Check Individual Services

```bash
# ZooKeeper (Kafka coordination)
docker logs zookeeper | grep "binding"
# Expected: "[network] binding..." message

# Kafka (message broker)
docker logs kafka | grep "started"
# Expected: "started" or "INFO" message

# Spark Master (web UI)
curl -s http://localhost:8082 | grep -q "Spark" && echo "✓ Spark OK" || echo "✗ Spark DOWN"

# Hive Metastore (schema catalog)
docker logs hive-metastore | grep "started"

# PostgreSQL (hive backend)
docker exec postgres pg_isready -U hive
# Expected: "accepting connections"

# Trino (SQL query engine)
curl -s http://localhost:8080 | grep -q "Trino" && echo "✓ Trino OK" || echo "✗ Trino DOWN"
```

#### Access Points

```
Spark Master UI    → http://localhost:8082
Trino Query UI     → http://localhost:8080
Kafka Broker       → localhost:9092
PostgreSQL         → localhost:5432
Hive Metastore     → localhost:9083
```

#### Health Checklist

- [ ] ZooKeeper: `docker logs zookeeper | grep "binding"`
- [ ] Kafka: `docker logs kafka | tail -10` (no errors)
- [ ] Spark Master: http://localhost:8082 shows Workers: 2 ALIVE
- [ ] Hive Metastore: `docker logs hive-metastore | tail -5` (no errors)
- [ ] PostgreSQL: `docker exec postgres pg_isready -U hive` returns "accepting"
- [ ] Trino: http://localhost:8080 loads successfully

---

## Phase 2: Run Data Ingestion

### Objective
Feed real-time and historical cryptocurrency data into the pipeline.

### 2.1 Setup Python Environment

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

### 2.2 Configure Environment Variables

#### Option A: Set in shell (Windows PowerShell)

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:MINIO_ENDPOINT = "http://localhost:9000"
$env:MINIO_ACCESS_KEY = "admin"
$env:MINIO_SECRET_KEY = "admin123"
$env:BINANCE_REST_URL = "https://api.binance.com"
$env:BINANCE_WS_URL = "wss://stream.binance.com:9443/stream"
$env:TOP_N_COINS = "50"
```

#### Option B: Set in shell (Linux / macOS)

```bash
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export MINIO_ENDPOINT="http://localhost:9000"
export MINIO_ACCESS_KEY="admin"
export MINIO_SECRET_KEY="admin123"
export BINANCE_REST_URL="https://api.binance.com"
export BINANCE_WS_URL="wss://stream.binance.com:9443/stream"
export TOP_N_COINS="50"
```

### 2.3 Run Batch Producer (Historical Data)

**What it does:**
- Fetches 1000 historical 1-minute candles per trading pair
- Covers top 50 Binance USDT pairs (BTCUSDT, ETHUSDT, etc.)
- Stores data in Google Cloud Storage or MinIO
- Takes ~5-15 minutes depending on network

```bash
python producer_batch.py
```

**Expected output:**
```
Fetching BTCUSDT...
Successfully fetched 1000 candles for BTCUSDT
Uploaded to gs://crypto-lakehouse-group8/raw-batch/BTCUSDT_2024-01-15.csv
Fetching ETHUSDT...
...
Total time: 12m 34s
Uploaded 50 files successfully
```

### 2.4 Run Stream Producer (Real-Time Data)

**What it does:**
- Connects to Binance WebSocket @trade stream
- Continuously streams real-time tick data for 50 pairs
- Auto-reconnects on failures
- Runs indefinitely until stopped (Ctrl+C)

```bash
python producer_stream.py
```

**Expected output:**
```
Starting Binance WebSocket stream...
Connected to stream
Symbol: BTCUSDT, Price: 45234.50, Qty: 0.5
Symbol: ETHUSDT, Price: 2341.45, Qty: 1.2
Symbol: BNBUSDT, Price: 631.23, Qty: 0.3
...
(continues indefinitely)
```

### 2.5 Verify Kafka Messages

**In another terminal:**

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic crypto_trades_raw \
  --from-beginning \
  --max-messages 10
```

**Expected output:**
```json
{"symbol":"BTCUSDT","price":"45234.50","quantity":"0.5","timestamp":1694594400000}
{"symbol":"ETHUSDT","price":"2341.45","quantity":"1.2","timestamp":1694594401000}
{"symbol":"BNBUSDT","price":"631.23","quantity":"0.3","timestamp":1694594402000}
...
(10 messages shown)
```

---

## Phase 3: Spark ETL Processing

### Objective
Transform data through Bronze → Silver → Gold layers using Spark.

### 3.1 Bronze Streaming (Real-Time to Delta Lake)

**What it does:**
- Consumes messages from Kafka
- Writes to Bronze layer (GCS Delta tables)
- Runs continuously, append-only

```bash
docker run --rm --network crypto-lakehouse_default \
  -v "$(pwd)/processing:/processing" \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:29092 \
  -e SPARK_MASTER_HOST=spark-master \
  finalproject-spark-master:latest \
  spark-submit \
    --packages io.delta:delta-spark_2.12:3.2.1 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    /processing/bronze_streaming.py
```

**Monitor in another terminal:**
```bash
# Watch Bronze layer rows being added
watch -n 5 'gsutil du -sh gs://crypto-lakehouse-group8/bronze/'

# Or check Spark Master UI
open http://localhost:8082
```

### 3.2 Silver Transformation (Deduplication & Validation)

**What it does:**
- Removes duplicate trades
- Applies data quality rules
- Enforces schemas
- Merges (upserts) to Silver layer

```bash
docker run --rm --network crypto-lakehouse_default \
  -v "$(pwd)/processing:/processing" \
  finalproject-spark-master:latest \
  spark-submit \
    --packages io.delta:delta-spark_2.12:3.2.1 \
    /processing/bronze_to_silver.py
```

**Typical duration:** 10-30 minutes per run

**Monitor:**
```bash
# In Spark Master UI (http://localhost:8082)
# Watch the "bronze_to_silver" job complete
```

### 3.3 Gold Aggregation (OHLCV Candles)

**What it does:**
- Aggregates trades into 1-minute candles
- Further aggregates to 5-minute candles
- Calculates moving averages
- Writes to Gold layer

```bash
docker run --rm --network crypto-lakehouse_default \
  -v "$(pwd)/processing:/processing" \
  finalproject-spark-master:latest \
  spark-submit \
    --packages io.delta:delta-spark_2.12:3.2.1 \
    /processing/silver_to_gold.py
```

**Typical duration:** 5-15 minutes per run

**Data generated:**
- `gold/ohlcv_1m` - 1-minute candles for all 50 pairs
- `gold/ohlcv_5m` - 5-minute candles for all 50 pairs

---

## Phase 4: Query with Trino

### Objective
Execute SQL queries on Delta Lake tables via Trino.

### 4.1 Open Trino UI

Navigate to **http://localhost:8080** in your web browser

**What you'll see:**
- Query editor
- Connected catalogs (should show "delta")
- Database browser

### 4.2 Example Queries

#### List Available Tables

```sql
SHOW CATALOGS;
-- Should show: delta, system, ...

SHOW SCHEMAS FROM delta;
-- Should show: default

SHOW TABLES FROM delta.default;
-- Should show: gold_ohlcv_1m, gold_ohlcv_5m, ...
```

#### Query Gold Layer OHLCV Data

```sql
SELECT
    symbol,
    window_start,
    open,
    high,
    low,
    close,
    volume,
    ma_5m,
    ma_15m
FROM delta.default.gold_ohlcv_1m
WHERE symbol = 'BTCUSDT'
    AND window_start >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
ORDER BY window_start DESC
LIMIT 100;
```

**Expected output:**
```
symbol | window_start | open | high | low | close | volume | ma_5m | ma_15m
BTCUSDT | 2024-01-15T15:45:00 | 45200.00 | 45300.00 | 45100.00 | 45250.00 | 123.45 | 45200.12 | 45215.34
BTCUSDT | 2024-01-15T15:44:00 | 45195.00 | 45200.00 | 45150.00 | 45200.00 | 98.76 | 45190.45 | 45200.12
...
```

#### Aggregated Statistics by Symbol

```sql
SELECT
    symbol,
    COUNT(*) as candle_count,
    AVG(close) as avg_price,
    MAX(high) as max_high,
    MIN(low) as min_low,
    SUM(volume) as total_volume
FROM delta.default.gold_ohlcv_1m
WHERE window_start >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
GROUP BY symbol
ORDER BY total_volume DESC
LIMIT 20;
```

#### Volume Analysis

```sql
SELECT
    symbol,
    window_start::date as date,
    SUM(volume) as daily_volume,
    AVG(close) as avg_close
FROM delta.default.gold_ohlcv_1m
WHERE window_start >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
GROUP BY symbol, window_start::date
ORDER BY daily_volume DESC
LIMIT 50;
```

### Performance Tips

```sql
-- ✓ GOOD: Filtered query (fast)
SELECT * FROM gold_ohlcv_1m
WHERE symbol = 'BTCUSDT'
  AND window_start >= CURRENT_TIMESTAMP - INTERVAL '7' DAY;

-- ✗ SLOW: Full table scan
SELECT * FROM gold_ohlcv_1m;

-- ✓ GOOD: Aggregation on filtered data
SELECT symbol, COUNT(*)
FROM gold_ohlcv_1m
WHERE symbol IN ('BTCUSDT', 'ETHUSDT')
GROUP BY symbol;

-- ✗ SLOW: All symbols
SELECT symbol, COUNT(*)
FROM gold_ohlcv_1m
GROUP BY symbol;
```

---

## Monitoring & Troubleshooting

### Check Service Health

```bash
# All containers running
docker ps

# Service status
docker-compose ps

# View logs
docker-compose logs -f [service_name]

# Check disk usage
df -h
du -sh gs://crypto-lakehouse-group8/*
```

### Common Issues

| Issue | Check | Solution |
|-------|-------|----------|
| Cannot connect to Kafka (9092) | `docker logs kafka` | Wait 60s for startup, check network |
| Spark job fails | `docker logs spark-master` | Check memory allocation, GCS credentials |
| Trino query slow | Spark Master UI 8082 | Check partition count, add workers |
| GCS permission denied | Check service account | Update GOOGLE_APPLICATION_CREDENTIALS |

### Debug Commands

```bash
# Test Kafka connection
docker exec kafka kafka-broker-api-versions \
  --bootstrap-server localhost:9092

# Test Spark connectivity
docker exec spark-master spark-shell \
  --master spark://spark-master:7077

# Test GCS connectivity
gsutil ls gs://crypto-lakehouse-group8/

# View Spark UI
curl -s http://localhost:8082/api/v1/applications | jq '.[0]'
```

---

## Next Steps

1. **View dashboard:** See real-time data in Trino (Phase 4)
2. **Setup Power BI:** Connect to Trino for visualization
3. **Configure Airflow:** Schedule DAGs for automated runs
4. **Scale up:** Add more Spark workers for processing speed
5. **Enable monitoring:** Setup Prometheus + Grafana for alerts

For detailed information:
- See [ARCHITECTURE.md](ARCHITECTURE.md) for system design
- See [DATA_FLOW.md](DATA_FLOW.md) for pipeline details
- See [INFRASTRUCTURE.md](INFRASTRUCTURE.md) for deployment options
- See [CONFIGURATION.md](CONFIGURATION.md) for all settings

