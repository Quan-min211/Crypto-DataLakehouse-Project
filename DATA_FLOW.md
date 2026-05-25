# Data Flow & Pipeline

## Streaming Path (Real-Time)

### Overview

The streaming path handles continuous real-time tick data from Binance WebSocket, processes it through Kafka, and lands it in the Bronze layer with minimal latency.

### Step-by-Step Flow

#### Step 1: Ingestion

```
Binance WebSocket Feed (@trade stream)
└── Continuous tick-level data for 50 USDT pairs
    ├── Symbol: BTCUSDT, ETHUSDT, ...
    ├── Price updates
    ├── Quantity traded
    └── Timestamp (milliseconds)
```

**Producer:** `ingestion/producer_stream.py`

**Features:**
- Auto-reconnect on disconnection (exponential backoff)
- Heartbeat monitoring
- Error handling and dead-letter queue

#### Step 2: Validation & Enrichment

```
┌─────────────────────────────────────┐
│ Validation Layer (Python)           │
├─────────────────────────────────────┤
│ ✓ Check required fields present     │
│   - symbol, price, quantity, time   │
│                                     │
│ ✓ Validate data types              │
│   - price: positive decimal        │
│   - quantity: positive decimal     │
│   - timestamp: long integer        │
│                                     │
│ ✓ Enrich with metadata             │
│   - ingested_at: current timestamp │
│   - source: "binance_ws"           │
│   - version: "1.0"                 │
│                                     │
│ Split outputs:                      │
│ ├─ Valid   → Kafka (main topic)    │
│ └─ Invalid → Kafka (DLQ)           │
└─────────────────────────────────────┘
```

#### Step 3: Message Broker (Kafka)

```
Topic: crypto_trades_raw
├── Partitions: 10 (by symbol hash)
├── Replication Factor: 1
├── Retention: 7 days
├── Compression: Snappy
└── Format: JSON
    {
      "symbol": "BTCUSDT",
      "price": "45234.50",
      "quantity": "0.5",
      "timestamp": 1694594400000,
      "ingested_at": 1694594401234,
      "source": "binance_ws"
    }
```

**Performance:**
- Throughput: 5,000-10,000 messages/sec
- Partition: By symbol → locality
- Consumer lag monitored continuously

#### Step 4: Spark Structured Streaming

```python
# Configuration
spark.streaming.kafka.maxOffsetsPerTrigger = 100000
spark.sql.streaming.checkpointLocation = "gs://...checkpoints/streaming"
spark.sql.shuffle.partitions = 50

# Processing
- Micro-batch interval: 30 seconds
- Watermark: 5 minutes (allow late data)
- Trigger mode: ProcessingTime (every 30 sec)
- Output mode: Append
```

**Code:**
```python
trades_df = (spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "kafka:29092")
  .option("subscribe", "crypto_trades_raw")
  .load()
)

# Transformations
parsed_df = (trades_df
  .select(
    from_json(col("value").cast("string"), schema)
    .alias("data")
  )
  .select("data.*")
  .withColumn("ingested_date", date_format(col("ingested_at"), "yyyy-MM-dd"))
)

# Write to Bronze
(parsed_df
  .writeStream
  .format("delta")
  .partitionBy("symbol", "ingested_date")
  .option("path", "gs://bucket/bronze/trades")
  .option("checkpointLocation", "gs://bucket/checkpoints/bronze")
  .start()
)
```

#### Step 5: Bronze Layer (Delta Lake)

```
Location: gs://crypto-lakehouse-group8/bronze/trades
├── Partitions
│   ├── symbol=BTCUSDT/
│   │   └── 2024-01-15/
│   │       ├── part-00000.parquet
│   │       ├── part-00001.parquet
│   │       └── _delta_log/
│   └── symbol=ETHUSDT/
│       └── 2024-01-15/
│           └── ...
│
├── Schema (ACID Delta)
│   ├── symbol: string (not null)
│   ├── price: decimal(20, 8)
│   ├── quantity: decimal(20, 8)
│   ├── timestamp: long
│   ├── ingested_at: long
│   ├── source: string
│   └── _change_type: string (implicit)
│
├── Retention: 90+ days
├── Update Pattern: Append-only
└── Compaction: OPTIMIZE run daily
```

**Row Count Example:**
- Per symbol per day: ~100,000-500,000 rows
- Total Bronze daily: ~5-25 million rows

---

## Batch Path (Historical)

### Overview

The batch path handles historical OHLCV data from Binance REST API, stores in GCS, and processes through all three layers.

### Step-by-Step Flow

#### Step 1: REST API Download

```
Binance REST API: GET /api/v3/klines
├── Endpoint: https://api.binance.com/api/v3/klines
├── Method: HTTPS GET with query parameters
└── Rate Limits: 1100 weight per minute (1200/3-min window)
```

**Query Parameters:**
```
symbol=BTCUSDT
interval=1m
limit=1000
startTime=(optional)
endTime=(optional)
```

**Response Schema:**
```json
[
  [
    1694594400000,      // Open time (ms)
    "45234.50",         // Open price
    "45300.00",         // High price
    "45100.00",         // Low price
    "45250.00",         // Close price
    "123.45",           // Volume (in coins)
    1694594459999,      // Close time (ms)
    "5678.90",          // Quote asset volume
    100,                // Number of trades
    "60.50",            // Taker buy base volume
    "2734.56"           // Taker buy quote volume
  ]
]
```

**Producer:** `ingestion/producer_batch.py`

**Behavior:**
- Top 50 USDT pairs
- 1000 candles per pair (1-minute interval)
- Rate-limited with weight tracking
- Exponential backoff on 429 (Too Many Requests)

#### Step 2: GCS Staging

```
Destination: gs://crypto-lakehouse-group8/raw-batch/
├── File format: CSV
├── Naming: {symbol}_{date}.csv
│   ├── BTCUSDT_2024-01-15.csv
│   ├── ETHUSDT_2024-01-15.csv
│   └── ...
│
└── CSV Schema
    open_time,open_price,high,low,close_price,volume,close_time,quote_volume,trades,taker_buy_base,taker_buy_quote
    1694594400000,45234.50,45300.00,45100.00,45250.00,123.45,1694594459999,5678.90,100,60.50,2734.56
    ...
```

**File Size:** ~100-500 KB per symbol per day

#### Step 3: Spark Batch Ingestion

```python
# Read CSV files from GCS
raw_batch_df = (spark
  .read
  .option("inferSchema", "false")
  .schema(batch_schema)
  .csv("gs://bucket/raw-batch/*.csv")
)

# Transform to Bronze schema
bronze_df = (raw_batch_df
  .withColumn("symbol", input_file_name()
    .substr(instr(input_file_name(), '/') + 1, 7))
  .withColumn("price", col("close_price"))
  .withColumn("quantity", col("volume"))
  .withColumn("timestamp", col("open_time"))
  .withColumn("ingested_at", current_timestamp() * 1000)
  .withColumn("ingested_date", date_format(col("ingested_at"), "yyyy-MM-dd"))
  .select("symbol", "price", "quantity", "timestamp", 
          "ingested_at", "ingested_date")
)

# Write to Bronze
(bronze_df
  .write
  .format("delta")
  .partitionBy("symbol", "ingested_date")
  .mode("append")
  .save("gs://bucket/bronze/trades")
)
```

#### Step 4: Cascading Transformations

The Bronze data cascades through Silver and Gold layers via separate batch jobs.

---

## Data Transformation Details

### Bronze → Silver Layer

**Objective:** Deduplicate, validate, apply data quality rules

```
Input: Bronze raw trades (potentially duplicate)
    ↓
Deduplication (by symbol + timestamp)
    ↓
Data Quality Rules
    ├─ Price must be positive
    ├─ Quantity must be positive
    ├─ Timestamp must be valid
    └─ No null values in key fields
    ↓
Schema Enforcement
    ├─ symbol: string
    ├─ price: decimal(20, 8)
    ├─ quantity: decimal(20, 8)
    ├─ timestamp: long
    └─ ingested_at: long
    ↓
Upsert to Silver
    └─ Output: Deduplicated, validated trades
```

**Job:** `processing/bronze_to_silver.py`

**Frequency:** Hourly

**Duration:** ~10-30 minutes per run

**Example SQL:**
```sql
-- Deduplication with ROW_NUMBER()
WITH ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY symbol, timestamp 
                       ORDER BY ingested_at DESC) as rn
  FROM bronze.trades
)
SELECT * EXCEPT(rn)
FROM ranked
WHERE rn = 1
```

### Silver → Gold Layer

**Objective:** Aggregate to OHLCV candles, calculate indicators

```
Input: Silver deduplicated trades
    ↓
1-minute Aggregation
    ├─ Group by (symbol, 1-min window)
    ├─ OHLC: Open, High, Low, Close
    ├─ Volume: Sum of quantities
    └─ Output: 1-min candles
    ↓
5-minute Aggregation
    ├─ Group by (symbol, 5-min window)
    ├─ Re-aggregate from 1-min candles
    └─ Output: 5-min candles
    ↓
Technical Indicators
    ├─ SMA 5,20,50: Simple Moving Average
    ├─ EMA 12,26: Exponential Moving Average
    └─ Other: Calculated on-demand
    ↓
Overwrite to Gold
    └─ Output: Business-ready OHLCV data
```

**Job:** `processing/silver_to_gold.py`

**Frequency:** Daily (after Silver completes)

**Duration:** ~5-15 minutes per run

**Example SQL:**
```sql
SELECT
  symbol,
  WINDOW(timestamp, '1 minute') as window_1m,
  FIRST(price) as open,
  MAX(price) as high,
  MIN(price) as low,
  LAST(price) as close,
  SUM(quantity) as volume,
  COUNT(*) as trade_count
FROM silver.trades
GROUP BY symbol, WINDOW(timestamp, '1 minute')
ORDER BY symbol, window_1m
```

---

## Comparison: Streaming vs Batch

| Aspect | Streaming | Batch |
|--------|-----------|-------|
| **Frequency** | Continuous (30s windows) | Daily (schedule) |
| **Latency** | ~30-60 seconds | Hours |
| **Data Volume** | Incremental ticks | Full 1000-candle dump |
| **Update Pattern** | Append-only | Merge for Bronze, Overwrite for Gold |
| **Use Case** | Real-time monitoring, alerts | Historical analysis, reports |
| **Storage Efficiency** | 5-25 MB/day per pair | Variable |
| **Recoverability** | Kafka replay within 7 days | Full replay from API |
| **Cost** | Continuous processing | On-demand, scheduled |

---

## Data Quality & Monitoring

### Quality Rules

| Rule | Bronze | Silver | Gold |
|------|--------|--------|------|
| Non-null key fields | Enforced | Enforced | Enforced |
| Positive prices | Monitored | Enforced | Enforced |
| No duplicates | Not checked | Enforced | Implied |
| Schema validation | Loose | Strict | Strict |

### Metrics Captured

```
Per job run:
├── Input record count
├── Output record count
├── Duplicate count (Silver)
├── Failed record count (DLQ)
├── Processing duration
├── Storage size added
└── Timestamp coverage (date range)
```

### Monitoring Dashboard

See `INFRASTRUCTURE.md` for Grafana/Prometheus setup

---

## Error Handling

### Streaming Pipeline

```
Exception Type          | Handling Strategy
────────────────────────|─────────────────────────
Connection timeout      | Auto-reconnect (exponential backoff)
Invalid JSON            | Route to DLQ topic
Out-of-order data       | Watermark handles with lateness
Spark job failure       | Checkpoint recovery on restart
GCS write error         | Fail fast, retry on restart
```

### Batch Pipeline

```
Exception Type          | Handling Strategy
────────────────────────|─────────────────────────
API rate limit (429)    | Wait and retry (backoff)
Network timeout         | Retry with jitter
File parse error        | Skip file, log warning
Spark job failure       | Manual retry trigger
Duplicate partition     | Merge conflict resolution
```

---

## Performance Tuning

### Streaming Optimization

```
# Kafka partitioning
- 10 partitions per topic
- Symbol-based partitioning (even distribution)

# Spark tuning
- Shuffle partitions: 50 (or 2x executor count)
- Micro-batch size: 30 seconds (balance latency vs throughput)
- Buffer size: 128 MB (adjust for network)

# Memory allocation
- Executor: 2 GB
- Driver: 1 GB
- Overhead: 384 MB per executor
```

### Batch Optimization

```
# CSV parsing
- inferSchema=false (provide explicit schema)
- compression=auto

# Spark tuning
- Shuffle partitions: 100 (larger dataset)
- Output partition count: Equal to input partitions
- Sort by (symbol, timestamp) before write

# Parallelism
- 4-8 parallel API calls (respect rate limits)
- Concurrent Spark jobs: 1 (avoid contention)
```

