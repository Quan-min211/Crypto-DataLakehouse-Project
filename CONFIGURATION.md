# Configuration Reference

Complete guide to all configurable parameters and environment variables.

---

## Environment Variables

### File Location

```
Crypto-DataLakehouse-Project/
└── .env                    (copy from .env.example)
```

### Core Configuration

#### Kafka Settings

```bash
# Kafka broker address for producers/consumers
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Topics
KAFKA_TOPIC_RAW=crypto_trades_raw                # Main ingestion topic
KAFKA_TOPIC_DLQ=crypto_trades_dlq                # Dead-letter queue

# Partition & Replication
KAFKA_NUM_PARTITIONS=10                          # Parallel processing
KAFKA_REPLICATION_FACTOR=1                       # High availability
KAFKA_RETENTION_MS=604800000                     # 7 days in milliseconds

# Performance
KAFKA_MAX_OFFSETS_PER_TRIGGER=100000             # Spark batch size
KAFKA_COMPRESSION_TYPE=snappy                    # Message compression
```

#### Object Storage

```bash
# MinIO (Local S3-compatible storage)
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=admin123
MINIO_BUCKET_NAME=lakehouse

# Google Cloud Storage (Production)
GCS_PROJECT_ID=your-gcp-project-id
GCS_BUCKET_NAME=crypto-lakehouse-group8
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

#### Binance API

```bash
# REST API
BINANCE_REST_URL=https://api.binance.com
BINANCE_API_KEY=                                 # Leave empty if public data only
BINANCE_API_SECRET=                              # Leave empty if public data only

# WebSocket
BINANCE_WS_URL=wss://stream.binance.com:9443/stream

# Data Selection
TOP_N_COINS=50                                   # Number of pairs to track
TRADING_QUOTE_ASSET=USDT                         # Quote currency
```

---

### Spark Configuration

#### Resource Allocation

```bash
# Memory
SPARK_EXECUTOR_MEMORY=2G                         # Per executor
SPARK_DRIVER_MEMORY=1G                           # Driver JVM
SPARK_EXECUTOR_MEMORY_OVERHEAD=384m              # Off-heap per executor

# CPU
SPARK_EXECUTOR_CORES=2                           # Cores per executor
SPARK_EXECUTOR_INSTANCES=2                       # Number of executors

# Calculated total:
# Total Memory = (2G * 2 executors) + (1G driver) + (384m * 2) = 6.768G
# Total Cores = (2 * 2 executors) + (1 driver) = 5 cores
```

#### Streaming

```bash
# Micro-batch interval
SPARK_STREAMING_BATCH_INTERVAL=30s               # Latency vs throughput tradeoff

# Watermarking
SPARK_STREAMING_WATERMARK_DELAY=5m               # Allow late arrivals

# Checkpointing
SPARK_CHECKPOINT_LOCATION=gs://bucket/checkpoints/
SPARK_CHECKPOINT_INTERVAL=30s                    # How often to save state

# Output
SPARK_STREAMING_OUTPUT_MODE=append                # Options: append, update, complete
```

#### SQL & Performance

```bash
# Shuffle partitions
SPARK_SQL_SHUFFLE_PARTITIONS=50                  # For joining/grouping

# Broadcast join threshold (in bytes)
SPARK_SQL_AUTOBCAST_JOIN_THRESHOLD=10485760     # 10 MB

# Delta Lake
SPARK_DATABRICKS_DELTA_LOG_RETENTION=7 days     # Metadata retention
SPARK_SQL_ADAPTIVE_EXECUTION_ENABLED=true       # Adaptive query optimization
```

---

### PostgreSQL & Hive Metastore

```bash
# PostgreSQL Connection
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=hive
POSTGRES_PASSWORD=hive_password
POSTGRES_DB=metastore

# Hive Metastore
HIVE_METASTORE_HOST=hive-metastore
HIVE_METASTORE_PORT=9083

# Connection String (for JDBC)
HIVE_JDBC_URL=jdbc:postgresql://postgres:5432/metastore
```

---

### Apache Airflow

```bash
# Home directory
AIRFLOW_HOME=/home/airflow/airflow
AIRFLOW__CORE__DAGS_FOLDER=/home/airflow/airflow/dags

# Executor type
AIRFLOW__CORE__EXECUTOR=LocalExecutor               # Options: Local, Celery, Kubernetes
AIRFLOW__CORE__PARALLELISM=32                       # Max tasks in parallel
AIRFLOW__CORE__DAG_CONCURRENCY=16                   # Max tasks per DAG

# Database
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://airflow:airflow@postgres:5432/airflow

# Logging
AIRFLOW__LOGGING__BASE_LOG_FOLDER=/home/airflow/airflow/logs
AIRFLOW__LOGGING__DAG_PROCESSOR_MANAGER_LOG_LOCATION=/home/airflow/airflow/logs/dag_processor_manager

# Scheduler
AIRFLOW__SCHEDULER__CATCHUP_BY_DEFAULT=false      # Don't backfill by default
AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=300     # Check for new DAGs

# Web UI
AIRFLOW__WEBSERVER__DAG_REFRESH_INTERVAL=60       # Refresh rate (seconds)
AIRFLOW__WEBSERVER__EXPOSE_CONFIG=false           # Hide config in UI
```

---

### Trino

```bash
# Coordinator
TRINO_COORDINATOR_HOST=trino
TRINO_COORDINATOR_PORT=8080

# Memory
TRINO_QUERY_MAX_MEMORY=2GB                        # Per query
TRINO_QUERY_MAX_TOTAL_MEMORY=8GB                  # Cluster total

# Catalogs
TRINO_CATALOG_DELTA_CONNECTOR_NAME=delta           # Delta Lake connector
TRINO_CATALOG_DELTA_HIVE_METASTORE_URI=thrift://hive-metastore:9083
```

---

## Docker Compose Configuration

### File Location

```
Crypto-DataLakehouse-Project/
└── docker-compose.yml
```

### Service Scaling

Modify `docker-compose.yml` to adjust deployment:

```yaml
services:
  # Add more Spark workers
  spark-worker-3:
    image: finalproject-spark-worker:latest
    environment:
      SPARK_MASTER: spark://spark-master:7077
    ports:
      - "8085:8081"
    volumes:
      - ${PWD}/processing:/processing

  # Increase Kafka brokers
  kafka-2:
    image: confluentinc/cp-kafka:7.5.0
    environment:
      KAFKA_BROKER_ID: 2
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-2:29092
    depends_on:
      - zookeeper
```

---

## Application Configuration Files

### dbt Configuration

**Location:** `dbt/dbt_project.yml`

```yaml
name: 'crypto_lakehouse'
version: '1.0.0'
config-version: 2

profile: 'default'

models:
  crypto_lakehouse:
    staging:
      materialized: table
      schema: staging
    marts:
      materialized: table
      schema: marts
      
seeds:
  csv_encoding: 'UTF-8'
```

### dbt Profiles

**Location:** `dbt/profiles_template.yml`

```yaml
default:
  target: dev
  outputs:
    dev:
      type: trino
      method: none  # or ldap
      host: trino
      port: 8080
      catalog: delta
      schema: default
      threads: 4
      timeout_seconds: 60
```

---

## Data Configuration

### Ingestion Settings

**Bronze Layer Partitioning:**
```python
# Processing jobs partition by:
partition_columns = ["symbol", "date"]

# Partition size target:
target_partition_size_mb = 100  # ~100 MB per partition file
```

**Data Retention:**
```python
# Bronze retention (90+ days)
BRONZE_RETENTION_DAYS = 90

# Silver retention (90+ days)
SILVER_RETENTION_DAYS = 90

# Gold retention (1+ year)
GOLD_RETENTION_DAYS = 365
```

---

## Advanced Tuning

### Performance Optimization

```bash
# Spark SQL Adaptive Execution
SPARK_SQL_ADAPTIVE_EXECUTION_ENABLED=true        # Adjust execution plans
SPARK_SQL_ADAPTIVE_SKEW_JOIN_ENABLED=true        # Handle skewed joins

# Dynamic partition pruning
SPARK_SQL_DYNAMIC_PARTITION_PRUNING_ENABLED=true

# Columnar caching
SPARK_SQL_COLUMNAR_ENABLED=true                  # Vectorized execution
```

### Memory Management

```bash
# Fraction for unified memory management
SPARK_MEMORY_FRACTION=0.6                        # 60% for storage/execution

# Storage memory fraction
SPARK_MEMORY_STORAGE_FRACTION=0.5                # 50% of unified for storage
```

### Network & I/O

```bash
# Block transfer service threads
SPARK_SHUFFLE_IO_NUM_CONNECTIONS_PER_PEER=1

# Network timeout
SPARK_NETWORK_TIMEOUT=120s

# GCS settings
SPARK_GCS_BATCH_SIZE=100                         # Files per batch
SPARK_GCS_PARALLEL_THREAD_COUNT=10                # Download threads
```

---

## Security Configuration

### Service Account Setup (GCS)

```bash
# Create service account
gcloud iam service-accounts create crypto-lakehouse-sa \
  --display-name="Crypto Lakehouse Service Account"

# Grant permissions
gcloud projects add-iam-policy-binding YOUR_PROJECT \
  --member=serviceAccount:crypto-lakehouse-sa@YOUR_PROJECT.iam.gserviceaccount.com \
  --role=roles/storage.objectAdmin

# Create and download key
gcloud iam service-accounts keys create service-account-key.json \
  --iam-account=crypto-lakehouse-sa@YOUR_PROJECT.iam.gserviceaccount.com

# Set environment variable
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/service-account-key.json"
```

### Authentication

```bash
# Airflow RBAC
AIRFLOW__SECURITY__RBAC_ENABLED=true

# Trino authentication
TRINO_AUTH_METHOD=none                           # Options: none, basic, oauth2, ldap

# PostgreSQL
POSTGRES_USER=hive
POSTGRES_PASSWORD=<strong_password>
```

---

## Monitoring & Logging

### Log Levels

```bash
# Spark
SPARK_LOG_LEVEL=INFO                             # Options: DEBUG, INFO, WARN, ERROR

# Airflow
AIRFLOW__LOGGING__LOGGING_LEVEL=INFO

# Trino
TRINO_LOG_LEVEL=INFO

# Kafka
KAFKA_LOG4J_LOGGERS=org.apache.kafka:INFO        # Reduce noise
```

### Metrics Collection

```bash
# Prometheus
PROMETHEUS_ENABLED=true
PROMETHEUS_PORT=9090

# Grafana
GRAFANA_ENABLED=true
GRAFANA_PORT=3000
GRAFANA_ADMIN_PASSWORD=admin
```

---

## File Locations Reference

### Configuration Files

| File | Purpose | Location |
|------|---------|----------|
| Environment variables | Runtime config | `.env` |
| Docker services | Container setup | `docker-compose.yml` |
| Spark config | Spark tuning | `spark/Dockerfile` or `spark/start-spark.sh` |
| Airflow config | DAG setup | Environment variables or `$AIRFLOW_HOME/airflow.cfg` |
| Hive config | Metastore setup | `hive/hive-site.xml` |
| dbt project | dbt settings | `dbt/dbt_project.yml` |
| dbt profiles | dbt connections | `dbt/profiles.yml` or `~/.dbt/profiles.yml` |

### Data Locations

| Layer | Storage | Path |
|-------|---------|------|
| Raw | GCS | `gs://crypto-lakehouse-group8/raw-batch/` |
| Bronze | Delta Lake | `gs://crypto-lakehouse-group8/bronze/` |
| Silver | Delta Lake | `gs://crypto-lakehouse-group8/silver/` |
| Gold 1m | Delta Lake | `gs://crypto-lakehouse-group8/gold/ohlcv_1m/` |
| Gold 5m | Delta Lake | `gs://crypto-lakehouse-group8/gold/ohlcv_5m/` |
| Checkpoints | Parquet | `gs://crypto-lakehouse-group8/checkpoints/` |

---

## Configuration Validation

### Pre-Deployment Checks

```bash
#!/bin/bash
# check_config.sh

echo "Checking configuration..."

# Check .env exists
[ -f .env ] && echo "✓ .env file found" || echo "✗ Missing .env file"

# Validate JAVA_HOME
[ -n "$JAVA_HOME" ] && echo "✓ JAVA_HOME set" || echo "⚠ JAVA_HOME not set"

# Check Docker
docker --version && echo "✓ Docker installed" || echo "✗ Docker not installed"

# Check Docker Compose
docker-compose --version && echo "✓ Docker Compose installed" || echo "✗ Docker Compose not installed"

# Check Python
python3 --version && echo "✓ Python 3 installed" || echo "✗ Python 3 not installed"

# Verify disk space
DISK_AVAILABLE=$(df -BG . | tail -1 | awk '{print $4}' | sed 's/G//')
[ "$DISK_AVAILABLE" -gt 20 ] && echo "✓ Sufficient disk space (${DISK_AVAILABLE}GB available)" || echo "✗ Insufficient disk space"

# Check memory
MEMORY_AVAILABLE=$(free -g | grep Mem | awk '{print $7}')
[ "$MEMORY_AVAILABLE" -gt 10 ] && echo "✓ Sufficient memory (${MEMORY_AVAILABLE}GB available)" || echo "✗ Insufficient memory"

echo "Configuration check complete!"
```

---

## Troubleshooting Configuration

### Common Issues

| Problem | Cause | Solution |
|---------|-------|----------|
| "KAFKA_BOOTSTRAP_SERVERS not found" | Variable not set in `.env` | Copy `.env.example` and configure |
| "Connection refused: kafka:9092" | Wrong host/port | Check docker network: `kafka:29092` for containers |
| "GCS permission denied" | Invalid credentials | Check service account permissions |
| "Spark job runs out of memory" | Too many executors | Reduce `SPARK_EXECUTOR_INSTANCES` |
| "Airflow DAGs not loading" | Wrong `AIRFLOW__CORE__DAGS_FOLDER` | Verify path exists and contains DAGs |

### Configuration Reload

```bash
# Restart services after config change
docker-compose down
docker-compose up -d --build

# Or restart specific service
docker-compose restart [service_name]

# Reload Airflow DAGs without restart
curl -X POST http://localhost:8081/api/v1/admin/clear_task_instances
```

