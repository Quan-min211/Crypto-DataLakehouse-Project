# Infrastructure & Deployment

## Running Services

### Service Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    CONTAINER SERVICES                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  🔴 Message Broker Layer                                        │
│  ├─ zookeeper (2181)         - Kafka coordination               │
│  └─ kafka (9092, 29092)      - Message broker                   │
│     └─ kafka-connect (8083)  - Kafka connectors                 │
│                                                                 │
│  🟢 Storage & Metadata Layer                                    │
│  ├─ postgres (5432)          - Hive metadata database           │
│  └─ hive-metastore (9083)    - Schema catalog                   │
│                                                                 │
│  🔵 Processing Layer                                            │
│  ├─ spark-master (7077, 8082) - Cluster manager                 │
│  └─ spark-worker (dynamic)    - Compute workers                 │
│                                                                 │
│  🟡 Query Layer                                                 │
│  └─ trino (8080)              - SQL query engine                │
│                                                                 │
│  🟣 Orchestration Layer                                         │
│  ├─ airflow-webserver (8081)  - DAG UI                          │
│  ├─ airflow-scheduler         - Task scheduling                 │
│  └─ airflow-worker            - Task execution                  │
│                                                                 │
│  ⚪ Optional Storage                                            │
│  └─ minio (9000, 9001)        - S3-compatible storage           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Detailed Service Configuration

| Service | Container Name | Port(s) | Memory | CPU | Health Check | Restart |
|---------|---|---|---|---|---|---|
| **ZooKeeper** | zookeeper | 2181 | 512 MB | 0.5 | Telnet 2181 | Always |
| **Kafka** | kafka | 9092, 29092 | 1 GB | 1 | Topic list OK | Always |
| **Kafka Connect** | kafka-connect | 8083 | 1 GB | 1 | GET /connectors | Always |
| **PostgreSQL** | postgres | 5432 | 512 MB | 1 | pg_isready | Always |
| **Hive Metastore** | hive-metastore | 9083 | 512 MB | 0.5 | Thrift port | Always |
| **Trino Coordinator** | trino | 8080 | 2 GB | 2 | HTTP 200 | Always |
| **Spark Master** | spark-master | 7077, 8082 | 1 GB | 1 | HTTP 8082 | Always |
| **Spark Worker 1** | spark-worker-1 | Dynamic | 2 GB | 2 | HTTP port | Always |
| **Spark Worker 2** | spark-worker-2 | Dynamic | 2 GB | 2 | HTTP port | Always |
| **Airflow Webserver** | airflow-webserver | 8081 | 1 GB | 1 | HTTP 8081 | Always |
| **Airflow Scheduler** | airflow-scheduler | N/A | 512 MB | 0.5 | Process check | Always |
| **Airflow Worker** | airflow-worker | 8793 | 512 MB | 1 | Celery ping | Always |
| **MinIO** (optional) | minio | 9000, 9001 | 512 MB | 1 | HTTP 9000 | Always |

**Total Resource Requirements:**
- Memory: ~12-14 GB
- CPU: ~10-12 cores
- Disk: 50+ GB (depends on data retention)

---

## Port Reference

### Network Diagram

```
┌────────────────────────────────────────────────────────┐
│              DOCKER NETWORK: bridge                    │
├────────────────────────────────────────────────────────┤
│                                                        │
│  🌍 EXTERNAL ACCESS (localhost:PORT)                  │
│  ├─ Spark Master UI    → http://localhost:8082        │
│  ├─ Trino UI           → http://localhost:8080        │
│  ├─ Airflow WebUI      → http://localhost:8081        │
│  ├─ MinIO UI           → http://localhost:9001        │
│  ├─ Kafka Bootstrap    → localhost:9092               │
│  └─ PostgreSQL         → localhost:5432               │
│                                                        │
│  🔗 INTER-CONTAINER (container_name:PORT)             │
│  ├─ Kafka internal     → kafka:29092                  │
│  ├─ Hive Metastore     → hive-metastore:9083          │
│  ├─ Spark Master API   → spark-master:7077            │
│  └─ PostgreSQL         → postgres:5432                │
│                                                        │
└────────────────────────────────────────────────────────┘
```

### Complete Port Mapping

#### Messaging & Coordination

```
2181   ← ZooKeeper (client connections)
9092   ← Kafka (external broker, localhost)
29092  ← Kafka (internal, container network)
8083   ← Kafka Connect REST API
```

#### Processing

```
7077   ← Spark Master (RPC cluster)
8082   ← Spark Master UI (web)
4040   ← Spark Driver UI (dynamic, per job)
6066   ← Spark History Server (if enabled)
```

#### Metadata & Catalog

```
9083   ← Hive Metastore (Thrift)
5432   ← PostgreSQL (Hive backend)
```

#### Query & Analytics

```
8080   ← Trino Coordinator (SQL queries)
8888   ← Jupyter Notebook (optional)
```

#### Orchestration

```
8081   ← Airflow Web UI
8793   ← Airflow Worker Logs
5555   ← Flower (Celery monitoring, optional)
```

#### Storage

```
9000   ← MinIO (S3 API)
9001   ← MinIO Console (web)
```

**In docker-compose.yml, ports are exposed as `HOST:CONTAINER`:**
```yaml
services:
  kafka:
    ports:
      - "9092:9092"    # External: 9092 → Internal: 9092
      - "29092:29092"  # Internal only for container network
```

---

## Cloud Storage Structure (Google Cloud Storage)

### Bucket Organization

```
gs://crypto-lakehouse-group8/

├── bronze/
│   ├── trades/
│   │   ├── symbol=BTCUSDT/
│   │   │   ├── 2024-01-15/
│   │   │   │   ├── part-00000.parquet
│   │   │   │   ├── part-00001.parquet
│   │   │   │   └── _delta_log/
│   │   │   ├── 2024-01-16/
│   │   │   └── ...
│   │   ├── symbol=ETHUSDT/
│   │   └── ...
│   └── _delta_log/
│
├── silver/
│   ├── trades/
│   │   ├── symbol=BTCUSDT/
│   │   ├── symbol=ETHUSDT/
│   │   └── ...
│   └── _delta_log/
│
├── gold/
│   ├── ohlcv_1m/
│   │   ├── symbol=BTCUSDT/
│   │   ├── symbol=ETHUSDT/
│   │   └── ...
│   ├── ohlcv_5m/
│   │   ├── symbol=BTCUSDT/
│   │   ├── symbol=ETHUSDT/
│   │   └── ...
│   └── _delta_log/
│
├── raw-batch/
│   ├── BTCUSDT_2024-01-15.csv
│   ├── ETHUSDT_2024-01-15.csv
│   └── ...
│
└── checkpoints/
    ├── bronze_streaming/
    │   ├── _spark_metadata
    │   ├── offset.json
    │   └── ...
    ├── bronze_to_silver/
    └── ...
```

### Storage Buckets Reference

| Bucket Path | Format | Purpose | Retention | Size/Day |
|---|---|---|---|---|
| `gs://crypto-.../bronze/trades` | Delta Lake | Raw, immutable tick data | 90+ days | 5-25 MB |
| `gs://crypto-.../silver/trades` | Delta Lake | Deduplicated, validated | 90+ days | 3-15 MB |
| `gs://crypto-.../gold/ohlcv_1m` | Delta Lake | 1-minute candles | 1+ year | 500 KB-2 MB |
| `gs://crypto-.../gold/ohlcv_5m` | Delta Lake | 5-minute candles | 1+ year | 100-500 KB |
| `gs://crypto-.../raw-batch/` | CSV | Historical staging | 7 days | ~500 KB-2 MB |
| `gs://crypto-.../checkpoints/` | Parquet | Spark streaming state | Temporary | Variable |

### File Size Estimates

**Per Day (50 symbols, continuous streaming):**
- Bronze: 5-25 MB (tick-level)
- Silver: 3-15 MB (deduplicated)
- Gold 1m: 500 KB-2 MB (aggregated)
- Gold 5m: 100-500 KB (aggregated)
- **Total: ~10-45 MB/day**

**Annual Projection (365 days):**
- Bronze: 1.8-9 GB
- Silver: 1-5.5 GB
- Gold: 500 MB-2 GB
- **Total: ~3-17 GB/year**

---

## Environment Variables

### Required Variables

Create `.env` file in project root (copy from `.env.example`):

```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_RAW=crypto_trades_raw
KAFKA_TOPIC_DLQ=crypto_trades_dlq

# Object Storage
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=admin123

# Google Cloud Storage (if using GCS)
GCS_PROJECT_ID=<your-gcp-project-id>
GCS_BUCKET_NAME=crypto-lakehouse-group8
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json

# Binance API
BINANCE_REST_URL=https://api.binance.com
BINANCE_WS_URL=wss://stream.binance.com:9443/stream
TOP_N_COINS=50

# Airflow Configuration
AIRFLOW_HOME=/home/airflow/airflow
AIRFLOW__CORE__DAGS_FOLDER=/home/airflow/airflow/dags
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql://airflow:airflow@postgres:5432/airflow

# PostgreSQL
POSTGRES_USER=hive
POSTGRES_PASSWORD=hive_password
POSTGRES_DB=metastore

# Spark Configuration
SPARK_MASTER_HOST=spark-master
SPARK_MASTER_PORT=7077
```

### Advanced Variables

```bash
# Spark Tuning
SPARK_EXECUTOR_MEMORY=2G
SPARK_DRIVER_MEMORY=1G
SPARK_EXECUTOR_CORES=2
SPARK_EXECUTOR_INSTANCES=2

# Streaming Configuration
KAFKA_MAX_OFFSETS_PER_TRIGGER=100000
SPARK_STREAMING_CHECKPOINT_INTERVAL=30

# Data Retention
BRONZE_RETENTION_DAYS=90
SILVER_RETENTION_DAYS=90
GOLD_RETENTION_DAYS=365

# Monitoring
PROMETHEUS_ENABLED=true
GRAFANA_ADMIN_PASSWORD=admin
```

---

## Deployment Options

### Option 1: Docker Compose (Development/Testing)

**Best for:** Local development, testing, small deployments

```bash
# Start all services
docker-compose up -d --build

# Check status
docker-compose ps

# View logs
docker-compose logs -f kafka

# Stop all services
docker-compose down
```

**Pros:**
- Simple setup
- Good for local development
- Minimal resource overhead

**Cons:**
- Single machine only
- No automatic scaling
- Limited fault tolerance

---

### Option 2: Kubernetes (Production)

**Best for:** Production, multi-node clusters, auto-scaling

**Prerequisites:**
- Kubernetes cluster (GKE, EKS, AKS)
- kubectl configured
- Helm (optional, for charts)

**High-level deployment:**

```bash
# 1. Build Docker images
docker build -t spark:3.5.8-custom spark/
docker push gcr.io/YOUR_PROJECT/spark:3.5.8-custom

# 2. Create Kubernetes manifests
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secrets.yaml
kubectl apply -f k8s/services.yaml
kubectl apply -f k8s/deployments.yaml

# 3. Verify deployment
kubectl get pods -n crypto-lakehouse
kubectl logs -f deployment/spark-master -n crypto-lakehouse
```

**Kubernetes advantages:**
- Automatic restart on failure
- Horizontal pod autoscaling
- Rolling updates
- Resource quotas and limits
- Multi-region deployment

---

### Option 3: Cloud Managed Services

**Best for:** Serverless, minimal ops, cloud-native

#### Google Cloud Dataflow

```bash
# Spark jobs → Dataflow
gcloud dataflow jobs create ...
```

#### Confluent Cloud (Kafka)

```bash
# Kafka → Managed Kafka
ccloud kafka cluster create
```

#### Cloud Composer (Airflow)

```bash
# Airflow → Cloud Composer
gcloud composer environments create
```

---

## Database Initialization

### PostgreSQL Setup (for Hive Metastore)

```bash
# Connect to PostgreSQL container
docker exec -it postgres psql -U postgres

# Run initialization script
docker exec postgres psql -U postgres < scripts/init_postgres.sql
```

**init_postgres.sql creates:**
- `metastore` database
- Hive metastore schema
- Appropriate permissions

### Hive Metastore Initialization

```bash
# Hive schema initialization (automatic in docker-compose)
# Manual if needed:
docker exec hive-metastore schematool -dbType postgres -initSchema
```

---

## Health Checks & Monitoring

### Service Readiness Checks

```bash
# ZooKeeper
docker exec zookeeper echo ruok | nc localhost 2181

# Kafka
docker exec kafka kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092

# Hive Metastore
docker logs hive-metastore | grep "HMS started"

# PostgreSQL
docker exec postgres pg_isready -U hive

# Trino
curl -s http://localhost:8080/ui/

# Spark Master
curl -s http://localhost:8082/api/v1/applications | jq '.'

# Airflow
curl -s http://localhost:8081/api/v1/health | jq '.'
```

### Health Dashboard

Create a simple monitoring script:

```bash
#!/bin/bash
# check_health.sh

services=(
  "zookeeper:2181"
  "kafka:9092"
  "postgres:5432"
  "hive-metastore:9083"
  "trino:8080"
  "spark-master:8082"
)

for service in "${services[@]}"; do
  host="${service%:*}"
  port="${service#*:}"
  
  if nc -z -w5 "$host" "$port"; then
    echo "✓ $service is UP"
  else
    echo "✗ $service is DOWN"
  fi
done
```

---

## Scaling & Performance Tuning

### Horizontal Scaling

**Add Spark Workers:**
```yaml
# In docker-compose.yml, add:
spark-worker-3:
  image: finalproject-spark-worker:latest
  depends_on:
    - spark-master
  environment:
    SPARK_MASTER: spark://spark-master:7077
  ports:
    - "8085:8081"
```

**Add Kafka Brokers:**
```bash
# Scale Kafka for higher throughput
docker-compose up -d --scale kafka=3
```

### Vertical Scaling

**Increase resource allocation:**
```yaml
services:
  spark-master:
    environment:
      SPARK_EXECUTOR_MEMORY: 4G  # was 2G
      SPARK_EXECUTOR_CORES: 4    # was 2
```

---

## Backup & Recovery

### Backup Strategy

| Component | Frequency | Method |
|-----------|-----------|--------|
| Delta Tables | Continuous | GCS versioning enabled |
| Hive Metadata | Daily | PostgreSQL `pg_dump` |
| Configurations | On change | Git version control |
| Kafka Topics | 7 days | Topic retention policy |

### Backup Commands

```bash
# Backup PostgreSQL (Hive metadata)
docker exec postgres pg_dump -U hive -d metastore > backup_$(date +%Y%m%d).sql

# Export Delta table schema
dbutils.fs.ls("gs://bucket/bronze/trades/_delta_log")

# List GCS versions
gsutil ls -L gs://bucket/bronze/trades/
```

### Recovery Procedures

```bash
# Restore PostgreSQL
docker exec postgres psql -U hive -d metastore < backup_20240115.sql

# Restore Delta table from version
spark.sql("""
  RESTORE TABLE bronze.trades
  TO VERSION AS OF 123
""")
```

---

## Troubleshooting

### Common Issues

| Issue | Symptoms | Solution |
|-------|----------|----------|
| Kafka not starting | Cannot connect 9092 | Check ZooKeeper is running |
| Spark job slow | Low throughput | Increase partitions, executor memory |
| OOM errors | Java heap space | Reduce batch size or executor count |
| Hive metadata loss | Table not found | Restore from backup |
| GCS access denied | 403 errors | Check service account permissions |

### Log Analysis

```bash
# View service logs
docker-compose logs -f [service_name]

# Search logs for errors
docker-compose logs | grep ERROR

# Export logs for analysis
docker-compose logs > debug_logs.txt
```

