# Project Structure

## Directory Hierarchy

```
Crypto-DataLakehouse-Project/
│
├─── 📋 Root Configuration Files
│    ├── docker-compose.yml           Multi-service container orchestration (v3.8)
│    ├── .env.example                 Environment variables template
│    ├── .gitignore                   Git ignore rules
│    ├── package.json                 npm dependencies (if applicable)
│    ├── requirements.txt             Global Python dependencies
│    ├── LICENSE                      MIT License
│    ├── README.md                    Quick overview & navigation
│    └── ActionPlan                   Project roadmap & milestones
│
├─── 📚 Documentation Files
│    ├── ARCHITECTURE.md              System design, medallion layers, tech stack
│    ├── QUICK_START.md               Setup guide with 4 phases
│    ├── INFRASTRUCTURE.md            Services, storage, ports, deployment
│    ├── DATA_FLOW.md                 Streaming & batch pipelines
│    ├── CONFIGURATION.md             Environment variables & setup
│    ├── PROJECT_STRUCTURE.md         This file - directory reference
│    └── docs/                        Additional technical documentation
│        ├── KIEN_TRUC_HE_THONG_DAY_DU.md    Full system architecture (Vietnamese)
│        ├── TASK2_ORCHESTRATION.md          Airflow orchestration guide
│        ├── Script_How_to_run_project.md    Setup procedures
│        └── ... (other technical docs)
│
├─── 🔌 Data Ingestion Module
│    ├── producer_stream.py           WebSocket producer for real-time Binance trades
│    │                                - Connects to @trade stream
│    │                                - Continuous data collection
│    │                                - Auto-reconnect with backoff
│    │
│    ├── producer_batch.py            REST API producer for historical data
│    │                                - Fetches 1000 candles per pair
│    │                                - Rate-limited (1100 weight/min)
│    │                                - Exponential backoff on 429 errors
│    │
│    └── requirements.txt             Ingestion dependencies
│                                     (websockets, kafka-python, requests)
│
├─── ⚙️ Data Processing Module (PySpark)
│    ├── bronze_streaming.py          Kafka → GCS Bronze (real-time)
│    │                                - Spark Structured Streaming
│    │                                - 30-second micro-batches
│    │                                - Append-only writes
│    │
│    ├── bronze_to_silver.py          Bronze → Silver (deduplication)
│    │                                - Remove duplicates by (symbol, timestamp)
│    │                                - Apply data quality rules
│    │                                - Schema enforcement
│    │                                - Upsert (merge) pattern
│    │
│    ├── silver_to_gold.py            Silver → Gold (OHLCV aggregation)
│    │                                - 1-minute candle aggregation
│    │                                - 5-minute candle aggregation
│    │                                - Moving averages (SMA, EMA)
│    │                                - Overwrite pattern
│    │
│    ├── check_delta_paths.py         Delta Lake validation script
│    │                                - Verify table schemas
│    │                                - Check partition structure
│    │                                - Audit data integrity
│    │
│    ├── check_count.py               Row count verification
│    │                                - Bronze layer row counts
│    │                                - Silver layer row counts
│    │                                - Data completeness checks
│    │
│    ├── gcs_auth.py                  Google Cloud Storage authentication
│    │                                - Service account setup
│    │                                - Credential management
│    │                                - Permission validation
│    │
│    ├── print_ohlcv_schema.py        Print Gold layer schema
│    │                                - Verify OHLCV structure
│    │                                - Column names & types
│    │
│    └── requirements.txt             Processing dependencies
│                                     (pyspark, delta-spark, google-cloud-storage)
│
├─── 📅 Apache Airflow DAGs (Orchestration)
│    ├── 01_ingestion_dag.py          Binance data ingestion DAG
│    │                                - Triggers batch producer
│    │                                - Schedule: Daily @00:00 UTC
│    │                                - Depends: None
│    │
│    ├── 02_bronze_streaming_dag.py   Bronze streaming job scheduler
│    │                                - Manages real-time ingestion
│    │                                - Schedule: Continuous
│    │                                - Depends: Kafka availability
│    │
│    ├── 03_silver_dag.py             Silver layer transformation scheduler
│    │                                - Triggers bronze_to_silver.py
│    │                                - Schedule: Hourly after Bronze
│    │                                - Depends: 01_ingestion_dag
│    │
│    ├── 04_gold_dag.py               Gold layer aggregation scheduler
│    │                                - Triggers silver_to_gold.py
│    │                                - Schedule: Daily @02:00 UTC
│    │                                - Depends: 03_silver_dag
│    │
│    └── 05_maintenance_dag.py        Delta Lake maintenance DAG
│                                     - OPTIMIZE tables
│                                     - VACUUM old versions
│                                     - Schedule: Weekly @Sunday 03:00 UTC
│                                     - Depends: None
│
├─── 📊 dbt Project (Data Build Tool)
│    ├── dbt_project.yml              dbt configuration
│    │                                - Project name: crypto_lakehouse
│    │                                - Profiles: dev, prod
│    │                                - Targets: Trino, Delta
│    │
│    ├── profiles_template.yml        dbt connection template
│    │                                - Trino connection settings
│    │                                - GCS credentials path
│    │
│    ├── packages.yml                 dbt package dependencies
│    │                                - dbt_expectations
│    │                                - dbt_utils
│    │
│    ├── models/                      SQL transformation models
│    │                                - {{ ref() }} and {{ source() }} references
│    │                                - Staging, intermediate, marts
│    │
│    ├── tests/                       Data quality test definitions
│    │                                - Unique tests
│    │                                - Not null tests
│    │                                - Referential integrity tests
│    │
│    ├── seeds/                       Reference data (CSV → table load)
│    │                                - Symbol metadata
│    │                                - Exchange information
│    │
│    ├── macros/                      Reusable SQL/Jinja logic
│    │                                - Custom transformations
│    │
│    ├── scripts/                     Custom SQL utilities
│    │                                - Manual data fixes
│    │                                - One-off analyses
│    │
│    ├── target/                      dbt build artifacts (git-ignored)
│    │                                - Compiled models
│    │                                - Manifest & documentation
│    │
│    ├── logs/                        dbt execution logs
│    │                                - Run history
│    │                                - Error details
│    │
│    ├── dbt_packages/                dbt dependency packages (git-ignored)
│    │
│    └── README.md                    dbt project documentation
│
├─── 🐳 Docker Spark Configuration
│    ├── Dockerfile                   Custom Spark 3.5.8 Docker image
│    │                                - Base: spark:3.5.8-scala2.12
│    │                                - + Delta Lake 3.2.1
│    │                                - + GCS connector
│    │                                - + Additional dependencies
│    │
│    └── start-spark.sh               Spark container entrypoint script
│                                     - Master/Worker initialization
│                                     - Environment setup
│
├─── 🔗 Kafka Connect Configuration
│    └── Dockerfile                   Kafka Connect integration layer
│                                     - Debezium connectors (optional)
│                                     - Sink connectors for external systems
│
├─── 🎯 Apache Airflow Configuration
│    ├── Dockerfile                   Airflow 2.8+ Docker image
│    │                                - Base: apache/airflow:2.8-python3.11
│    │                                - + GCP provider
│    │                                - + Apache Spark provider
│    │                                - + Additional executors
│    │
│    ├── airflow_logs/                Airflow execution logs (git-ignored)
│    │                                - dag_id=*/
│    │                                  └── run_id=*/
│    │                                - scheduler/
│    │                                - processor/
│    │
│    └── dags.py (virtual)            DAG directory (see dags/ folder)
│
├─── 🤖 Machine Learning Module
│    ├── app.py                       Flask web application
│    │                                - Routes for model inference
│    │                                - Real-time prediction endpoint
│    │                                - Historical analysis routes
│    │
│    ├── train_all.py                 Model training orchestrator
│    │                                - Feature engineering
│    │                                - Model selection
│    │                                - Hyperparameter tuning
│    │                                - Model serialization
│    │
│    ├── requirements.txt             ML dependencies
│    │                                (flask, scikit-learn, tensorflow, pandas)
│    │
│    ├── models/                      Trained model artifacts
│    │                                - .pkl files (scikit-learn)
│    │                                - .h5 files (TensorFlow)
│    │                                - Model versioning
│    │
│    ├── data/                        Training & inference data
│    │                                - train.csv
│    │                                - test.csv
│    │                                - validation.csv
│    │
│    ├── templates/                   HTML templates (Jinja2)
│    │                                - index.html
│    │                                - dashboard.html
│    │                                - prediction_form.html
│    │
│    ├── static/                      Frontend assets
│    │                                - css/
│    │                                - js/
│    │                                - images/
│    │
│    ├── __pycache__/                 Python cache (git-ignored)
│    │
│    └── README.md                    ML module documentation
│
├─── ☁️ Cloud Infrastructure Setup
│    ├── gcs_setup.ps1                Google Cloud Storage bucket provisioning
│    │                                - Create GCS buckets
│    │                                - Set up folder structure
│    │                                - Configure access permissions
│    │
│    └── docker_gcs_auth.md           GCS authentication guide
│                                     - Service account creation
│                                     - Key file location & permissions
│                                     - Docker environment setup
│
├─── 🗄️ Hive Metastore Configuration
│    └── hive-site.xml                Hive JDBC & metastore configuration
│                                     - PostgreSQL backend connection
│                                     - HMS Thrift server settings
│                                     - Delta Lake integration
│
├─── 📝 Database & Utility Scripts
│    ├── scripts/
│    │   ├── init_postgres.sql        PostgreSQL initialization script
│    │   │                            - Create databases
│    │   │                            - Initialize schemas
│    │   │                            - Set up user permissions
│    │   │
│    │   └── ... (other utility scripts)
│    │
│    └── requirements.txt             Script dependencies (if any)
│
├─── ✅ Test Suite
│    ├── test_dags.py                 Airflow DAG validation tests
│    │                                - DAG loading tests
│    │                                - Dependency validation
│    │                                - Scheduling tests
│    │
│    ├── validate_pipeline.py         End-to-end pipeline tests
│    │                                - Data integrity checks
│    │                                - Schema validation
│    │                                - Count verification
│    │
│    ├── run_task2_tests.ps1          PowerShell test runner
│    │                                - Execute pytest suite
│    │                                - Collect results
│    │                                - Generate report
│    │
│    └── __pycache__/                 Python cache (git-ignored)
│
├─── 📦 MinIO Data Storage (Optional)
│    └── minio_data/                  MinIO bucket storage (git-ignored)
│        ├── bronze/                  S3 Bronze layer
│        ├── silver/                  S3 Silver layer
│        ├── gold/                    S3 Gold layer
│        ├── raw-batch/               Raw batch staging
│        └── checkpoints/             Spark checkpoints
│
├─── 🔐 PostgreSQL Data Directory
│    └── postgres_data/               PostgreSQL container volume (git-ignored)
│        ├── base/                    Database files
│        ├── pg_wal/                  Write-ahead logs
│        ├── pg_tblspc/               Tablespace files
│        └── ...
│
├─── 📜 Airflow Logs Directory
│    └── airflow_logs/                DAG execution logs (git-ignored)
│        ├── dag_id=*/
│        ├── scheduler/
│        └── dag_processor_manager/
│
└─── 🗂️ Trino Configuration (Optional)
     └── trino/
         └── catalog/
             └── delta.properties     Delta Lake connector config
```

---

## Key Files & Their Purposes

### Configuration Files

| File | Purpose | When to Edit |
|------|---------|-------------|
| `docker-compose.yml` | Service orchestration | Add/remove services, change versions |
| `.env.example` | Template for env vars | Add new configuration options |
| `requirements.txt` | Python dependencies | Add new Python packages |
| `dbt_project.yml` | dbt configuration | Change dbt settings, targets |

### Entry Points

| Script | Trigger | Frequency | Duration |
|--------|---------|-----------|----------|
| `producer_stream.py` | Manual or Airflow | Continuous | Indefinite |
| `producer_batch.py` | Airflow DAG | Daily @00:00 UTC | 5-15 min |
| `bronze_streaming.py` | Airflow DAG | Continuous | Indefinite |
| `bronze_to_silver.py` | Airflow DAG | Hourly | 10-30 min |
| `silver_to_gold.py` | Airflow DAG | Daily @02:00 UTC | 5-15 min |

### Data Paths (GCS)

| Path | Layer | Contents | Size |
|------|-------|----------|------|
| `gs://.../bronze/trades` | Bronze | Raw tick data | 5-25 MB/day |
| `gs://.../silver/trades` | Silver | Deduplicated data | 3-15 MB/day |
| `gs://.../gold/ohlcv_1m` | Gold | 1-min candles | 500 KB-2 MB/day |
| `gs://.../gold/ohlcv_5m` | Gold | 5-min candles | 100-500 KB/day |
| `gs://.../raw-batch/` | Staging | CSV files | ~100-500 KB/run |
| `gs://.../checkpoints/` | Metadata | Streaming state | Variable |

---

## Development Workflow

### Adding a New Transformation

1. **Create processing script** in `processing/` (e.g., `silver_to_platinum.py`)
2. **Add dbt model** in `dbt/models/` if using dbt
3. **Create Airflow DAG** in `dags/` (e.g., `06_platinum_dag.py`)
4. **Write tests** in `tests/` (e.g., `test_platinum_transform.py`)
5. **Document** in `DATA_FLOW.md`
6. **Commit** with meaningful message

### Adding a New Ingestion Source

1. **Create producer** in `ingestion/` (e.g., `producer_kraken.py`)
2. **Create Kafka topic** (via manual command or automation)
3. **Update `01_ingestion_dag.py`** to trigger new producer
4. **Add documentation** in `ARCHITECTURE.md`
5. **Test** with small sample before production

### Adding ML Features

1. **Create feature engineering script** in `ML/`
2. **Update `ML/train_all.py`** with new features
3. **Save model** to `ML/models/`
4. **Update Flask routes** in `ML/app.py`
5. **Test** with `ML/templates/prediction_form.html`

---

## Important Notes

### Git Ignore Patterns

Files automatically excluded from version control:

```
__pycache__/          Python cache files
*.pyc                 Compiled Python
.venv/                Virtual environment
venv/                 Virtual environment
postgres_data/        Database files
minio_data/           Object storage files
airflow_logs/         Execution logs
dbt_packages/         dbt dependencies
target/               dbt build outputs
.env                  Environment file (use .env.example as template)
```

### Directory Naming Conventions

```
lowercase_with_underscores/    Directories
lowercase_with_underscores.py  Python files
UPPERCASE_SNAKE_CASE.md        Documentation files
CamelCase                      Docker/K8s resources
```

### Performance Considerations

- **Bronze layer:** Partitioned by `symbol, date` for parallelism
- **Silver layer:** Partitioned by `symbol, date` to match Bronze
- **Gold layer:** Partitioned by `symbol, date, interval` (1m/5m)
- **Checkpoints:** Small files (~10-100 MB) compressed in GCS

---

## Quick Reference

### To find a specific component:

| What | Where |
|------|-------|
| Orchestration DAGs | `dags/` |
| Spark ETL jobs | `processing/` |
| Data producers | `ingestion/` |
| SQL transformations | `dbt/models/` |
| Data quality tests | `dbt/tests/` |
| ML models | `ML/models/` |
| Infrastructure setup | `docker-compose.yml` |
| Architecture docs | `ARCHITECTURE.md` |
| Setup guide | `QUICK_START.md` |

