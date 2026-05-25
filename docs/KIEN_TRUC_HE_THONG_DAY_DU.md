# Kiến trúc hệ thống tổng thể — Crypto Data Lakehouse Project

Tài liệu này tổng hợp **toàn bộ kiến trúc hệ thống** dựa trên cấu trúc mã nguồn hiện tại trong repo, bao gồm: ingestion, medallion pipeline (Bronze/Silver/Gold), orchestration, data quality (dbt), ML serving, realtime dashboard (SSE), và hạ tầng Docker.

---

## 1) Sơ đồ kiến trúc tổng thể (End-to-End)

```mermaid
flowchart TB
    %% ========================
    %% SOURCES
    %% ========================
    subgraph SRC[External Data Sources]
        BWS[Binance WebSocket\n@trade stream]
        BREST[Binance REST API\n/klines + /ticker/24hr]
    end

    %% ========================
    %% INGESTION
    %% ========================
    subgraph ING[Ingestion Layer]
        PSTREAM[producer_stream.py\nTop-N symbols\nreconnect + DLQ]
        PBATCH[producer_batch.py\nHistorical 1m klines\nREST -> Kafka]
    end

    %% ========================
    %% BROKER
    %% ========================
    subgraph MQ[Message Broker]
        KAFKA[(Kafka\ncrypto_trades_raw)]
        KDLQ[(Kafka DLQ\ncrypto_trades_dlq)]
        ZK[(ZooKeeper)]
        KCONN[Kafka Connect]
    end

    %% ========================
    %% PROCESSING
    %% ========================
    subgraph PROC[Processing Layer (Spark + Delta)]
        BRONZEJOB[bronze_streaming.py\nKafka -> Bronze Delta\nStreaming micro-batch]
        SILVERJOB[bronze_to_silver.py\nDQ + quarantine + dedup\nMERGE into Silver]
        GOLDJOB[silver_to_gold.py\nAggregate OHLCV 1m/5m\nMA7/MA20/MA50]
        MAINT[Maintenance DAG\nOPTIMIZE + VACUUM]
    end

    %% ========================
    %% STORAGE
    %% ========================
    subgraph STOR[Storage Layer]
        BRONZE[(Bronze Delta\ngs://.../bronze)]
        SILVER[(Silver Delta\ngs://.../silver)]
        QUAR[(Quarantine Delta\ngs://.../silver/quarantine)]
        GOLD[(Gold Delta\ngs://.../gold)]
        CHK[(Checkpoints\ngs://.../checkpoints)]
        RAWB[(raw-batch files\nMinIO/GCS path)]
        MINIO[(MinIO local S3)]
        GCS[(Google Cloud Storage)]
    end

    %% ========================
    %% METADATA + QUERY
    %% ========================
    subgraph META[Metadata & Query]
        PG[(PostgreSQL\nmetastore + airflow DB)]
        HMS[Hive Metastore]
        TRINO[Trino\n(delta, delta_gcs catalogs)]
    end

    %% ========================
    %% ORCHESTRATION
    %% ========================
    subgraph ORCH[Orchestration (Airflow DAGs)]
        D01[01_ingestion_dag\nDaily batch ingest]
        D02[02_bronze_streaming_continuous\nKafka -> Bronze]
        D03[03_silver_dag\nBronze -> Silver]
        D04[04_gold_dag\nSilver -> Gold + register table + checks]
        D05[05_delta_lake_maintenance\nNightly optimize/vacuum]
    end

    %% ========================
    %% DATA QUALITY
    %% ========================
    subgraph DQ[Data Quality Layer (dbt)]
        DBT[dbt project\nsource tests + staging + marts]
        MART[(delta.marts.mart_crypto_dashboard)]
    end

    %% ========================
    %% ML + APP
    %% ========================
    subgraph MLAPP[ML + Serving + Visualization]
        TRAIN[ML/train_all.py\nTrain XGBoost/LightGBM\nLSTM + IsolationForest]
        MODELS[(ML/models/saved/*)]
        APP[ML/app.py\nFlask REST + SSE]
        API1[/api/predictions]
        API2[/api/price-history]
        API3[/api/anomalies]
        API4[/stream SSE 30s]
        WEB[Dashboard\nChart.js + HTML/CSS/JS]
    end

    %% ========================
    %% FLOWS
    %% ========================
    BWS --> PSTREAM
    BREST --> PBATCH
    BREST --> RAWB

    PSTREAM --> KAFKA
    PBATCH --> KAFKA
    PSTREAM -.malformed.-> KDLQ
    ZK --- KAFKA
    KCONN --- KAFKA

    KAFKA --> BRONZEJOB
    BRONZEJOB --> BRONZE
    BRONZEJOB --> CHK

    BRONZE --> SILVERJOB
    SILVERJOB --> SILVER
    SILVERJOB --> QUAR

    SILVER --> GOLDJOB
    GOLDJOB --> GOLD

    D01 --> PBATCH
    D02 --> BRONZEJOB
    D03 --> SILVERJOB
    D04 --> GOLDJOB
    D05 --> MAINT
    MAINT --> BRONZE
    MAINT --> SILVER
    MAINT --> GOLD

    PG --> HMS
    HMS --> TRINO
    GOLD --> HMS
    SILVER --> HMS
    BRONZE --> HMS
    TRINO --> DBT
    DBT --> MART

    GOLD --> TRINO
    TRINO --> TRAIN
    TRAIN --> MODELS

    TRINO --> APP
    MODELS --> APP
    APP --> API1
    APP --> API2
    APP --> API3
    APP --> API4
    API1 --> WEB
    API2 --> WEB
    API3 --> WEB
    API4 --> WEB

    MINIO --- HMS
    MINIO --- TRINO
    GCS --- BRONZE
    GCS --- SILVER
    GCS --- GOLD
    GCS --- CHK
```

---

## 2) Luồng nghiệp vụ theo lớp (Medallion + ML)

### 2.1 Ingestion

- **Streaming path:** `ingestion/producer_stream.py`
  - Lấy top coin từ Binance REST (`/ticker/24hr`), mở WebSocket nhiều stream `@trade`.
  - Đẩy dữ liệu realtime vào Kafka topic `crypto_trades_raw`.
  - Message lỗi/thiếu field được đẩy vào `crypto_trades_dlq`.
- **Batch path:** `ingestion/producer_batch.py`
  - Lấy klines lịch sử 1 phút từ Binance REST (`/api/v3/klines`).
  - Chuẩn hoá thành dạng tick-compatible và gửi vào cùng topic Kafka.

### 2.2 Bronze

- Job: `processing/bronze_streaming.py`
- Đọc Kafka stream, parse schema, ghi **append-only Delta** vào Bronze.
- Bronze giữ raw truth (không dedup tại Bronze).

### 2.3 Silver

- Job: `processing/bronze_to_silver.py`
- Áp data quality rules, tách lỗi vào **quarantine**.
- Deduplicate theo `(symbol, trade_id)` và `MERGE` vào Silver.
- Silver là lớp dữ liệu sạch, chuẩn hoá kiểu dữ liệu.

### 2.4 Gold

- Job: `processing/silver_to_gold.py`
- Tổng hợp OHLCV theo 2 khung nến: **1m** và **5m**.
- Tính MA (`ma_7`, `ma_20`, `ma_50`) phục vụ phân tích và ML.
- Ghi Delta partition theo `symbol`, `candle_date`.

### 2.5 Data Quality (dbt)

- Module: `dbt/`
- Kết nối Trino (`dbt-trino`), khai báo source Gold.
- Chạy:
  - source/singular/generic tests,
  - staging models,
  - mart `mart_crypto_dashboard`.
- Đảm bảo lớp Gold/mart ổn định trước khi tiêu thụ.

### 2.6 ML + Dashboard realtime

- **Training:** `ML/train_all.py`
  - Train 4 hướng mô hình: XGBoost, LightGBM, LSTM, IsolationForest.
  - Lưu artifact vào `ML/models/saved/`.
- **Serving:** `ML/app.py`
  - Đọc Gold qua Trino, chạy inference định kỳ.
  - REST endpoints chính:
    - `/api/predictions`
    - `/api/price-history`
    - `/api/anomalies`
  - SSE endpoint `/stream` push dữ liệu realtime mỗi **30 giây**.
  - Refresh Gold trong backend theo chu kỳ ~**5 phút** (`GOLD_REFRESH_INTERVAL = 10`, mỗi cycle 30s).

---

## 3) Orchestration & vận hành

Airflow DAGs trong thư mục `dags/`:

1. `01_ingestion_dag.py`: chạy batch ingestion theo lịch.
2. `02_bronze_streaming_dag.py`: điều phối Bronze streaming liên tục.
3. `03_silver_dag.py`: chạy Bronze -> Silver.
4. `04_gold_dag.py`: chạy Silver -> Gold, đăng ký bảng Gold cho Trino, trigger kiểm tra.
5. `05_maintenance_dag.py`: nightly `OPTIMIZE` + `VACUUM` để tối ưu hiệu năng và chi phí lưu trữ.

---

## 4) Hạ tầng triển khai (Docker Compose)

Các dịch vụ cốt lõi trong `docker-compose.yml`:

- Broker: `zookeeper`, `kafka`, `kafka-connect`
- Storage: `minio` (dev/local), `gcs` (cloud)
- Metadata: `postgres`, `hive-metastore`
- Compute/query: `spark-master`, `spark-worker`, `trino`
- Orchestration: `airflow-webserver`, `airflow-scheduler`
- Ingestion runtime: `producer-stream`

---

## 5) Ghi chú kiến trúc quan trọng

- Hệ thống hiện hỗ trợ **hybrid storage context**:
  - Local/dev thiên về MinIO (S3-compatible).
  - Pipeline chính và production path dùng GCS (`gs://crypto-lakehouse-group8/...`).
- Trino có cả catalog cho local (`delta.properties`) và GCS (`delta_gcs.properties`).
- ML app tiêu thụ trực tiếp Gold đã được kiểm định nhằm giảm sai lệch đầu vào.

---

## 6) Gợi ý dùng file này trong báo cáo

- Dùng phần **Mục 1 (Mermaid)** làm sơ đồ kiến trúc chính.
- Dùng **Mục 2** làm mô tả pipeline end-to-end.
- Dùng **Mục 3 + 4** để chứng minh khả năng vận hành thực tế (production readiness).

