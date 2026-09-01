# CV Skills — Crypto Data Lakehouse Project

> Tài liệu này tổng hợp các **kỹ năng, công nghệ, và bullet points** từ dự án Crypto Data Lakehouse để bạn điền vào CV.
> Format theo đúng mẫu CV hiện tại của bạn.

---

## 1. TECHNICAL SKILLS (bổ sung vào CV)

Dưới đây là những kỹ năng mới **chưa có trong CV hiện tại** mà dự án này chứng minh:

### Big Data (cập nhật dòng hiện tại)

**Hiện tại trong CV:**
> Big Data: Apache Hadoop (HDFS), PySpark

**Đề xuất cập nhật:**
> Big Data: Apache Spark (Structured Streaming, Batch ETL), Apache Kafka, Delta Lake, Apache Airflow, Hive Metastore, Trino (SQL Federation)

### Data Engineering (mục MỚI — thêm vào CV)

> Data Engineering: Medallion Architecture (Bronze/Silver/Gold), ETL/ELT Pipeline Design, Data Quality & Validation, Stream Processing, Batch Processing

### Cloud & DevOps (mục MỚI — thêm vào CV)

> Cloud & DevOps: Google Cloud Storage (GCS), Docker Compose, Container Orchestration, MinIO (S3-compatible)

### Machine Learning (cập nhật dòng hiện tại)

**Hiện tại trong CV:**
> Machine Learning: Supervised & Unsupervised Learning, Classification, Regression, Clustering, Feature Selection, CRISP-DM methodology

**Đề xuất cập nhật (thêm vào cuối):**
> Machine Learning: ..., XGBoost, LightGBM, LSTM (Time Series Forecasting), Isolation Forest (Anomaly Detection), Feature Engineering

### Data Visualization & BI (cập nhật)

**Đề xuất bổ sung:**
> Data Visualization & BI: ..., Flask Dashboard, Server-Sent Events (SSE), Real-time Monitoring

### Programming (cập nhật)

**Đề xuất bổ sung:**
> Programming: Python (Pandas, NumPy, Scikit-learn, PySpark, PySpark SQL, TensorFlow/Keras), SQL, ...

### Tools & Version Control (cập nhật)

**Đề xuất cập nhật:**
> Tools & Version Control: Git, GitHub, Docker, Linux (Ubuntu), Jupyter Notebook

---

## 2. PROJECTS (mục viết cho CV)

### Mẫu hoàn chỉnh — Copy trực tiếp vào CV

---

**Real-Time Crypto Data Lakehouse** &emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&emsp; 2025
*Technologies: Python, PySpark, Apache Kafka, Delta Lake, Apache Airflow, Trino, Docker, GCS* &emsp; [GitHub]

- Architected a **Medallion data lakehouse** (Bronze → Silver → Gold) on Google Cloud Storage, processing **50 cryptocurrency trading pairs** with both real-time streaming and historical batch pipelines.

- Implemented a **real-time streaming pipeline** using Binance WebSocket → Apache Kafka → Spark Structured Streaming with **30-second micro-batches**, achieving **~30–60 second end-to-end latency** for tick-level trade data.

- Built a **batch ingestion pipeline** fetching **1,000 OHLCV candles per pair** via Binance REST API with rate-limiting (1,200 weight budget) and exponential backoff retry strategy.

- Developed a **3-layer ETL pipeline** in PySpark: Bronze (append-only raw storage), Silver (deduplication via window functions, data quality rules with quarantine table, DecimalType(38,18) precision), and Gold (OHLCV aggregation with 1m/5m candles + moving averages).

- Orchestrated **5 Airflow DAGs** with dependency management, anti-collision locks, and self-healing retry logic, achieving fully automated data-aware scheduling across all pipeline stages.

- Enabled **federated SQL analytics** via Trino over Delta Lake tables, supporting ad-hoc queries on 90+ days of historical data with partition pruning optimization.

- Containerized **10+ microservices** (Kafka, Spark, Trino, Airflow, Hive Metastore, PostgreSQL, MinIO) using Docker Compose with health checks, resource limits, and multi-network isolation.

- Built an **ML prediction dashboard** (Flask + SSE) with XGBoost, LightGBM, and LSTM models for real-time price prediction and Isolation Forest for anomaly detection on OHLCV data.

---

## 3. Phiên bản tiếng Việt (tham khảo)

**Real-Time Crypto Data Lakehouse** &emsp; 2025
*Công nghệ: Python, PySpark, Apache Kafka, Delta Lake, Apache Airflow, Trino, Docker, GCS*

- Thiết kế kiến trúc **Medallion Lakehouse** (Bronze → Silver → Gold) trên Google Cloud Storage, xử lý **50 cặp tiền mã hóa** với cả pipeline streaming real-time và batch lịch sử.

- Xây dựng pipeline **streaming real-time** sử dụng Binance WebSocket → Apache Kafka → Spark Structured Streaming với **micro-batch 30 giây**, đạt **độ trễ ~30–60 giây** cho dữ liệu trade tick-level.

- Triển khai pipeline **batch ingestion** lấy **1,000 nến OHLCV/cặp** từ Binance REST API với cơ chế rate-limiting và exponential backoff.

- Phát triển pipeline **ETL 3 lớp** bằng PySpark: Bronze (lưu trữ thô), Silver (khử trùng lặp bằng window functions, kiểm tra chất lượng dữ liệu với bảng quarantine), Gold (tổng hợp OHLCV nến 1m/5m + đường trung bình động).

- Quản lý **5 DAGs Airflow** với dependency management, anti-collision locks, và retry logic tự phục hồi.

- Tích hợp **Trino SQL federation** truy vấn Delta Lake tables, hỗ trợ truy vấn ad-hoc trên 90+ ngày dữ liệu lịch sử.

- Đóng gói **10+ microservices** bằng Docker Compose với health checks và resource limits.

- Xây dựng **ML dashboard** (Flask + SSE) với XGBoost, LightGBM, LSTM cho dự đoán giá real-time và Isolation Forest cho phát hiện bất thường.

---

## 4. Tổng hợp công nghệ (Full List)

### Theo nhóm chức năng

| Nhóm | Công nghệ sử dụng |
|------|--------------------|
| **Message Broker** | Apache Kafka 7.5.0, ZooKeeper, Kafka Connect |
| **Stream Processing** | Apache Spark 3.5.8 Structured Streaming (PySpark) |
| **Batch Processing** | Apache Spark 3.5.8 Batch (PySpark) |
| **Table Format** | Delta Lake 3.x (ACID transactions trên cloud storage) |
| **Cloud Storage** | Google Cloud Storage (GCS) |
| **Local Storage** | MinIO (S3-compatible) |
| **Metadata Catalog** | Hive Metastore 3.1.2 + PostgreSQL 15 |
| **SQL Query Engine** | Trino 432 (Federated SQL) |
| **Orchestration** | Apache Airflow 2.8+ (5 DAGs) |
| **Data Quality** | PySpark DQ rules, Quarantine table, dbt Core |
| **Containerization** | Docker Compose v3.8 (10+ services) |
| **ML — Classification** | XGBoost, LightGBM (Bullish/Bearish prediction) |
| **ML — Time Series** | LSTM (TensorFlow/Keras) — price forecasting |
| **ML — Anomaly Detection** | Isolation Forest (scikit-learn) |
| **ML — Feature Engineering** | RSI, Moving Averages, Volatility, Volume Spikes |
| **Web Framework** | Flask + Server-Sent Events (SSE) |
| **Data Ingestion** | Binance WebSocket API, Binance REST API |
| **Language** | Python 3.10+ |
| **Libraries** | PySpark, kafka-python, websocket-client, tenacity, pandas, numpy, scikit-learn, xgboost, lightgbm, tensorflow, flask |

### Theo kỹ năng kỹ thuật

| Kỹ năng | Chi tiết thể hiện trong dự án |
|---------|-------------------------------|
| **Data Architecture** | Thiết kế Medallion Architecture 3 lớp (Bronze/Silver/Gold) |
| **Stream Processing** | Kafka → Spark Structured Streaming, micro-batch 30s, watermarking |
| **Batch ETL** | REST API → GCS staging → Spark batch → Delta Lake |
| **Data Quality** | Inline DQ rules, quarantine table, deduplication, schema enforcement |
| **Data Modeling** | OHLCV aggregation, window functions, moving averages (SMA) |
| **Schema Design** | DecimalType(38,18) cho tránh rounding errors, partition strategy |
| **Workflow Orchestration** | 5 Airflow DAGs với dependency chains, anti-collision, self-healing |
| **Cloud Engineering** | GCS authentication (Service Account + ADC), bucket management |
| **Containerization** | Multi-service Docker Compose, health checks, resource limits |
| **SQL Analytics** | Trino federated queries, partition pruning, Delta Lake integration |
| **ML Pipeline** | Feature engineering → Train → Inference → Dashboard (end-to-end) |
| **Real-time Dashboard** | Flask + SSE, auto-refresh predictions mỗi 30s |
| **API Integration** | Binance WebSocket + REST API, rate limiting, exponential backoff |
| **Error Handling** | Dead-Letter Queue (DLQ), retry strategies, graceful degradation |

---

## 5. Keywords cho ATS (Applicant Tracking System)

Các từ khóa nên xuất hiện trong CV để qua vòng lọc tự động:

```
Apache Spark, PySpark, Apache Kafka, Delta Lake, Medallion Architecture,
Data Lakehouse, ETL, ELT, Structured Streaming, Batch Processing,
Apache Airflow, DAG, Trino, SQL, Google Cloud Storage, GCS,
Docker, Docker Compose, Hive Metastore, PostgreSQL,
Data Quality, Data Pipeline, Data Engineering,
XGBoost, LightGBM, LSTM, Isolation Forest,
Python, REST API, WebSocket, Real-time Processing,
OHLCV, Time Series, Feature Engineering, CI/CD
```
