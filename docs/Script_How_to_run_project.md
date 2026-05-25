# Hướng dẫn chạy đồ án từ đầu đến cuối (E2E)

Tài liệu này hướng dẫn chạy toàn bộ project **Crypto Data Lakehouse** theo thứ tự thực tế: setup môi trường, chạy Docker, ingest dữ liệu, xử lý Bronze/Silver/Gold, test chất lượng, train ML và chạy web dashboard.

---

## 1. Tổng quan luồng chạy

Luồng end-to-end của đồ án:

1. **Khởi động hạ tầng Docker**: Kafka, Spark, Trino, Hive Metastore, Postgres, Airflow, MinIO.
2. **Ingestion dữ liệu**:
   - Realtime: `producer_stream.py` (WebSocket -> Kafka)
   - Batch lịch sử: `producer_batch.py` (REST -> Kafka)
3. **Processing Medallion**:
   - `bronze_streaming.py`: Kafka -> Bronze Delta
   - `bronze_to_silver.py`: Bronze -> Silver (DQ + dedup + quarantine)
   - `silver_to_gold.py`: Silver -> Gold (OHLCV 1m/5m + MA)
4. **Query/Serving**: Trino đọc Gold.
5. **Data Quality**: dbt test/source test.
6. **ML + Web**:
   - `ML/train_all.py` train model
   - `ML/app.py` chạy Flask + SSE realtime dashboard

---

## 2. Yêu cầu trước khi chạy

- Windows 10/11 + PowerShell
- Docker Desktop (khuyến nghị bật WSL2)
- Python 3.10+
- Git
- (Khuyến nghị) VS Code

Tài nguyên máy tối thiểu:
- RAM khả dụng cho Docker: **>= 10 GB**
- Disk trống: **>= 20 GB**

---

## 3. Chuẩn bị project

### 3.1 Clone và vào thư mục project

```powershell
git clone https://github.com/Quan-min211/Crypto-DataLakehouse-Project.git
cd Crypto-DataLakehouse-Project
```

### 3.2 Cấu hình `.env`

Project đã có file `.env` ở root.

Nếu cần reset, copy từ mẫu:

```powershell
Copy-Item .env.example .env
```

Các biến chính:
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_TOPIC_RAW`, `KAFKA_TOPIC_DLQ`
- `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`
- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`
- `BINANCE_REST_URL`, `BINANCE_WS_URL`

### 3.3 (Nếu dùng GCS) đăng nhập gcloud

Một số job Spark/Hive/Trino trong repo dùng mount ADC từ `%APPDATA%\gcloud`.

```powershell
gcloud auth login
gcloud auth application-default login
```

---

## 4. Khởi động toàn bộ hạ tầng Docker

Chạy từ thư mục root project:

```powershell
docker compose up -d --build
```

Kiểm tra container:

```powershell
docker ps
```

### 4.1 Các cổng quan trọng

- Trino UI: `http://localhost:8080`
- Spark Master UI: `http://localhost:8082`
- Airflow UI: `http://localhost:8888` (user/pass mặc định: `admin/admin`)
- MinIO Console: `http://localhost:9001` (admin/admin123)
- Kafka broker host: `localhost:9092`

### 4.2 Kiểm tra nhanh service

```powershell
docker logs kafka --tail 30
docker logs trino --tail 30
docker logs hive-metastore --tail 30
docker logs airflow-webserver --tail 30
```

---

## 5. Chạy lấy dữ liệu (Ingestion)

> Lưu ý: trong `docker-compose.yml`, service `producer-stream` đã chạy 24/7 bằng Docker (`restart: always`).

### 5.1 Realtime stream (auto qua Docker)

Kiểm tra log:

```powershell
docker logs producer-stream --tail 50
```

Kết quả mong đợi: có log kết nối Binance WebSocket và publish dữ liệu vào topic `crypto_trades_raw`.

### 5.2 Batch lịch sử (chạy thủ công)

Bạn có 2 cách:

#### Cách A: chạy trực tiếp trong container `producer-stream` (khuyên dùng)

```powershell
docker exec -i producer-stream python producer_batch.py
```

#### Cách B: chạy local bằng venv ingestion

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python ingestion/producer_batch.py
```

### 5.3 Kiểm tra Kafka đã có dữ liệu

```powershell
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic crypto_trades_raw --from-beginning --max-messages 5
```

---

## 6. Chạy pipeline Bronze -> Silver -> Gold

Bạn có thể chạy theo **Airflow DAG** hoặc chạy tay từng script.

## 6.1 Cách 1: chạy bằng Airflow (khuyên dùng)

1. Mở Airflow UI: `http://localhost:8888`
2. Bật DAG:
   - `01_ingestion_dag`
   - `02_bronze_streaming_continuous`
   - `03_silver_dag`
   - `04_gold_dag`
   - `05_delta_lake_maintenance`
3. Trigger theo thứ tự:
   - Trigger `02_bronze_streaming_continuous` (stream vào Bronze)
   - Trigger `01_ingestion_dag` (batch -> trigger Silver)
   - `03` và `04` sẽ trigger dây chuyền

## 6.2 Cách 2: chạy tay từng bước

### Bước 1: Kafka -> Bronze

```powershell
docker exec -i spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client --driver-memory 512m --executor-memory 512m --num-executors 1 --executor-cores 1 --packages "io.delta:delta-spark_2.12:3.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,commons-pool:commons-pool:1.6" --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --conf "spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore" /processing/bronze_streaming.py
```

### Bước 2: Bronze -> Silver

```powershell
docker exec -i spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client --driver-memory 1g --executor-memory 768m --num-executors 1 --executor-cores 1 --packages io.delta:delta-spark_2.12:3.2.1 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --conf "spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore" /processing/bronze_to_silver.py
```

### Bước 3: Silver -> Gold

```powershell
docker exec -i spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client --driver-memory 512m --executor-memory 512m --num-executors 1 --executor-cores 1 --packages io.delta:delta-spark_2.12:3.2.1 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --conf "spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore" /processing/silver_to_gold.py
```

---

## 7. Kiểm tra dữ liệu Gold bằng Trino

Mở Trino UI: `http://localhost:8080`.

SQL kiểm tra nhanh:

```sql
SHOW CATALOGS;
SHOW SCHEMAS FROM delta;
SHOW TABLES FROM delta.default;

SELECT symbol, candle_time, open, high, low, close, volume
FROM delta.default.gold_ohlcv
ORDER BY candle_time DESC
LIMIT 20;
```

---

## 8. Chạy test cho pipeline

## 8.1 Test Airflow DAG (Task 2)

```powershell
.\tests\run_task2_tests.ps1
```

## 8.2 Validate pipeline data quality end-to-end

```powershell
# Copy test script vào container (vì spark-master chỉ mount thư mục /processing)
docker cp .\tests\validate_pipeline.py spark-master:/processing/validate_pipeline.py

# Chạy spark-submit trong container
docker exec -i spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client --packages io.delta:delta-spark_2.12:3.2.1 /processing/validate_pipeline.py
```

Nội dung kiểm tra chính:
- Data integrity (Kafka vs Bronze)
- Latency
- Precision kiểu Decimal ở Silver
- Deduplication `(symbol, trade_id)`

---

## 9. Chạy dbt để test chất lượng Gold

### 9.1 Tạo môi trường dbt

```powershell
python -m venv .venv-dbt
.\.venv-dbt\Scripts\Activate.ps1
pip install dbt-core dbt-trino
```

### 9.2 Tạo profile dbt

Tạo file tại:
`C:\Users\<YourUser>\.dbt\profiles.yml`

Dựa trên `dbt/profiles_template.yml`.

Yêu cầu quan trọng: phải có trường `user` trong output `trino`.

### 9.3 Chạy dbt

```powershell
cd dbt
dbt debug
dbt deps
dbt run
dbt test
```

Nếu chỉ test source Gold:

```powershell
dbt test --select source:gold source:gold_gcs
```

---

## 10. Chạy ML từ Gold

### 10.1 Tạo môi trường ML

```powershell
cd ..
python -m venv .venv-ml
.\.venv-ml\Scripts\Activate.ps1
pip install -r ML/requirements.txt
```

### 10.2 Train model

```powershell
.\.venv-ml\Scripts\python.exe ML\train_all.py
```

Kết quả model lưu tại:
- `ML/models/saved/*.pkl`
- `ML/models/saved/lstm_model.h5`
- `ML/models/saved/training_results.json`

---

## 11. Chạy web dashboard realtime

```powershell
.\.venv-ml\Scripts\python.exe ML\app.py
```

Mở web:
- `http://localhost:5000`

Endpoints chính:
- `GET /api/predictions`
- `GET /api/price-history`
- `GET /api/anomalies`
- `GET /stream` (SSE, chu kỳ 30 giây)

Log backend mong đợi:
- `[SSE] Updated: price=..., xgb=..., lstm=...`
- `[GOLD REFRESH] ...` (chu kỳ gần 5 phút theo `GOLD_REFRESH_INTERVAL`)

---

## 12. Kịch bản chạy nhanh đề xuất (không lỗi)

1. `docker compose up -d --build`
2. Chờ Trino + Spark + Airflow healthy (`docker logs ... --tail 30`)
3. `docker exec -i producer-stream python producer_batch.py`
4. Trigger DAG `02_bronze_streaming_continuous` (hoặc chạy tay Bronze job)
5. Trigger DAG `03_silver_dag` rồi `04_gold_dag` (hoặc chạy tay script)
6. Kiểm tra Gold trên Trino
7. Chạy `dbt debug && dbt test`
8. Chạy `ML/train_all.py`
9. Chạy `ML/app.py` và mở dashboard

---

## 13. Lỗi thường gặp và cách xử lý

### Lỗi 1: `dbt debug` bị reset connection
- Nguyên nhân: Trino chưa ready.
- Cách xử lý: chờ thêm, kiểm tra `docker logs trino --tail 100`, chạy lại `dbt debug`.

### Lỗi 2: `ModuleNotFoundError: trino` khi train ML
- Nguyên nhân: chạy sai virtual env.
- Cách xử lý: dùng đúng interpreter `.venv-ml\Scripts\python.exe`.

### Lỗi 3: Airflow có DAG nhưng không chạy được Spark
- Kiểm tra container `spark-master`, `spark-worker` đang chạy.
- Kiểm tra command có `docker exec spark-master spark-submit`.

### Lỗi 4: Gold chưa có dữ liệu
- Chưa có input từ Silver hoặc Silver fail.
- Chạy lại theo chuỗi: batch ingest -> Bronze -> Silver -> Gold.
- Kiểm tra log từng bước.

---

## 14. Lệnh dọn dẹp

```powershell
# stop service
docker compose down

# stop + xóa volume local
docker compose down -v

# xóa image dangling (tuỳ chọn)
docker image prune -f
```

---

## 15. Checklist nghiệm thu

- [ ] Docker stack chạy đủ service
- [ ] Kafka có message `crypto_trades_raw`
- [ ] Bronze có dữ liệu
- [ ] Silver có dữ liệu sạch + dedup
- [ ] Gold có OHLCV 1m/5m
- [ ] Trino query được Gold
- [ ] dbt test pass
- [ ] ML train hoàn tất và có model artifact
- [ ] Dashboard chạy tại `localhost:5000`
- [ ] SSE log cập nhật định kỳ

---

**Ghi chú:** Tài liệu này bám theo code hiện tại trong repo ở branch `feature/ML` và ưu tiên cách chạy thực tế trên Windows.
