# 📊 Tài Liệu Dữ Liệu — Crypto Data Lakehouse

> **Mục đích tài liệu:** Mô tả toàn diện nguồn gốc, quy trình thu thập, làm sạch, và đặc tính dữ liệu của hệ thống **Crypto-DataLakehouse-Project**, phục vụ cho việc tái sử dụng trong đồ án **Lambda Architecture Data Lake** tiếp theo.

---

## 1. 📍 Nguồn Dữ Liệu — Lấy Từ Đâu?

### 1.1. Nhà cung cấp chính

**Binance** — Sàn giao dịch tiền điện tử lớn nhất thế giới theo khối lượng giao dịch.

| Thông tin | Chi tiết |
|-----------|---------|
| **Nhà cung cấp** | Binance Global |
| **Website chính thức** | https://www.binance.com |
| **API Documentation** | https://binance-docs.github.io/apidocs/spot/en/ |
| **Loại dữ liệu** | Cryptocurrency Market Data (Trades & OHLCV Candles) |
| **Loại tài sản** | 50 cặp tiền tệ giao dịch nhiều nhất theo khối lượng (USDT pairs) |
| **Chi phí** | **Miễn phí** — Binance Public API không yêu cầu API Key cho dữ liệu public |
| **Rate Limit** | 1.200 request weight/phút (REST API) |

### 1.2. Địa chỉ API cụ thể

#### 🔴 Real-Time WebSocket Stream (Dữ liệu tức thì)

```
wss://stream.binance.com:9443/stream?streams={symbol1}@trade/{symbol2}@trade/...
```

- **Giao thức:** WebSocket (WSS)
- **Endpoint:** `wss://stream.binance.com:9443/stream`
- **Kiểu stream:** Combined Multi-Stream (`{symbol}@trade`)
- **Ví dụ đầy đủ:**

```
wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/bnbusdt@trade/...
```

- **Tần suất:** Gửi message mỗi khi có giao dịch khớp lệnh (thường vài ms đến vài giây)
- **Giữ kết nối:** Server ping mỗi 3 phút, client phải pong trong 10 phút

#### 📦 REST API — Lấy Danh Sách Top Coins (24hr Ticker)

```
GET https://api.binance.com/api/v3/ticker/24hr
```

- **Mục đích:** Lấy danh sách tất cả cặp giao dịch, sắp xếp theo `quoteVolume` (khối lượng USDT giao dịch 24h) để chọn Top-N coins
- **Không cần API Key**
- **Kết quả:** Mảng JSON gồm tất cả cặp giao dịch với thống kê 24h

#### 📦 REST API — Lấy Lịch Sử Nến OHLCV (Klines / Candlestick)

```
GET https://api.binance.com/api/v3/klines?symbol={SYMBOL}&interval=1m&limit=1000
```

| Tham số | Giá trị | Mô tả |
|---------|---------|-------|
| `symbol` | `BTCUSDT`, `ETHUSDT`,... | Cặp giao dịch |
| `interval` | `1m` | Khung thời gian nến 1 phút |
| `limit` | `1000` | Số nến tối đa mỗi request (tối đa 1.000/request) |

- **Request weight:** 2 (giới hạn 1.200/phút)
- **1 request = 1.000 nến ≈ ~16.7 giờ dữ liệu lịch sử** (ở khung 1m)

---

## 2. 🔄 Quy Trình Thu Thập Dữ Liệu (Step-by-Step)

### 2.1. Bước 0 — Chọn Top-N Coins (Chạy trước mỗi phiên)

**Công cụ:** Python `requests`, Binance REST API

```
[Binance /api/v3/ticker/24hr]
        ↓
Lọc các cặp kết thúc bằng "USDT"
        ↓
Sắp xếp giảm dần theo quoteVolume (Khối lượng 24h)
        ↓
Chọn Top-50 (hoặc TOP_N_COINS từ .env)
        ↓
[Danh sách ký hiệu: BTCUSDT, ETHUSDT, BNBUSDT, ...]
```

**Kết quả mẫu (Top 10 ngày điển hình):**
`BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT, DOGEUSDT, ADAUSDT, AVAXUSDT, SHIBUSDT, DOTUSDT`

---

### 2.2. Bước 1A — Real-Time WebSocket Stream

**File thực thi:** `ingestion/producer_stream.py`
**Công cụ:** `websocket-client`, `kafka-python`, `tenacity`

```
Bước 1: Gọi /api/v3/ticker/24hr → Lấy Top-N USDT pairs
Bước 2: Xây dựng WebSocket URL combined stream (N symbols @trade)
Bước 3: Kết nối wss://stream.binance.com:9443/stream
Bước 4: Với mỗi message nhận được:
         │
         ├── Validate các trường bắt buộc {e, E, s, t, p, q, T, m}
         │         OK  → Thêm field ingested_at → Publish → Kafka topic: crypto_trades_raw
         │         ERR → Publish → Kafka topic: crypto_trades_dlq (Dead Letter Queue)
         │
Bước 5: ping/pong mỗi 30s để duy trì kết nối (Binance timeout 10 phút)
Bước 6: Khi mất kết nối → tenacity tự động retry (exponential backoff: 2s → 60s, tối đa 20 lần)
```

**Tham số vận hành:**
- `KAFKA_BOOTSTRAP_SERVERS`: `kafka:29092` (trong Docker) hoặc `localhost:9092` (host)
- `KAFKA_TOPIC_RAW`: `crypto_trades_raw`
- `KAFKA_TOPIC_DLQ`: `crypto_trades_dlq`
- `TOP_N_COINS`: `50` (mặc định)
- Container **chạy 24/7** với `restart: always` trong docker-compose

---

### 2.3. Bước 1B — Batch Historical OHLCV (Lịch sử)

**File thực thi:** `ingestion/producer_batch.py`
**Công cụ:** `requests`, `kafka-python`

```
Bước 1: Gọi /api/v3/ticker/24hr → Lấy Top-N USDT pairs (giống stream)
Bước 2: Vòng lặp qua từng symbol:
         │
         ├── GET /api/v3/klines?symbol=BTCUSDT&interval=1m&limit=1000
         │         → 1.000 nến 1-phút gần nhất (~16.7 giờ lịch sử)
         │
         ├── Chuyển đổi mỗi kline row → Trade-compatible tick (cùng schema với WebSocket)
         │         (Dùng close_price làm price, volume làm quantity, event_type="kline_batch")
         │
         ├── Publish từng tick → Kafka topic: crypto_trades_raw
         │
         └── Quản lý Rate Limit:
               - Sleep 0.5s giữa các symbol
               - Nếu tổng weight >= 1.100 → Sleep 60s rồi reset counter
Bước 3: Hoàn thành: ~50 symbols × 1.000 candles = ~50.000 ticks/lần chạy
```

**Lưu ý thiết kế quan trọng:** Batch data được chuẩn hóa về cùng schema với WebSocket data trước khi vào Kafka, giúp Bronze pipeline xử lý thống nhất 1 luồng duy nhất.

---

### 2.4. Bước 2 — Kafka → Bronze (Spark Structured Streaming)

**File thực thi:** `processing/bronze_streaming.py`
**Công cụ:** Apache Spark 3.5.8, Delta Lake 3.x, Spark-Kafka connector

```
Kafka topic: crypto_trades_raw (JSON)
        ↓
Spark Structured Streaming (trigger: 60 giây / micro-batch)
        ↓
Đọc column `value` → cast STRING → rename trường case-conflict
    (e→event_type, E→event_time_ms, t→trade_id, T→trade_time, m→buyer_maker, M→ignore_m)
        ↓
Parse JSON theo schema cứng (TRADE_SCHEMA)
        ↓
Thêm cột `processing_date` = DATE(event_time_ms / 1000)
        ↓
Ghi Append → Delta Lake (GCS)
  Path: gs://crypto-lakehouse-group8/bronze
  Partition: (processing_date, s)    ← date-first để tránh small files
  Checkpoint: gs://crypto-lakehouse-group8/checkpoints/kafka_to_bronze
```

---

### 2.5. Bước 3 — Bronze → Silver (Spark Batch Incremental)

**File thực thi:** `processing/bronze_to_silver.py`
**Công cụ:** Apache Spark, Delta Lake (MERGE INTO), `delta.tables.DeltaTable`

Xem chi tiết tại Mục 3 (Làm sạch dữ liệu).

---

### 2.6. Bước 4 — Silver → Gold (Spark Batch Aggregation)

**File thực thi:** `processing/silver_to_gold.py`
**Công cụ:** Apache Spark Window Functions, `F.window()` groupBy

Xem chi tiết tại Mục 4 (Xử lý dữ liệu).

---

## 3. 🧹 Làm Sạch Dữ Liệu (Data Cleaning — Bronze → Silver)

### 3.1. Kiểm Tra Chất Lượng Dữ Liệu (Data Quality Rules)

Được thực hiện trong `bronze_to_silver.py` — hàm `split_valid_quarantine()`:

| # | Rule | Điều kiện lỗi | Hành động |
|---|------|---------------|-----------|
| 1 | **Không null trade_id** | `trade_id IS NULL` | Quarantine |
| 2 | **Event type hợp lệ** | `event_type NOT IN ('trade', 'kline_batch')` | Quarantine |
| 3 | **Giá không null sau ép kiểu** | `price_decimal IS NULL` sau CAST | Quarantine |
| 4 | **Giá > 0** | `price_decimal <= 0` | Quarantine |
| 5 | **Quantity không null sau ép kiểu** | `quantity_decimal IS NULL` sau CAST | Quarantine |
| 6 | **Quantity > 0** | `quantity_decimal <= 0` | Quarantine |

**Nguyên tắc:** Dữ liệu lỗi **không bao giờ bị xóa im lặng** — luôn được ghi vào bảng Quarantine tại `gs://crypto-lakehouse-group8/silver/quarantine/` để audit và tái xử lý. Pipeline không dừng vì dữ liệu xấu (chỉ dừng khi hạ tầng gặp sự cố).

### 3.2. Ép Kiểu & Làm Giàu Dữ Liệu

Hàm `cast_and_enrich()`:

| Cột nguồn (Bronze) | Cột đích (Silver) | Phép biến đổi |
|--------------------|--------------------|---------------|
| `p` (string) | `price_decimal` | `CAST → DecimalType(38, 18)` — 18 chữ số thập phân |
| `q` (string) | `quantity_decimal` | `CAST → DecimalType(38, 18)` |
| `event_time_ms` (long ms) | `event_time` | `(ms / 1000).cast(TimestampType())` |
| `trade_time` (long ms) | `trade_time` | `(ms / 1000).cast(TimestampType())` |
| `event_time_ms` | `dt` (DateType) | `TO_DATE(FROM_UNIXTIME(event_time_ms / 1000))` |
| `s` | `symbol` | Rename → uppercase string |
| `buyer_maker` | `buyer_is_maker` | Rename |
| — | `silver_ingested_at` | `datetime.now(UTC).isoformat()` |

> **Lý do dùng `DecimalType(38, 18)` thay vì `DoubleType`:**
> Các meme coin như PEPEUSDT, SHIBUSDT có giá rất nhỏ (ví dụ: `0.000008234`). `DoubleType` IEEE-754 sẽ làm tròn sai ở chữ số thứ 15+, gây ra sai lệch trong tính toán tài chính.

### 3.3. Khử Trùng Lặp (Deduplication)

Hàm `deduplicate()` — chiến lược dedup theo `(symbol, trade_id)`:

```sql
-- Giữ lại 1 bản ghi mới nhất theo ingested_at cho mỗi (symbol, trade_id)
ROW_NUMBER() OVER (PARTITION BY symbol, trade_id ORDER BY ingested_at DESC) = 1
```

**Lý do cần dedup:** Dual ingestion (WebSocket + REST API) đảm bảo dữ liệu luôn đến, nhưng có thể dẫn đến cùng 1 `trade_id` xuất hiện 2 lần trong Kafka.

### 3.4. Chiến Lược Ghi Silver (MERGE INTO / Upsert)

```
Nếu bảng Silver đã tồn tại:
    MERGE INTO silver AS target
    USING new_data AS source ON (target.trade_id = source.trade_id AND target.symbol = source.symbol)
    WHEN NOT MATCHED THEN INSERT ALL  ← Chỉ chèn bản ghi mới (trade là immutable)

Nếu chưa tồn tại:
    CREATE TABLE với PARTITION BY (symbol, dt)
```

---

## 4. ⚙️ Xử Lý Dữ Liệu (Data Processing — Silver → Gold)

### 4.1. Tổng Hợp OHLCV

Hàm `build_ohlcv_candles()` — dùng `F.window()` + `groupBy`:

```python
# Gom tick vào khung thời gian (1 phút hoặc 5 phút)
windowed = df.withColumn("time_bucket", F.window(F.col("event_time"), "1 minute"))
             .withColumn("candle_time", F.col("time_bucket.start"))

# Tổng hợp OHLCV — sử dụng struct trick để đảm bảo tính xác định (deterministic)
ohlcv = windowed.groupBy("symbol", "candle_time").agg(
    F.min(F.struct("event_time", "price")).getField("price").alias("open"),   # Giá đầu tiên
    F.max("price").alias("high"),                                              # Giá cao nhất
    F.min("price").alias("low"),                                               # Giá thấp nhất
    F.max(F.struct("event_time", "price")).getField("price").alias("close"),   # Giá cuối cùng
    F.sum("quantity").alias("volume"),                                         # Tổng khối lượng
    F.count("*").alias("tick_count"),                                          # Số lệnh khớp
)
```

> **Kỹ thuật `F.min(struct(event_time, price))`:** Thay vì `F.first()` / `F.last()` (non-deterministic trong Spark parallel), cách này đảm bảo `open` LUÔN là giá tại `event_time` nhỏ nhất (sớm nhất), `close` tại `event_time` lớn nhất (muộn nhất), bất kể thứ tự partition.

### 4.2. Chỉ Báo Kỹ Thuật (Moving Averages)

Hàm `compute_moving_averages()`:

```python
symbol_window = Window.partitionBy("symbol").orderBy("candle_time")

# MA7, MA20, MA50 tính trên cột close của nến
F.avg("close").over(symbol_window.rowsBetween(-(period - 1), 0))
```

| Chỉ báo | Chu kỳ | Ý nghĩa | Ghi chú |
|---------|--------|---------|---------|
| `ma_7` | 7 nến gần nhất | Xu hướng ngắn hạn | NULL cho 6 nến đầu tiên |
| `ma_20` | 20 nến gần nhất | Xu hướng trung hạn | NULL cho 19 nến đầu tiên |
| `ma_50` | 50 nến gần nhất | Xu hướng dài hạn | NULL cho 49 nến đầu tiên |

---

## 5. 📋 Thuộc Tính Dữ Liệu Hiện Tại — Schema Từng Tầng

### 5.1. Schema Lớp Bronze (`gs://crypto-lakehouse-group8/bronze`)

| Tên Cột | Kiểu Dữ Liệu | Nullable | Mô Tả |
|---------|-------------|----------|-------|
| `event_type` | `StringType` | YES | Loại sự kiện Binance: `"trade"` (WebSocket) hoặc `"kline_batch"` (REST) |
| `event_time_ms` | `LongType` | YES | Thời điểm sự kiện, Unix epoch milliseconds (UTC) |
| `s` | `StringType` | YES | Ký hiệu cặp giao dịch, VD: `"BTCUSDT"` |
| `trade_id` | `LongType` | YES | ID giao dịch Binance, duy nhất theo symbol |
| `p` | `StringType` | YES | Giá giao dịch (chuỗi để tránh mất độ chính xác) |
| `q` | `StringType` | YES | Khối lượng giao dịch (chuỗi) |
| `trade_time` | `LongType` | YES | Thời điểm khớp lệnh, Unix epoch milliseconds |
| `buyer_maker` | `BooleanType` | YES | `true` nếu người mua là market maker |
| `ignore_m` | `BooleanType` | YES | Trường dự phòng từ Binance (bỏ qua) |
| `ingested_at` | `StringType` | YES | ISO 8601 timestamp khi producer nhận message |
| `processing_date` | `DateType` | YES | **Partition column** — DATE từ `event_time_ms` |

**Partition:** `(processing_date, s)` — date-first
**Format lưu trữ:** Delta Lake (Parquet + transaction log)
**Ghi mode:** Append-only (immutable raw truth)

---

### 5.2. Schema Lớp Silver (`gs://crypto-lakehouse-group8/silver`)

| Tên Cột | Kiểu Dữ Liệu | Nullable | Mô Tả |
|---------|-------------|----------|-------|
| `event_type` | `StringType` | YES | `"trade"` hoặc `"kline_batch"` |
| `symbol` | `StringType` | YES | Ký hiệu uppercase: `"BTCUSDT"`, `"ETHUSDT"`,... |
| `trade_id` | `LongType` | NO | **Khóa chính** — ID giao dịch Binance (đã lọc null) |
| `price_decimal` | `DecimalType(38,18)` | NO | Giá giao dịch — 18 chữ số thập phân |
| `quantity_decimal` | `DecimalType(38,18)` | NO | Khối lượng giao dịch — 18 chữ số thập phân |
| `event_time` | `TimestampType` | YES | Thời điểm sự kiện (UTC Timestamp) |
| `trade_time` | `TimestampType` | YES | Thời điểm khớp lệnh (UTC Timestamp) |
| `buyer_is_maker` | `BooleanType` | YES | Người mua là market maker hay không |
| `ingested_at` | `StringType` | YES | Thời điểm producer thu thập |
| `silver_ingested_at` | `StringType` | YES | Thời điểm Silver job xử lý |
| `dt` | `DateType` | YES | **Partition column** — DATE từ event_time |

**Partition:** `(symbol, dt)` — symbol-first cho query per-coin
**Format lưu trữ:** Delta Lake
**Ghi mode:** MERGE INTO (Upsert) — idempotent

---

### 5.3. Schema Lớp Gold (`gs://crypto-lakehouse-group8/gold`)

| Tên Cột | Kiểu Dữ Liệu | Nullable | Mô Tả |
|---------|-------------|----------|-------|
| `symbol` | `StringType` | NO | Ký hiệu cặp giao dịch |
| `candle_time` | `TimestampType` | NO | Thời điểm BẮT ĐẦU của nến |
| `candle_date` | `DateType` | NO | **Partition column** — DATE từ candle_time |
| `candle_duration` | `StringType` | NO | `"1 minute"` hoặc `"5 minutes"` |
| `open` | `DoubleType` | NO | Giá mở cửa (giá giao dịch đầu tiên trong nến) |
| `high` | `DoubleType` | NO | Giá cao nhất trong nến |
| `low` | `DoubleType` | NO | Giá thấp nhất trong nến |
| `close` | `DoubleType` | NO | Giá đóng cửa (giá giao dịch cuối cùng trong nến) |
| `volume` | `DoubleType` | NO | Tổng khối lượng giao dịch trong nến (đơn vị coin) |
| `tick_count` | `LongType` | YES | Số lệnh khớp tạo nên nến |
| `ma_7` | `DoubleType` | YES | Moving Average 7 nến gần nhất (null nếu chưa đủ lịch sử) |
| `ma_20` | `DoubleType` | YES | Moving Average 20 nến gần nhất |
| `ma_50` | `DoubleType` | YES | Moving Average 50 nến gần nhất |
| `processing_timestamp` | `StringType` | YES | ISO 8601 timestamp khi Gold job chạy |

**Partition:** `(symbol, candle_date)` — tối ưu truy vấn Trino/Power BI
**Format lưu trữ:** Delta Lake
**Ghi mode:** Overwrite (full recompute từ Silver — idempotent)

---

### 5.4. Schema Quarantine (`gs://crypto-lakehouse-group8/silver/quarantine`)

| Tên Cột | Mô Tả |
|---------|-------|
| *(Tất cả cột Bronze)* | Giữ nguyên dữ liệu gốc |
| `quarantine_reason` | Chuỗi mô tả lý do bị loại (ghép nhiều rule bằng `"; "`) |
| `quarantine_dt` | `DateType` — ngày xử lý để partition |

---

## 6. 🌐 Tổng Quan Về Dữ Liệu

### 6.1. Phạm Vi & Quy Mô

| Thông số | Giá trị |
|----------|---------|
| **Số cặp giao dịch** | Tối đa 50 cặp USDT (top theo volume 24h) |
| **Khung thời gian** | Từ ngày chạy pipeline trở đi (không giới hạn ngược) |
| **Lịch sử khởi tạo** | ~1.000 nến × 50 symbols = ~50.000 records/lần batch đầu |
| **Tần suất streaming** | Liên tục (Binance trade events ~vài ms đến vài giây/event) |
| **Micro-batch Bronze** | Mỗi 60 giây |
| **Batch Silver/Gold** | Mỗi khi được trigger (theo DAG Airflow) |
| **Kích thước dữ liệu ước tính** | Bronze: ~GB/ngày; Silver: ~GB/ngày; Gold: ~MB/ngày |
| **Retention** | Bronze: 90+ ngày; Silver: 90+ ngày; Gold: 1+ năm |
| **Time Travel (Delta Lake)** | Có thể rollback 7 ngày (trước khi VACUUM chạy) |

### 6.2. Đặc Điểm Dữ Liệu

- **Dữ liệu tài chính thời gian thực:** Mỗi record = 1 lệnh khớp trên sàn Binance
- **Append-heavy:** Bronze nhận hàng triệu records/ngày (thị trường mở 24/7)
- **Biến động cao:** Giá có thể thay đổi từng giây, đặc biệt trong sự kiện thị trường lớn
- **Multi-symbol:** Dữ liệu đồng thời từ 50 cặp, cần partition theo symbol để query hiệu quả
- **Dual ingestion:** Cùng 1 trade_id có thể đến từ cả WebSocket lẫn batch REST → cần dedup
- **Precision-sensitive:** Small-cap coin (PEPE, SHIB) có giá ~ `0.0000001` → bắt buộc dùng `Decimal(38,18)`
- **UTC timezone:** Tất cả timestamp đều là UTC (Binance standard)

### 6.3. Top Coins Điển Hình (USDT Pairs — Top Volume)

```
BTCUSDT   — Bitcoin / USDT         (Khối lượng lớn nhất)
ETHUSDT   — Ethereum / USDT
BNBUSDT   — BNB / USDT
SOLUSDT   — Solana / USDT
XRPUSDT   — Ripple / USDT
DOGEUSDT  — Dogecoin / USDT
ADAUSDT   — Cardano / USDT
AVAXUSDT  — Avalanche / USDT
SHIBUSDT  — Shiba Inu / USDT       (Meme coin, giá rất nhỏ)
PEPEUSDT  — PEPE / USDT            (Meme coin, giá cực nhỏ)
... (top 50 thay đổi theo thị trường, cập nhật mỗi lần khởi động producer)
```

---

## 7. 📝 Mô Tả Chi Tiết Các Cột Quan Trọng

### 7.1. Các Trường Gốc Từ Binance WebSocket (Bronze)

| Trường gốc (Binance) | Tên sau rename | Ý nghĩa chi tiết |
|---------------------|----------------|-----------------|
| `e` → `event_type` | `event_type` | Luôn là `"trade"` cho WebSocket trade stream. Batch REST API dùng `"kline_batch"` để phân biệt nguồn dữ liệu. |
| `E` → `event_time_ms` | `event_time_ms` | Unix epoch milliseconds khi Binance server phát event. |
| `s` | `symbol` (sau Silver) | Cặp giao dịch: luôn UPPERCASE, ví dụ `"BTCUSDT"`. |
| `t` → `trade_id` | `trade_id` | ID giao dịch duy nhất của Binance, tăng dần theo từng symbol. **Đây là khóa dedup.** |
| `p` → `price_decimal` | `price_decimal` | Giá khớp lệnh. Dạng chuỗi ở Bronze, `Decimal(38,18)` ở Silver. |
| `q` → `quantity_decimal` | `quantity_decimal` | Khối lượng coin được giao dịch. Đơn vị là coin (không phải USDT). |
| `T` → `trade_time` | `trade_time` | Thời điểm lệnh thực sự được khớp (thường chênh vài ms so với event_time). |
| `m` → `buyer_is_maker` | `buyer_is_maker` | `true`: người MUA là market maker (đặt lệnh limit). `false`: người MUA là taker (đặt lệnh market). |

### 7.2. Các Trường Được Tính Toán Trong Pipeline

| Cột | Tầng | Công thức / Nguồn |
|-----|------|-------------------|
| `ingested_at` | Bronze | `datetime.utcnow().isoformat()` — thời điểm producer nhận message |
| `processing_date` | Bronze | `DATE(FROM_UNIXTIME(event_time_ms / 1000))` |
| `event_time` | Silver | `TIMESTAMP(event_time_ms / 1000)` |
| `dt` | Silver | `DATE(event_time)` |
| `silver_ingested_at` | Silver | `datetime.now(UTC).isoformat()` |
| `candle_time` | Gold | Start timestamp của time window |
| `candle_date` | Gold | `DATE(candle_time)` |
| `open` | Gold | Giá tại `min(event_time)` trong window |
| `close` | Gold | Giá tại `max(event_time)` trong window |
| `high` | Gold | `MAX(price)` trong window |
| `low` | Gold | `MIN(price)` trong window |
| `volume` | Gold | `SUM(quantity_decimal)` trong window |
| `tick_count` | Gold | `COUNT(*)` trong window |
| `ma_7 / ma_20 / ma_50` | Gold | `AVG(close)` trong cửa sổ 7/20/50 nến trước, partition by symbol |

### 7.3. Các Trường Tính Trong dbt Mart Layer

| Cột | Công thức | Dùng để |
|-----|-----------|---------|
| `candle_range` | `high - low` | Đo biên độ biến động giá trong nến |
| `typical_price` | `(high + low + close) / 3` | Giá đại diện cho VWAP |
| `window_end` | `candle_time + INTERVAL N MINUTE` | Thời điểm kết thúc nến |
| `window_minutes` | `CASE candle_duration WHEN '1 minute' THEN 1 WHEN '5 minutes' THEN 5` | Giá trị số nguyên của candle_duration |
| `vwap_cumulative` | `SUM(typical_price × volume) / SUM(volume)` | VWAP tích lũy trong ngày (partition by symbol, trade_date) |
| `price_change_pct` | `(close - LAG(close)) / LAG(close) × 100` | % thay đổi so với nến trước |
| `candle_direction` | `CASE WHEN close >= open THEN 'BULLISH' ELSE 'BEARISH'` | Phân loại xu hướng nến |

---

## 8. Ghi Chú Cho Đồ Án Lambda Architecture

> Phần này được bổ sung dựa trên kinh nghiệm đồ án hiện tại, nhằm giúp thiết kế hệ thống Lambda Architecture mới hiệu quả hơn.

### 8.1. Điểm Tương Thích & Tái Sử Dụng

Kiến trúc hiện tại đã có **nền tảng rất gần** với Lambda Architecture:

| Thành phần Lambda | Tương đương hiện tại | Trạng thái |
|-------------------|---------------------|------------|
| **Speed Layer** | `bronze_streaming.py` (Kafka → Bronze, 60s micro-batch) | Cần tối ưu latency xuống thấp hơn |
| **Batch Layer** | `producer_batch.py` + `bronze_to_silver.py` + `silver_to_gold.py` | Hoàn chỉnh |
| **Serving Layer** | Gold + Trino + dbt Marts | Cần thêm real-time view và merge logic |
| **Message Bus** | Apache Kafka | Tái sử dụng được ngay |

### 8.2. Vấn Đề Cần Giải Quyết Khi Chuyển Sang Lambda

#### Vấn đề 1 — Low-Latency Speed Layer (QUAN TRỌNG)

**Hiện tại:** Bronze được query qua Trino với latency vài giây. Đủ cho batch nhưng chưa đủ cho Speed Layer của Lambda cần latency < 1 giây.

**Giải pháp đề xuất:**
- Thêm **Apache Flink** hoặc **Spark Structured Streaming với `foreachBatch` → Redis/DynamoDB** để Speed Layer có latency thấp.
- Hoặc dùng **Kafka Streams / ksqlDB** trực tiếp cho real-time aggregation mà không cần Spark.

#### Vấn đề 2 — Serving Layer Merge Logic (QUAN TRỌNG)

**Hiện tại:** Gold là batch-only view (Overwrite mỗi batch). Không có cơ chế hợp nhất kết quả Speed + Batch.

**Cần thêm cho Lambda:** Một Serving Layer Query hợp nhất kết quả:

```sql
-- Pseudo-code: Lambda Serving Query
SELECT * FROM batch_gold_view  WHERE candle_time < watermark
UNION ALL
SELECT * FROM speed_realtime_view WHERE candle_time >= watermark
```

#### Vấn đề 3 — Watermark Management

**Hiện tại:** Không có watermark rõ ràng giữa Speed và Batch. `availableNow=True` chỉ xử lý data mới kể từ checkpoint.

**Cần thêm:** Cơ chế lưu watermark để biết ngưỡng thời gian nào đã "xử lý xong bởi Batch Layer" và từ ngưỡng nào cần dùng Speed Layer.

#### Vấn đề 4 — Schema Registry

**Hiện tại:** Schema được định nghĩa cứng trong code Python (`TRADE_SCHEMA` trong `bronze_streaming.py`).

**Cần thêm cho Lambda:** Schema Registry (Confluent Schema Registry hoặc Avro schema) để đảm bảo Speed Layer và Batch Layer không bị schema drift.

### 8.3. Những Thứ Có Thể Giữ Nguyên Cho Lambda Project

| Thành phần | Lý do giữ |
|-----------|-----------|
| **Kafka** làm message broker | Phù hợp cả Speed lẫn Batch Layer, đã ổn định |
| **GCS + Delta Lake** làm Batch Layer storage | ACID, time-travel, schema evolution — hoàn hảo cho Batch Layer |
| **Binance API** | Đã kiểm thử, stable, miễn phí |
| **Dedup logic** `(symbol, trade_id)` | Phải giữ nguyên — dual ingestion vẫn cần |
| **Decimal(38, 18)** cho giá tài chính | Bắt buộc — không thể thay bằng Double |
| **Date-first partitioning** trên Bronze | Giảm small files hiệu quả |
| **Airflow** | Làm orchestrator cho Batch Layer |
| **dbt** | Data quality testing và Serving Layer transformation |

### 8.4. Dữ Liệu Gợi Ý Bổ Sung Cho Lambda Project

| Nguồn bổ sung | Mục đích | Công cụ gợi ý |
|--------------|---------|---------------|
| **Binance Order Book** (`wss://.../depth`) | Real-time bid/ask spread, depth thị trường | WebSocket → Kafka → Flink |
| **Binance Funding Rate** (`/api/v3/fundingRate`) | Tâm lý thị trường Futures | REST batch |
| **CoinGecko API** | Market cap, rank, social metrics | REST batch (miễn phí) |
| **Fear & Greed Index** (alternative.me) | Sentiment thị trường tổng thể | REST daily batch |
| **On-chain metrics** (Glassnode/CryptoQuant) | Dữ liệu blockchain nâng cao | REST (có phí) |

### 8.5. Lưu Ý Quan Trọng Về Rate Limit Khi Scale

| API | Giới hạn | Chiến lược xử lý |
|-----|---------|-----------------|
| Binance WebSocket | 300 kết nối/IP, tối đa 1024 streams/kết nối | Dùng combined stream (`/stream?streams=`) |
| Binance REST 24hr ticker | 40 weight/request | Chỉ gọi 1 lần khi khởi động, cache kết quả |
| Binance REST klines | 2 weight/request, giới hạn 1.200/phút | Sleep 0.5s/symbol, reset counter sau 60s |
| Binance WebSocket uptime | Binance ngắt kết nối sau 24h | Tự động reconnect với exponential backoff |

### 8.6. Checklist Trước Khi Bắt Đầu Lambda Project

- [ ] Xác định SLA rõ ràng: Speed Layer cần latency bao nhiêu? (< 1s, < 5s, < 30s?)
- [ ] Chọn Speed Layer technology: Flink? Spark Streaming? Kafka Streams?
- [ ] Thiết kế Serving Layer merge logic: hợp nhất Speed + Batch như thế nào?
- [ ] Định nghĩa Schema Registry để tránh schema drift giữa Speed và Batch
- [ ] Xác định Batch Window: Batch chạy mỗi bao lâu? (1h? 6h? 24h?)
- [ ] Kế hoạch backfill: Có tái sử dụng GCS data hiện tại từ đồ án này không?
- [ ] Test Coverage: Unit test cho cả Speed và Batch output, đặc biệt là test Serving Layer merge

---

## 9. 📂 Vị Trí File & Tài Nguyên Tham Khảo

| Tài nguyên | Đường dẫn |
|-----------|---------|
| Producer WebSocket (real-time) | `ingestion/producer_stream.py` |
| Producer Batch (historical) | `ingestion/producer_batch.py` |
| Bronze Streaming (Kafka → GCS) | `processing/bronze_streaming.py` |
| Bronze → Silver (DQ + Dedup) | `processing/bronze_to_silver.py` |
| Silver → Gold (OHLCV + MA) | `processing/silver_to_gold.py` |
| GCS Auth Helper (SA + ADC) | `processing/gcs_auth.py` |
| Airflow DAGs | `dags/` |
| dbt Staging + Mart Models | `dbt/models/` |
| Trino Catalog Config | `trino/catalog/` |
| Docker Compose (toàn bộ infra) | `docker-compose.yml` |
| Pipeline Data Quality Validator | `tests/validate_pipeline.py` |

---

## 10. 📊 Tóm Tắt Toàn Bộ Data Flow

```
[BINANCE]
    │
    ├── WebSocket (wss://stream.binance.com:9443/stream)
    │       Trade ticks (real-time, ~ms latency)
    │       ↓
    │   [KAFKA]  topic: crypto_trades_raw
    │       ↓ Spark Structured Streaming (60s micro-batch)
    │   [BRONZE]  gs://crypto-lakehouse-group8/bronze/
    │             Delta Lake, Append-only, Partition: (processing_date, s)
    │
    └── REST API (https://api.binance.com/api/v3/klines)
            1.000 nến lịch sử × 50 symbols
            ↓ Normalize → Trade-compatible tick schema
        [KAFKA]  topic: crypto_trades_raw (cùng topic với stream)
            ↓ (xử lý bởi cùng Bronze pipeline)

[BRONZE]
    │
    ↓ Spark Batch (foreachBatch, availableNow=True)
    │   - DQ Check: 6 rules
    │   - Cast: Decimal(38,18)
    │   - Dedup: (symbol, trade_id)
    │   - Write: MERGE INTO
    │
    ├── [SILVER]  gs://crypto-lakehouse-group8/silver/
    │             Delta Lake, Upsert, Partition: (symbol, dt)
    │
    └── [QUARANTINE]  gs://crypto-lakehouse-group8/silver/quarantine/
                      Bad records với quarantine_reason

[SILVER]
    │
    ↓ Spark Batch (Overwrite, Full recompute)
    │   - OHLCV 1-minute candles
    │   - OHLCV 5-minute candles
    │   - MA7 / MA20 / MA50
    │
    [GOLD]  gs://crypto-lakehouse-group8/gold/
            Delta Lake, Overwrite, Partition: (symbol, candle_date)

[GOLD]
    │
    ↓ Trino (Federated SQL Query Engine)
    │
    ├── dbt Marts → VWAP, price_change_pct, BULLISH/BEARISH
    ├── Power BI Dashboard (via Trino ODBC)
    └── ML Flask App (XGBoost, LSTM, Isolation Forest)
```

---

*Tài liệu được tạo: 2026-09-01*
*Phiên bản dự án: Crypto-DataLakehouse v1.0*
*Tác giả hệ thống gốc: Team Group 8 — HCMUTE Big Data*
