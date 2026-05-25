# Task 1 — Data Quality trên Gold bằng dbt Core + Trino (Hướng dẫn chi tiết)

Tài liệu này giải thích **từng bước, từng phần** cho Task 1 của bạn:

> Set up dbt Core + dbt-trino, viết và chạy các test chất lượng cho Gold layer (Trino), gồm:
> - `high >= low`
> - không null định danh
> - không timestamp tương lai / kiểm soát missing timestamp theo điều kiện

---

## 1) Mục tiêu Task 1 là gì?

Mục tiêu không chỉ là “chạy dbt cho pass”, mà là tạo một **hàng rào chất lượng dữ liệu** trước khi dữ liệu Gold được dùng cho:

- Dashboard nghiệp vụ (Power BI / web realtime)
- Mô hình ML (XGBoost, LightGBM, LSTM, Isolation Forest)

Nếu Gold sai logic, mọi kết quả dashboard/ML sẽ sai theo.

---

## 2) Bạn đã làm gì (tóm tắt ngắn)

Bạn đã hoàn thành đúng trọng tâm Task 1:

1. Setup `dbt Core` + `dbt-trino` trong môi trường riêng (`.venv-dbt`).
2. Cấu hình `profiles.yml` để dbt kết nối Trino.
3. Khai báo source Gold trong `dbt/models/sources.yml`.
4. Viết/ràng buộc test chất lượng:
   - Custom generic test: `assert_high_gte_low.sql`
   - Singular tests: null symbol, future timestamp, missing 1-minute candles
5. Chạy test thực tế trên source Gold:
   - `dbt test --select source:gold source:gold_gcs`
   - Kết quả: **Exit Code = 0** (pass)

---

## 3) Kiến trúc file liên quan Task 1

Các file quan trọng trong repo:

- `dbt/dbt_project.yml` — cấu hình project dbt
- `dbt/profiles_template.yml` — template profile kết nối Trino
- `dbt/models/sources.yml` — khai báo source `gold` và `gold_gcs`
- `dbt/tests/generic/assert_high_gte_low.sql`
- `dbt/tests/singular/test_no_null_symbol.sql`
- `dbt/tests/singular/test_no_future_timestamps.sql`
- `dbt/tests/singular/test_no_missing_1min_candles.sql`
- `dbt/models/staging/schema.yml` — nơi gắn test schema/model

---

## 4) Setup từ đầu (step-by-step)

## Bước 1: đảm bảo Trino sẵn sàng

```powershell
docker compose up -d trino hive-metastore postgres minio
```

Kiểm tra nhanh:

```powershell
docker ps
```

---

## Bước 2: tạo môi trường dbt riêng

```powershell
python -m venv .venv-dbt
.\.venv-dbt\Scripts\Activate.ps1
pip install dbt-core dbt-trino
dbt --version
```

**Vì sao làm vậy?**
- Tách môi trường dbt khỏi Spark/ML để tránh xung đột package.

---

## Bước 3: tạo `profiles.yml` cho dbt-trino

Tạo file:

- `C:\Users\<YourUser>\.dbt\profiles.yml`

Copy từ `dbt/profiles_template.yml` và đảm bảo có `user`:

```yaml
crypto_lakehouse:
  target: dev
  outputs:
    dev:
      type: trino
      method: none
      user: dbt_user
      host: localhost
      port: 8080
      database: delta
      schema: default
      threads: 4
      http_scheme: http
```

**Lưu ý quan trọng:** dbt-trino bắt buộc có trường `user` dù `method: none`.

---

## Bước 4: vào thư mục dbt và kiểm tra kết nối

```powershell
cd dbt
dbt debug
```

Kỳ vọng: `All checks passed!`

Nếu fail, thường do Trino chưa ready -> chờ thêm 20-60 giây rồi chạy lại.

---

## Bước 5: tải package phụ thuộc dbt

```powershell
dbt deps
```

Mục đích: cài `dbt_utils` theo `packages.yml`.

---

## Bước 6: chạy build và test

```powershell
dbt run
dbt test
```

Với Task 1, lệnh trọng tâm bạn đã chạy:

```powershell
dbt test --select source:gold source:gold_gcs
```

---

## 5) Giải thích từng rule test và tại sao cần

### 5.1 Rule: `high >= low`

- File: `dbt/tests/generic/assert_high_gte_low.sql`
- Ý nghĩa: trong OHLCV, giá cao nhất không thể thấp hơn giá thấp nhất.

SQL logic (ý tưởng):
- tìm các dòng vi phạm `high < low`
- nếu có dòng trả ra => test fail

**Tại sao phải có rule này?**
- Đây là invariant cốt lõi của candle data.
- Vi phạm nghĩa là Gold aggregation có bug hoặc dữ liệu đã hỏng.

---

### 5.2 Rule: không null định danh (`symbol`)

- File: `dbt/tests/singular/test_no_null_symbol.sql`
- Ý nghĩa: `symbol` là khóa nhận diện đồng coin.

SQL logic (ý tưởng):
- bắt các dòng có `symbol IS NULL` hoặc rỗng.

**Tại sao phải có rule này?**
- Không có định danh thì không thể nhóm/so sánh/visualize theo coin.
- Dễ gây lỗi khi join hoặc tạo dashboard theo ticker.

---

### 5.3 Rule: không timestamp tương lai

- File: `dbt/tests/singular/test_no_future_timestamps.sql`
- Điều kiện hiện tại: `candle_time <= NOW() + 10 phút` (buffer lệch thời gian).

**Tại sao cần?**
- Tránh clock skew nghiêm trọng và dữ liệu “đi trước thời gian thật”.
- Tránh leakage trong ML (model học từ dữ liệu tương lai giả).

---

### 5.4 Rule: kiểm soát missing 1-minute candles (theo cửa sổ kiểm tra)

- File: `dbt/tests/singular/test_no_missing_1min_candles.sql`
- Cách làm:
  1. Lấy symbol đang active 2 giờ gần nhất.
  2. Tạo chuỗi minute kỳ vọng bằng `SEQUENCE(...)`.
  3. Left join với dữ liệu thực.
  4. Dòng nào thiếu minute => trả về vi phạm.

**Tại sao cần?**
- Phát hiện gián đoạn pipeline, rơi message, hoặc gap trong aggregation.
- Đặc biệt quan trọng cho dashboard realtime và model time-series.

> Ghi chú: test này có thể fail nếu đang chạy trên dữ liệu mock/không realtime liên tục.

---

## 6) Chuỗi lệnh chuẩn để bạn demo Task 1

Chạy từ root project:

```powershell
docker compose up -d trino hive-metastore postgres minio
.\.venv-dbt\Scripts\Activate.ps1
cd dbt
dbt debug
dbt deps
dbt run
dbt test --select source:gold source:gold_gcs
```

---

## 7) Kết quả cần báo cáo (acceptance)

Task 1 coi là đạt khi:

- `dbt debug` pass (kết nối Trino thành công)
- `dbt test --select source:gold source:gold_gcs` pass
- Không có vi phạm ở các rule trọng tâm:
  - high/low invariant
  - null identifier
  - future timestamp
  - missing minute (trong điều kiện kiểm soát)

---

## 8) Bạn làm vậy để làm gì? (phần giải thích cho báo cáo)

Bạn triển khai Task 1 nhằm biến Gold thành lớp dữ liệu có **độ tin cậy vận hành**:

1. **Đúng logic thị trường** (`high >= low`)
2. **Đủ khóa định danh** (không null symbol)
3. **Đúng trục thời gian** (không future)
4. **Đủ tính liên tục chuỗi thời gian** (không missing minute trong cửa sổ kiểm soát)

Nhờ vậy:

- Dashboard nghiệp vụ tránh hiển thị sai lệch.
- Pipeline ML giảm rủi ro học từ dữ liệu lỗi.
- Team có thể phát hiện lỗi sớm tại tầng dữ liệu thay vì đợi fail ở tầng ứng dụng.

---

## 9) Minh chứng bạn nên chụp (gợi ý)

1. `dbt debug` -> `All checks passed!`
2. Command: `dbt test --select source:gold source:gold_gcs`
3. Kết quả tổng: `PASS=... ERROR=0`.
4. Ảnh code 4 file test chính:
   - `assert_high_gte_low.sql`
   - `test_no_null_symbol.sql`
   - `test_no_future_timestamps.sql`
   - `test_no_missing_1min_candles.sql`

---

## 10) Kết luận ngắn cho Task 1

Bạn đã hoàn thành Task 1 đúng yêu cầu đề bài:

- Setup `dbt Core + dbt-trino`
- Viết/chạy bộ test chất lượng nghiêm ngặt trên Gold
- Chứng minh dữ liệu đầu vào cho Dashboard và ML đã được kiểm định trước khi tiêu thụ

