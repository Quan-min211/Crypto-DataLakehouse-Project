# TIỂU LUẬN KHOA HỌC — PHẦN VIỆC CÁ NHÂN TRONG ĐỒ ÁN CRYPTO DATALAKEHOUSE

## Thông tin phạm vi

Tài liệu này là tiểu luận khoa học cho phần việc cá nhân trong đồ án nhóm. Nội dung chỉ tập trung vào ba nhiệm vụ được giao, trình bày theo đúng thứ tự triển khai kỹ thuật từ trên xuống dưới.

---

## Mục lục

1. Task 1 — Data Quality với dbt Core và Trino trên Gold layer  
   1.1 Bối cảnh và mục tiêu  
   1.2 Thiết kế bộ kiểm thử nghiêm ngặt  
   1.3 Quy trình setup và chạy dbt  
   1.4 Kết quả và nhận xét khoa học  
2. Task 2 — Xây dựng 3 model ML đọc dữ liệu Gold từ Trino  
   2.1 Tổ chức thư mục và mục tiêu mô hình  
   2.2 Luồng lấy dữ liệu, xử lý đặc trưng và huấn luyện  
   2.3 Đánh giá kỹ thuật cho cụm mô hình  
3. Task 3 — Xây dựng Web Dashboard realtime bằng Flask, Chart.js, SSE  
   3.1 Tổ chức thư mục ML và luồng dữ liệu huấn luyện  
   3.2 Thiết kế Flask Dashboard realtime với SSE  
   3.3 Live Crypto Ticker truy vấn `gold_ohlcv`  
   3.4 Kết quả thực nghiệm và bình luận  
4. Kết luận phần việc cá nhân  
5. Phụ lục hướng dẫn chụp minh chứng

---

## 1. Task 1 — Data Quality với dbt Core và Trino trên Gold layer

### 1.1 Bối cảnh và mục tiêu

Trong kiến trúc Data Lakehouse, Gold layer là lớp dữ liệu phục vụ trực tiếp cho phân tích nghiệp vụ và học máy. Nếu lớp này sai lệch, toàn bộ kết quả ở các tầng tiêu thụ sẽ mất độ tin cậy. Vì vậy, nhiệm vụ đầu tiên là thiết lập cơ chế kiểm thử dữ liệu nghiêm ngặt bằng dbt Core trên Trino.

Mục tiêu kiểm thử tập trung vào bốn nhóm:

- Logic giá tài chính: High luôn lớn hơn hoặc bằng Low
- Tính liên tục chuỗi thời gian: không thiếu nến 1 phút trong cửa sổ kiểm tra
- Tính toàn vẹn định danh: không null mã tài sản
- Tính hợp lệ thời gian: không có bản ghi từ tương lai

Cách tiếp cận này phù hợp chuẩn kỹ thuật dữ liệu hiện đại vì kiểm thử được mã hóa thành SQL có thể lặp lại, có thể mở rộng và dễ kiểm toán.

### 1.2 Thiết kế bộ kiểm thử nghiêm ngặt

Hệ thống sử dụng hai lớp kiểm thử:

- Generic tests cho các ràng buộc phổ quát, ví dụ not null, accepted values
- Singular tests cho quy tắc nghiệp vụ đặc thù thị trường tài chính

Các thành phần chính trong mã nguồn:

- "`dbt/models/sources.yml`" — khai báo nguồn Gold cho dbt
- "`dbt/tests/generic/assert_high_gte_low.sql`" — kiểm tra logic High và Low
- "`dbt/tests/singular/test_no_missing_1min_candles.sql`" — kiểm tra thiếu mốc thời gian
- "`dbt/tests/singular/test_no_null_symbol.sql`" — kiểm tra định danh rỗng
- "`dbt/tests/singular/test_no_future_timestamps.sql`" — kiểm tra timestamp tương lai

Thiết kế này đảm bảo chất lượng dữ liệu được kiểm định ngay tại nơi dữ liệu được tiêu thụ, tức SQL engine Trino.

### 1.3 Quy trình setup và chạy dbt

Các bước triển khai:

"`python -m venv .venv-dbt`"

"`.venv-dbt\\Scripts\\activate`"

"`pip install dbt-core dbt-trino`"

"`cd dbt`"

"`dbt debug`"

"`dbt deps`"

"`dbt run`"

"`dbt test`"

Nếu cần chụp minh chứng thao tác, chụp terminal tại thời điểm kết thúc lệnh test.

### 1.4 Kết quả và nhận xét khoa học

Kết quả kiểm thử cuối cùng đạt trạng thái thành công toàn bộ:

"`Done. PASS=55 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=55`"

Một điểm kỹ thuật quan trọng là test kiểm tra nến 1 phút đã được hiệu chỉnh để tương thích kiểu timestamp của Trino. Điều này cho thấy kiểm thử dữ liệu không chỉ là thao tác chạy lệnh, mà là quá trình chuẩn hóa biểu diễn dữ liệu theo đúng ngữ nghĩa của từng engine truy vấn.

Nhận xét khoa học:

- Data Quality không thể tách rời kiến trúc Lakehouse
- Kiểm thử tự động giúp giảm rủi ro lan truyền lỗi sang BI và ML
- Tập test nghiệp vụ tài chính có giá trị thực tiễn cao hơn các kiểm tra hình thức

---

## 2. Task 2 — Xây dựng 3 model ML đọc dữ liệu Gold từ Trino

### 2.1 Tổ chức thư mục và mục tiêu mô hình

Task 2 tập trung xây dựng cụm mô hình học máy cho dữ liệu crypto theo ba hướng dự báo bổ trợ nhau:

- XGBoost và LightGBM — phân loại xu hướng tăng giảm
- LSTM — dự báo chuỗi giá
- Isolation Forest — phát hiện bất thường

Cấu trúc triển khai nằm trong thư mục `ML/`:

- "`ML/models/xgboost_lgbm.py`"
- "`ML/models/lstm_model.py`"
- "`ML/models/isolation_forest.py`"

Thiết kế này đáp ứng yêu cầu vừa dự báo xu hướng, vừa dự báo trị số, vừa giám sát rủi ro bất thường.

### 2.2 Luồng lấy dữ liệu, xử lý đặc trưng và huấn luyện

Dữ liệu huấn luyện được lấy từ Gold trên Trino trong `ML/data/fetch_data.py`.

Đoạn truy vấn cốt lõi:

"`SELECT * FROM gold_ohlcv WHERE symbol = '{symbol}' AND candle_duration = '5 minutes' ORDER BY candle_time ASC LIMIT {limit}`"

Đoạn kết nối Trino cốt lõi:

"`conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog=TRINO_CATALOG, schema=TRINO_SCHEMA)`"

Script huấn luyện tổng hợp:

"`python ML/train_all.py`"

Luồng xử lý:

Gold trên Trino -> fetch data -> feature engineering -> train 3 nhóm mô hình -> lưu artifacts.

### 2.3 Đánh giá kỹ thuật cho cụm mô hình

Cách tổ hợp nhiều mô hình trong cùng pipeline có ba lợi ích kỹ thuật:

- Tăng độ bao phủ hiện tượng thị trường do mỗi mô hình tối ưu cho một loại tín hiệu
- Giảm phụ thuộc vào một thuật toán đơn lẻ trong bối cảnh dữ liệu nhiễu cao
- Thuận lợi cho triển khai realtime vì mô hình đã được đóng gói artifact

Về mặt học thuật, cách tiếp cận này phù hợp với bài toán tài chính số có tính phi tuyến, biến động mạnh và cần cơ chế cảnh báo sớm.

---

## 3. Task 3 — Xây dựng Web Dashboard realtime bằng Flask, Chart.js, SSE

### 3.1 Tổ chức thư mục ML và luồng dữ liệu huấn luyện

Phần ML được xây dựng trong thư mục `ML/` với cấu trúc mô-đun rõ ràng:

- `ML/data` — lấy dữ liệu từ Trino và tạo đặc trưng
- `ML/models` — mã nguồn mô hình
- `ML/train_all.py` — huấn luyện tập trung
- `ML/app.py` — backend Flask và API realtime
- `ML/static`, `ML/templates` — giao diện hiển thị

Câu lệnh train tổng hợp:

"`python ML/train_all.py`"

Luồng dữ liệu huấn luyện đi theo chuỗi:

Gold trên Trino -> fetch_data -> feature_engineering -> train models -> lưu artifacts

### 3.2 Thiết kế Flask Dashboard realtime với SSE

Web UI local được xây dựng bằng Flask và Chart.js, cập nhật theo cơ chế SSE.

Lệnh chạy ứng dụng:

"`python ML/app.py`"

Các endpoint trọng tâm:

- "`/api/predictions`"
- "`/api/price-history`"
- "`/api/anomalies`"
- "`/stream`"

Tần suất cập nhật realtime:

- Chu kỳ SSE: 30 giây mỗi lần cập nhật
- Chu kỳ refresh Gold trong backend: xấp xỉ 5 phút

Minh chứng log backend:

"`[SSE] Updated: price=..., xgb=..., lstm=...`"

### 3.3 Live Crypto Ticker truy vấn `gold_ohlcv`

Dashboard hiển thị live ticker dựa trên dữ liệu truy vấn từ bảng `gold_ohlcv` thông qua backend Flask. Dữ liệu được render thành các panel dự báo, biểu đồ giá và trạng thái anomaly trong cùng một màn hình.

Minh chứng JSON API:

"`http://localhost:5000/api/predictions`"

### 3.4 Kết quả thực nghiệm và bình luận

Kết quả thực nghiệm cho thấy dashboard hiển thị đồng thời:

- Dữ liệu mới vừa cập nhật
- Tín hiệu dự báo từ ba cụm mô hình
- Trạng thái và cảnh báo anomaly

Bình luận khoa học:

- Mô hình kết hợp nhiều phương pháp giúp tăng độ bao phủ hiện tượng thị trường
- Kênh SSE phù hợp cho bài toán cập nhật liên tục với chi phí thấp
- Việc gắn inference trực tiếp lên dữ liệu Gold đã kiểm định giúp giảm rủi ro sai lệch đầu vào

---

## 4. Kết luận phần việc cá nhân

Trong phạm vi được giao, tôi đã hoàn thành đầy đủ ba trục công việc theo đúng thứ tự kỹ thuật:

1. Thiết lập lớp Data Quality bằng dbt Core trên Trino
2. Xây dựng cụm mô hình ML đọc dữ liệu Gold từ Trino
3. Xây dựng Web Dashboard realtime bằng Flask, Chart.js và SSE

Giá trị học thuật của phần việc nằm ở tính liên kết chặt giữa kiểm định dữ liệu, tiêu thụ dữ liệu và dự báo dữ liệu. Hệ thống không chỉ chạy được, mà còn có khả năng giải thích, kiểm chứng và tái lập.

---

## 5. Phụ lục hướng dẫn chụp minh chứng

### 5.1 Ảnh cho Task 1

- Chụp code test `assert_high_gte_low`
- Chụp code test `test_no_missing_1min_candles.sql`
- Chụp terminal dòng PASS tổng kết

### 5.2 Ảnh cho Task 2

- Chụp cấu trúc thư mục `ML/models/`
- Chụp code 3 model trong các file model
- Chụp code truy vấn Trino trong `ML/data/fetch_data.py`
- Chụp terminal chạy `python ML/train_all.py`

### 5.3 Ảnh cho Task 3

- Chụp cấu trúc thư mục `ML/`
- Chụp code khai báo model trong `ML/models/`
- Chụp terminal backend có dòng `[SSE] Updated...`
- Chụp response JSON `/api/predictions`
- Chụp dashboard tổng quan có tín hiệu dự báo và anomaly
