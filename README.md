# FPT Shop Laptop Advisor

Chatbot tư vấn laptop cho FPT Shop. Dự án tập trung vào bài toán nhận nhu cầu bằng tiếng Việt, lọc và xếp hạng Top-K laptop phù hợp từ dữ liệu FPT, sau đó trả lời theo dạng hội thoại kèm product cards.

## Mục tiêu

- Thu thập dữ liệu laptop từ FPT Shop.
- Chuẩn hóa thông số sản phẩm thành bảng đặc trưng.
- Phân tích dữ liệu, kiểm tra chất lượng dữ liệu và tạo biểu đồ báo cáo.
- Xây dựng recommendation engine theo hướng constraint filtering + multi-criteria scoring.
- Cung cấp API chatbot bằng FastAPI và giao diện web đơn giản.
- Đánh giá chất lượng gợi ý bằng benchmark offline.

## Cấu trúc chính

```text
config/shops/fpt.json              Cấu hình nguồn FPT
data/fpt_laptops.json              Dữ liệu raw sau crawl
data/fpt_laptops_features.csv      Dataset đặc trưng cho chatbot
data/fpt_test_queries.json         Bộ câu hỏi benchmark
frontend/index.html                Giao diện chatbot
reports/fpt_laptop_advisor_report.md
src/run_shop.py                    Crawl dữ liệu FPT
src/build_dataset.py               Build feature CSV
src/eda/visualize_fpt.py           Tạo biểu đồ EDA
src/preprocessing/audit_data_quality.py
src/api/main.py                    FastAPI chatbot API
src/advisor/                       Filter, scorer, advisor layer
src/llm/                           Gemini client, prompt, schema
src/evaluation/                    Benchmark/evaluator
tests/                             Unit tests
```

## Cài đặt

Yêu cầu Python 3.10+.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Nếu muốn dùng Gemini để trích xuất intent và sinh câu trả lời, tạo file `.env`:

```text
GEMINI_API_KEY=your_api_key_here
```

Nếu không có `GEMINI_API_KEY`, hệ thống vẫn chạy bằng rule-based fallback.

## Chạy nhanh với dữ liệu có sẵn

Project hiện đã có sẵn:

- `data/fpt_laptops.json`
- `data/fpt_laptops_features.csv`
- `data/fpt_test_queries.json`

Chạy API:

```bash
python3 -m uvicorn src.api.main:app --host 127.0.0.1 --port 8000
```

Kiểm tra API:

```bash
curl http://127.0.0.1:8000/health
```

Chạy frontend:

```bash
python3 -m http.server 8080 --bind 127.0.0.1 --directory frontend
```

Mở trình duyệt tại:

```text
http://127.0.0.1:8080
```

## Chạy bằng Docker

Yêu cầu Docker + Docker Compose. Build và chạy cả API lẫn frontend:

```bash
docker compose up -d --build
```

Sau khi chạy:

- API: `http://127.0.0.1:8000` (health: `http://127.0.0.1:8000/health`)
- Frontend: `http://127.0.0.1:8080`

Bật Gemini bằng cách tạo file `.env` ở thư mục gốc (Compose tự nạp):

```text
GEMINI_API_KEY=your_api_key_here
```

Không có key thì API vẫn chạy bằng rule-based fallback (`use_llm=false`).

Dừng:

```bash
docker compose down
```

Lưu ý: nếu cổng 8000 hoặc 8080 đang bị tiến trình khác chiếm trên máy, hãy tắt tiến trình đó trước (vd. một uvicorn đang chạy thủ công), nếu không trình duyệt có thể gọi nhầm server cũ.

## Chạy pipeline từ đầu

### 1. Crawl dữ liệu FPT

```bash
python3 src/run_shop.py --max-clicks 50 --out data/fpt_laptops.json
```

Crawler ưu tiên FPT category API để lấy SKU/link sản phẩm; Selenium được dùng như fallback cho luồng giao diện "xem thêm".

### 2. Build dataset đặc trưng

```bash
python3 src/build_dataset.py
```

Output chính:

```text
data/fpt_laptops_features.csv
```

### 3. Audit chất lượng dữ liệu

```bash
python3 src/preprocessing/audit_data_quality.py \
  --raw data/fpt_laptops.json \
  --csv data/fpt_laptops_features.csv \
  --out-dir reports/preprocessing \
  --fig-dir reports/figures
```

Output gồm file summary, bảng missing values, quality issues, retail availability và các hình phục vụ báo cáo.

### 4. Tạo biểu đồ EDA

```bash
python3 src/eda/visualize_fpt.py \
  --csv data/fpt_laptops_features.csv \
  --out-dir reports/figures
```

Output là các hình trong:

```text
reports/figures/
```

### 5. Chạy benchmark

```bash
python3 -m src.evaluation.run_evaluation --mode direct --judge rule --top-k 3
```

Output:

```text
data/fpt_evaluation_results.json
data/fpt_metrics_summary.json
```

Có thể benchmark qua HTTP API nếu server đang chạy:

```bash
python3 -m src.evaluation.run_evaluation --mode api --api-url http://127.0.0.1:8000/chat --judge rule --top-k 3
```

## Chạy test

```bash
python3 -m pytest tests/test_chatbot.py tests/test_features.py -q
```

## API chính

### `GET /health`

Trả trạng thái server, số dòng dataset đã load, chế độ LLM và đường dẫn dữ liệu.

### `POST /chat`

Request:

```json
{
  "text": "Gợi ý top 3 laptop gaming dưới 30 triệu"
}
```

Response gồm:

- `intent`: intent đã trích xuất.
- `query`: query có cấu trúc cho recommendation engine.
- `recommendations`: danh sách laptop Top-K.
- `answer`: câu tư vấn tiếng Việt.

### `POST /chat/stream`

Tương tự `/chat`, nhưng trả JSON-lines để frontend render metadata trước và stream text sau.

## Logic gợi ý

Luồng recommendation:

1. Trích xuất intent từ câu người dùng bằng Gemini hoặc rule fallback.
2. Patch thêm các tín hiệu tiếng Việt như ngân sách, RAM, SSD, hãng ưu tiên/loại trừ, màn hình, cân nặng, pin Wh, "giá hợp lý", trả góp và quà tặng.
3. Lọc hard constraints như giá, RAM, storage, brand exclude, CPU, display, battery và stock.
4. Chấm điểm theo intent bằng task score, affordability score, weight score và soft bonuses.
5. Sắp xếp theo `final_score`, khử trùng lặp theo tên sản phẩm, lấy Top-K.
6. Tạo lý do gợi ý và câu trả lời hội thoại.

## Kết quả hiện tại

Dataset hiện tại:

- 417 SKU laptop FPT.
- 53 cột đặc trưng.
- 364 tên sản phẩm duy nhất.

Benchmark rule-based hiện tại:

- 16 query.
- Top-K = 3.
- Strict Precision@K: 0.9375.
- Normalized Relevance@K: 0.9688.
- Full-Match Query Rate: 0.9375.
- CSR: 0.9375.

Các metric đạt 1.0000 như Precision@K, NDCG@K, MRR và Unique Name Rate được dùng như metric chẩn đoán, không phải kết luận hệ thống hoàn hảo.

## Hạn chế

- `Gifts`, `Original Price`, `Student Discount` và tồn kho thực tế chưa được populate đầy đủ.
- `Weight` và `Battery` vẫn có coverage thấp, nên các yêu cầu về tính di động/pin còn phụ thuộc nhiều vào dữ liệu hiện có.
- Benchmark chính đang dùng rule-based judge; có thể chạy thêm Gemini judge khi có API key.


