# ĐẠI HỌC BÁCH KHOA HÀ NỘI
# TRƯỜNG CÔNG NGHỆ THÔNG TIN VÀ TRUYỀN THÔNG

## NHẬP MÔN KHOA HỌC DỮ LIỆU - IT4142

# ĐỀ TÀI: FPT Shop Laptop Advisor Chatbot



Hà Nội, tháng 6 năm 2026

---

## Tóm tắt

Thị trường laptop hiện nay có rất nhiều lựa chọn với thông số kỹ thuật phức tạp, khiến người dùng khó xác định sản phẩm phù hợp với nhu cầu thực tế. Các trang thương mại điện tử thường cung cấp bộ lọc theo thông số như giá, CPU, RAM hoặc dung lượng lưu trữ, nhưng chưa giải thích rõ vì sao một sản phẩm phù hợp với từng bối cảnh sử dụng như học tập, văn phòng, gaming, lập trình hoặc nhu cầu di chuyển.

Dự án này xây dựng **FPT Shop Laptop Advisor Chatbot**, một hệ thống tư vấn laptop tập trung vào một nhà bán lẻ duy nhất là FPT Shop. Hệ thống được thiết kế theo pipeline Data Science: thu thập dữ liệu sản phẩm từ trang FPT Shop, chuẩn hóa thông số kỹ thuật, xây dựng feature phục vụ xếp hạng, áp dụng bộ lọc cứng và scoring mềm để chọn Top-K laptop phù hợp, sau đó dùng LLM để diễn giải nhu cầu người dùng và tạo câu trả lời tư vấn tự nhiên.

Khác với recommender system dựa trên học máy hoặc lịch sử người dùng, hệ thống hiện tại ưu tiên tính minh bạch và khả năng giải thích. Logic matching, filtering và scoring được triển khai theo luật xác định, trong khi LLM chỉ đảm nhiệm vai trò trích xuất ý định và tạo phản hồi hội thoại. Điều này giúp hệ thống dễ kiểm soát, dễ benchmark và phù hợp với bài toán tư vấn bán lẻ nơi độ tin cậy của thông tin sản phẩm là yêu cầu quan trọng.

---

## Mục lục

1. [Giới thiệu](#1-giới-thiệu)  
2. [Thu thập dữ liệu](#2-thu-thập-dữ-liệu)  
   2.1 [Giới thiệu](#21-giới-thiệu)  
   2.2 [Pipeline thu thập dữ liệu tổng thể](#22-pipeline-thu-thập-dữ-liệu-tổng-thể)  
   2.3 [Giai đoạn 1: Thu thập đường dẫn sản phẩm](#23-giai-đoạn-1-thu-thập-đường-dẫn-sản-phẩm)  
   2.4 [Giai đoạn 2: Crawl trang sản phẩm và trích xuất giá](#24-giai-đoạn-2-crawl-trang-sản-phẩm-và-trích-xuất-giá)  
   2.5 [Giai đoạn 3: Parse và chuẩn hóa thông số sản phẩm](#25-giai-đoạn-3-parse-và-chuẩn-hóa-thông-số-sản-phẩm)  
   2.6 [Tổng kết](#26-tổng-kết)  
3. [Phân tích khám phá dữ liệu](#3-phân-tích-khám-phá-dữ-liệu)  
4. [Tiền xử lý dữ liệu](#4-tiền-xử-lý-dữ-liệu)  
5. [Xây dựng đặc trưng](#5-xây-dựng-đặc-trưng)  
6. [Chatbot tư vấn laptop dựa trên AI](#6-chatbot-tư-vấn-laptop-dựa-trên-ai)  
7. [Đánh giá](#7-đánh-giá)  
8. [Kết luận](#8-kết-luận)  
9. [Phân công thành viên](#9-phân-công-thành-viên)

---

# 1. Giới thiệu

Sự phát triển nhanh của thị trường laptop tạo ra nhiều lựa chọn cho người dùng, nhưng đồng thời làm tăng độ khó trong việc lựa chọn thiết bị phù hợp. Một khách hàng không chuyên thường gặp khó khăn khi phải so sánh CPU, GPU, RAM, SSD, màn hình, trọng lượng, pin và giá bán để đưa ra quyết định mua hàng.

Trong bối cảnh bán lẻ, bài toán không chỉ là tìm sản phẩm có cấu hình mạnh nhất, mà là tìm sản phẩm phù hợp nhất với nhu cầu, ngân sách và chính sách mua hàng. Với FPT Shop, các yếu tố như trả góp 0%, quà tặng, ưu đãi học sinh - sinh viên, tình trạng hàng và link mua hàng trực tiếp cũng là những tín hiệu quan trọng trong quá trình tư vấn.

Dự án hiện tại chuyển hướng từ một hệ thống tư vấn laptop tổng quát sang chatbot chuyên biệt cho FPT Shop. Hệ thống tập trung vào:

- Dữ liệu sản phẩm FPT-only.
- Matching Top-K dựa trên bộ lọc cứng và scoring mềm.
- Output tư vấn mượt qua API streaming.
- Giao diện chat có thẻ sản phẩm và link mua tại FPT Shop.
- Benchmark bằng các tiêu chí `Precision@K`, `NDCG@K`, `MRR` và `CSR`.

Phương pháp được lựa chọn là rule-based scoring kết hợp LLM. Phần scoring giữ tính xác định và dễ kiểm chứng; phần LLM hỗ trợ hiểu ngôn ngữ tự nhiên và diễn giải kết quả theo phong cách tư vấn viên FPT Shop.

---

# 2. Thu thập dữ liệu

## 2.1 Giới thiệu

Data crawling là bước đầu tiên trong pipeline xây dựng hệ thống tư vấn laptop. Chất lượng dữ liệu ảnh hưởng trực tiếp đến các bước phía sau như preprocessing, feature engineering, filtering, scoring và evaluation.

Trong project hiện tại, nguồn dữ liệu được giới hạn ở **FPT Shop** thay vì nhiều website bán lẻ. Việc tập trung vào một nhà bán lẻ giúp hệ thống:

- Phản ánh đúng kho hàng và giá bán của FPT Shop.
- Tránh so sánh sản phẩm giữa nhiều website có chính sách giá khác nhau.
- Dễ triển khai persona tư vấn viên FPT Shop.
- Dễ gắn CTA trực tiếp về trang sản phẩm FPT.
- Phù hợp với mục tiêu chatbot cho một công ty bán lẻ cụ thể.

Nguồn dữ liệu chính:

| Thành phần | Giá trị |
|---|---|
| Retailer | FPT Shop |
| Category URL | `https://fptshop.com.vn/may-tinh-xach-tay` |
| Raw JSON | `data/fpt_laptops.json` |
| Feature CSV | `data/fpt_laptops_features.csv` |
| Config | `config/shops/fpt.json` |

## 2.2 Pipeline thu thập dữ liệu tổng thể

Pipeline crawl hiện tại được tổ chức thành ba stage chính: thu thập link/SKU sản phẩm, tải trang chi tiết sản phẩm, và parse/chuẩn hóa thông số. Khác với bản thiết kế ban đầu dựa trên giao diện "xem thêm", pipeline hiện tại ưu tiên FPT category API để lấy đầy đủ SKU; Selenium chỉ còn là fallback khi API không trả dữ liệu.

```mermaid
flowchart TD
    A["Bắt đầu thu thập"] --> B["Đọc cấu hình FPT"]
    B --> C["Giai đoạn 1: Lấy link/SKU<br/>(ưu tiên API)"]
    C --> D["Giai đoạn 2: Tải trang sản phẩm<br/>(HTML, thông số, giá)"]
    D --> E["Giai đoạn 3: Gộp dữ liệu API<br/>và chuẩn hóa thông số"]
    E --> F["Lưu dữ liệu thô FPT"]
    F --> G["Tạo bảng đặc trưng CSV"]
    G --> H["Kết thúc"]
```

**Hình 1:** Pipeline crawl dữ liệu FPT.

Pipeline gồm các bước:

1. Đọc cấu hình FPT trong `config/shops/fpt.json`.
2. Gọi FPT category API theo batch để lấy danh sách sản phẩm/SKU laptop.
3. Trích xuất slug SKU và cache metadata từ API như tên SKU, giá, giá gốc, ảnh, brand và key selling points.
4. Nếu API không trả dữ liệu, dùng Selenium để cuộn trang và bấm "xem thêm" như phương án fallback.
5. Tải từng trang chi tiết sản phẩm và parse giá, ảnh, specs.
6. Merge metadata từ API với JSON-LD/specs trên trang chi tiết.
7. Lưu raw JSON vào `data/fpt_laptops.json`.
8. Build feature CSV bằng `src/build_dataset.py`.

Lệnh chạy sau này:

```bash
python3 src/run_shop.py --max-clicks 50 --out data/fpt_laptops.json
python3 src/build_dataset.py
```

Kết quả chạy ngày 2026-06-11:

| Artifact | Kết quả |
|---|---:|
| Số link/SKU sản phẩm thu được | 417 |
| Số sản phẩm raw hợp lệ | 417 |
| Số sản phẩm vào feature CSV | 417 |
| File output | `data/fpt_laptops_features.csv` |

## 2.3 Giai đoạn 1: Thu thập đường dẫn sản phẩm

Stage đầu tiên thu thập toàn bộ URL/SKU laptop từ trang danh mục FPT Shop. Dù giao diện người dùng có cơ chế cuộn và bấm "xem thêm", hệ thống hiện tại dùng API danh mục của FPT làm nguồn chính vì API trả được danh sách SKU ổn định hơn và tránh phụ thuộc vào trạng thái render của trình duyệt.

Quy trình:

```mermaid
flowchart TD
    A["Bắt đầu giai đoạn 1"] --> B["Gọi API danh mục FPT"]
    B --> C["Tham số: slug, skipCount,<br/>maxResultCount, categoryType"]
    C --> D{"API có dữ liệu?"}
    D -- Có --> E["Lấy product.skus[].slug"]
    E --> F["Lưu metadata SKU<br/>(tên, giá, hãng, ảnh, KSP)"]
    F --> G{"Còn batch tiếp?"}
    G -- Có --> C
    G -- Không --> H["Khử trùng lặp URL SKU"]
    D -- Không --> I["Dự phòng Selenium<br/>cuộn/bấm xem thêm"]
    I --> J["Tách link từ HTML đang hiển thị"]
    J --> H
    H --> K["Trả về URL sản phẩm FPT"]
```

**Hình 2:** Thu thập link/SKU laptop FPT.

Thông tin triển khai chính:

| Thành phần | Giá trị |
|---|---|
| Hàm API-first | `crawl_fpt_links_via_api()` |
| FPT API | `https://papi.fptshop.com.vn/gw/v1/public/fulltext-search-service/category` |
| Category slug | `may-tinh-xach-tay` |
| Batch size hiện tại | `24` SKU/lần gọi |
| Trường URL chính | `product.skus[].slug` |
| Metadata cache | `displayName`, `currentPrice`, `originalPrice`, `image`, `brand`, `keySellingPoints` |
| Fallback UI | `crawl_fpt_links()` bằng Selenium |

Selector fallback được lưu trong:

```json
{
  "shop": "fpt",
  "product_link_selector": "a[href*='/may-tinh-xach-tay/']",
  "load_more_selectors": [
    ".st-pd-btnShowmore",
    ".light-btn",
    ".btn-show-more",
    ".show-more",
    ".view-more"
  ]
}
```

Các URL không phải trang sản phẩm được loại bỏ bằng hàm `_is_fpt_product_url()`, ví dụ các slug danh mục như `asus`, `lenovo`, `gaming-do-hoa`, `apple-macbook`.

## 2.4 Giai đoạn 2: Crawl trang sản phẩm và trích xuất giá

Sau khi có danh sách URL, hệ thống tải từng trang chi tiết sản phẩm và trích xuất thông tin cần thiết.

### 2.4.1 Tải HTML của từng laptop

Module `src/dynamic_load_crawler.py` dùng `requests` để tải trang chi tiết sản phẩm theo URL đã thu được. Việc tải trang chi tiết tách khỏi bước thu thập link giúp pipeline nhẹ hơn; Selenium chỉ được dùng khi API danh mục không hoạt động hoặc cần debug giao diện.

Hàm chính:

```text
crawl_and_parse_fpt_products(urls, max_workers=5, save_html=False)
```

Chế độ `save_html=True` chỉ dùng khi cần debug selector, tránh giữ raw HTML nặng trong repo chính.

### 2.4.2 Trích xuất giá

Giá bán được trích xuất theo thứ tự ưu tiên:

1. JSON-LD hoặc structured data có `"priceCurrency": "VND"`.
2. Meta tag như `meta[itemprop='price']`.
3. CSS selector giá phổ biến trên FPT.
4. Lọc token số nằm trong khoảng giá laptop hợp lý.

Giá được chuẩn hóa về số nguyên VND. Ví dụ:

```text
"21.790.000đ" -> 21790000
```

Kết quả hiện tại trong CSV:

| Chỉ số giá | Giá trị hiện tại |
|---|---:|
| Min price | 6,890,000 VND |
| Median price | 30,690,000 VND |
| Max price | 199,490,000 VND |

### 2.4.3 Crawl đa luồng với ThreadPoolExecutor

Việc tải trang chi tiết sản phẩm được thực hiện song song bằng `ThreadPoolExecutor`. Mục tiêu là giảm thời gian crawl nhưng vẫn giữ pipeline đơn giản.

Thông số hiện tại:

| Thành phần | Giá trị |
|---|---:|
| Default workers | 5 |
| Request timeout | 20 giây |
| Output raw JSON | `data/fpt_laptops.json` |

Kết quả chạy chính thức ngày 2026-06-11: crawler thu được 417 sản phẩm/SKU hợp lệ, không thiếu URL, tên, giá hoặc ảnh.

## 2.5 Giai đoạn 3: Parse và chuẩn hóa thông số sản phẩm

### 2.5.1 Chiến lược parse

Trang sản phẩm FPT có thể trình bày specs trong nhiều dạng HTML khác nhau. Parser hiện dùng chiến lược nhiều tầng theo thứ tự ưu tiên:

1. Metadata đã cache từ FPT category API.
2. JSON-LD trên trang chi tiết, đặc biệt là `additionalProperty`.
3. Các container HTML có class/id liên quan đến specs như:

   - `spec`
   - `thong-so`
   - `parameter`
   - `config`
   - `product-info-table`
   - `product-specs`

Nếu không tìm thấy container rõ ràng, parser quét bảng HTML và danh sách `li` có cấu trúc `key: value`.

### 2.5.2 Thuộc tính được parse và logic riêng cho FPT

Các thuộc tính chuẩn hóa hiện tại:

| Nhóm | Thuộc tính |
|---|---|
| Product | `Product Name`, `Manufacturer`, `url`, `image`, `source` |
| CPU | `CPU manufacturer`, `CPU brand modifier`, `CPU generation`, `CPU Speed (GHz)` |
| Memory | `RAM (GB)`, `RAM Type`, `Bus (MHz)` |
| Storage | `Storage (GB)` |
| Display | `Screen Size (inch)`, `Screen Resolution`, `Refresh Rate (Hz)` |
| Graphics | `GPU manufacturer` |
| Portability | `Weight (kg)`, `Battery`, `Battery (Wh)` |
| Retail | `Price (VND)`, `Original Price (VND)`, `Is Installment 0%`, `Student Discount (VND)`, `Gifts`, `Stock Status` |

Vì dữ liệu FPT hiện có một số trang thiếu bảng specs đầy đủ, hệ thống có thêm inference từ tên sản phẩm, đặc biệt cho:

- Manufacturer
- RAM
- Storage
- GPU/gaming signal
- Installment 0%

Riêng với FPT, metadata từ API category giúp ổn định các trường retail và nhận diện SKU:

| Nguồn tín hiệu | Mục đích |
|---|---|
| `sku.displayName` | Tên SKU đầy đủ để infer RAM/Storage/CPU/GPU |
| `currentPrice`, `originalPrice` | Giá bán và giá gốc |
| `brand` | Manufacturer fallback |
| `image` | Ảnh sản phẩm |
| `keySellingPoints` | Bổ sung thông số nổi bật khi trang chi tiết thiếu specs |

### 2.5.3 Chuẩn hóa và làm sạch dữ liệu

Chuẩn hóa bao gồm:

- Chuyển giá, RAM, SSD, màn hình, refresh rate, weight sang numeric.
- Chuẩn hóa pin Wh nếu có.
- Gán `Stock Status = In Stock` nếu nguồn dữ liệu chưa có trạng thái rõ ràng.
- Bổ sung score chuẩn hóa (`norm_ram`, `norm_storage`, `norm_price`, `norm_weight`, `norm_screen`, `norm_battery`).
- Loại bỏ dòng không có tên hoặc không có giá.

### 2.5.4 Định dạng đầu ra

Output chính:

```text
data/fpt_laptops_features.csv
```

Hiện trạng sau khi chạy lại pipeline ngày 2026-06-11:

| Chỉ số | Giá trị hiện tại |
|---|---:|
| Số dòng | 417 |
| Số cột | 50 |
| Số brand | 11 |
| Price fill rate | 100.0% |
| RAM fill rate | 100.0% |
| Storage fill rate | 99.8% |
| Screen size fill rate | 100.0% |
| GPU manufacturer fill rate | 99.0% |
| CPU manufacturer fill rate | 86.1% |
| Weight fill rate | 0.0% |
| Battery fill rate | 0.0% |
| Stock status fill rate | 100.0% |

## 2.6 Tổng kết

Pipeline crawl hiện tại đã được thu gọn thành FPT-only. Project chính không còn crawler hoặc config cho các shop khác. Dữ liệu giữ lại phục vụ trực tiếp cho bài toán hiện tại:

- `data/fpt_laptops.json`
- `data/fpt_laptops_features.csv`
- `data/fpt_test_queries.json`
- `data/fpt_evaluation_results.json`
- `data/fpt_metrics_summary.json`

Sau khi chạy lại crawler, dataset đã tăng từ 162 lên 417 sản phẩm/SKU hợp lệ. Các cột phục vụ matching chính như giá, RAM, Storage, Screen Size và GPU đã có fill rate cao; Weight và Battery vẫn cần cải thiện ở các lần phát triển sau.

---

# 3. Phân tích khám phá dữ liệu

## 3.1 Tổng quan dữ liệu

Dataset hiện tại gồm 417 sản phẩm/SKU hợp lệ từ FPT Shop sau khi build từ `data/fpt_laptops.json`. Mỗi sản phẩm có 50 cột sau khi bổ sung feature runtime/scoring.

Code tạo biểu đồ EDA:

```bash
python3 src/eda/visualize_fpt.py --csv data/fpt_laptops_features.csv --out-dir reports/figures
```

**Hình 8:** Tổng quan dataset.  
Trạng thái: đã cập nhật số liệu; chưa tạo hình.

Checklist biểu đồ:

| Hình | Nội dung | Dữ liệu | Biểu đồ |
|---|---|---|---|
| Hình 8 | Tổng quan dataset | Đã có | Chưa tạo |
| Hình 9 | Phân phối giá | Đã có | Chưa tạo |
| Hình 10 | Phân khúc giá | Cần tính | Chưa tạo |
| Hình 11 | Số mẫu theo hãng | Đã có | Chưa tạo |
| Hình 12 | Cấu hình phổ biến | Cần tổng hợp | Chưa tạo |
| Hình 13 | Giá theo RAM | Đã có | Chưa tạo |
| Hình 14 | Nhóm GPU | Đã có | Chưa tạo |
| Hình 15 | Model GPU | Chưa có cột | Chưa áp dụng |
| Hình 16 | Giá theo GPU | Đã có mức hãng | Chưa tạo |

Key observations hiện tại:

- Dataset đã được thu gọn về FPT-only.
- Giá bán có fill rate 100%.
- RAM có fill rate 100.0%, Storage 99.8%, Screen Size 100.0% và GPU manufacturer 99.0%, đủ tốt cho matching Top-K theo cấu hình.
- CPU manufacturer đạt 86.1%; phần thiếu chủ yếu rơi vào các model có tên CPU chưa thể map chắc chắn.
- Weight và Battery hiện chưa có trong dataset, nên các scoring liên quan portability/battery dùng fallback trung lập.

## 3.2 Phân tích đơn biến

### 3.2.1 Phân phối giá và phân khúc ngân sách

**Hình 9:** Phân phối giá laptop.  
Trạng thái: đã có số liệu; chưa tạo biểu đồ.

Thống kê giá hiện tại:

| Chỉ số | Giá trị |
|---|---:|
| Thấp nhất | 6,890,000 VND |
| Trung vị | 30,690,000 VND |
| Cao nhất | 199,490,000 VND |

Phân khúc đề xuất để tính khi chạy EDA:

| Phân khúc | Khoảng giá |
|---|---|
| Giá rẻ | `< 15M` |
| Tầm trung | `15M - 25M` |
| Cận cao cấp | `25M - 40M` |
| Cao cấp | `> 40M` |

**Hình 10:** Số laptop theo phân khúc giá.  
Trạng thái: cần chạy EDA để tạo.

### 3.2.2 Thị phần nhà sản xuất

Phân bố hãng hiện tại:

| Hãng | Số mẫu |
|---|---:|
| Apple | 105 |
| Asus | 84 |
| Acer | 52 |
| HP | 50 |
| Lenovo | 38 |
| Dell | 35 |
| MSI | 33 |
| Gigabyte | 11 |
| Colorful | 7 |
| LG | 1 |
| Masstel | 1 |

**Hình 11:** Số mẫu theo hãng.  
Trạng thái: đã có bảng; chưa tạo biểu đồ.

## 3.3 Xu hướng phần cứng

### 3.3.1 Cấu hình RAM sau chuẩn hóa

Phân bố RAM hiện tại:

| RAM | Số mẫu |
|---:|---:|
| 4GB | 1 |
| 8GB | 20 |
| 12GB | 1 |
| 16GB | 259 |
| 24GB | 47 |
| 32GB | 69 |
| 36GB | 5 |
| 48GB | 8 |
| 64GB | 6 |
| 128GB | 1 |

Phân bố ổ cứng hiện tại:

| Storage | Số mẫu |
|---:|---:|
| 128GB | 1 |
| 256GB | 6 |
| 512GB | 279 |
| 1TB | 91 |
| 2TB | 27 |
| 3TB | 1 |
| 4TB | 9 |
| 6TB | 1 |
| 8TB | 1 |
| Thiếu | 1 |

**Hình 12:** Top 10 cấu hình phổ biến.  
Trạng thái: cần chạy EDA để tổng hợp.

**Hình 13:** Giá theo dung lượng RAM.  
Trạng thái: đã có dữ liệu; chưa tạo biểu đồ.

### 3.3.2 Bức tranh GPU

Tỷ lệ nhận diện hãng GPU hiện tại là 99.0%. Dataset dùng metadata API, tên SKU và JSON-LD để suy luận GPU.

| Hãng GPU | Số mẫu |
|---|---:|
| Intel | 137 |
| NVIDIA | 112 |
| Apple | 110 |
| AMD | 50 |
| Qualcomm | 4 |
| Thiếu | 4 |

**Hình 14:** Nhóm laptop theo GPU.  
Trạng thái: đã có dữ liệu; chưa tạo biểu đồ.

**Hình 15:** Top 10 model GPU.  
Trạng thái: chưa áp dụng vì chưa có cột `gpu_model`.

**Hình 16:** Giá theo nhóm GPU.  
Trạng thái: có thể tạo theo hãng GPU; muốn chi tiết hơn cần thêm `gpu_model`.

## 3.4 Phân tích tính di động và đặc điểm vật lý

Weight hiện chưa có trong dataset FPT hiện tại. Do đó, phân tích portability chưa thể kết luận chính thức.

Khi chạy lại crawler và parser, cần ưu tiên trích xuất:

- `Weight (kg)`
- `Battery`
- `Battery (Wh)`
- Kích thước màn hình

## 3.5 Phân tích đánh đổi tính di động

**Hình 17:** Màn hình và trọng lượng.  
Trạng thái: chờ dữ liệu cân nặng.

Khi có dữ liệu weight đầy đủ, phân tích sẽ tập trung vào:

- Nhóm 13-14 inch mỏng nhẹ.
- Nhóm 15.6-16 inch phổ thông/gaming.
- Quan hệ giữa trọng lượng, giá và scoring hiệu năng.

### 3.5.1 "Chi phí trọng lượng" của hiệu năng

**Hình 18:** Cân nặng và giá của laptop hiệu năng cao.  
Trạng thái: chờ dữ liệu cân nặng và GPU chi tiết hơn.

Dự kiến insight cần kiểm chứng: laptop gaming hoặc workstation thường có trọng lượng cao hơn do yêu cầu tản nhiệt, trong khi ultrabook và MacBook tập trung vào tính di động.

---

# 4. Tiền xử lý dữ liệu

Data preprocessing chuyển raw FPT data thành format phù hợp cho recommendation engine.

## 4.1 Làm sạch dữ liệu và điền khuyết

Các bước đã triển khai:

- Loại sản phẩm không có tên.
- Loại sản phẩm không có giá bán.
- Deduplicate theo URL.
- Convert numeric columns bằng `pd.to_numeric`.
- Fill `Stock Status = In Stock` nếu thiếu.
- Fill retail fields mặc định: `Student Discount = 0`, `Gifts = ""`.
- Infer RAM/Storage/Manufacturer từ tên sản phẩm khi specs thiếu.

Các cột còn thiếu nhiều và cần cải thiện parser:

| Cột | Fill rate hiện tại | Ghi chú |
|---|---:|---|
| CPU manufacturer | 86.1% | Có thể cải thiện thêm với mapping CPU mới |
| GPU manufacturer | 99.0% | Đủ tốt cho scoring theo GPU manufacturer |
| Screen Size | 100.0% | Đã infer tốt từ SKU/specs |
| Weight | 0.0% | Cần selector/spec parser bổ sung |
| Battery | 0.0% | Cần selector/spec parser bổ sung |

## 4.2 Phân loại GPU

### 4.2.1 Trích xuất và chuẩn hóa

Hiện tại GPU được chuẩn hóa ở mức manufacturer:

- NVIDIA
- AMD
- Intel
- Apple

Ngoài ra, hệ thống dùng tín hiệu tên dòng máy để hỗ trợ gaming score trong trường hợp GPU thiếu:

- `tuf`
- `rog`
- `nitro`
- `loq`
- `legion`
- `victus`
- `predator`
- `gaming`

### 4.2.2 Logic phân loại model

Logic scoring GPU hiện tại nằm trong `src/advisor/features.py`:

| Tín hiệu | Điểm gần đúng |
|---|---:|
| RTX 40/50 | 1.00 |
| RTX 30 | 0.88 |
| RTX 20/GTX | 0.72 |
| NVIDIA/GeForce | 0.66 |
| Gaming line name | 0.64 |
| Radeon RX | 0.70 |
| Intel Iris/Arc | 0.55 |
| Integrated/common GPU | 0.48 |

Mục tiêu là giữ recommendation không bị rỗng khi specs chưa đầy đủ, nhưng vẫn ưu tiên dòng gaming khi user hỏi gaming.

## 4.3 Chuẩn hóa và scoring

Các feature numeric được chuẩn hóa bằng robust quantile scaling:

```text
norm_x = clip((x - q05) / (q95 - q05), 0, 1)
```

Các cột normalize chính:

- `norm_ram`
- `norm_storage`
- `norm_price`
- `norm_weight`
- `norm_screen`
- `norm_battery`
- `norm_cpu`

Với trường thiếu dữ liệu, hệ thống dùng fallback trung lập để scoring không bị lỗi.

---

# 5. Xây dựng đặc trưng

## 5.1 Tổng quan

Feature engineering biến thông số kỹ thuật thành các tín hiệu phục vụ recommendation. Mục tiêu không phải huấn luyện model, mà tạo ra các score dễ giải thích cho từng nhu cầu.

Các nhóm feature chính:

- Base performance score
- GPU score
- Task-oriented scores
- Retail preference bonuses
- Binary tags for explainability

## 5.2 Điểm cơ sở

Base performance score tổng hợp CPU, GPU, RAM và Storage:

```text
base_performance_score =
    norm_cpu * 0.45
  + gpu_score * 0.30
  + norm_ram * 0.15
  + norm_storage * 0.10
```

Score này đóng vai trò nền cho gaming, AI/graphics và general-purpose ranking.

## 5.3 Điểm hiệu năng GPU

GPU score được xác định bằng luật dựa trên tên GPU hoặc tên sản phẩm. Với dataset hiện tại, gaming-line signals đóng vai trò quan trọng vì nhiều sản phẩm FPT chưa có GPU specs đầy đủ.

Ví dụ:

```text
Asus TUF / ROG / Acer Nitro / Lenovo LOQ / HP Victus -> gaming signal
```

## 5.4 Điểm tổng hợp theo tác vụ

Các score theo mục đích sử dụng được xây dựng để mapping từ nhu cầu tự nhiên sang ranking kỹ thuật.

### 5.4.1 Điểm gaming

```text
gaming_score =
    gpu_score * 0.60
  + base_performance_score * 0.25
  + norm_ram * 0.15
```

Gaming ưu tiên GPU và hiệu năng tổng thể.

### 5.4.2 Điểm AI và đồ họa

```text
ai_graphics_score =
    gpu_score * 0.55
  + base_performance_score * 0.30
  + norm_ram * 0.15
```

Score này dùng cho query liên quan AI, graphics, rendering hoặc lập trình cần cấu hình mạnh.

### 5.4.3 Điểm văn phòng và doanh nghiệp

```text
office_score =
    base_performance_score * 0.45
  + norm_weight * 0.35
  + battery_score * 0.20
```

Vì weight/battery hiện còn thiếu, score này đang phụ thuộc nhiều vào performance fallback. Sau khi parser thu được weight/battery, score sẽ phản ánh tốt hơn nhu cầu văn phòng/di chuyển.

### 5.4.4 Điểm tính di động

```text
portability_score =
    norm_weight * 0.45
  + battery_score * 0.35
  + (1 - norm_screen) * 0.20
```

Portability score cần được cập nhật lại sau khi crawler trích xuất được weight/battery.

### 5.4.5 Điểm sử dụng tổng quát

```text
general_score =
    office_score * 0.45
  + base_performance_score * 0.35
  + portability_score * 0.20
```

Score này dùng cho query không có nhu cầu rõ ràng hoặc nhu cầu phổ thông.

## 5.5 Nhãn nhị phân phục vụ giải thích

Các binary tags phục vụ giải thích và UI:

| Tag | Ý nghĩa |
|---|---|
| `is_gaming_ready` | Có tín hiệu phù hợp gaming |
| `is_ai_ready` | Phù hợp AI/graphics |
| `is_business_ready` | Phù hợp văn phòng/doanh nhân |
| `is_ultrabook` | Mỏng nhẹ/pin tốt theo feature hiện có |
| `is_light` | Trọng lượng <= 1.7kg nếu có dữ liệu |
| `is_small_screen` | Màn hình nhỏ gọn |
| `is_large_screen` | Màn hình lớn |

## 5.6 Bộ dữ liệu đặc trưng đầu ra

Output feature dataset hiện tại:

```text
data/fpt_laptops_features.csv
```

Thông tin hiện tại:

| Chỉ số | Giá trị |
|---|---:|
| Rows | 417 |
| Columns | 50 |
| Main source | FPT Shop |
| Recommendation input | Có |
| Benchmark input | Có |

---

# 6. Chatbot tư vấn laptop dựa trên AI

## 6.1 Mục tiêu của tính năng

Mục tiêu của chatbot là cho phép người dùng nhập nhu cầu bằng ngôn ngữ tự nhiên và nhận lại danh sách Top-K laptop FPT phù hợp, kèm giải thích dễ hiểu và link mua hàng.

Ví dụ nhu cầu:

```text
"Mình là sinh viên cần laptop văn phòng nhẹ dưới 20 triệu, trả góp 0% càng tốt"
```

Hệ thống cần trả về:

- Intent đã trích xuất.
- Query object cho recommendation engine.
- Top-K laptop phù hợp.
- Câu trả lời tư vấn tự nhiên.
- Product cards trên frontend.

## 6.2 Tổng quan tính năng

Luồng chatbot:

1. User nhập message.
2. API trích xuất intent bằng Gemini nếu có API key, fallback rule-based nếu không.
3. Intent được patch bằng regex/rule để bắt ngân sách, RAM, SSD, brand, trả góp, quà tặng.
4. Recommendation engine lọc và chấm điểm.
5. API trả metadata sản phẩm trước, sau đó stream câu trả lời.
6. Frontend render product cards và text streaming.

## 6.3 Kiến trúc hệ thống và công nghệ

```mermaid
flowchart TD
    U["Người dùng"] --> FE["Giao diện HTML/JS"]
    FE --> API["FastAPI /chat hoặc /chat/stream"]
    API --> INTENT["Trích xuất ý định<br/>Gemini + luật bổ sung"]
    INTENT --> REC["Bộ máy gợi ý"]
    REC --> CSV["data/fpt_laptops_features.csv"]
    REC --> API
    API --> LLM["Sinh câu tư vấn<br/>Gemini hoặc fallback"]
    LLM --> FE
```

**Hình 19:** Kiến trúc chatbot tư vấn laptop FPT.

### 6.3.1 Giao diện người dùng (Frontend)

Frontend nằm trong:

```text
frontend/index.html
```

Chức năng chính:

- Chat input.
- Gọi `/chat/stream`.
- Đọc streaming JSON-lines.
- Render bot answer theo từng chunk.
- Render product cards.
- Modal xem thông số và link mua FPT.

### 6.3.2 Dịch vụ backend

Backend nằm trong:

```text
src/api/main.py
```

Endpoints:

| Endpoint | Vai trò |
|---|---|
| `GET /health` | Kiểm tra trạng thái API và dataset |
| `POST /chat` | Trả full JSON response |
| `POST /chat/stream` | Trả metadata + text streaming |

Dataset mặc định:

```text
data/fpt_laptops_features.csv
```

### 6.3.3 Thành phần trí tuệ nhân tạo

LLM được dùng cho:

- Structured intent extraction.
- Advice generation.

Nếu không có `GEMINI_API_KEY` hoặc SDK chưa sẵn sàng, hệ thống vẫn chạy bằng fallback rule-based:

- Intent mặc định `general`, sau đó patch từ text.
- Advice fallback bằng template tiếng Việt.

## 6.4 Quy trình hoạt động của chatbot

```mermaid
sequenceDiagram
    participant U as Người dùng
    participant FE as Giao diện
    participant API
    participant REC as Bộ gợi ý
    participant LLM

    U->>FE: Nhập nhu cầu
    FE->>API: POST /chat/stream
    API->>API: Trích xuất và vá intent
    API->>REC: build_query_from_intent()
    REC->>REC: Lọc ứng viên
    REC->>REC: Chấm điểm
    REC-->>API: Top-K sản phẩm
    API-->>FE: Metadata JSON-line
    API->>LLM: generate_advice_stream()
    LLM-->>API: Luồng nội dung
    API-->>FE: Text JSON-lines
```

## 6.5 Thiết kế recommendation engine

### 6.5.1 Module lọc (Constraint-Based Filtering)

Filter module nằm trong:

```text
src/advisor/filters.py
```

Hard filters:

- `price_min`
- `price_max`
- `ram_exact_gb`
- `min_ram_gb`
- `min_storage_gb`
- `min_weight_kg`
- `max_weight_kg`
- Brand exclude
- CPU requirements
- Display requirements
- Battery requirements
- Stock status

Gaming không còn bị hard-filter tuyệt đối để tránh trả rỗng khi dữ liệu GPU thiếu; thay vào đó gaming được ưu tiên trong scoring.

### 6.5.2 Module scoring (Multi-Criteria Ranking)

Scorer nằm trong:

```text
src/advisor/scorer.py
```

Scorer kết hợp:

- Task score theo intent.
- Affordability score.
- Weight score.
- Brand preference bonus.
- Retail bonuses:
  - `pref_installment`
  - `is_student`
  - `need_gifts`
- Battery/display/ready flag bonuses.

Final score:

```text
final_score = weighted_task_affordability_weight + soft_bonuses
```

Sau đó kết quả được sort giảm dần theo `final_score`.

### 6.5.3 Module advisor (Explainability Layer)

Advisor layer gồm:

- `src/advisor/recommend_service.py`: chuyển dataframe Top-K thành JSON.
- `src/advisor/advisor.py`: gọi filter + scorer.
- `src/llm/prompts.py`: persona tư vấn viên FPT Shop.

Output recommendation JSON gồm:

- Tên máy.
- Brand.
- Giá bán.
- Giá gốc nếu có.
- RAM/Storage/Screen/CPU.
- Retail fields: trả góp, quà tặng, ưu đãi HSSV.
- Scores và flags.
- Link mua hàng.

---

# 7. Đánh giá

## 7.1 Bộ câu hỏi đánh giá

Benchmark hiện tại dùng:

```text
data/fpt_test_queries.json
```

Số query hiện tại: 10.

Các nhóm query:

- Sinh viên/văn phòng/ngân sách.
- Gaming dưới ngân sách.
- Brand preference.
- Laptop nhẹ.
- MacBook/pin tốt/doanh nhân.
- RAM/SSD constraints.
- AI/lập trình.
- Query phổ thông.

## 7.2 Chấm điểm độ phù hợp

Evaluator nằm trong:

```text
src/evaluation/evaluator.py
```

Hai chế độ judge:

| Judge | Mục đích |
|---|---|
| `rule` | Offline benchmark, deterministic, không cần API key |
| `gemini` | LLM relevance judge nếu có Gemini key |

Rule-based judge chấm điểm:

- `2`: phù hợp đầy đủ.
- `1`: phù hợp một phần hoặc là lựa chọn thay thế hợp lý.
- `0`: không phù hợp.

## 7.3 Chỉ số đánh giá

Các metric giống cấu trúc benchmark mẫu:

### Precision@K

```text
Precision@K = số item relevant trong Top-K / K
```

### NDCG@K

Đo chất lượng thứ tự ranking, ưu tiên item relevance cao ở vị trí đầu.

### MRR

Mean Reciprocal Rank đo vị trí của item relevant đầu tiên.

### CSR

Constraint Satisfaction Rate đo tỷ lệ recommendations thỏa tất cả hard constraints.

```text
CSR = số recommendation thỏa constraints / số recommendation
```

## 7.4 Kết quả

Kết quả benchmark hiện tại:

| Metric | Value |
|---|---:|
| Num queries | 10 |
| Top-K | 3 |
| Precision@K | 1.0000 |
| NDCG@K | 0.9766 |
| MRR | 1.0000 |
| CSR | 1.0000 |

Lệnh chạy:

```bash
python3 -m src.evaluation.run_evaluation \
  --file data/fpt_test_queries.json \
  --top-k 3 \
  --judge rule \
  --mode direct \
  --output data/fpt_evaluation_results.json \
  --metrics-output data/fpt_metrics_summary.json
```

Kết quả này là benchmark offline/rule-based trên dataset hiện tại. Sau khi crawl lại FPT, cần chạy lại benchmark và cập nhật bảng này.

## 7.5 Phân tích

Kết quả hiện tại cho thấy:

- Matching Top-K hoạt động ổn trên các query benchmark đã định nghĩa.
- CSR đạt 100% vì hard constraints được phân biệt rõ với soft preferences.
- Query gaming không bị trả rỗng nhờ chuyển gaming từ hard-filter sang scoring.
- Brand, RAM, Storage, khoảng giá và retail intent được patch bằng rule từ text.

Các hạn chế cần tiếp tục cải thiện:

- Weight và Battery chưa có trong dataset hiện tại.
- GPU extraction còn thiếu, phải dùng thêm tín hiệu tên dòng máy.
- Retail fields như `Gifts`, `Original Price`, `Student Discount` cần parse tốt hơn từ trang FPT.
- Gemini judge chưa chạy trong benchmark chính thức.

## 7.6 Thảo luận

Hướng đánh giá tiếp theo:

1. Chạy lại crawl FPT để lấy dữ liệu mới.
2. Build lại `fpt_laptops_features.csv`.
3. Kiểm tra fill rate sau crawl.
4. Chạy benchmark rule-based.
5. Nếu có Gemini key, chạy thêm Gemini relevance judge.
6. Cập nhật các bảng Section 2, 3 và 7.

Checklist cập nhật sau khi chạy:

| Bước | Command | Kết quả cần ghi |
|---|---|---|
| Crawl FPT | `python3 src/run_shop.py --max-clicks 50 --out data/fpt_laptops.json` | Số sản phẩm raw |
| Build CSV | `python3 src/build_dataset.py` | Rows, columns, fill rates |
| API smoke test | `POST /chat` | Số recommendations, sample output |
| Benchmark | `python3 -m src.evaluation.run_evaluation ...` | Precision@K, NDCG@K, MRR, CSR |

---

# 8. Kết luận

Dự án hiện tại đã chuyển từ hướng laptop advisor tổng quát sang chatbot tư vấn laptop chuyên biệt cho FPT Shop. Pipeline chính đã được thu gọn và tập trung vào dữ liệu FPT-only, với dataset chính là `data/fpt_laptops_features.csv`.

Hệ thống hiện có đầy đủ các thành phần cốt lõi:

- FPT crawler và parser.
- Feature extraction và preprocessing.
- Rule-based scoring engine.
- FastAPI backend với streaming.
- Frontend chat UI.
- LLM intent/advice integration có fallback.
- Evaluation pipeline với các metric IR và CSR.

Điểm mạnh của hệ thống là tính minh bạch: recommendation không phụ thuộc vào model học máy khó giải thích, mà dựa trên scoring rõ ràng và có thể kiểm thử. Điều này phù hợp với bài toán tư vấn bán lẻ, nơi hệ thống cần tránh bịa thông tin và chỉ tư vấn dựa trên dữ liệu sản phẩm có thật.

Các bước tiếp theo là chạy pipeline chính thức, cải thiện parser cho specs/retail fields, tạo biểu đồ EDA, và cập nhật báo cáo bằng kết quả thực nghiệm mới.

---

# 9. Phân công thành viên

| Thành viên | MSSV | Phân công | Trạng thái |
|---|---|---|---|
| Cập nhật sau | Cập nhật sau | Data crawling / preprocessing | TBD |
| Cập nhật sau | Cập nhật sau | Feature engineering / scoring | TBD |
| Cập nhật sau | Cập nhật sau | Backend / LLM / streaming | TBD |
| Cập nhật sau | Cập nhật sau | Frontend / evaluation / report | TBD |

---

## Nhật ký chạy

Phần này dùng để cập nhật kết quả sau mỗi lần chạy pipeline.

| Date | Step | Command | Result | Notes |
|---|---|---|---|---|
| 2026-06-11 | Initial report draft | N/A | Created report structure | Chờ chạy pipeline chính thức |
| 2026-06-11 | Crawl FPT laptop data | `python3 src/run_shop.py --max-clicks 50 --out data/fpt_laptops.json` | 417 raw products/SKUs | Không thiếu URL, tên, giá, ảnh |
| 2026-06-11 | Build feature dataset | `python3 src/build_dataset.py` | 417 rows, 50 columns | RAM 100.0%, Storage 99.8%, GPU 99.0% |
