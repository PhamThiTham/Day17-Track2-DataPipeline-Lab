# Bonus Design: Flywheel cho Chatbot CSKH Thương Mại Điện Tử Việt Nam

## Bài toán & Ràng buộc

Một nền tảng thương mại điện tử cỡ vừa tại Việt Nam (200K đơn hàng/tháng, 15K hội thoại chat mỗi ngày) muốn xây dựng một **flywheel dữ liệu agent** cho chatbot chăm sóc khách hàng. Chatbot hiện tại chạy trên FAQ tĩnh kết hợp retrieval pipeline với tỷ lệ giải quyết ngay lần đầu ~72%. Mục tiêu: dùng trace sản xuất để sinh eval set và dữ liệu preference cho fine-tuning, cải thiện bot hàng tháng.

**Ràng buộc thực tế:**
- **Ngôn ngữ:** Tiếng Việt (dấu đầy đủ, biến thể vùng miền như "gadget" vs "đồ điện tử", pha trộn Anh-Việt)
- **Khối lượng dữ liệu:** ~500 hội thoại/ngày × 30 ngày = 15K traces/tháng, mỗi trace 3-8 spans
- **Tuân thủ:** PDPL (Luật 91/2025) yêu cầu đồng ý rõ ràng trước khi dùng dữ liệu hội thoại khách hàng để train model; người dùng có thể từ chối và yêu cầu xóa
- **Chi phí:** Không có đội ML infra riêng; một kỹ sư dữ liệu dùng chung cho nhiều sản phẩm
- **Hạ tầng hiện tại:** PostgreSQL order DB, Elasticsearch cho tìm kiếm sản phẩm, log chatbot dạng JSON Lines trên object storage tương thích S3

---

## 1. Nguồn & Hình dạng — Data Drift là Kẻ Giết Người Thầm Lặng

**Quyết định:** Schema-on-read với trường **contract version** trong mỗi trace, xác thực bằng Pandera (giống pattern lab §3).

**Lý do:** Trace chatbot tiến hóa khi đội sản phẩm thêm intent mới ("trả gadget" vs "hủy đơn"). Schema Avro/Protobuf cố định sẽ làm hỏng triển khai. Thay vào đó, mỗi trace mang `schema_version: "v2"` và tập trường `attributes.*` tùy chọn. Pandera xác thực *tối thiểu* bắt buộc; trường thừa được giữ nguyên nhưng đánh dấu.

**Đánh đổi:** Schema-on-read chuyển phát hiện lỗi từ compile-time sang runtime. Dashboard cảnh báo (`pipeline.monitor.quarantine_spike`) kích hoạt khi >5% trace hàng ngày fails validation. Phương án thay thế — Avro với compatibility modes — bị loại vì cần schema registry và thêm độ trễ vào trace ingestion (đội chatbot deploy 3 lần/tuần; registry updates không theo kịp).

**Phương án bị loại:** Lưu trace dưới dạng JSON thô trong MongoDB. Lý do: không có lớp typed, các consumer downstream (eval set builder, DPO miner) tự implement parsing khác nhau, gây mâu thuẫn thầm lặng. Pattern Bronze → typed Silver từ lab tỏ ra cần thiết ở đây.

---

## 2. Batch hay Streaming — Batch Hàng Đêm Thắng (Hiện Tại)

**Quyết định:** Batch hàng ngày. Trace đổ vào object storage mỗi giờ (micro-batch), nhưng flywheel đầy đủ (flatten → eval set → DPO pairs → PIT features) chạy một lần lúc 02:00.

**Lý do:** Chatbot không cần fine-tuning cùng ngày. Cửa sổ 24h cho phép người đánh giá gán nhãn các turn mơ hồ (xem §5) và để kiểm tra đồng ý PDPL: trace từ user đã từ chối trong 24h qua bị loại trước khi flywheel khởi động.

**Khi nào hỏng:** Ở 5× khối lượng (~75K hội thoại/ngày), micro-batch hàng giờ vẫn ổn, nhưng DAG flywheel có thể vượt quá SLA 4h. Giảm nhẹ: xử lý tăng dần với watermark (chỉ xử lý trace mới hơn lần chạy thành công cuối cùng).

**Phương án bị loại:** Streaming Redpanda/Kafka (như Docker bonus). Loại vì team không có chuyên môn vận hành Kafka; object storage + cron DAG có thể debug bởi một kỹ sư duy nhất. Độ trễ 24h chấp nhận được cho use case này.

---

## 3. Chất lượng & Hợp đồng — Ba Cổng Trước Khi Dữ Liệu Chạm Model

**Quyết định:** Ba cổng, mỗi cổng có quarantine riêng:

1. **Cổng Schema** (Pandera, giống lab): cấu trúc trace hợp lệ? Thiếu `span_id`, `parent_id`, `status`? → quarantine.
2. **Cổng Đồng ý** (custom): user có bản ghi đồng ý PDPL hợp lệ trong PostgreSQL tại thời điểm trace không? → loại bỏ, không quarantine (dữ liệu không nên tồn tại).
3. **Cổng Ngữ nghĩa** (LLM-as-a-judge, nhẹ): với error traces, `output` có thực sự sai không? Một số trace "error" chứa câu trả lời đúng nhưng bị gán nhãn sai do lỗi tool. Model rẻ (Gemini 2.0 Flash, $0.15/1M tok) chấm điểm; các điểm thấp vào hàng đợi đánh giá thủ công.

**Đánh đổi:** Cổng LLM-as-a-judge thêm độ trễ (~3s/trace) và chi phí (~$2.25/ngày cho 15K traces). Nhưng không có nó, chúng ta train trên cặp false-error (vd: câu trả lời đúng bị gán nhãn "ToolError"), đầu độc dataset DPO. Hàng đợi đánh giá thủ công là bottleneck: giới hạn throughput ~200 traces/ngày. Chấp nhận trong 3 tháng đầu và lên kế hoạch thay thế bằng classifier học từ các đánh giá đã tích lũy.

**Phương án bị loại:** Chỉ dùng cổng rule chính xác (schema + đồng ý). Điều này để lọt ~8% trace false-error (đo trong pilot), làm dataset DPO ~8% rác — đủ để giảm chất lượng fine-tune một cách thầm lặng.

---

## 4. Train/Serve Parity — Hai Rò Rỉ Cần Theo Dõi

**Quyết định:** Ba đòn bẩy cho tính chính xác point-in-time:

1. **Feature store với ngữ nghĩa `ASOF`** (DuckDB trong dev, lên kế hoạch migrate lên Redis+Postgres cho prod): features được vật chất hóa với timestamp `valid_from`/`valid_to`; joins dùng `ASOF` (lab §11).
2. **Logging feature tại thời điểm inference:** mỗi response chatbot ghi lại feature vector chính xác đã dùng. Trong training, chúng ta join trên `(user_id, event_id)` thay vì tính lại features — loại bỏ recency bias.
3. **Backtest với cutoff dates:** trước mỗi lần chạy training, mô phỏng "features trông thế nào tại thời điểm T?" cho 10 cutoff dates ngẫu nhiên. Nếu kết quả `ASOF` khác với logged features >1%, cảnh báo.

**Rò rỉ #1:** `user_total_orders` tính bằng `GROUP BY` không có bộ lọc timestamp. Sửa: đổi thành `SUM(orders) WHERE event_ts <= current_event_ts`.

**Rò rỉ #2:** Product embeddings cập nhật hàng ngày; hội thoại lúc 09:00 có thể dùng embedding hôm nay được tính lúc 02:00. Đây là *lệch thời gian* nhưng không phải rò rỉ (embedding có timestamp). Đã ghi nhận và chấp nhận.

**Phương án bị loại:** Tính lại tất cả features từ đầu cho mỗi training dataset (như backfill hoàn toàn xác định). Loại vì làm cho iterative feature engineering chậm không chấp nhận được (15K traces × 20 features × 10 iterations = 3M tính toán lại).

---

## 5. Decontamination — Tiếng Việt Thêm Phần Phức Tạp

**Quyết định:** Decontamination hai cấp: khớp chính xác (giống lab) + **fuzzy n-gram** (character 13-grams, Jaccard similarity ≥0.85).

**Lý do:** Prompt tiếng Việt thường được diễn đạt lại. "Có thể trả lại widget đã mua 10 ngày trước không?" và "Widget mua 10 ngày trước có trả lại được không?" không chứa từ nào giống hệt nhau nhưng đồng nghĩa. Decontamination khớp chính xác sẽ bỏ lỡ, âm thầm rò rỉ eval prompts vào training.

**Thách thức đặc thù tiếng Việt:** Phân đoạn từ tiếng Việt (vd: "trả_lại" vs "trả lại") tạo nhiễu trong n-gram matching. Giải pháp: chuẩn hóa bằng cách loại bỏ khoảng trắng trong các từ ghép đã biết dùng pyvi tokenizer trước khi xây dựng n-grams.

**Chi phí:** Character 13-gram decontamination trên 15K traces mất ~4 phút. Đánh đổi chấp nhận: có thể decontaminate quá mức (drop một prompt thực sự mới tình cờ chia sẻ n-grams với eval prompt). Giám sát: theo dõi "false positive rate" bằng cách cho người đánh giá nhãn mẫu ngẫu nhiên 1% số cặp bị drop hàng quý.

**Phương án bị loại:** Embedding-based decontamination (dùng lại `embed.py` từ lab). Loại vì embeddings nhạy với dữ liệu huấn luyện của embedding model; một model fine-tuned trên dữ liệu CSKH có thể nhúng các prompts tương tự gần nhau dù chúng hỏi khác nhau, gây over-decontamination.

---

## Sơ đồ Kiến trúc

```
                          ┌──────────────────────────┐
                          │  Log Chatbot Trace       │
                          │ (JSON Lines, S3)         │
                          └──────────┬───────────────┘
                                     │
                                     ▼
                          ┌──────────────────────────┐
                          │  Bronze (raw, typed)     │
                          │  DuckDB :memory:         │  ← cổng Schema
                          └──────────┬───────────────┘
                                     │
                   ┌─────────────────┼────────────────────┐
                   ▼                 ▼                    ▼
          ┌──────────────┐  ┌──────────────┐  ┌─────────────────┐
          │ Cổng Đồng ý  │  │ Flatten spans │  │ Cổng Ngữ nghĩa  │
          │ (PostgreSQL)  │  │ → Bronze_flat │  │ (LLM judge)     │
          └──────┬───────┘  └──────┬───────┘  └──────┬──────────┘
                 │                 │                  │
                 ▼                 ▼                  ▼
          ┌──────────────────────────────────────────────┐
          │  Silver: Clean, typed, deduped spans         │
          └──────┬───────────────────────────────────────┘
                 │
         ┌───────┴───────────┬─────────────────┐
         ▼                   ▼                  ▼
  ┌─────────────┐   ┌──────────────┐   ┌──────────────┐
  │ Eval Set    │   │ DPO Pairs    │   │ PIT Features  │
  │ (holdout)   │   │ (chosen/     │   │ (ASOF join)   │
  │             │   │  rejected)   │   │              │
  └──────┬──────┘   └──────┬───────┘   └──────┬───────┘
         │                 │                  │
         └─────────────────┼──────────────────┘
                           ▼
                  ┌──────────────────┐
                  │ Decontaminate    │
                  │ (exact + 13-gram)│
                  └───────┬──────────┘
                          ▼
                  ┌──────────────────┐
                  │ Train Dataset    │ → Day 22 SFT/DPO
                  └──────────────────┘
```

---

## Chi phí & Bối cảnh Việt Nam

**Ước tính chi phí hàng tháng tại 15K traces:**
- Object storage: ~$3 (200MB/tháng)
- Compute (EC2 t3.medium, reserved): ~$25
- LLM judge API: ~$70 (Gemini Flash)
- Đánh giá thủ công (bán thời gian): ~$200
- **Tổng: ~$300/tháng**

**80% chi phí:** LLM judge + đánh giá thủ công. Để cắt giảm, có thể thay LLM judge bằng PhoBERT classifier fine-tuned sau khi tích lũy 3 tháng dữ liệu đã đánh giá. Nhưng đây là bài toán bootstrap kinh điển — bạn cần judge để sinh dữ liệu thay thế judge.

**Quyết định đặc thù Việt Nam:**
- Tuân thủ PDPL yêu cầu cổng đồng ý *trước khi* dữ liệu chạm pipeline training, không phải sau. Điều này ngược với pattern US/EU nơi consent được kiểm tra tại serving time.
- Object storage là S3-compatible (Viettel Cloud / VNG Cloud) thay vì AWS S3 — cùng API, nhưng chi phí egress cao gấp 2 ($0.09/GB vs $0.05/GB). Chúng tôi giảm thiểu egress bằng cách chạy flywheel trong cùng region cloud.
- Người đánh giá thủ công phải nói tiếng Việt, thêm ràng buộc tuyển dụng. Chúng tôi dự trù nhà thầu bán thời gian thay vì nhân viên chính thức.