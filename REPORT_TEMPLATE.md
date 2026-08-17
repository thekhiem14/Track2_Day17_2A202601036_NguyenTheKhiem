# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Thế Khiêm  **Lớp:** E403  **Ngày:** 17/08/2004

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

*(dòng "dashboard rows scanned" / "số file parquet" trong output thật không đưa vào đây — đó là kiểm tra riêng cho Bài A của EXTRA.md, không thuộc 3 nhiệm vụ chính, xem Mục 4.)*

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Sau mỗi lần chạy lại `make pipeline` (không đổi dữ liệu nguồn), `gold_training_set` phình to thêm — quan sát được 13.790 → 26.270 → 38.750 hàng qua 3 lượt liên tiếp, dù chỉ có 12.480 ticket đang "sống". Không có lỗi nào được báo ra (đúng như phiếu #1041 mô tả). |
| **Nguyên nhân** | *(bạn xác nhận/chỉnh lại theo đúng lời của mình — gợi ý dựa trên phần mình đã trao đổi):* Model khai `materialized='incremental'` nhưng ban đầu thiếu `unique_key`, nên dbt sinh câu lệnh `INSERT` thuần — không có khoá để kiểm tra "dòng này đã tồn tại chưa". Vì nguồn CDC có bản ghi `op='u'` khiến cùng một ticket rơi vào nhiều partition ngày khác nhau trong 1 lượt chạy, và vì INSERT không ghi đè, mỗi lượt `make pipeline` (kể cả không đổi gì ở nguồn) lại chèn thêm một bản sao trạng thái mới nhất của toàn bộ 12.480 ticket đang sống. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` vào `config()`. `dags/ai_training_pipeline.py`: sửa `catchup=False`, thêm `max_active_runs=1` — chặn Airflow tự backfill hàng loạt và chặn nhiều lượt DAG ghi đồng thời vào cùng file DuckDB. |
| **Bằng chứng** | trước: 38.750 hàng, không ổn định (tăng dần mỗi lượt, checksum đổi) · sau: **12.480 hàng**, ổn định 3/3 lượt (checksum `8622572a97` giống hệt cả 3 lượt) · `gold_training_set: 1 hàng/1 ticket` ✓ không lặp · `DAG: catchup/max_active_runs` ✓ |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` thiếu 455/9.100 hàng (~5%) so với kỳ vọng — chỉ thiếu ở các cặp (ngày, khách hàng) thuộc những ngày đã chạy xong từ lâu, ngày mới nhất thì luôn đủ (đúng như phiếu #1043 mô tả). |
| **P99 độ trễ đo được** | **2,73 ngày** (đo trên `bronze_events`: P50 ≈ 0,13 ngày, P95 ≈ 1,81 ngày, P99 ≈ 2,73 ngày, max ≈ 2,94 ngày; tỷ lệ bản ghi trễ hơn 1 ngày ≈ 5,05% — khớp với "thiếu ~5%") |
| **Lookback đã chọn** | 3 ngày (làm tròn lên từ P99 = 2,73) |
| **Nguyên nhân** | *(xác nhận/chỉnh lại):* Điều kiện lọc `where event_date > (select max(event_date) from {{ this }})` chỉ nhận dữ liệu có `event_date` lớn hơn giá trị max đã có trong bảng đích. Một bản ghi trễ (event_date cũ, `_ingested_at` mới) sẽ không bao giờ thoả điều kiện này ở bất kỳ lượt chạy nào về sau, vì `max(event_date)` trong đích chỉ tăng theo thời gian trong khi `event_date` của bản ghi trễ là cố định — nên bản ghi bị loại **vĩnh viễn** ngay từ lượt đầu tiên nó xuất hiện, không phải do xử lý chậm rồi tự khắc phục sau. |
| **Cách khắc phục** | Nới điều kiện lọc thành `where event_date > (select max(event_date) from {{ this }}) - interval 3 day`, đồng thời thêm `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'` — vì grain gồm 2 cột và cùng một cặp (ngày, khách) giờ sẽ được tính lại ở nhiều lượt chạy khác nhau, cần ghi đè thay vì cộng dồn (nếu không sẽ tái tạo đúng lỗi Nhiệm vụ 1 trên bảng này). |
| **Bằng chứng** | trước: 8.645 hàng (thiếu 455) · sau: **9.100 hàng**, đúng kỳ vọng, ổn định 3/3 lượt (checksum `3db448685c` giống hệt cả 3 lượt) · `gold_training_set` vẫn giữ nguyên 12.480 — chứng minh sửa Nhiệm vụ 2 không làm hỏng Nhiệm vụ 1 |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> lookback = 3 ngày, chọn theo P99 (không theo max), vì max là outlier — chỉ một bản ghi bất thường cũng có thể kéo nó lên rất cao — còn P99 đại diện cho phân bố thật của phần lớn dữ liệu trễ. Chi phí: mỗi ngày lùi thêm không phải trả một lần, mà bị trả lại ở **mọi lượt chạy tương lai** (mỗi lượt đều phải quét và tính lại thêm N ngày dữ liệu cũ), nên chọn theo max sẽ tốn kém không cần thiết chỉ để vớt thêm rất ít bản ghi ở phần đuôi phân bố.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | `silver_tickets.priority` có 6.606 hàng sai (phần lớn là NULL bất thường), cộng thêm các giá trị ngoài miền hợp lệ (`0`, `5`, `-1`); model phân loại dự đoán kém hẳn kể từ 2026-08-10 — đúng thời điểm team backend đổi `priority` từ số sang nhãn chữ (phiếu #1047). |
| **Nguyên nhân** | *(xác nhận/chỉnh lại):* Macro `normalize_priority` dùng `try_cast(priority_raw as integer)` — biểu thức này sai theo 2 hướng cùng lúc: (1) biến toàn bộ nhãn chữ hợp lệ (`urgent/high/medium/low`) thành NULL vì không parse được sang số, dù chúng vẫn mang đúng ý nghĩa cũ; (2) lại chấp nhận `0`, `5`, `-1` vì chúng ép kiểu số thành công, dù nằm ngoài miền `1..4` mà contract quy định. Đồng thời `contract.enforced` đang tắt và test miền giá trị bị comment, nên không có gì chặn lại — dữ liệu sai lọt thẳng vào Gold mà `dbt test` vẫn báo pass. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | **Nhóm 1** — `1,2,3,4`: đúng contract cũ → giữ nguyên. **Nhóm 2** — `urgent,high,medium,low`: schema evolution (backend đổi cách biểu diễn, ý nghĩa không đổi) → map về `1,2,3,4` theo tài liệu API. **Nhóm 3** — `0,5,-1,'',NULL,P1,P2,unknown`: không có căn cứ xác nhận ý nghĩa (kể cả `P1`/`P2` trông giống nhưng không được tài liệu nào xác nhận) → quarantine. Tổng nhóm 3 = 312 bản ghi, khớp chính xác `expected/quarantine_tickets.count`. |
| **Cách khắc phục** | `dbt/macros/normalize_priority.sql`: viết lại thành `CASE` xử lý đủ 3 nhóm, trả `NULL` cho nhóm 3. `dbt/models/silver/silver_tickets.sql`: lọc bỏ bản ghi có priority NULL **trước khi** `row_number()` xếp hạng (không phải sau) — để ticket có bản ghi mới nhất hỏng vẫn giữ được trạng thái hợp lệ gần nhất, không biến mất khỏi Silver. `dbt/models/silver/quarantine_tickets.sql`: điều kiện `where normalize_priority(...) is null`. `dbt/models/silver/schema.yml`: bật `contract.enforced: true` + thêm test `accepted_values: [1,2,3,4]` cho cột `priority`. |
| **Bằng chứng** | `quarantine_tickets` = **312 / 312 hàng** ✓, ổn định 3/3 lượt · `dbt test`: **9 → 11/11 pass** (thêm 2 test mới ở cột `priority`) · `silver_tickets.priority ∈ 1..4, không NULL`: ✓ sạch (trước: 6.606 hàng sai) |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> Nên chặn ở **Silver**, không phải Bronze. Bronze cần giữ nguyên dữ liệu **thô, chưa chỉnh sửa** — đó là "hiện trường vụ án" để điều tra lại sau này (ví dụ muốn biết chính xác backend đã gửi giá trị gì trước khi bị chuẩn hoá). Nếu Bronze từ chối luôn bản ghi lỗi, bằng chứng gốc mất, việc debug về sau sẽ khó hơn nhiều. Silver là nơi áp business logic/contract, phù hợp hơn để quyết định hợp lệ hay không.
>
> Không để `dbt test` dừng cả DAG vì **quy mô**: chỉ 312 bản ghi lỗi so với hơn 12.000 ticket hợp lệ, 130.000+ event và 31.200 chunk đang chờ phục vụ người dùng thật (RAG, classifier, routing agent). Để một phần rất nhỏ dữ liệu lỗi chặn đứng toàn bộ hệ thống production là cái giá quá đắt — quarantine tách riêng để pipeline chạy tiếp, người trực xử lý riêng phần lỗi sau.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | A / B / không làm |
| **Nguyên nhân** | |
| **Cách khắc phục** | |
| **Bằng chứng** | |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Model incremental có khai `unique_key` và `incremental_strategy` phù hợp với grain không — thiếu 1 trong 2 thứ này, dbt sẽ mặc định `INSERT` thuần và âm thầm phình bảng mỗi lần chạy lại, không báo lỗi gì cả. |
| 2 | Điều kiện lọc incremental dựa trên "so với max đã xử lý" có tính đến dữ liệu đến muộn không (đo P99 độ trễ thực tế) — nếu không có lookback window, dữ liệu trễ bị loại vĩnh viễn ngay từ lượt đầu tiên nó xuất hiện, không phải lỗi tạm thời tự hết. |
| 3 | Contract/test miền giá trị có thực sự được bật không, và có nơi tiếp nhận riêng (quarantine) cho dữ liệu bất thường không — đồng thời phải phân biệt được "đổi format hợp lệ" (map lại) với "dữ liệu lỗi thật" (loại bỏ), vì xử lý nhầm 1 trong 2 đều gây hại. |
