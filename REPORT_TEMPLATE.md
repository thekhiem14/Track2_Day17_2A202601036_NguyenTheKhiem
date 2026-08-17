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
  quarantine_tickets    ✓ ok                   0         312   ✗ thiếu 312 hàng

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8622572a97    8622572a97    8622572a97   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    empty         empty         empty        ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 9/9 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✗ 6,606 hàng sai
  quarantine_tickets đúng số bản ghi lỗi      ✗ 0 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✗  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  3/4 tiêu chí đạt
```

*(Nhiệm vụ 3 làm xong sẽ chạy lại `make verify` và dán bản output 4/4 cuối cùng đè lên khối này.)*

</details>

Tổng kết: **3 / 4 tiêu chí đạt** *(sẽ cập nhật 4/4 sau khi xong Nhiệm vụ 3)*

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
| **Triệu chứng** | |
| **Nguyên nhân** | |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | |
| **Cách khắc phục** | |
| **Bằng chứng** | `quarantine_tickets` = … hàng · `dbt test` … pass |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> …

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
| 1 | |
| 2 | |
| 3 | |
