# SDI — Báo cáo tăng trưởng dữ liệu (1M / 1Q / 1Y)

Dự phóng số bản ghi & dung lượng theo thời gian, cho 3 kịch bản quy mô (small/medium/large) và 2 kiểu tăng trưởng khách hàng: **đều** (linear) và **nóng** (compounding).

> Liên quan: [SDI-eod-data-exchange.md](./SDI-eod-data-exchange.md) (volume mỗi feed/ngày), [SDI-db-architecture.md](./SDI-db-architecture.md). Đo perf thực: `db/bench.ps1`.

---

## 1. Bảng nào PHÌNH theo ngày, bảng nào CỐ ĐỊNH

**Mấu chốt:** chỉ bảng **append dated** mới tích luỹ theo thời gian. Bảng **overwrite** chỉ to theo *số KH*, KHÔNG theo ngày.

| Loại | Bảng | Cơ chế | Tăng theo |
|---|---|---|---|
| 🔴 **APPEND (phình/ngày)** | `T_CUSTOMER_HOLDING_DAILY` | +1 snapshot holdings/EOD | **ngày × KH** ← driver chính |
| 🔴 | `T_CUSTOMER_NAV_DAILY` | +1 dòng/tiểu khoản/EOD | ngày × KH |
| 🟠 (nhỏ) | `T_SI_NAV_DAILY`, `T_SI_INDEX_DAILY`, `T_SI_HOLDING_DAILY` | +SI (×mã)/EOD | ngày × SI |
| 🟠 (sparse) | `T_CASHFLOW_EVENT`, `T_CUSTOMER_FEE_INCOME`, `T_UNIT_LEDGER` | theo sự kiện | tần suất nạp/rút/cổ tức |
| 🟠 (nhỏ) | `T_PRICE_DAILY`, `T_BENCHMARK_DAILY` | +universe mã/EOD | ngày × universe (cố định) |
| 🟢 **FIXED (overwrite)** | `T_INDEXING_PORTFOLIO_TICKER` (current) | TRUNCATE+reload/EOD | **chỉ theo KH, KHÔNG theo ngày** |
| 🟢 | `T_CUSTOMER_NAV_CURRENT`, `T_SI_NAV_CURRENT` | overwrite | chỉ theo KH |
| 🟢 | master (`T_MASTER_PORTFOLIO*`, `T_INDEXING_PORTFOLIO`) | config | chỉ khi thêm SI/KH |
| ⚪ transient | `T_EOD_WORK` | xoá/ghi mỗi run | 1 ngày |

→ **Driver tăng trưởng = `T_CUSTOMER_HOLDING_DAILY`** (holdings/ngày). `T_CUSTOMER_NAV_DAILY` = holdings ÷ số mã (≈ +4–5% rows). Còn lại nhỏ. Các bảng 🟢 KHÔNG cộng dồn theo ngày — chúng to lên *một bậc* khi số KH tăng, rồi giữ nguyên.

---

## 2. Giả định

| Tham số | Giá trị |
|---|---|
| Phiên giao dịch | 1M = **21** · 1Q = **63** · 1Y = **252** |
| Kịch bản (KH × SI/KH × mã/SI) | small 1.000×3×20 · medium 10.000×5×25 · large 50.000×5×25 |
| Holdings/ngày tại base (H₀ = KH×SI×mã) | small **60K** · medium **1,25M** · large **6,25M** |
| Tăng **đều** (linear) | +50%/năm → ~+4,2%/tháng (của base). Hệ số TB kỳ: 1M≈1,02 · 1Q≈1,06 · 1Y≈1,25 |
| Tăng **nóng** (compounding) | ×4/năm → ~+12%/tháng. Hệ số TB kỳ: 1M≈1,06 · 1Q≈1,18 · 1Y≈2,05 |
| Bytes/row (raw) holdings | ~60 B; nén CCI prod **~3–5×** nhỏ hơn |

> **Cách tính tích luỹ:** rows ≈ H₀ × (số phiên) × (hệ số TB kỳ). Hệ số TB phản ánh KH vào giữa kỳ chỉ đóng góp số ngày còn lại. **Tốc độ tăng là THAM SỐ — chỉnh theo thực tế kinh doanh.** "flat" = không tăng KH (chỉ hiệu ứng thời gian).

---

## 3. `T_CUSTOMER_HOLDING_DAILY` — số bản ghi tích luỹ (triệu dòng)

| Kịch bản | Kỳ | flat (không tăng) | đều (+50%/y) | nóng (×4/y) |
|---|---|---:|---:|---:|
| **small** | 1M | 1,3 | 1,3 | 1,3 |
| | 1Q | 3,8 | 4,0 | 4,5 |
| | 1Y | 15,1 | 18,9 | 31,0 |
| **medium** | 1M | 26,3 | 26,8 | 27,8 |
| | 1Q | 78,8 | 83,5 | 93,0 |
| | 1Y | 315 | 394 | **646** |
| **large** | 1M | 131 | 134 | 139 |
| | 1Q | 394 | 419 | 469 |
| | 1Y | 1.575 | 1.969 | **3.228** |

*(Tổng bảng APPEND ≈ cột trên × ~1,05 do `customer_nav_daily` + SI-level nhỏ.)*

---

## 4. Dung lượng `T_CUSTOMER_HOLDING_DAILY` (raw / ~nén CCI)

> raw = rows × 60 B; nén ≈ raw ÷ 4 (ước lượng PAGE/CCI).

| Kịch bản | Kỳ | flat | đều | nóng |
|---|---|---:|---:|---:|
| **medium** | 1Q | ~4,7 GB / ~1,2 GB | ~5,0 / ~1,3 | ~5,6 / ~1,4 |
| | 1Y | ~18,9 GB / ~4,7 GB | ~23,6 / ~5,9 | **~38,8 GB / ~9,7 GB** |
| **large** | 1Q | ~23,6 / ~5,9 | ~25,1 / ~6,3 | ~28,1 / ~7,0 |
| | 1Y | ~94,5 GB / ~23,6 GB | ~118 / ~29,5 | **~194 GB / ~48 GB** |

*(small không đáng kể: 1Y nóng ~31M dòng ≈ ~1,9 GB raw / ~0,5 GB nén.)*

---

## 5. Bảng FIXED (overwrite) — KHÔNG cộng dồn theo ngày

Các bảng 🟢 chỉ phụ thuộc **số KH hiện hữu**, không nhân theo ngày. Khi KH tăng (nóng/đều) chúng to lên *theo mức KH cuối kỳ*, rồi đứng yên:

| Bảng | medium base | medium 1Y nóng (×4 KH) | large base | large 1Y nóng (×4 KH) |
|---|---:|---:|---:|---:|
| `T_INDEXING_PORTFOLIO_TICKER` (current holdings) | 1,25M | ~5M | 6,25M | ~25M |
| `T_CUSTOMER_NAV_CURRENT` | 50K | ~200K | 250K | ~1M |

→ Dù KH ×4, current holdings chỉ ~25M (large) — **không đáng** so với history `holding_daily` 1Y nóng ~3,2 **tỷ** dòng. Khẳng định lại: cái cần lo là **history append**, không phải current.

---

## 6. Tham chiếu prod thực (200K KH, ~20M holdings/ngày)

| Kỳ | flat | đều | nóng |
|---|---:|---:|---:|
| holding_daily rows | 1Y: ~5,0 tỷ | ~6,3 tỷ | ~10,3 tỷ |
| holding_daily raw / nén | ~300 GB / ~75 GB | ~378 / ~95 | **~620 GB / ~155 GB** |

---

## 7. Kết luận & khuyến nghị
1. **History append (`*_DAILY`) là khối tăng trưởng duy nhất đáng kể** — đặc biệt `customer_holding_daily` (×số mã) và `customer_nav_daily`. Bảng overwrite (current) không cộng dồn theo ngày.
2. **Bắt buộc partition theo năm + CCI** cho `*_DAILY` (giảm ~4× dung lượng + segment-elimination khi đọc). Đã ghi trong [SDI-db-architecture.md].
3. **Chiến lược archive/retention:** holding_daily quá khứ (>N năm) → archive partition/cold storage; hoặc **lấy điểm thưa** lịch sử (chỉ giữ unit_price cho chart) nếu không cần holdings từng-ngày-từng-mã đủ sâu.
4. **Kịch bản stress = large/prod × nóng × 1Y**: medium ~0,65 tỷ; large ~3,2 tỷ; prod ~10 tỷ dòng/năm — phải có partition + archive trước khi tới mốc này.
5. **Tốc độ tăng là tham số** — bảng trên dùng đều=+50%/y, nóng=×4/y. Khi có số kinh doanh thật, thay vào công thức `H₀ × phiên × hệ số_TB` để cập nhật.
6. EOD time cũng tăng theo holdings/ngày (driver = MTM J07). Khi KH tăng → chạy `db/bench.ps1` ở mức KH dự kiến để kiểm cửa sổ EOD còn đủ.

---

## 8. Giả định & lưu ý
- Bytes/row + tỷ lệ nén là ước lượng raw; số thật phụ thuộc kiểu cột/nén.
- Sparse (cashflow, cổ tức/phí, CA, unit_ledger) chưa đưa vào tổng vì biến động theo lịch sự kiện (SIP/chia cổ tức) — cần đo riêng; thường << holding_daily.
- "đều/nóng" là 2 kịch bản minh hoạ; thay tốc độ thật để ra số chính xác.
- Số phiên/năm = 252 (chuẩn); điều chỉnh nếu lịch nghỉ khác.
