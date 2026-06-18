# SDI — Báo cáo tăng trưởng dữ liệu (1M / 1Q / 1Y)

Dự phóng số bản ghi & dung lượng theo thời gian, cho 3 kịch bản quy mô (small/medium/large) và 2 kiểu tăng trưởng khách hàng: **đều** (linear) và **nóng** (compounding).

> Liên quan: [SDI-eod-data-exchange.md](./SDI-eod-data-exchange.md), [SDI-db-architecture.md](./SDI-db-architecture.md). Đo perf thực: `db/bench.ps1`.

> **Cập nhật quan trọng:** `T_CUSTOMER_HOLDING_DAILY` nay **chỉ giữ rolling 1 THÁNG** (J16 archive droppable, đã decouple khỏi EOD core) → **KHÔNG còn là khối tăng trưởng**. Driver tăng trưởng dài hạn duy nhất giờ là **`T_CUSTOMER_NAV_DAILY`** (lịch sử NAV/unit per-KH cho chart FR-03).

---

## 1. Bảng nào PHÌNH theo ngày, bảng nào KHÔNG

| Loại | Bảng | Cơ chế | Tăng theo |
|---|---|---|---|
| 🔴 **GROW không chặn** | `T_CUSTOMER_NAV_DAILY` | +1 dòng/tiểu khoản/EOD (history NAV/unit) | **ngày × KH** ← driver chính (cho FR-03) |
| 🟠 (nhỏ, không chặn) | `T_SI_NAV_DAILY`, `T_SI_INDEX_DAILY`, `T_SI_HOLDING_DAILY` | +SI(×mã)/EOD | ngày × SI |
| 🟠 (sparse) | `T_CASHFLOW_EVENT`, `T_CUSTOMER_FEE_INCOME`, `T_UNIT_LEDGER` | theo sự kiện | tần suất nạp/rút/cổ tức |
| 🟡 **GROW có CHẶN (rolling 1M)** | `T_CUSTOMER_HOLDING_DAILY` (archive) | +1 snapshot/EOD, **purge >1 tháng** | **bão hoà ~21 phiên × H₀** (không tích luỹ tiếp) |
| 🟢 **FIXED (overwrite)** | `T_INDEXING_PORTFOLIO_TICKER` (current, đích FO) | overwrite/EOD | chỉ theo KH |
| 🟢 | `T_CUSTOMER_NAV_CURRENT`, `T_SI_NAV_CURRENT`, master | overwrite/config | chỉ theo KH |
| ⚪ transient | `T_EOD_WORK` | xoá/ghi mỗi run | 1 ngày |

→ **Driver dài hạn = `T_CUSTOMER_NAV_DAILY`** (per-KH/ngày). `T_CUSTOMER_HOLDING_DAILY` đã bị **cap 1 tháng** → đứng yên ở mức rolling, không phải lo dài hạn nữa. Bảng 🟢 chỉ to theo số KH.

---

## 2. Giả định

| Tham số | Giá trị |
|---|---|
| Phiên giao dịch | 1M = **21** · 1Q = **63** · 1Y = **252** |
| Kịch bản (KH × SI/KH × mã/SI) | small 1.000×3×20 · medium 10.000×5×25 · large 50.000×5×25 |
| Tiểu khoản S₀ = KH×SI | small **3.000** · medium **50.000** · large **250.000** |
| Holdings/ngày H₀ = KH×SI×mã | small 60K · medium 1,25M · large 6,25M *(chỉ ảnh hưởng bảng cap 1M)* |
| Tăng **đều** (linear, +50%/năm) | hệ số TB kỳ: 1M≈1,02 · 1Q≈1,06 · 1Y≈1,25 |
| Tăng **nóng** (compounding, ×4/năm) | hệ số TB kỳ: 1M≈1,06 · 1Q≈1,18 · 1Y≈2,05 |
| Bytes/row (raw): nav_daily ~50 · holding ~60 | nén CCI prod **~3–5×** |

> Tích luỹ ≈ (rows/ngày tại base) × (số phiên) × (hệ số TB). Tốc độ tăng là **tham số** — chỉnh theo thực tế.

---

## 3. Driver chính: `T_CUSTOMER_NAV_DAILY` — số bản ghi tích luỹ (triệu dòng)

> rows/ngày = S₀ (tiểu khoản).

| Kịch bản | Kỳ | flat | đều (+50%/y) | nóng (×4/y) |
|---|---|---:|---:|---:|
| **small** (S₀=3K) | 1M | 0,06 | 0,06 | 0,07 |
| | 1Q | 0,19 | 0,20 | 0,22 |
| | 1Y | 0,76 | 0,95 | 1,55 |
| **medium** (S₀=50K) | 1M | 1,05 | 1,07 | 1,11 |
| | 1Q | 3,15 | 3,34 | 3,72 |
| | 1Y | 12,6 | 15,8 | **25,8** |
| **large** (S₀=250K) | 1M | 5,25 | 5,36 | 5,57 |
| | 1Q | 15,8 | 16,7 | 18,6 |
| | 1Y | 63,0 | 78,8 | **129** |

**Dung lượng** (raw ~50B/row; nén ~÷4): medium 1Y nóng ~1,3 GB raw / ~0,3 GB nén; large 1Y nóng ~6,5 GB / ~1,6 GB. → **Khiêm tốn** (vì nav_daily = holdings ÷ số mã, nhỏ hơn ~25×).

---

## 4. `T_CUSTOMER_HOLDING_DAILY` — đã CHẶN 1 tháng (không tích luỹ)

Rolling ~21 phiên × H₀, **đứng yên** (purge mỗi EOD):

| Kịch bản | Mức bão hoà (rolling 1M) | raw / nén |
|---|---:|---:|
| small | ~1,3M | ~0,08 GB / ~0,02 GB |
| medium | ~26M | ~1,6 GB / ~0,4 GB |
| large | ~131M | ~7,9 GB / ~2,0 GB |

→ Trước khi cap (giữ 10 năm): large 1Y nóng từng dự ~3,2 **tỷ** dòng (~48 GB nén). Sau cap 1M: **~131M cố định** (~2 GB). **Decouple + retention 1M cắt ~95% khối tăng trưởng lớn nhất.**

---

## 5. Bảng FIXED (overwrite) — chỉ theo số KH
| Bảng | medium base | medium ×4 KH | large base | large ×4 KH |
|---|---:|---:|---:|---:|
| `T_INDEXING_PORTFOLIO_TICKER` (current) | 1,25M | ~5M | 6,25M | ~25M |
| `T_CUSTOMER_NAV_CURRENT` | 50K | ~200K | 250K | ~1M |

Không cộng dồn theo ngày — to lên một bậc khi KH tăng rồi đứng yên.

---

## 6. Tham chiếu prod thực (200K KH, S₀≈1M tiểu khoản)
| | flat | đều | nóng |
|---|---:|---:|---:|
| `customer_nav_daily` rows 1Y | ~252M | ~315M | ~516M |
| 10 năm | ~2,5 tỷ | ~3,1 tỷ | ~5,2 tỷ |
| holding_daily (cap 1M, rolling) | ~420M cố định | (theo KH cuối) | ~1,7 tỷ cố định nếu KH×4 |
| Dung lượng nav_daily 10y nén | ~30 GB | ~40 | ~65 |

> `customer_nav_daily` 10 năm ~2,5 tỷ (khớp [SDI-spec §11]) — đây là khối lớn nhất cần partition/CCI/điểm thưa. holding_daily prod cap 1M ≈ 420M cố định (~10 GB nén), không tích luỹ.

---

## 7. Kết luận & khuyến nghị
1. **Sau decouple + retention 1M:** `customer_holding_daily` không còn là vấn đề tăng trưởng (đứng yên ~rolling 1 tháng, và **droppable** — bỏ hẳn cũng được, EOD core không đổi).
2. **Khối tăng trưởng dài hạn duy nhất = `T_CUSTOMER_NAV_DAILY`** (lịch sử NAV/unit cho chart FR-03) — nhưng nhỏ hơn holdings ~25× (per-tiểu-khoản, không per-mã). Prod 10y ~2,5 tỷ dòng / ~30–65 GB nén.
3. **Bắt buộc với `customer_nav_daily`:** CCI + partition theo năm; cân nhắc **điểm thưa** (chỉ lưu unit_price tuần/tháng cho chart range dài) để giảm tiếp.
4. **Kịch bản stress giờ nhẹ hơn nhiều:** large 1Y nóng ~129M dòng nav_daily (~1,6 GB nén) thay vì ~3,2 tỷ holdings trước đây.
5. **Tốc độ tăng là tham số** (đều=+50%/y, nóng=×4/y) — thay số kinh doanh thật vào `S₀ × phiên × hệ_số_TB`.
6. EOD time: driver vẫn là **MTM J07** (đọc current ~H₀, không đổi theo retention). J16 archive thêm 1 bước copy H₀ + purge (droppable — bỏ nếu không cần history). Đo bằng `db/bench.ps1`.

---

## 8. Giả định & lưu ý
- Bytes/row + tỷ lệ nén là ước lượng raw.
- Sparse (cashflow, cổ tức/phí, unit_ledger) chưa gộp — biến động theo lịch sự kiện, thường << nav_daily.
- "đều/nóng" minh hoạ; thay tốc độ thật để ra số chính xác.
- holding_daily cap 1M là **tạm chốt** (tuỳ yêu cầu compliance về lưu trữ holdings lịch sử — nếu pháp lý bắt giữ lâu hơn thì nới retention + archive cold).
