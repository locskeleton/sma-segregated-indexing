# SDI — Báo cáo tăng trưởng dữ liệu (1M / 1Q / 1Y)

Dự phóng số bản ghi & dung lượng theo thời gian, cho 3 kịch bản quy mô (small/medium/large) và 2 kiểu tăng trưởng khách hàng: **đều** (linear) và **nóng** (compounding).

> Liên quan: [SDI-eod-data-exchange.md](./SDI-eod-data-exchange.md), [SDI-db-architecture.md](./SDI-db-architecture.md). Đo perf thực: `db/bench.ps1`.

> **Cập nhật quan trọng (spec interval):** Holdings & cash history nay **BẮT BUỘC lưu FULL history** (rule compliance) — nhưng lưu theo **KHOẢNG (interval / SCD-2)** trong `T_CUSTOMER_HOLDING_HIST` & `T_CUSTOMER_CASH_HIST`: `valid_from..valid_to`, holding bất biến N năm = **1 dòng**. J14b droppable maintain bằng **DIFF** current vs dòng open. Vì danh mục index ít biến động → history **không phình theo ngày** mà chỉ **theo số lần thay đổi (churn Δ)**. Snapshot dated cũ (mỗi EOD +H₀ dòng) đã bị loại bỏ hoàn toàn.

---

## 1. Bảng nào PHÌNH theo ngày, bảng nào KHÔNG

| Loại | Bảng | Cơ chế | Tăng theo |
|---|---|---|---|
| 🔴 **GROW không chặn (dense/ngày)** | `T_CUSTOMER_NAV_BALANCE` | +1 dòng/tiểu khoản/EOD (history NAV/unit) | **ngày × KH** ← driver chính (cho FR-03) |
| 🟠 **GROW theo CHURN (interval, full history)** | `T_CUSTOMER_HOLDING_HIST`, `T_CUSTOMER_CASH_HIST` | base H₀/S₀ + **1 dòng mỗi lần đổi** (J14b DIFF) | **số lần thay đổi** (rebalance/nạp-rút), KHÔNG theo ngày |
| 🟠 (nhỏ, không chặn) | `T_SI_NAV_BALANCE`, `T_SI_INDEX_DAILY`, `T_SI_HOLDING_BALANCE` | +SI(×mã)/EOD | ngày × SI |
| 🟠 (sparse) | `T_CASHFLOW_EVENT`, `T_CUSTOMER_FEE_INCOME`, `T_UNIT_LEDGER` | theo sự kiện | tần suất nạp/rút/cổ tức |
| 🟢 **FIXED (overwrite)** | `T_INDEXING_PORTFOLIO_TICKER` (current, đích FO), `T_FO_CASH_SYNC` (feed @d) | overwrite/EOD | chỉ theo KH |
| 🟢 | `T_CUSTOMER_NAV_CURRENT`, `T_SI_NAV_CURRENT`, master | overwrite/config | chỉ theo KH |
| ⚪ transient | `T_EOD_WORK` | xoá/ghi mỗi run | 1 ngày |

→ **Hai driver dài hạn:** (1) `T_CUSTOMER_NAV_BALANCE` dense theo ngày×KH (lớn nhất); (2) interval hist theo **churn** — full history nhưng tăng chậm vì danh mục index ít biến động. Bảng 🟢 chỉ to theo số KH.

---

## 2. Giả định

| Tham số | Giá trị |
|---|---|
| Phiên giao dịch | 1M = **21** · 1Q = **63** · 1Y = **252** |
| Kịch bản (KH × SI/KH × mã/SI) | small 1.000×3×20 · medium 10.000×5×25 · large 50.000×5×25 |
| Tiểu khoản S₀ = KH×SI | small **3.000** · medium **50.000** · large **250.000** |
| Holdings H₀ = KH×SI×mã | small 60K · medium 1,25M · large 6,25M |
| **Churn holdings** (số lần đổi/vị thế/năm) | **base = 4/y** (tái cân bằng quý) · **cao = 12/y** (tháng + nạp/rút lẻ) |
| Churn cash (số lần đổi/tiểu khoản/năm) | ~ số ngày có giao dịch (nạp/rút/cổ tức/phí); base ~12/y |
| Tăng **đều** (linear, +50%/năm) | hệ số TB kỳ: 1M≈1,02 · 1Q≈1,06 · 1Y≈1,25 |
| Tăng **nóng** (compounding, ×4/năm) | hệ số TB kỳ: 1M≈1,06 · 1Q≈1,18 · 1Y≈2,05 |
| Bytes/row (raw): nav_balance ~50 · hist ~64 | nén CCI/PAGE prod **~3–5×** |

> nav_balance tích luỹ ≈ S₀ × phiên × hệ_số_TB. Interval hist tích luỹ ≈ H₀ (base mở) + H₀ × churn × năm × hệ_số_TB.

---

## 3. Driver #1: `T_CUSTOMER_NAV_BALANCE` — số bản ghi tích luỹ (triệu dòng)

> rows/ngày = S₀ (tiểu khoản). Dense, không né được (FO snapshot ⇒ không event-source ⇒ phải materialize NAV/unit cho chart).

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

**Dung lượng** (raw ~50B/row; nén ~÷4): medium 1Y nóng ~1,3 GB raw / ~0,3 GB nén; large 1Y nóng ~6,5 GB / ~1,6 GB. → Khiêm tốn (nav_balance = holdings ÷ số mã).

---

## 4. Driver #2: `T_CUSTOMER_HOLDING_HIST` (interval) — full history, tăng theo CHURN

Cơ chế: ngày đầu mở H₀ dòng open; mỗi lần một vị thế đổi qty/avg_cost ⇒ **đóng dòng cũ (UPDATE valid_to, không sinh dòng) + mở 1 dòng mới**. Vị thế bất biến = 1 dòng suốt đời. Tích luỹ ≈ **H₀ + H₀ × churn × năm**.

| Kịch bản | base (H₀ mở) | +1Y churn 4/y | +1Y churn 12/y | +10Y churn 4/y |
|---|---:|---:|---:|---:|
| small (H₀=60K) | 0,06M | ~0,3M | ~0,8M | ~2,5M |
| medium (H₀=1,25M) | 1,25M | ~6,3M | ~16,3M | ~51M |
| large (H₀=6,25M) | 6,25M | ~31M | ~81M | ~256M |

**So với snapshot dated cũ (đã bỏ):** snapshot = H₀ × 252/năm. medium 10 năm snapshot ≈ **3,15 tỷ** dòng; interval churn-4 ≈ **51M** → **giảm ~98% STORAGE**, mà vẫn **full history** (tái dựng mọi ngày qua `valid_from≤D<valid_to`). Ngày không biến động: J14b DIFF (EXCEPT) thấy 0 thay đổi ⇒ **0 dòng ghi** (storage đứng yên). **Lưu ý THỜI GIAN:** DIFF vẫn quét **current ⋈ open-rows (2×H₀) mỗi ngày** dù 0 thay đổi → thời gian J14b **KHÔNG giảm** vào ngày không đổi (chỉ storage giảm). Đo medium (1,25M, SQL Express): J14b ~10–15s, là job nặng nhất EOD (xem §7).

`T_CUSTOMER_CASH_HIST` cùng cơ chế nhưng grain per-tiểu-khoản (S₀, không ×mã): base S₀ + S₀ × churn_cash × năm. medium 10Y churn-12 ≈ 50K + 50K×12×10 = ~6M dòng (nhỏ).

`T_FO_CASH_SYNC` = feed transient (overwrite/short-retention) → không tích luỹ.

---

## 5. Bảng FIXED (overwrite) — chỉ theo số KH
| Bảng | medium base | medium ×4 KH | large base | large ×4 KH |
|---|---:|---:|---:|---:|
| `T_INDEXING_PORTFOLIO_TICKER` (current) | 1,25M | ~5M | 6,25M | ~25M |
| `T_CUSTOMER_NAV_CURRENT` | 50K | ~200K | 250K | ~1M |

Không cộng dồn theo ngày — to lên một bậc khi KH tăng rồi đứng yên.

---

## 6. Tham chiếu prod thực (200K KH, S₀≈1M tiểu khoản, H₀≈25M holdings)
| | flat | đều | nóng |
|---|---:|---:|---:|
| `customer_nav_balance` rows 1Y | ~252M | ~315M | ~516M |
| 10 năm | ~2,5 tỷ | ~3,1 tỷ | ~5,2 tỷ |
| `customer_holding_hist` 10Y (churn 4/y) | ~1,0 tỷ | (theo KH cuối) | ~1,3 tỷ |
| `customer_holding_hist` 10Y (churn 12/y) | ~3,0 tỷ | — | — |
| Dung lượng nav_balance 10y nén | ~30 GB | ~40 | ~65 |

> `customer_nav_balance` 10 năm ~2,5 tỷ (khớp [SDI-spec §11]) — khối lớn nhất, cần partition/CCI/điểm thưa. `customer_holding_hist` full history nhưng nhờ interval chỉ ~1 tỷ (churn 4/y) thay vì ~63 tỷ nếu snapshot dated → tiết kiệm ~98%, vẫn đủ tái dựng mọi ngày.

---

## 7. Kết luận & khuyến nghị
1. **Interval thắng cả hai mục tiêu:** giữ **full history BẮT BUỘC** (compliance) NHƯNG loại bỏ trùng lặp — holding bất biến = 1 dòng. Tăng trưởng đổi từ "theo ngày" (snapshot) sang "theo churn", giảm ~98% khối holdings history.
2. **Hai khối tăng trưởng dài hạn:** (a) `T_CUSTOMER_NAV_BALANCE` (dense ngày×KH, ~2,5 tỷ/10y — lớn nhất); (b) `T_CUSTOMER_HOLDING_HIST` (interval churn-driven, ~1 tỷ/10y churn-4). Cash hist nhỏ.
3. **Bắt buộc với cả hai:** CCI + partition theo năm (`business_date` cho nav_balance; `valid_from` cho hist); cân nhắc **điểm thưa** nav_balance (unit_price tuần/tháng cho chart range dài).
4. **Churn là tham số nhạy nhất của hist:** index SMA tái cân bằng quý ⇒ churn ~4/y là thực tế; nếu sản phẩm cho phép giao dịch chủ động nhiều thì churn tăng tuyến tính số dòng. Theo dõi churn thật để hiệu chỉnh.
5. **EOD time (đo thật medium 1,25M, SQL Express, commit 216fea7):** J07 MTM ~4–8s; **J14b history ~10–15s — job NẶNG NHẤT EOD** (hơn cả MTM). Quan trọng: J14b DIFF quét **2×H₀ (current ⋈ open-rows) mỗi ngày** nên thời gian **KHÔNG giảm** vào ngày không biến động (storage thì đứng yên — 0 dòng ghi). Storage thắng lớn, thời gian thì không. **J14b droppable** (bỏ = EOD core không đổi, history dừng cập nhật). *Tối ưu tiềm năng (chưa làm):* checksum per (cust,si) / filter theo nhóm FO báo đổi để bỏ qua nhóm bất biến. Đo bằng `db/bench.ps1`.

---

## 8. Giả định & lưu ý
- Bytes/row + tỷ lệ nén là ước lượng raw.
- Churn 4/y (rebalance quý) là giả định trung tâm; nạp/rút/SIP/cổ tức-reinvest làm tăng churn — thay số thật vào `H₀ × churn × năm`.
- Sparse (cashflow, cổ tức/phí, unit_ledger) chưa gộp — theo lịch sự kiện, thường << nav_balance.
- "đều/nóng" minh hoạ tăng trưởng KH; thay tốc độ thật để ra số chính xác.
- Interval lưu **full history vĩnh viễn** (compliance) — partition năm theo `valid_from` + filegroup nóng/lạnh (2 năm SSD + cũ HDD) thay cho purge.
