# SDI — Hợp đồng trao đổi dữ liệu EOD: FO ↔ SDI ↔ Asset

Tài liệu tổng hợp **dữ liệu cuối ngày (EOD)** các hệ thống cần trao đổi: ai gửi gì cho ai, payload cụ thể, và **báo cáo định lượng** theo 3 kịch bản small / medium / large.

> Liên quan: [SDI-spec.md](./SDI-spec.md) (công thức, job EOD J0–J14 + ingest), [SDI-db-architecture.md](./SDI-db-architecture.md) (kiến trúc DB, roll-forward, set-based).

---

## 1. Các hệ thống & vai trò

| Hệ thống | Vai trò trong luồng EOD |
|---|---|
| **FO** (Front Office) | Tính tỷ trọng danh mục mẫu (model_weight); **đặt & khớp lệnh MP trực tiếp trên TK từng KH**; sở hữu tiền (trừ thuế GD vào cash). **Nguồn sự thật: holdings + tiền (3 khoản) + cổ tức/phí lưu ký + cashflow.** |
| **BO** (Back Office) | **Cắt phí quản lý** của KH (1 cục/tháng) → báo event Kafka cho SDI `{si_account, amount, charge_date}`. SDI net-off vào payable. |
| **Market data** | Cấp giá EOD, corporate action, chỉ số benchmark (VN-Index…). (Nguồn riêng, không phải FO.) |
| **SDI** | Nhận holdings + tiền (FO) + event cắt phí (BO) → tính NAV (= tổng tài sản − payable), Unit/Unit Price, PnL, TWR, MWR, Master Index; **accrue payable phí QL hằng ngày + net-off khi BO cắt**. → đẩy kết quả sang Asset. |
| **Asset** | Nhận current snapshot + chuỗi master từ SDI; phục vụ **SMO** đọc/hiển thị (read-only, không tính). |
| **SMO** | Tầng hiển thị, đọc qua Asset. |

**Nguyên tắc nền:** FO đồng bộ **snapshot overwrite** mỗi EOD (không event-source từng lệnh). `NAV = stock_value + FO cash`; FO cash **đã NET** phí QL + thuế GD + SIP → SDI tuyệt đối không trừ lại (tránh double-count).

---

## 2. Sơ đồ luồng EOD

```
        ┌─────────────────────── trong ngày ───────────────────────┐
SDI ──(1) rebalance_request (trigger REBALANCE/DEPLOY/REDEEM) ──────▶ FO
        └────────────────────────────────────────────────────────────┘

        ┌──────────────────────── cuối ngày (EOD) ─────────────────────┐
FO  ──(2) model_weight ─────────────────────────────────────────────▶ SDI
FO  ──(3) holdings snapshot (KH×master×mã)  ◀── volume chính ───────▶ SDI
FO  ──(4) tiền 3 khoản (mặt + bán chờ về + cổ tức tiền) ────────────▶ SDI
FO  ──(5) cổ tức/phí per-KH (sparse) ──────────────────────────────▶ SDI
FO  ──(6) cashflow nạp/rút/SIP (sparse) ───────────────────────────▶ SDI
Mkt ──(7) giá EOD + corporate action + benchmark ──────────────────▶ SDI
        │
        │  INGEST (Kafka per-KH, NGOÀI EOD): SP_INGEST_CUSTOMER → overwrite current
        │       holdings+cash + maintain interval hist + cổ tức/phí
        │  SDI EOD (SP_EOD_RUN): J0 GATE → J07 COMPUTE (MTM→NAV→PnL→Unit)
        │       → J11 master agg → J12 Index → J12B TE accum → J13 RECONCILE (cổng) → J14 snapshot (push Asset)
        ▼
SDI ──(8) current snapshot + master series ────────────────────────▶ Asset ──▶ SMO
SDI ──(9) (API pull) customer NAV/holdings lịch sử theo yêu cầu ◀───── Asset
        └────────────────────────────────────────────────────────────┘
```

Thứ tự: **(1) trong ngày** (SDI kích FO rebalance) → **(2–7) FO/Market đẩy EOD qua INGEST (Kafka per-KH)** → SDI EOD tính + **J13 reconcile là cổng** (break > ngưỡng ⇒ chặn J14 snapshot) → **(8) SDI push Asset**. (9) Asset/SMO đọc lịch sử qua API (pull), không nhận bulk.

---

## 3. Chi tiết từng luồng (payload cụ thể)

### A. SDI → FO

| # | Luồng | Bảng/payload | Trường | Tần suất |
|---|---|---|---|---|
| 1 | **Rebalance trigger** | `T_REBALANCE_REQUEST` | request_id, C_MASTER_CODE, business_date, type[REBALANCE\|DEPLOY\|REDEEM], status | **SPARSE** — chỉ khi cần tái cân bằng/giải ngân/rút. KHÔNG chứa weights (FO tự tính). |

### B. FO → SDI (feed EOD)

> **Cơ chế: Kafka per-KH (1 event = 1 KH, gồm các sub-account).** App đọc event → `SP_INGEST_CUSTOMER` (JSON) xử lý NGAY khi nhận (forward): cash→state + holdings→current + interval history + cổ tức/phí. (Thay batch STAGE/SYNC_FO/J14b cũ.) Cổ tức/phí dedup theo `event_id`. Forward-only (history: FO resync full D→nay + replay — chưa làm).

| # | Luồng | Bảng/payload | Trường | Tính chất |
|---|---|---|---|---|
| 2 | **Model weight** | `T_MASTER_PORTFOLIO_TICKER` | C_MASTER_CODE, effective_date, ticker, target_weight (Σ=100%) | Version theo effective_date; **chỉ đẩy khi đổi** rổ. |
| 3 | **Holdings** (trong event KH) | → `T_SI_PORTFOLIO_HOLDING` (current) + diff `T_SI_HOLDING_HIST` | C_SI_ACCOUNT, ticker, quantity, avg_cost | Mỗi event mang holdings từng sub-account của KH; ingest overwrite current + đóng/mở interval (no-dup). |
| 4 | **Tiền (3 khoản)** (trong event KH) | → state `T_SI_NAV_CURRENT` (`C_CASH`+`C_PENDING_CASH`+`C_DIV_CASH`) + diff `T_SI_CASH_HIST` | C_SI_ACCOUNT, **tiền mặt, tiền bán chờ về, cổ tức tiền** | FO đồng bộ 3 khoản → `Tiền = Σ`. Tiền mặt đã NET thuế/phí. Tiền bán chờ về (T0+T1+T2, lưu **tổng**) + cổ tức tiền **vào tài sản** → `total_asset = stock + Tiền`, `NAV = total_asset − payable`. |
| 5 | **Cổ tức + phí lưu ký** | `T_SI_FEE_INCOME` | business_date, C_SI_ACCOUNT, type[DIVIDEND\|CUSTODY_FEE], ticker, amount, event_id | **SPARSE** — chỉ ngày có sự kiện. Cho FR-06. (Phí QL KHÔNG ở đây — BO cắt, xem luồng BO→SDI.) |
| 5b | **Phí QL đã cắt** (BO→SDI) | `T_SI_FEE_CHARGE` → net-off `payable` | si_account, amount, charge_date, period?, source_event_id | **Event Kafka từ BO** (`SP_INGEST_FEE_CHARGE`). BO cắt 1 cục/tháng; SDI net-off payable (dedup source_event_id). |
| 6 | **Cashflow** | `T_SI_CASHFLOW_EVENT` | C_SI_ACCOUNT, business_date, event_type[INITIAL\|TOPUP\|SIP\|INTEREST_IN\|WITHDRAW], amount | **SPARSE** — chỉ KH có nạp/rút/SIP. Dùng cho CF_t (PnL/unit), KHÔNG cộng lại cash. |

### C. Market data → SDI (feed EOD)

| # | Luồng | Bảng/payload | Trường | Tính chất |
|---|---|---|---|---|
| 7a | **Giá EOD (gộp CA)** | `T_PRICE_DAILY` | ticker, business_date, **ref_price** (NOT NULL), close_price, **is_ex_rights** (1/0) | Theo **universe mã** (không theo KH). `ref_price` = giá tham chiếu đầu phiên sở publish MỖI ngày (phiên thường = close hôm trước; ex-rights = giá sau chia) → J12 self-contained, không tra ngày trước. `is_ex_rights` = metadata đánh dấu ngày có quyền. Bỏ bảng CA riêng — type/ratio/cash_div đã vào NAV qua FO sync. |
| 7b | **Benchmark** | `T_BENCHMARK_DAILY` | benchmark_code (VNINDEX…), business_date, index_value | 1 dòng/benchmark/ngày. |

### D. SDI → Asset (J14 SNAPSHOT, sau reconcile)

| # | Luồng | Bảng/payload | Trường | Tính chất |
|---|---|---|---|---|
| 8a | **Current snapshot KH** | `T_SI_NAV_CURRENT` | C_SI_ACCOUNT, unit, cash, last_nav, last_unit_price, status, last_business_date | Push **current/delta** (1 dòng/tiểu khoản). |
| 8b | **Current snapshot master** | `T_MASTER_NAV_CURRENT` | C_MASTER_CODE, cash, stock_value, total_asset, last_nav, unit, last_unit_price | Push current toàn quỹ (overview/AUM). |
| 8c | **Master series ngày** | `T_MASTER_NAV_BALANCE`, `T_MASTER_INDEX_DAILY` | nav/unit/up/pnl/return + index_value (PR) | Append dòng master của ngày @d (nhỏ). |
| 9 | **Lịch sử KH (API pull)** | `T_SI_NAV_BALANCE`, `T_MASTER_HOLDING_BALANCE`, `T_SI_FEE_INCOME` | NAV/UP/return chart, holdings top20, cổ tức/phí | Asset/SMO **đọc qua API** (`SP_GET_*`) on-demand — **KHÔNG** push bulk lịch sử. |

> **Điểm mấu chốt:** FO→SDI nặng (per-mã, dense, nạp THẲNG current); SDI→Asset nhẹ (per-tiểu-khoản current). Lịch sử dài hạn = `T_SI_NAV_BALANCE` (~2,5 tỷ dòng) SDI giữ + serve API. Holdings/cash history = **interval (SCD-2) full history, KHÔNG trùng lặp** (holding bất biến = 1 dòng) maintain bằng DIFF **tại INGEST (per-event)** — không trong EOD core.

> **⚠️ Cập nhật mô hình SDI→Asset (2026-06-21):** SMO **đọc tài sản KH từ ASSET**, KHÔNG gọi API SDI (dòng 9 "API pull" ở trên LỆCH thực tế). SDI **đồng bộ qua Kafka** như mọi hệ, 2 mode **EOD** (snapshot ngày) + **HISTORY** (đẩy LẠI ngày quá khứ). Producer: **`SP_GET_ASSET_SNAPSHOT @p_business_date,@p_mode`** (db/05_API.sql) — build 1 payload JSON/sub-account (`FOR JSON`), app đọc result set → publish Kafka (key=`C_SI_ACCOUNT`). **RECONSTRUCT-ONLY** từ bảng DATED (`T_SI_NAV_BALANCE`+`T_SI_CASH_HIST`+`T_SI_HOLDING_HIST`×giá+`T_SI_FEE_*`) ⇒ EOD & HISTORY replay cùng ngày ra payload **y hệt**. `pending_cash`/`div_cash` KHÔNG có lịch sử per-ngày → ngoài payload (chỉ `total_asset = nav+payable` authoritative). **Payload hiện là DRAFT — map lại theo schema Asset thật khi có.** Xem memory `sdi-asset-sync-architecture`.

---

## 4. Báo cáo định lượng (small / medium / large)

**Tham số kịch bản** (theo `db/bench.ps1`):

| Kịch bản | KH | master/KH | Mã/master | Tiểu khoản (KH×master) | **Holdings (KH×master×mã)** |
|---|---|---|---|---|---|
| small | 1.000 | 3 | 20 | 3.000 | **60.000** |
| medium | 10.000 | 5 | 25 | 50.000 | **1.250.000** |
| large | 50.000 | 5 | 25 | 250.000 | **6.250.000** |
| *(prod tham chiếu)* | *200.000* | *5* | *~20* | *~1.000.000* | *~20.000.000* |

### 4.1 Số bản ghi mỗi feed/ngày

| Feed | Grain | small | medium | large |
|---|---|---:|---:|---:|
| **(3) Holdings snapshot** FO→SDI | KH×master×mã | 60.000 | 1.250.000 | 6.250.000 |
| **(4) Cash snapshot** FO→SDI | KH×master | 3.000 | 50.000 | 250.000 |
| (2) Model weight FO→SDI | master×mã (khi đổi) | 60 | 125 | 125 |
| (5) Cổ tức/phí FO→SDI | sparse | *0 → ~tiểu khoản nắm mã chia* | | |
| (6) Cashflow FO→SDI | sparse | *0 → ~tiểu khoản có SIP/nạp/rút* | | |
| (7a) Giá EOD Mkt→SDI | universe mã | 20 | 25 | 25 *(prod ~1.600)* |
| (7b/7c) CA + benchmark | sparse / 1-vài | nhỏ | nhỏ | nhỏ |
| **(8a) Current snapshot KH** SDI→Asset | KH×master | 3.000 | 50.000 | 250.000 |
| (8b) Current snapshot master SDI→Asset | master | 3 | 5 | 5 |
| (8c) master series/ngày SDI→Asset | master | 3 | 5 | 5 |

**Sparse feeds (5)(6):** phụ thuộc sự kiện —
- *Ngày thường:* cashflow ≈ % nhỏ tiểu khoản (chỉ KH có SIP/nạp/rút); cổ tức/phí ≈ 0.
- *Ngày cao điểm:* SIP định kỳ → tới ~50% tiểu khoản; ngày chia cổ tức 1 mã phổ biến → ~tiểu khoản đang nắm mã đó. Ví dụ medium, mã chia được ~30% tiểu khoản nắm → ~15.000 dòng.

### 4.2 Ước lượng dung lượng (raw, chưa nén)

> Ước theo bytes/row xấp xỉ; prod nén PAGE/CCI thường giảm **~3–5×**.

| Feed | ~bytes/row | small | medium | large |
|---|---:|---:|---:|---:|
| (3) Holdings snapshot | ~60 | ~3,6 MB | ~75 MB | ~375 MB |
| (4) Cash snapshot | ~28 | ~0,08 MB | ~1,4 MB | ~7 MB |
| (8a) Current snapshot KH | ~50 | ~0,15 MB | ~2,5 MB | ~12,5 MB |
| **Tổng FO→SDI/ngày** (3+4) | | **~3,7 MB** | **~76 MB** | **~382 MB** |
| **Tổng SDI→Asset/ngày** (8a+b+c) | | **~0,15 MB** | **~2,5 MB** | **~12,5 MB** |

→ **FO→SDI lớn gấp ~25–30× SDI→Asset** (vì holdings theo từng mã; Asset chỉ nhận current cấp tiểu khoản). *(prod ~20M holdings ⇒ feed ~1,2 GB/ngày raw.)*

### 4.3 Thời gian xử lý EOD (đo thực, SQL Express 1 máy)

| Kịch bản | Holdings | TOTAL EOD | J07 MTM (nặng nhất) | Ghi chú |
|---|---:|---:|---:|---|
| small | 60.000 | ~0,9–1,2 s | ~0,5 s | — |
| medium | 1.250.000 | ~16–19 s | ~8–9 s | đo A/B `db/bench.ps1` |
| large | 6.250.000 | *chưa đo* | *~5× medium* | ~80–100 s ước lượng tuyến tính |

> Đo bằng `db/bench.ps1` → ghi `db/perf-history.csv`. Điểm nóng cố định = **J07 MTM** (định giá lại toàn bộ holdings) — bám cột này khi theo dõi regression. Lưu ý: `C_CUST_CODE` VARCHAR(10) chậm hơn BIGINT ~13% total (xem perf note); chấp nhận vì là định danh KH xuyên hệ.

---

## 5. Quy tắc hợp đồng (contract)

1. **Snapshot overwrite, idempotent:** FO nạp toàn bộ holdings THẲNG vào current + cash mỗi EOD; chạy lại 1 ngày cho cùng kết quả (overwrite, không cộng dồn).
8. **Holdings/cash history tách rời (interval):** DIFF **tại INGEST (per-event Kafka)** current → `T_SI_HOLDING_HIST` & `T_SI_CASH_HIST` (SCD-2 valid_from/valid_to, **full history BẮT BUỘC, no-dup**) — EOD core chỉ đọc current, KHÔNG chạy DIFF.
2. **FO cash là nguồn tiền duy nhất, đã NET** phí QL + thuế GD + SIP → SDI không re-apply.
3. **Biến động holdings/ngày** SDI suy ra on-demand (qty(D)−qty(D-1)), KHÔNG cần FO gửi delta.
4. **Cổ tức/phí & cashflow là sự kiện sparse** — FO chỉ gửi khi phát sinh; capture đúng ngày + số tiền khớp thời điểm FO ghi vào cash.
5. **J13 RECONCILE là cổng:** Σ holdings/NAV SDI vs FO, Σ customer NAV vs master NAV, Σ unit — lệch > ngưỡng ⇒ **chặn J14 snapshot**.
6. **SDI→Asset chỉ push current/delta + chuỗi master**; lịch sử KH SDI giữ và serve qua API (pull), không đổ bulk.
7. **J0 GATE** chờ đủ nguồn (FO holdings+cash, model_weight, giá, CA, benchmark, cashflow) sẵn sàng cho @d mới chạy.

---

## 6. Giả định & lưu ý
- Bytes/row là ước lượng raw để so sánh tương đối — số thật phụ thuộc kiểu cột/nén.
- Kịch bản small/medium/large theo `bench.ps1`; **prod thực** ≈ 200K KH (~20M holdings) — large (6,25M) ≈ 31% prod.
- Sparse feeds (cổ tức/phí, cashflow, CA, rebalance) biến động mạnh theo lịch sự kiện → cần đo riêng theo lịch SIP/chia cổ tức thực tế, không suy tuyến tính từ holdings.
- Thời gian EOD large chưa đo — chạy `./bench.ps1 -Scale large` để lấy số thật trước khi cam kết SLA cửa sổ EOD.
