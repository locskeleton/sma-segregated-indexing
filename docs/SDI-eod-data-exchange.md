# SDI — Hợp đồng trao đổi dữ liệu EOD: FO/Market/BO → SDI

Tài liệu tổng hợp **dữ liệu cuối ngày (EOD)** các hệ thống cần trao đổi: ai gửi gì cho ai, payload cụ thể, và **báo cáo định lượng** theo 3 kịch bản small / medium / large.

> **⚠️ BRD 2026-06-22:** SDI **KHÔNG còn đồng bộ tài sản KH/NAV-perf master sang Asset** (2 producer per-SI + master ĐÃ GỠ). BO/FO/Market đẩy dữ liệu **THẲNG sang Asset** (Asset tự tính); **BO còn đẩy phí QL lũy kế/ngày → Asset có payable, tự tính NAV ròng**. **SDI VẪN đẩy riêng Master Index** (`SP_GET_ASSET_INDEX_SNAPSHOT`, luồng price-ready BO) — luồng SDI→Asset DUY NHẤT còn lại. Tài liệu này tập trung **FO/Market/BO → SDI** (SDI phục vụ UI riêng qua read API). Đối chiếu 2 hệ + điểm reconcile: [SDI-asset-gap.md](./SDI-asset-gap.md).

> Liên quan: [SDI-spec.md](./SDI-spec.md) (công thức, job EOD J0–J14 + ingest), [SDI-db-architecture.md](./SDI-db-architecture.md) (kiến trúc DB, roll-forward, set-based).

---

## 1. Các hệ thống & vai trò

| Hệ thống | Vai trò trong luồng EOD |
|---|---|
| **FO** (Front Office) | Tính tỷ trọng danh mục mẫu (model_weight); **đặt & khớp lệnh MP trực tiếp trên TK từng KH**; sở hữu tiền (trừ thuế GD vào cash). **Nguồn sự thật: holdings + tiền (3 khoản) + cổ tức/phí lưu ký + cashflow.** |
| **BO** (Back Office) | **Cắt phí** của KH (phí QL/thuế/perf…, 1 cục/tháng) → báo event Kafka cho SDI `{si_account, amount, charge_date, fee_type?}`. SDI net-off vào payable. **BRD 2026-06-22: BO còn đẩy phí QL LŨY KẾ theo ngày (accrued) THẲNG sang Asset** (ngoài event cắt) → Asset có payable, tự tính NAV ròng. |
| **Market data** | Cấp giá EOD, corporate action, chỉ số benchmark (VN-Index…). (Nguồn riêng, không phải FO.) |
| **SDI** | Nhận holdings + tiền (FO) + event cắt phí (BO) → tính NAV (= tổng tài sản − payable), Unit/Unit Price, PnL, TWR, MWR, Master Index; **accrue payable đa-loại hằng ngày (theo `T_FEE_CONFIG`, dòng group=PAYABLE & rate>0) + net-off khi BO cắt**. Phục vụ **giao diện riêng của SDI** (NAV/index/perf) qua read API (FR-01..06, PM). |
| **Asset** | **Nhận dữ liệu THẲNG từ BO/FO/Market (BRD 2026-06-22) → Asset tự tính** (tài sản gộp + NAV ròng + Unit/UnitPrice/TWR/return). SDI **KHÔNG còn đẩy** tài sản KH/NAV-perf master sang Asset, **CHỈ còn đẩy Master Index** (`SP_GET_ASSET_INDEX_SNAPSHOT`, price-ready BO). Asset đủ data; còn lại là điểm reconcile (payable BO-vs-SDI, TWR methodology). Xem [SDI-asset-gap.md](./SDI-asset-gap.md). |
| **SMO** | Tầng hiển thị (đọc qua Asset). |

**Nguyên tắc nền:** FO đồng bộ **snapshot overwrite** mỗi EOD (không event-source từng lệnh). `NAV = stock_value + FO cash − payable`; FO cash **đã NET** phí GD + thuế GD + SIP → SDI tuyệt đối không trừ lại các khoản đó (tránh double-count). Phí ACCRUE (QL/thuế/perf…) SDI quản riêng qua payable (catalog `T_FEE_CONFIG`, type+group khớp `T_SI_INCOME_FEE`), BO cắt → net-off.

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
        │       → J11 master agg → J12 Index → J12B TE accum → J13 RECONCILE (cổng publish nội bộ) → J14 build snapshot (nội bộ)
        ▼
SDI ──▶ giao diện riêng SDI (read API FR-01..06, PM) — NAV/index/perf
SDI ──(8d) Master Index snapshot (SP_GET_ASSET_INDEX_SNAPSHOT, khi BO price-ready) ──▶ Asset   ✅ GIỮ
BO  ──(BO→Asset) phí QL lũy kế/ngày (accrued) + event cắt phí ─────────────────────▶ Asset   ✅ MỚI

   ❌ (8a per-SI tài sản)/(8b·8c master NAV-perf)/(9 API pull) SDI → Asset — ĐÃ GỠ per BRD 2026-06-22.
      BO/FO/Market nay đẩy THẲNG sang Asset, Asset tự tính NAV ròng + Unit/TWR. Xem SDI-asset-gap.md.
        └────────────────────────────────────────────────────────────┘
```

Thứ tự: **(1) trong ngày** (SDI kích FO rebalance) → **(2–7) FO/Market đẩy EOD qua INGEST (Kafka per-KH)** → SDI EOD tính + **J13 reconcile là cổng publish nội bộ** (break > ngưỡng ⇒ chặn publish kết quả EOD). **(8a/8b/8c/9) SDI→Asset ĐÃ GỠ (BRD 2026-06-22):** per-SI tài sản + master NAV/perf không còn push (BO/FO/Market đẩy thẳng, Asset tự tính). **Vẫn GIỮ (8d): SDI đẩy Master Index** (`SP_GET_ASSET_INDEX_SNAPSHOT`) khi BO price-ready — luồng SDI→Asset DUY NHẤT còn lại. **BO còn đẩy phí QL accrued/ngày → Asset**. Xem [SDI-asset-gap.md](./SDI-asset-gap.md).

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
| 5 | **Cổ tức + phí lưu ký** (FO→SDI) | `T_SI_INCOME_FEE` | business_date, C_SI_ACCOUNT, group[INCOME\|PAYABLE], type[DIVIDEND\|CUSTODY_FEE], ticker, amount, source_event_id | **SPARSE** — chỉ ngày có sự kiện. Cho FR-06. DIVIDEND→INCOME, CUSTODY_FEE→PAYABLE (source FO). (Phí ACCRUE BO cắt cũng đổ vào bảng này — xem 5b.) |
| 5b | **Phí ACCRUE đã cắt** (BO→SDI) | `T_SI_INCOME_FEE` (type theo fee_type [default MGMT_FEE], group PAYABLE) → net-off `payable` | si_account, amount, charge_date, fee_type?, source_event_id | **Event Kafka từ BO** (`SP_INGEST_FEE_CHARGE`). BO cắt 1 cục/tháng (phí QL/thuế/perf…); charge_date→C_BUSINESS_DATE; SDI net-off payable (trừ tổng mọi loại; dedup source_event_id). |
| 6 | **Cashflow** | `T_SI_CASHFLOW_EVENT` | C_SI_ACCOUNT, business_date, event_type[INITIAL\|TOPUP\|SIP\|INTEREST_IN\|WITHDRAW], amount | **SPARSE** — chỉ KH có nạp/rút/SIP. Dùng cho CF_t (PnL/unit), KHÔNG cộng lại cash. |

### C. Market data → SDI (feed EOD)

> **Cơ chế nạp giá (an toàn 2 lớp):** app gọi **`SP_INGEST_PRICE_DAILY(@p_json, @p_business_date, @p_expected_count?)`** (db/02_SP_ENGINE.sql) — 1 batch JSON. **(1) Type-safe:** `OPENJSON WITH` ép kiểu (string sai/thiếu → NULL → bắt ở validate). **(2) All-or-nothing:** validate TOÀN batch trước (ticker rỗng / ref·close NULL hoặc ≤0 / is_ex_rights ∉{0,1} / TRÙNG mã / lệch `@p_expected_count`) → sai bất kỳ ⇒ **TỪ CHỐI cả batch, KHÔNG ghi dòng nào** (err 20/21/22); hợp lệ → `MERGE` upsert (date,ticker) atomic, idempotent. ⇒ **không bao giờ nạp một-phần**. Chốt chặn cuối: **completeness gate trong `SP_EOD_RUN_INDEX`** — thiếu giá DÙ 1 mã danh mục mẫu (active) @d ⇒ `err=11`, **KHÔNG tính index** (tránh master index SAI). `SP_INGEST_PRICE_DAILY` chỉ NẠP giá, KHÔNG set MKT_DATA READY (app gọi `SP_EOD_SET_SOURCE_READY` sau).

| # | Luồng | Bảng/payload | Trường | Tính chất |
|---|---|---|---|---|
| 7a | **Giá EOD (gộp CA)** — `SP_INGEST_PRICE_DAILY` | `T_PRICE_DAILY` | ticker, business_date, **ref_price** (NOT NULL), close_price, **is_ex_rights** (1/0, default 0) | Theo **universe mã** (không theo KH). `ref_price` = giá tham chiếu đầu phiên sở publish MỖI ngày (phiên thường = close hôm trước; ex-rights = giá sau chia) → J12 self-contained, không tra ngày trước. `is_ex_rights` = metadata đánh dấu ngày có quyền. Bỏ bảng CA riêng — type/ratio/cash_div đã vào NAV qua FO sync. |
| 7b | **Benchmark** | `T_BENCHMARK_DAILY` | benchmark_code (VNINDEX…), business_date, index_value | 1 dòng/benchmark/ngày. |

### D. SDI → Asset — chỉ còn Master Index (BRD 2026-06-22)

> **GỠ 2 producer / GIỮ 1 (BRD 2026-06-22).** BO, FO, Market data nay **đẩy dữ liệu THẲNG sang Asset** và **Asset tự tính** (tài sản gộp + NAV ròng + Unit/TWR/return). **Đã GỠ khỏi `db/05_API.sql` 2 producer:** `SP_GET_ASSET_SNAPSHOT` (8a — per-SI tài sản KH), `SP_GET_ASSET_MASTER_SNAPSHOT` (8b/8c — master NAV/perf). **VẪN GIỮ `SP_GET_ASSET_INDEX_SNAPSHOT`** (8d — Master Index): SDI **vẫn đẩy danh mục mẫu sang Asset** theo **luồng RIÊNG kích khi BO báo price-ready** (path `SP_EOD_RUN_INDEX`) — đây là luồng SDI→Asset DUY NHẤT còn lại.
>
> **Asset ĐỦ data, KHÔNG thiếu gap:** payable = **BO đẩy phí QL lũy kế/ngày** (accrued) → Asset tự tính NAV ròng; Master Index = SDI vẫn đẩy (8d); Unit/UnitPrice/TWR/return = Asset tự tính từ NAV series + cashflow (FO). Còn lại chỉ là **điểm reconcile** (R1 payable BO-vs-SDI: rate/day_count/AUM/quy ước ngày dương lịch; R3 TWR methodology khớp seed+công thức SDI) — **không phải "thiếu dữ liệu"**. Xem **[SDI-asset-gap.md](./SDI-asset-gap.md)** (nguồn sự thật reconcile).
>
> SDI **giữ engine + read API** (`SP_GET_SI_*` FR-01..06, PM `SP_GET_MASTER_*`) phục vụ **giao diện riêng của SDI** (NAV/index/perf) — không đổi.
>
> *(Bảng cũ liệt kê 8a current snapshot KH `T_SI_NAV_CURRENT` / 8b current snapshot master `T_MASTER_NAV_CURRENT` / 8c master series `T_MASTER_NAV_BALANCE` / 9 lịch sử KH API pull — KHÔNG còn áp dụng. Master Index series `T_MASTER_INDEX_DAILY` VẪN đẩy qua 8d.)*

> **Điểm mấu chốt (vẫn đúng):** FO→SDI nặng (per-mã, dense, nạp THẲNG current). Lịch sử dài hạn = `T_SI_NAV_BALANCE` (~2,5 tỷ dòng) SDI giữ + serve **read API cho UI SDI** (FR-01..06). Holdings/cash history = **interval (SCD-2) full history, KHÔNG trùng lặp** (holding bất biến = 1 dòng) maintain bằng DIFF **tại INGEST (per-event)** — không trong EOD core.

> **Lưu ý FR-06:** `SP_GET_ASSET_REPORT` (FR-06, báo cáo tài sản KH) là **read API của KH — VẪN GIỮ** (đừng nhầm với `SP_GET_ASSET_SNAPSHOT` producer đã gỡ).

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
| ~~(8a) Current snapshot KH SDI→Asset~~ | — | N/A | N/A | N/A |
| ~~(8b) Current snapshot master SDI→Asset~~ | — | N/A | N/A | N/A |
| ~~(8c) master series/ngày SDI→Asset~~ | — | N/A | N/A | N/A |

> **(8a/8b/8c) ĐÃ GỠ (BRD 2026-06-22):** SDI không còn feed sang Asset (BO/FO/Market đẩy thẳng). Xem [SDI-asset-gap.md](./SDI-asset-gap.md).

**Sparse feeds (5)(6):** phụ thuộc sự kiện —
- *Ngày thường:* cashflow ≈ % nhỏ tiểu khoản (chỉ KH có SIP/nạp/rút); cổ tức/phí ≈ 0.
- *Ngày cao điểm:* SIP định kỳ → tới ~50% tiểu khoản; ngày chia cổ tức 1 mã phổ biến → ~tiểu khoản đang nắm mã đó. Ví dụ medium, mã chia được ~30% tiểu khoản nắm → ~15.000 dòng.

### 4.2 Ước lượng dung lượng (raw, chưa nén)

> Ước theo bytes/row xấp xỉ; prod nén PAGE/CCI thường giảm **~3–5×**.

| Feed | ~bytes/row | small | medium | large |
|---|---:|---:|---:|---:|
| (3) Holdings snapshot | ~60 | ~3,6 MB | ~75 MB | ~375 MB |
| (4) Cash snapshot | ~28 | ~0,08 MB | ~1,4 MB | ~7 MB |
| ~~(8a) Current snapshot KH SDI→Asset~~ | — | N/A | N/A | N/A |
| **Tổng FO→SDI/ngày** (3+4) | | **~3,7 MB** | **~76 MB** | **~382 MB** |
| ~~**Tổng SDI→Asset/ngày** (8a+b+c)~~ | | N/A | N/A | N/A |

→ FO→SDI feed = holdings theo từng mã (dense). **SDI→Asset feed ĐÃ GỠ (BRD 2026-06-22)** — BO/FO/Market đẩy thẳng sang Asset (xem [SDI-asset-gap.md](./SDI-asset-gap.md)). *(prod ~20M holdings ⇒ feed FO→SDI ~1,2 GB/ngày raw.)*

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
2. **FO cash là nguồn tiền duy nhất, đã NET** phí GD + thuế GD + SIP → SDI không re-apply (phí ACCRUE QL/thuế/perf SDI quản riêng qua payable theo `T_FEE_CONFIG`).
3. **Biến động holdings/ngày** SDI suy ra on-demand (qty(D)−qty(D-1)), KHÔNG cần FO gửi delta.
4. **Cổ tức/phí & cashflow là sự kiện sparse** — FO chỉ gửi khi phát sinh; capture đúng ngày + số tiền khớp thời điểm FO ghi vào cash.
5. **J13 RECONCILE là cổng publish nội bộ:** Σ holdings/NAV SDI vs FO, Σ customer NAV vs master NAV, Σ unit — lệch > ngưỡng ⇒ **chặn publish kết quả EOD** (RECONCILE=BREAK, không COMPLETED). *(Trước đây "chặn J14 snapshot push Asset"; nay không còn push SDI→Asset — reconcile vẫn là cổng nội bộ.)*
6. **SDI→Asset: chỉ còn Master Index (BRD 2026-06-22).** Đã GỠ 2 producer push tài sản KH + master NAV/perf (`SP_GET_ASSET_SNAPSHOT`, `SP_GET_ASSET_MASTER_SNAPSHOT`) — BO/FO/Market đẩy thẳng, Asset tự tính. **VẪN GIỮ `SP_GET_ASSET_INDEX_SNAPSHOT`**: SDI đẩy Master Index khi BO price-ready (luồng `SP_EOD_RUN_INDEX`). BO còn đẩy phí QL accrued/ngày → Asset. Lịch sử KH SDI giữ + serve qua **read API cho UI riêng của SDI**. Reconcile R1/R3 — xem [SDI-asset-gap.md](./SDI-asset-gap.md).
7. **J0 GATE** chờ đủ nguồn (FO holdings+cash, model_weight, giá, CA, benchmark, cashflow) sẵn sàng cho @d mới chạy.

---

## 6. Giả định & lưu ý
- Bytes/row là ước lượng raw để so sánh tương đối — số thật phụ thuộc kiểu cột/nén.
- Kịch bản small/medium/large theo `bench.ps1`; **prod thực** ≈ 200K KH (~20M holdings) — large (6,25M) ≈ 31% prod.
- Sparse feeds (cổ tức/phí, cashflow, CA, rebalance) biến động mạnh theo lịch sự kiện → cần đo riêng theo lịch SIP/chia cổ tức thực tế, không suy tuyến tính từ holdings.
- Thời gian EOD large chưa đo — chạy `./bench.ps1 -Scale large` để lấy số thật trước khi cam kết SLA cửa sổ EOD.
