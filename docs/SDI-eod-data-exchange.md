# SDI — Hợp đồng trao đổi dữ liệu EOD: FO/Market/BO → SDI

Tài liệu tổng hợp **dữ liệu cuối ngày (EOD)** các hệ thống cần trao đổi: ai gửi gì cho ai, payload cụ thể, và **báo cáo định lượng** theo 3 kịch bản small / medium / large.

> **⚠️ BRD 2026-06-22 + asset-sync:** SDI **KHÔNG còn đồng bộ tài sản KH/NAV-perf master sang Asset** (2 producer per-SI + master ĐÃ GỠ). BO/FO/Market đẩy dữ liệu **THẲNG sang Asset** (Asset tự tính NAV RÒNG). **[BRD asset-sync] Asset gửi NAV RÒNG trực tiếp về SDI** (đã trừ phí QL — model realized; **BO KHÔNG gửi số phí lũy kế accrued**) → SDI **KHÔNG accrue phí QL**, `AUM = NAV` (không tách payable). **SDI VẪN đẩy riêng Master Index** (`SP_GET_ASSET_INDEX_SNAPSHOT`, luồng price-ready BO) — luồng SDI→Asset DUY NHẤT còn lại. Tài liệu này tập trung **FO/Market/BO/Asset → SDI** (SDI phục vụ UI riêng qua read API). Đối chiếu 2 hệ + điểm reconcile: [SDI-asset-gap.md](./SDI-asset-gap.md).

> Liên quan: [SDI-spec.md](./SDI-spec.md) (công thức, job EOD J0–J14 + ingest), [SDI-db-architecture.md](./SDI-db-architecture.md) (kiến trúc DB, roll-forward, set-based).

---

## 1. Các hệ thống & vai trò

| Hệ thống | Vai trò trong luồng EOD |
|---|---|
| **FO** (Front Office) | Tính tỷ trọng danh mục mẫu (model_weight); **đặt & khớp lệnh MP trực tiếp trên TK từng KH**; sở hữu tiền (trừ thuế GD vào cash). **[BRD asset-sync] Với SDI: nguồn sự thật = holdings** (composition); NAV/tiền nay từ Asset, cổ tức/phí không gửi SDI nữa. |
| **BO** (Back Office) | **Cắt phí** của KH (phí QL/thuế/perf…, 1 cục/tháng) khi phát sinh thật (qua cash). **[BRD asset-sync] BO KHÔNG gửi số phí lũy kế (accrued)** — phí chỉ giảm tài sản khi cắt thật, đã phản ánh trong NAV ròng Asset gửi (model realized). SDI không nhận event cắt phí, không quản payable. |
| **Market data** | Cấp giá EOD, corporate action, chỉ số benchmark (VN-Index…). (Nguồn riêng, không phải FO.) |
| **SDI** | **[BRD asset-sync]** Nhận **NAV RÒNG từ Asset** (per-SI) + FO holdings (composition) → **ingest NAV thẳng** (`SP_INGEST_ASSET_NAV`), derive Unit/Unit Price, PnL, TWR, MWR; tính Master Index (giá BO × weight). **KHÔNG accrue phí QL, không quản payable** (`AUM = stock + cash = NAV`). Phục vụ **giao diện riêng của SDI** (NAV/index/perf) qua read API (FR-01..06, PM). |
| **Asset** | **Nhận dữ liệu THẲNG từ BO/FO/Market → Asset tự tính NAV RÒNG** (đã trừ phí QL — model realized). **[BRD asset-sync] Asset GỬI NAV RÒNG + components (stock/cash) + cash_in/out per-SI về SDI** (nguồn NAV của SDI). SDI **KHÔNG còn đẩy** tài sản KH/NAV-perf master sang Asset, **CHỈ còn đẩy Master Index** (`SP_GET_ASSET_INDEX_SNAPSHOT`, price-ready BO). Reconcile còn lại: NAV_CONSISTENCY, cashflow 2 nguồn, holdings, TWR methodology. Xem [SDI-asset-gap.md](./SDI-asset-gap.md). |
| **SMO** | Tầng hiển thị (đọc qua Asset). |

**Nguyên tắc nền [BRD asset-sync]:** **Asset gửi NAV RÒNG trực tiếp** (`NAV = nav`, đã trừ phí QL — model realized) + components `stock_value` + `cash` (tổng). `AUM = stock + cash = NAV` (không tách payable). SDI **không tự định giá NAV, không accrue/quản phí**. FO holdings GIỮ cho composition (đối soát `Σ FO×giá ≈ Asset stock_value`).

---

## 2. Sơ đồ luồng EOD

```
        ┌─────────────────────── trong ngày ───────────────────────┐
SDI ──(1) rebalance_request (trigger REBALANCE/DEPLOY/REDEEM) ──────▶ FO
        └────────────────────────────────────────────────────────────┘

        ┌──────────────────────── cuối ngày (EOD) ─────────────────────┐
FO  ──(2) model_weight ─────────────────────────────────────────────▶ SDI
FO  ──(3) holdings snapshot (KH×master×mã) — chỉ composition/near-realtime ▶ SDI
Asset ─(4) [BRD asset-sync] NAV RÒNG + stock_value + cash(tổng) + cash_in/out per-SI ▶ SDI
SDI ──(6) cashflow nạp/rút/SIP (SDI là originator) ─────────────────── (đối soát Asset)
Mkt ──(7) giá EOD + corporate action + benchmark ──────────────────▶ SDI
        │
        │  INGEST-NAV (Asset, per-SI/ngày GD): SP_INGEST_ASSET_NAV → T_SI_ASSET_DAILY + derive
        │  FO holdings ingest (Kafka per-KH): SP_INGEST_CUSTOMER → holdings + interval hist (composition)
        │  SDI EOD (SP_EOD_RUN): J0 GATE → INGEST-NAV derive (NAV→PnL→Unit)
        │       → J11 master agg → J12 Index → J12B TE accum → J13 RECONCILE (cổng publish nội bộ) → J14 build snapshot (nội bộ)
        ▼
SDI ──▶ giao diện riêng SDI (read API FR-01..06, PM) — NAV/index/perf
SDI ──(8d) Master Index snapshot (SP_GET_ASSET_INDEX_SNAPSHOT, khi BO price-ready) ──▶ Asset   ✅ GIỮ

   ❌ (8a per-SI tài sản)/(8b·8c master NAV-perf)/(9 API pull) SDI → Asset — ĐÃ GỠ per BRD 2026-06-22.
      BO/FO/Market đẩy THẲNG sang Asset; Asset tính NAV RÒNG (phí QL đã trừ) rồi GỬI về SDI. Xem SDI-asset-gap.md.
        └────────────────────────────────────────────────────────────┘
```

Thứ tự: **(1) trong ngày** (SDI kích FO rebalance) → **(2,3,7) FO/Market đẩy EOD** + **[BRD asset-sync] (4) Asset gửi NAV RÒNG per-SI** (`SP_INGEST_ASSET_NAV`) → SDI ingest NAV thẳng + derive + **J13 reconcile là cổng publish nội bộ** (break > ngưỡng ⇒ chặn publish kết quả EOD). **(8a/8b/8c/9) SDI→Asset ĐÃ GỠ:** per-SI tài sản + master NAV/perf không còn push. **Vẫn GIỮ (8d): SDI đẩy Master Index** (`SP_GET_ASSET_INDEX_SNAPSHOT`) khi BO price-ready — luồng SDI→Asset DUY NHẤT còn lại. **BO KHÔNG gửi số phí lũy kế** (phí QL đã trừ sẵn trong NAV ròng Asset). Xem [SDI-asset-gap.md](./SDI-asset-gap.md).

---

## 3. Chi tiết từng luồng (payload cụ thể)

### A. SDI → FO

| # | Luồng | Bảng/payload | Trường | Tần suất |
|---|---|---|---|---|
| 1 | **Rebalance trigger** | `T_REBALANCE_REQUEST` | request_id, C_MASTER_CODE, business_date, type[REBALANCE\|DEPLOY\|REDEEM], status | **SPARSE** — chỉ khi cần tái cân bằng/giải ngân/rút. KHÔNG chứa weights (FO tự tính). |

### B. FO → SDI (feed EOD)

> **Cơ chế: Kafka per-KH (1 event = 1 KH, gồm các sub-account).** App đọc event → `SP_INGEST_CUSTOMER` (JSON) xử lý NGAY khi nhận (forward). **[BRD asset-sync]** FO event giờ **HOLDINGS-ONLY** (holdings→current + interval history) — **bỏ phần cash-state + cổ tức/phí** (NAV/tiền nay từ Asset; phí QL không thuộc SDI). Forward-only.

| # | Luồng | Bảng/payload | Trường | Tính chất |
|---|---|---|---|---|
| 2 | **Model weight** | `T_MASTER_PORTFOLIO_TICKER` | C_MASTER_CODE, effective_date, ticker, target_weight (Σ=100%) | Version theo effective_date; **chỉ đẩy khi đổi** rổ. |
| 3 | **Holdings** (trong event KH) | → `T_SI_PORTFOLIO_HOLDING` (current) + diff `T_SI_HOLDING_HIST` | C_SI_ACCOUNT, ticker, quantity, avg_cost | Mỗi event mang holdings từng sub-account của KH; ingest overwrite current + đóng/mở interval (no-dup). |
| ~~4~~ | ~~**Tiền (3 khoản)** FO→SDI~~ **[BRD asset-sync] GỠ khỏi FO** — tiền nay từ Asset (mục E); `T_SI_CASH_HIST` đã bỏ | — | NAV/tiền không còn lấy từ FO cash-state | |
| ~~5~~ | ~~**Cổ tức + phí lưu ký** FO→SDI~~ **[BRD asset-sync] ĐÃ GỠ** (`T_SI_INCOME_FEE` bỏ) — cổ tức/income đã trong NAV/cash Asset | — | — | |
| ~~5b~~ | ~~**Phí ACCRUE đã cắt** BO→SDI~~ **[BRD asset-sync] ĐÃ GỠ** (`SP_INGEST_FEE_CHARGE` bỏ) — BO KHÔNG gửi số phí lũy kế; phí QL đã trừ trong NAV ròng Asset (model realized) | — | — | |
| 6 | **Cashflow** (SDI là originator) | `T_SI_CASHFLOW_EVENT` | C_SI_ACCOUNT, business_date, event_type[INITIAL\|TOPUP\|SIP\|INTEREST_IN\|WITHDRAW], amount | **SPARSE** — chỉ KH có nạp/rút/SIP. Dùng cho CF_t (PnL/unit) + đối soát với `cash_in/out` Asset. |

### C. Market data → SDI (feed EOD)

> **Cơ chế nạp giá (an toàn 2 lớp):** app gọi **`SP_INGEST_PRICE_DAILY(@p_json, @p_business_date, @p_expected_count?)`** (db/02_SP_ENGINE.sql) — 1 batch JSON. **(1) Type-safe:** `OPENJSON WITH` ép kiểu (string sai/thiếu → NULL → bắt ở validate). **(2) All-or-nothing:** validate TOÀN batch trước (ticker rỗng / ref·close NULL hoặc ≤0 / is_ex_rights ∉{0,1} / TRÙNG mã / lệch `@p_expected_count`) → sai bất kỳ ⇒ **TỪ CHỐI cả batch, KHÔNG ghi dòng nào** (err 20/21/22); hợp lệ → `MERGE` upsert (date,ticker) atomic, idempotent. ⇒ **không bao giờ nạp một-phần**. Chốt chặn cuối: **completeness gate trong `SP_EOD_RUN_INDEX`** — thiếu giá DÙ 1 mã danh mục mẫu (active) @d ⇒ `err=11`, **KHÔNG tính index** (tránh master index SAI). `SP_INGEST_PRICE_DAILY` chỉ NẠP giá, KHÔNG set MKT_DATA READY (app gọi `SP_EOD_SET_SOURCE_READY` sau).

| # | Luồng | Bảng/payload | Trường | Tính chất |
|---|---|---|---|---|
| 7a | **Giá EOD (gộp CA)** — `SP_INGEST_PRICE_DAILY` | `T_PRICE_DAILY` | ticker, business_date, **ref_price** (NOT NULL), close_price, **is_ex_rights** (1/0, default 0) | Theo **universe mã** (không theo KH). `ref_price` = giá tham chiếu đầu phiên sở publish MỖI ngày (phiên thường = close hôm trước; ex-rights = giá sau chia) → J12 self-contained, không tra ngày trước. `is_ex_rights` = metadata đánh dấu ngày có quyền. Bỏ bảng CA riêng — type/ratio/cash_div đã vào NAV qua FO sync. |
| 7b | **Benchmark** | `T_BENCHMARK_DAILY` | benchmark_code (VNINDEX…), business_date, index_value | 1 dòng/benchmark/ngày. |

### E. Asset → SDI (feed EOD) — **[BRD asset-sync] MỚI**

> **Cơ chế:** Asset gửi 1 batch JSON per-SI/ngày GD → `SP_INGEST_ASSET_NAV` (MERGE `T_SI_ASSET_DAILY` idempotent theo date,si) rồi SDI **derive** Unit/UP/PnL/return. Carry-forward T7/CN/lễ (Asset chỉ gửi ngày GD).

| # | Luồng | Bảng/payload | Trường | Tính chất |
|---|---|---|---|---|
| 4 | **NAV ròng + components** Asset→SDI | `T_SI_ASSET_DAILY` → derive `T_SI_BALANCE`/`_CURRENT` | si_account, **nav** (RÒNG, đã trừ phí QL), stock_value, **cash** (TỔNG tiền 1 số), cash_in, cash_out | **DENSE** per-SI/ngày GD. `AUM = stock + cash = NAV`. **KHÔNG `fee_accum`** (BO không gửi số phí lũy kế — model realized). `cash_in/out` để đối soát cashflow SDI tự nhập (mục 6). |

### D. SDI → Asset — chỉ còn Master Index (BRD 2026-06-22)

> **GỠ 2 producer / GIỮ 1 (BRD 2026-06-22).** BO, FO, Market data nay **đẩy dữ liệu THẲNG sang Asset** và **Asset tự tính** (tài sản gộp + NAV ròng + Unit/TWR/return). **Đã GỠ khỏi `db/05_API.sql` 2 producer:** `SP_GET_ASSET_SNAPSHOT` (8a — per-SI tài sản KH), `SP_GET_ASSET_MASTER_SNAPSHOT` (8b/8c — master NAV/perf). **VẪN GIỮ `SP_GET_ASSET_INDEX_SNAPSHOT`** (8d — Master Index): SDI **vẫn đẩy danh mục mẫu sang Asset** theo **luồng RIÊNG kích khi BO báo price-ready** (path `SP_EOD_RUN_INDEX`) — đây là luồng SDI→Asset DUY NHẤT còn lại.
>
> **[BRD asset-sync] Asset ĐỦ data:** Asset tự tính **NAV RÒNG** (phí QL đã trừ — model realized; BO KHÔNG gửi số phí lũy kế) rồi GỬI về SDI (mục E); Master Index = SDI vẫn đẩy (8d). Còn lại chỉ là **điểm reconcile** (NAV_CONSISTENCY `nav` vs `stock+cash`; cashflow 2 nguồn SDI vs Asset; holdings `ΣFO×giá` vs Asset stock; TWR methodology khớp seed+công thức) — **không phải "thiếu dữ liệu"**. Xem **[SDI-asset-gap.md](./SDI-asset-gap.md)** (nguồn sự thật reconcile).
>
> SDI **giữ engine + read API** (`SP_GET_SI_*` FR-01..06, PM `SP_GET_MASTER_*`) phục vụ **giao diện riêng của SDI** (NAV/index/perf) — không đổi.
>
> *(Bảng cũ liệt kê 8a current snapshot KH `T_SI_CURRENT` / 8b current snapshot master `T_MASTER_CURRENT` / 8c master series `T_MASTER_BALANCE` / 9 lịch sử KH API pull — KHÔNG còn áp dụng. Master Index series `T_MASTER_INDEX_DAILY` VẪN đẩy qua 8d.)*

> **Điểm mấu chốt (vẫn đúng):** FO→SDI nặng (per-mã, dense, nạp THẲNG current). Lịch sử dài hạn = `T_SI_BALANCE` (~2,5 tỷ dòng) SDI giữ + serve **read API cho UI SDI** (FR-01..06). Holdings history = **interval (SCD-2) full history, KHÔNG trùng lặp** (holding bất biến = 1 dòng) maintain bằng DIFF **tại INGEST (per-event)** — không trong EOD core. *([BRD asset-sync] `T_SI_CASH_HIST` đã bỏ — tiền/NAV nay từ Asset.)*

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
| **(4) NAV ròng + components** **[BRD asset-sync] Asset→SDI** | per-SI (KH×master) | 3.000 | 50.000 | 250.000 |
| (2) Model weight FO→SDI | master×mã (khi đổi) | 60 | 125 | 125 |
| ~~(5) Cổ tức/phí FO→SDI~~ **[BRD asset-sync] ĐÃ GỠ** | — | N/A | N/A | N/A |
| (6) Cashflow (SDI originator) | sparse | *0 → ~tiểu khoản có SIP/nạp/rút* | | |
| (7a) Giá EOD Mkt→SDI | universe mã | 20 | 25 | 25 *(prod ~1.600)* |
| (7b/7c) CA + benchmark | sparse / 1-vài | nhỏ | nhỏ | nhỏ |
| ~~(8a) Current snapshot KH SDI→Asset~~ | — | N/A | N/A | N/A |
| ~~(8b) Current snapshot master SDI→Asset~~ | — | N/A | N/A | N/A |
| ~~(8c) master series/ngày SDI→Asset~~ | — | N/A | N/A | N/A |

> **(8a/8b/8c) ĐÃ GỠ (BRD 2026-06-22):** SDI không còn feed sang Asset (BO/FO/Market đẩy thẳng). Xem [SDI-asset-gap.md](./SDI-asset-gap.md).

**Sparse feed (6) cashflow:** phụ thuộc sự kiện —
- *Ngày thường:* cashflow ≈ % nhỏ tiểu khoản (chỉ KH có SIP/nạp/rút).
- *Ngày cao điểm:* SIP định kỳ → tới ~50% tiểu khoản. *([BRD asset-sync] cổ tức/phí không còn là feed riêng — đã nằm trong NAV/cash Asset.)*

### 4.2 Ước lượng dung lượng (raw, chưa nén)

> Ước theo bytes/row xấp xỉ; prod nén PAGE/CCI thường giảm **~3–5×**.

| Feed | ~bytes/row | small | medium | large |
|---|---:|---:|---:|---:|
| (3) Holdings snapshot FO→SDI | ~60 | ~3,6 MB | ~75 MB | ~375 MB |
| (4) NAV ròng + components **[BRD asset-sync] Asset→SDI** | ~40 | ~0,12 MB | ~2 MB | ~10 MB |
| ~~(8a) Current snapshot KH SDI→Asset~~ | — | N/A | N/A | N/A |
| **Tổng feed→SDI/ngày** (3+4) | | **~3,7 MB** | **~77 MB** | **~385 MB** |
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

1. **Snapshot overwrite, idempotent:** FO nạp toàn bộ holdings THẲNG vào current mỗi EOD; **[BRD asset-sync]** Asset NAV ingest idempotent (MERGE theo date,si); chạy lại 1 ngày cho cùng kết quả (overwrite, không cộng dồn).
8. **Holdings history tách rời (interval):** DIFF **tại INGEST (per-event Kafka)** current → `T_SI_HOLDING_HIST` (SCD-2 valid_from/valid_to, **full history, no-dup**) — EOD core chỉ đọc current. *([BRD asset-sync] `T_SI_CASH_HIST` đã bỏ — tiền từ Asset.)*
2. **[BRD asset-sync] NAV/tiền từ Asset** (NAV ròng, đã trừ phí QL — model realized). SDI **không tự định giá, không accrue/quản phí** (`AUM = stock + cash = NAV`). FO cash-state không còn gửi SDI.
3. **Biến động holdings/ngày** SDI suy ra on-demand (qty(D)−qty(D-1)), KHÔNG cần FO gửi delta.
4. **Cashflow là sự kiện sparse** — SDI là originator (nạp/rút/SIP), đối soát với `cash_in/out` Asset. *([BRD asset-sync] cổ tức/phí không còn là feed FO→SDI.)*
5. **J13 RECONCILE là cổng publish nội bộ:** **[BRD asset-sync]** NAV_CONSISTENCY (`nav` vs `stock+cash`) + cashflow 2 nguồn + holdings (`ΣFO×giá` vs Asset stock) + Σ customer NAV vs master NAV — lệch > ngưỡng ⇒ **chặn publish kết quả EOD** (RECONCILE=BREAK). Reconcile vẫn là cổng nội bộ (không còn push SDI→Asset).
6. **SDI→Asset: chỉ còn Master Index (BRD 2026-06-22).** Đã GỠ 2 producer push tài sản KH + master NAV/perf (`SP_GET_ASSET_SNAPSHOT`, `SP_GET_ASSET_MASTER_SNAPSHOT`). **VẪN GIỮ `SP_GET_ASSET_INDEX_SNAPSHOT`**: SDI đẩy Master Index khi BO price-ready (`SP_EOD_RUN_INDEX`). **[BRD asset-sync] BO KHÔNG gửi số phí lũy kế** (phí QL đã trừ trong NAV ròng Asset). Lịch sử KH SDI giữ + serve qua **read API cho UI riêng của SDI**. Reconcile — xem [SDI-asset-gap.md](./SDI-asset-gap.md).
7. **J0 GATE** chờ đủ nguồn (**[BRD asset-sync] Asset NAV per-SI**, FO holdings, model_weight, giá, benchmark, cashflow) sẵn sàng cho @d mới chạy.

---

## 6. Giả định & lưu ý
- Bytes/row là ước lượng raw để so sánh tương đối — số thật phụ thuộc kiểu cột/nén.
- Kịch bản small/medium/large theo `bench.ps1`; **prod thực** ≈ 200K KH (~20M holdings) — large (6,25M) ≈ 31% prod.
- Sparse feeds (cashflow, CA, rebalance) biến động mạnh theo lịch sự kiện → cần đo riêng theo lịch SIP thực tế, không suy tuyến tính từ holdings. *([BRD asset-sync] cổ tức/phí không còn feed riêng.)*
- Thời gian EOD large chưa đo — chạy `./bench.ps1 -Scale large` để lấy số thật trước khi cam kết SLA cửa sổ EOD.

### 6.1 Sửa quá khứ — RE-INGEST (`SP_EOD_RECOMPUTE_RANGE` đã gỡ)

> **[BRD asset-sync]** Vì NAV là số **Asset gửi** (không phải SDI dựng từ holdings×giá−payable), `SP_EOD_RECOMPUTE_RANGE` (reconstruct NAV từ history) **đã GỠ**. Sửa quá khứ = **RE-INGEST**.

- **Use case:** NAV một ngày quá khứ SAI → **Asset gửi lại `T_SI_ASSET_DAILY` ngày đó** (`SP_INGEST_ASSET_NAV` idempotent MERGE date,si) → SDI **derive lại** unit/UP/PnL/return + lũy kế TE **từ ngày sửa trở đi** (đã có NAV+flow history per (date,si) ở `T_SI_BALANCE`).
- **Index master** sửa riêng (độc lập NAV): `SP_EOD_RECOMPUTE_INDEX_RANGE(@from,@to)` — chỉ cần giá BO + target weight.
