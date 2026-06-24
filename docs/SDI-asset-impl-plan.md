# Kế hoạch implement BRD Asset-NAV-sync (nhánh `feat/brd-asset-nav-sync`)

> **CHƯA CODE** — bản kế hoạch để duyệt. Quyết định nghiệp vụ: xem [SDI-asset-handover.md](SDI-asset-handover.md).
> Nhánh độc lập, KHÔNG merge `main`. Build/test: fresh DB mỗi lần (DDL edit an toàn).

## Nguyên tắc
- Asset = nguồn NAV/tiền/phí (số tổng per-SI, không per-mã). SDI: ingest → derive unit/UP/PnL/return → SUM master → index (của SDI) → PM serve → reconcile (đo vênh).
- FO holdings **giữ** (composition + near-realtime future). Cashflow nạp/rút **SDI vẫn nhập** (đối soát với Asset).

---

## P0 — Schema (`01_TABLES.sql`)

**THÊM:**
- `T_SI_ASSET_DAILY` (raw feed Asset, audit + re-ingest history): PK `(C_BUSINESS_DATE, C_SI_ACCOUNT)` · `C_STOCK_VALUE, C_CASH, C_PENDING_CASH, C_DIV_CASH, C_FEE_ACCUM, C_CASH_IN, C_CASH_OUT` · `C_INGESTED_AT`.
- `T_EOD_PIPELINE.C_ASSET_NAV_STATUS` (PENDING|READY) + `C_ASSET_NAV_AT` — nguồn mới để gate.
- `T_EOD_RECON_BREAK`: đã có `C_VALUE_SDI/C_VALUE_CHECK/C_DIFF` → tái dùng; thêm `C_CHECK_NAME` mới (CASHFLOW_MISMATCH, HOLDINGS_MISMATCH, NAV_BRIDGE). Cân nhắc cột `C_WITHIN_THRESHOLD BIT` để log diff cả khi không break (đo vênh).

**SỬA:**
- `T_SI_NAV_BALANCE`: bỏ `C_PAYABLE_FEE`; thêm `C_FEE_ACCUM` (lũy kế từ Asset), `C_CASH_IN`, `C_CASH_OUT`. Giữ NAV/UNIT/UNIT_PRICE/PNL/RETURN (giờ derive từ ingest) + cột TE.
- `T_SI_NAV_CURRENT`: bỏ `C_PAYABLE_FEE`; cash/pending/div giờ từ Asset (giữ cột, đổi nguồn ghi).
- `T_EOD_WORK`: bỏ cột phí (`C_PAYABLE_FEE`, `C_FEE_CUT`); có thể bỏ luôn nếu ingest ghi thẳng (đánh giá ở P2).

**BỎ:**
- `T_FEE_CONFIG`, `T_SI_INCOME_FEE` (chi tiết phí), `T_SI_CASH_HIST` (interval cash cho NAV).

**GIỮ NGUYÊN:** `T_PRICE_DAILY`, `T_MASTER_*`, `T_SI_PORTFOLIO(_HOLDING)`, `T_SI_HOLDING_HIST`, `T_SI_CASHFLOW_EVENT`, `T_TICKER_INDUSTRY`, `T_EOD_RUN/PIPELINE`, `T_MASTER_PM_CONFIG`.

---

## P1 — Ingest + derive (`02_SP_ENGINE.sql`)

**THÊM `SP_INGEST_ASSET_NAV @p_json`** (per-SI, 1 batch/ngày GD):
- Parse JSON per-SI `{si_account, stock_value, cash, pending, div_cash, fee_accum, cash_in, cash_out}` → ghi `T_SI_ASSET_DAILY` (idempotent MERGE theo date,si).
- **Derive** per-SI (quy ước §4 handover, init UP=10.000, prior-day historic):
  ```
  NAV   = stock_value + cash + pending + div_cash − fee_accum   (fee lũy kế đã trừ? -> xác nhận net; nếu Asset gửi NAV net thì lắp thẳng)
  UP_{t-1}, units_{t-1} ← T_SI_NAV_BALANCE @prev (UDF_PREV_BUSINESS_DATE); thiếu → UP=10.000
  Δunits = (cash_in − cash_out)/UP_{t-1};  units_t = units_{t-1}+Δunits
  UP_t   = NAV_t/units_t;  return_t = UP_t/UP_{t-1}−1;  PnL_t = NAV_t−NAV_{t-1}+cash_out−cash_in
  ```
- Ghi `T_SI_NAV_BALANCE` (DELETE+INSERT @d) + roll `T_SI_NAV_CURRENT` (nếu @d mới nhất). Idempotent + scoped như engine cũ.
- err convention chuẩn (@p_err_code/@p_err_msg OUT).

**BỎ:** `SP_EOD_COMPUTE`, `SP_EOD_COMPUTE_CORE` (J06 accrue/J07 MTM/J08 NAV/J09/J10) cho EOD; `SP_INGEST_FEE_CHARGE`; `SP_EOD_RECOMPUTE_RANGE` (NAV).
**GIỮ năng lực MTM** (holdings×giá) tách riêng cho near-realtime future (chưa build — ghi TODO).

**SỬA `SP_INGEST_CUSTOMER`** → **holdings-only** (bỏ phần cash-state + fees; giữ holdings + interval `T_SI_HOLDING_HIST`).

---

## P2 — Pipeline (`02_SP_ENGINE.sql`)

**SỬA `SP_EOD_RUN`** — thứ tự mới:
```
gate: MKT_DATA + FO_INGEST(holdings) + ASSET_NAV + INDEX = READY/DONE
J_INGEST_NAV : SP_INGEST_ASSET_NAV đã chạy (hoặc EXEC trong pipeline)  → derive
J11 SI_AGG   : SUM per-SI (ingest) → master  (sửa: nguồn = NAV_BALANCE ingest, không từ work)
J12B TE_ACCUM: như cũ (đọc return derive + index)
J13 RECONCILE: 3 check + log diff (P4)
J14 SNAPSHOT : composition từ FO holdings × giá (giữ)
```
- `SP_EOD_SET_SOURCE_READY` thêm `@p_source='ASSET_NAV'`.
- `SP_EOD_RESET`: giữ cơ chế watermark (đã có ở main) — re-ingest = chạy lại ingest.
- **Sửa quá khứ = RE-INGEST** Asset NAV ngày cũ → derive lại từ ngày đó (thay `SP_EOD_RECOMPUTE_RANGE`). Index vẫn `SP_EOD_RECOMPUTE_INDEX_RANGE`.

---

## P3 — Reconcile 3 check (`02_SP_ENGINE.sql` — `SP_EOD_RECONCILE`)

Ghi `T_EOD_RECON_BREAK` (lưu `C_DIFF` **kể cả trong ngưỡng** để đo vênh):
1. **NAV_BRIDGE** per-SI: `|NAV_t − (NAV_{t-1}+cash_in−cash_out+Δval)|` > ngưỡng.
2. **CASHFLOW_MISMATCH**: `cash_in/out` SDI (`T_SI_CASHFLOW_EVENT`) vs Asset (`T_SI_ASSET_DAILY`).
3. **HOLDINGS_MISMATCH**: `Σ(FO holdings × giá BO)` vs Asset `stock_value` per-SI/master.
- Cổng publish ở `SP_EOD_RUN` (như hiện tại): có break "cứng" → chặn J14/publish; break "đo lường" (trong ngưỡng) → chỉ log.

---

## P4 — Smoke (`03_SMOKE.sql`) + PM smoke (`07` giữ)

- **BỎ** test EOD-compute (MTM/accrue/fee breakdown/idempotent NAV).
- **THÊM**: ingest Asset NAV → assert derive đúng:
  - ngày thường (no flow): `return = NAV_t/NAV_{t-1}−1`, units giữ nguyên.
  - **ngày có nạp/rút**: units phát hành theo `UP_{t-1}`, UP_t đúng (case then chốt convention).
  - ngày đầu: UP=10.000.
- **THÊM** reconcile: bơm lệch cashflow → break CASHFLOW_MISMATCH + `C_DIFF` đúng; lệch holdings → HOLDINGS_MISMATCH; log diff trong-ngưỡng.
- **GIỮ**: index (J12 + completeness hard-fail + weight guard), PM smoke (US1–US5), composition.
- Bench (`04`): seed NAV ingest thay vì compute; đo SP_INGEST_ASSET_NAV scale 50k SI.

---

## P5 — Docs + memory (CHỈ trên nhánh này)

- Cập nhật `SDI-asset-handover.md` (đánh dấu implemented), `SDI-asset-gap.md`, `README.md`, `SDI-spec.md`.
- Memory (nhánh): `mgmt-fee-accrual-decision` (phí về Asset), `sdi-asset-sync-architecture`, `eod-pipeline-control` (luồng mới), `recompute-granularity-limits` (NAV recompute → re-ingest).

---

## Thứ tự thực thi + cổng kiểm
P0 → P1 (build + unit test derive) → P2 (pipeline chạy 1 phiên) → P3 (reconcile) → P4 (smoke xanh) → P5 (docs).
Mỗi P: build fresh DB + smoke phần liên quan trước khi sang P kế. Self-review ruthless cuối mỗi P.

## Rủi ro / cần xác nhận khi code
- `fee_accum` Asset gửi: NAV gửi đã **net** (trừ phí) hay SDI phải tự trừ? → quyết ở P1.
- Ngưỡng reconcile (b/c) — cấu hình per-master hay global.
- Near-realtime (d) — out of scope đợt này (chỉ giữ holdings + ghi TODO).
