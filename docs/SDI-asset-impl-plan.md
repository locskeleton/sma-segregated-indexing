# Kế hoạch implement BRD Asset-NAV-sync (nhánh `feat/brd-asset-nav-sync`)

> **⚠️ SUPERSEDED bởi THIN-LAYER (2026-06-26):** plan này (P0→P5, 2026-06-24) build engine **SDI derive unit/UP/PnL/TWR** từ NAV+flow. Refactor thin-layer sau đó: **Asset gửi CẢ `daily_return` (TWR)** → SDI **KHÔNG derive** (LƯU `aum`+`daily_return`, SERVE compound). Gỡ unit/unit_price/PnL-tiền/`T_SI_UNIT_LEDGER`/MWR/J09/J10. Các block "derive Δunits/UP_{t-1}" dưới = LỊCH SỬ — engine hiện hành ở [SDI-spec.md](SDI-spec.md) §9 (thin-layer).
>
> **ĐÃ IMPLEMENT (2026-06-24)** — P0→P5 xong, build + all tests GREEN trên nhánh. Quyết định nghiệp vụ: [SDI-asset-handover.md](SDI-asset-handover.md).
> Nhánh độc lập, KHÔNG merge `main`. Build/test: fresh DB mỗi lần (DDL edit an toàn).

## TRẠNG THÁI IMPLEMENT (2026-06-24)

| Phase | Kết quả |
|---|---|
| P0 schema | ✅ drop T_FEE_CONFIG/T_SI_INCOME_FEE/T_SI_CASH_HIST; thêm T_SI_ASSET_DAILY + C_ASSET_NAV_STATUS |
| P1 ingest+derive | ✅ SP_INGEST_ASSET_NAV; SP_EOD_COMPUTE seed từ Asset (bỏ MTM/accrue); bỏ fee-charge + recompute-range |
| P2 pipeline | ✅ gate ASSET_NAV + SET_SOURCE_READY 'ASSET_NAV' |
| P3 reconcile | ✅ CASHFLOW_MISMATCH + HOLDINGS_MISMATCH + lưu diff |
| P4 ripple+test | ✅ 05_API FR-06 rewrite; 03_SMOKE (15 assert GREEN); 04/08 bench; 07 PM smoke GREEN |
| P5 self-review | ✅ thêm guard ASSET_NAV completeness per-SI (err=12); docs |

**Quyết định khi code (ĐÃ CHỐT với user 2026-06-25):**
- **[BRD asset-sync] GỠ phí hoàn toàn (chốt 2026-06-25):** không còn `C_PAYABLE_FEE`/`C_FEE_ACCUM`/`fee_accum`. Asset gửi **NAV RÒNG** (phí QL đã trừ sẵn — model realized); **[thin-layer] `AUM = NAV`** (gross = net, không tách payable; bản 2026-06-25 viết `= stock + cash`, nay Asset không gửi `stock_value`). *(Thiết kế trung gian "giữ tên C_PAYABLE_FEE đổi nghĩa lũy kế / AUM_gross = NAV + fee" đã bị thay.)*
- **Asset GỬI CẢ NAV (ròng) — SDI KHÔNG tự tính/trừ.** `T_SI_ASSET_DAILY.C_AUM` ingest trực tiếp; CORE **bỏ J08** (không lắp NAV). Components (`stock`, `cash` tổng) gửi kèm để display (FR-06) + AUM + reconcile.
- **Reconcile** (2026-06-24): NAV_CONSISTENCY + HOLDINGS_MISMATCH + NAV_NEGATIVE + SI_NAV_MISMATCH + CASHFLOW_MISMATCH (5 check). **[thin-layer 2026-06-26] còn 3:** NAV_NEGATIVE, SI_NAV_MISMATCH, CASHFLOW_MISMATCH (gỡ NAV_CONSISTENCY + HOLDINGS_MISMATCH — Asset không gửi `stock_value`).
- **Sửa quá khứ = RE-INGEST** (Asset gửi lại asset_daily → chạy lại EOD ngày đó). SP_EOD_RECOMPUTE_RANGE đã bỏ.
- **err mới**: SP_EOD_RUN err=12 = thiếu Asset NAV per-SI (completeness).
- FO holdings GIỮ (composition + near-realtime future). T_EOD_WORK giữ (transient).
- **Rename `C_TOTAL_ASSET`→`C_AUM`** (master tables + PM SP + FR-06 + bench) + **[thin-layer] rename bảng** `T_SI_NAV_BALANCE`→`T_SI_BALANCE`, `T_MASTER_NAV_BALANCE`→`T_MASTER_BALANCE`, `T_SI_NAV_CURRENT`→`T_SI_CURRENT`, `T_MASTER_NAV_CURRENT`→`T_MASTER_CURRENT`; `C_LAST_NAV`→`C_LAST_AUM`. **Bỏ `C_STOCK_VALUE` cấp MASTER**; `AUM = NAV` (Asset gửi). **[thin-layer]** Asset KHÔNG gửi `stock_value` ⇒ stock không còn ở feed/reconcile. "Cổ phiếu chờ về"/"cổ tức cổ phiếu" không tồn tại trong model.

**Tests:** `03_SMOKE` (customer ingest/derive incl cashflow-day, reconcile vênh, reset, date-guard, index guards, asset-completeness) · `07_PM_SMOKE` (PM serve, seed NAV trực tiếp) · `04_BENCH`/`08_PM_BENCH` (seed asset_daily). Build 01/02/05/06 clean.

---


## Nguyên tắc
- Asset = nguồn NAV/tiền/phí + **[thin-layer] `daily_return` (TWR)** (số tổng per-SI, không per-mã). SDI: ingest → **LƯU `aum`+`daily_return`** (KHÔNG derive) → SUM master (AUM-weighted) → index (của SDI) → PM serve (compound) → reconcile.
- FO holdings **giữ** (composition + near-realtime future). Cashflow nạp/rút **SDI vẫn nhập** (đối soát với Asset).

---

## P0 — Schema (`01_TABLES.sql`)

**THÊM:**
- `T_SI_ASSET_DAILY` (raw feed Asset, audit + re-ingest history): PK `(C_BUSINESS_DATE, C_SI_ACCOUNT)` · **[thin-layer 2026-06-26]** `C_AUM, C_DAILY_RETURN, C_CASH, C_CASH_IN, C_CASH_OUT` · `C_INGESTED_AT`. **KHÔNG có `C_STOCK_VALUE`/`C_FEE_ACCUM`** (Asset gửi NAV ròng + `daily_return`; `cash` = tổng tiền 1 số). *(Bản 2026-06-24 có `C_STOCK_VALUE`, không có `C_DAILY_RETURN`.)*
- `T_EOD_PIPELINE.C_ASSET_NAV_STATUS` (PENDING|READY) + `C_ASSET_NAV_AT` — nguồn mới để gate.
- `T_EOD_RECON_BREAK`: đã có `C_VALUE_SDI/C_VALUE_CHECK/C_DIFF` → tái dùng; thêm `C_CHECK_NAME` mới (CASHFLOW_MISMATCH, HOLDINGS_MISMATCH, NAV_BRIDGE). Cân nhắc cột `C_WITHIN_THRESHOLD BIT` để log diff cả khi không break (đo vênh).

**SỬA:**
- `T_SI_BALANCE`: bỏ `C_PAYABLE_FEE`; **KHÔNG thêm `C_FEE_ACCUM`** (Asset gửi NAV ròng); thêm `C_CASH_IN`, `C_CASH_OUT`. **[thin-layer 2026-06-26]** giữ `C_AUM` + `C_DAILY_RETURN` (Asset gửi) + cột TE; **gỡ `C_UNIT`/`C_UNIT_PRICE`/`C_DAILY_PNL`** (2026-06-24 còn giữ để derive — nay Asset cấp `daily_return`).
- `T_SI_CURRENT`: bỏ `C_PAYABLE_FEE`; NAV + components (stock, cash tổng) từ Asset (đổi nguồn ghi).
- `T_EOD_WORK`: bỏ cột phí (`C_PAYABLE_FEE`, `C_FEE_CUT`).

**BỎ:**
- `T_FEE_CONFIG`, `T_SI_INCOME_FEE` (chi tiết phí), `T_SI_CASH_HIST` (interval cash cho NAV).

**GIỮ NGUYÊN:** `T_PRICE_DAILY`, `T_MASTER_*`, `T_SI_PORTFOLIO(_HOLDING)`, `T_SI_HOLDING_HIST`, `T_SI_CASHFLOW_EVENT`, `T_TICKER_INDUSTRY`, `T_EOD_RUN/PIPELINE`, `T_MASTER_PM_CONFIG`.

---

## P1 — Ingest + derive (`02_SP_ENGINE.sql`)

**THÊM `SP_INGEST_ASSET_NAV @p_json`** (per-SI, 1 batch/ngày GD):
- **[thin-layer]** Parse JSON per-SI `{si_account, aum, daily_return, cash, cash_in, cash_out}` → ghi `T_SI_ASSET_DAILY` (idempotent DELETE+INSERT theo date,si). KHÔNG còn `stock_value`/`fee_accum`/`pending`/`div_cash`.
- **[thin-layer] LƯU THẲNG** per-SI (KHÔNG derive):
  ```
  AUM = NAV = aum         (Asset GỬI, đã trừ phí QL — model realized; SDI KHÔNG lắp/trừ/derive)
  daily_return = daily_return  (Asset GỬI, TWR đã khử dòng tiền)
  → ghi T_SI_BALANCE (aum + daily_return). KHÔNG tính units/UP/PnL.
  ```
  *(Bản 2026-06-24 derive `Δunits=(cash_in−cash_out)/UP_{t-1}; UP_t=NAV/units; return_t=UP_t/UP_{t-1}−1; PnL_t=...` từ NAV+flow — đã GỠ: Asset cấp luôn `daily_return`.)*
- Ghi `T_SI_BALANCE` (DELETE+INSERT @d) + roll `T_SI_CURRENT` (nếu @d mới nhất). Idempotent + scoped.
- err convention chuẩn (@p_err_code/@p_err_msg OUT).

**BỎ:** `SP_EOD_COMPUTE`, `SP_EOD_COMPUTE_CORE` (J06 accrue/J07 MTM/J08 NAV/**[thin-layer] J09 PnL/J10 unit**) cho EOD; `SP_INGEST_FEE_CHARGE`; `SP_EOD_RECOMPUTE_RANGE` (NAV); **[thin-layer] `T_SI_UNIT_LEDGER`**.
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

## P3 — Reconcile (`02_SP_ENGINE.sql` — `SP_EOD_RECONCILE`)

> **[thin-layer 2026-06-26]** còn **3 check**: NAV_NEGATIVE, SI_NAV_MISMATCH, CASHFLOW_MISMATCH. NAV_BRIDGE/NAV_CONSISTENCY + HOLDINGS_MISMATCH GỠ (Asset không gửi `stock_value`). Block dưới = thiết kế 2026-06-24.

Ghi `T_EOD_RECON_BREAK` (lưu `C_DIFF` **kể cả trong ngưỡng** để đo vênh):
1. ~~**NAV_BRIDGE** per-SI: `|NAV_t − (NAV_{t-1}+cash_in−cash_out+Δval)|`~~ **[thin-layer] GỠ** (cần stock).
2. **CASHFLOW_MISMATCH**: `cash_in/out` SDI (`T_SI_CASHFLOW_EVENT`) vs Asset (`T_SI_ASSET_DAILY`).
3. ~~**HOLDINGS_MISMATCH**: `Σ(FO holdings × giá BO)` vs Asset `stock_value`~~ **[thin-layer] GỠ** (Asset không gửi `stock_value`).
- Cổng publish ở `SP_EOD_RUN`: có break "cứng" → chặn J14/publish; break "đo lường" (trong ngưỡng) → chỉ log.

---

## P4 — Smoke (`03_SMOKE.sql`) + PM smoke (`07` giữ)

- **BỎ** test EOD-compute (MTM/accrue/fee breakdown/idempotent NAV).
- **[thin-layer] THÊM**: ingest Asset `aum`+`daily_return` → assert **LƯU đúng** (`C_AUM`=aum, `C_DAILY_RETURN`=daily_return) + serve %PnL = compound. *(Bản 2026-06-24 assert derive: ngày thường `return=NAV_t/NAV_{t-1}−1`; ngày nạp/rút `units` phát hành theo `UP_{t-1}`; ngày đầu UP=10.000 — đã GỠ vì SDI không derive.)*
- **THÊM** reconcile: bơm lệch cashflow → break CASHFLOW_MISMATCH + `C_DIFF` đúng; log diff trong-ngưỡng. *([thin-layer] HOLDINGS_MISMATCH GỠ.)*
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
- ~~`fee_accum` Asset gửi: NAV net hay SDI tự trừ?~~ **[BRD asset-sync] ĐÃ QUYẾT (2026-06-25):** Asset gửi NAV RÒNG (đã net phí QL); KHÔNG gửi `fee_accum`. SDI lắp thẳng, `AUM = NAV`.
- Ngưỡng reconcile (b/c) — cấu hình per-master hay global.
- Near-realtime (d) — out of scope đợt này (chỉ giữ holdings + ghi TODO).
