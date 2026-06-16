# SDI Module — Implementation Plan (Asset & Performance Engine)

> Phạm vi: module **SDI** — engine tính tài sản, hiệu suất danh mục SI & từng khách hàng, cấp dữ liệu cho Asset → SMO.
> Phiên bản: **v1.0** — 2026-06-15. Đi kèm: [SDI-concepts-glossary.md](./SDI-concepts-glossary.md) (khái niệm & công thức).
> Mô hình đã chốt: **SMA — segregated custody + gộp lệnh**, hiệu suất **per (KH × SI)**, **historic pricing (giá t-1) chốt EOD**, **event-sourced**.

---

## 1. Mục tiêu & phạm vi

### Trong phạm vi (SDI)
- Ingest: FO (tài sản/holdings + **execution feed** = kết quả khớp MP per TK), Market data (giá, VN-Index, corporate action), Danh mục mẫu (weights).
- Tính: NAV per (KH×SI); Unit/Unit Price per KH (historic t-1, EOD); PnL; **TWR + MWR** (E chốt cả hai); SI tổng hợp; **SI Index** (danh mục mẫu); chuỗi benchmark (VN-Index PR).
- Lưu: event-sourced ledger + chuỗi daily SI-level; derive customer-level on read.
- Phục vụ: API cho Asset (nguồn duy nhất cho SMO) — FR-01…FR-06.
- Batch EOD + khả năng recompute (replay).

### NGOÀI phạm vi (thuộc module khác)
- **Sinh & khớp lệnh: thuộc FO** (đã chốt O4). Sau khi rebalance model, **SDI gửi yêu cầu rebalance sang FO**; **FO đặt lệnh MP trực tiếp trên TK từng KH** (KHÔNG gom + phân bổ → không có lô lẻ phân bổ, O5). Khớp → cổ phiếu; không khớp → tiền KH. SDI **chỉ tiêu thụ execution feed** từ FO.
- Tính toán tại SMO/Asset (cấm — chỉ đọc).

### Nguyên tắc
1. **SSOT tại SDI**; Asset chỉ lưu kết quả; SMO chỉ đọc.
2. **EOD là đơn vị tính**; chốt 1 lần cuối ngày.
3. **Event sourcing**: lưu snapshot/cashflow/execution feed/CA để tính lại.
4. **Derive > materialize** cho dữ liệu customer-level (tránh 2,5 tỷ dòng — §7).

---

## 2. Kiến trúc & luồng dữ liệu

```
   BO ──┐
   FO ──┤ (tài sản, holdings, tiền, phí, + EXECUTION FEED: kết quả khớp MP per tiểu khoản)
Market ─┤ (giá đóng cửa, giá ref điều chỉnh quyền, VN-Index, CA)
 Model ─┘ (danh mục mẫu: weights theo effective_date)
        │
        ▼
┌─────────────────────────────┐         rebalance request
│      SDI CALC ENGINE         │ ───────────────────────────►  FO
│  B1 Sync → B2..B8 tính → B9  │   (FO đặt lệnh MP trực tiếp trên TK từng KH;
│  Event ledger + Daily series │    khớp→CP / không→tiền → execution feed về SDI)
└─────────────────────────────┘
        │ push (B9)
        ▼
      ASSET  ──→  SMO (read-only, FR-01..06)
```

---

## 3. Mô hình dữ liệu

> Quy ước: prefix `sdi_`, khóa thời gian `business_date` (DATE). Số tiền `BIGINT` (VND), tỷ lệ/giá `NUMERIC` đủ thập phân. **Unit lưu `NUMERIC(38,10)` (full precision)**.

### 3.1 Master / cấu hình

**`sdi_strategy`** — định nghĩa SI
| cột | kiểu | ghi chú |
|---|---|---|
| si_id (PK) | BIGINT | |
| si_code | VARCHAR | mã hiển thị (SDI01…) |
| si_name | VARCHAR | |
| status | ENUM(ACTIVE, CLOSED) | CLOSED không hiện (BR-01.3) |
| inception_date | DATE | |
| mgmt_fee_rate | NUMERIC | %/năm (mặc định, KH có thể override) |
| benchmark_id | BIGINT | FK → benchmark (VN-Index PR — #D giữ PR) |
| model_holds_cash | BOOL | điểm #7 — danh mục mẫu có giữ tiền không |

**`sdi_model_weight`** — danh mục mẫu (điểm #10), version theo ngày
| cột | kiểu | ghi chú |
|---|---|---|
| si_id, effective_date, ticker (PK) | | weights net **hiệu lực tại close** |
| target_weight | NUMERIC | Σ theo (si_id, effective_date) = 100% (CP) [+ cash nếu #7] |

**`sdi_customer_si`** — cấu hình đầu tư KH (FR-04, điểm thiếu của plan)
| cột | kiểu | ghi chú |
|---|---|---|
| customer_id, si_id (PK) | | = 1 tiểu khoản |
| sub_account_no | VARCHAR | tiểu khoản tại VPS |
| join_date | DATE | gốc tính hiệu suất nếu vào sau (BR-03.3) |
| status | ENUM(ACTIVE, CLOSED) | |
| initial_amount | BIGINT | đầu tư ban đầu |
| sip_amount | BIGINT | số tiền SIP |
| sip_schedule | VARCHAR | vd "ngày 3 hàng tháng" |
| mgmt_fee_rate | NUMERIC | theo tham số SP |
| min_invest | BIGINT | đầu tư tối thiểu (dùng cho "tài sản có thể rút") |

### 3.2 Market data

**`sdi_price_daily`** (ticker, business_date PK; close_price, ref_price_adjusted) — `ref_price_adjusted` = giá tham chiếu điều chỉnh quyền (điểm #8/CA).
**`sdi_corporate_action`** (ticker, ex_date PK + ca_type; ratio, cash_div_per_share, adjusted_ref_price) — điểm #12.
**`sdi_benchmark_daily`** (benchmark_id, business_date PK; index_value) — **VN-Index (PR)** [điểm #11; #D chốt giữ PR, không dùng VN30TRI].

### 3.3 Event / ledger (nguồn sự thật)

**`sdi_cashflow_event`** — chỉ external cashflow (NAV in/out); **KHÔNG** chứa cổ tức/income
| cột | kiểu | ghi chú |
|---|---|---|
| event_id (PK) | | |
| customer_id, si_id | | |
| business_date | DATE | |
| event_type | ENUM(INITIAL, TOPUP, SIP, INTEREST_IN, WITHDRAW) | INTEREST_IN = "lãi Infy" (cash-in) |
| amount | BIGINT | (+) vào, (−) ra |
| created_time | TIMESTAMP | |

**`sdi_rebalance_request`** (request_id PK; si_id, business_date, model_effective_date, type ENUM(REBALANCE, DEPLOY, REDEEM), status) — **SDI → FO** (output). SDI gửi yêu cầu; FO đặt lệnh MP **trực tiếp trên TK từng KH** (không gom/phân bổ — O4).
**`sdi_execution_feed`** (exec_id PK; customer_id, si_id, ticker, side, qty, exec_price, business_date) — **FO → SDI** (ingest, source = FO). Kết quả khớp MP per tiểu khoản; giá MP là giá khớp thật trên sàn (biết sau khi khớp → EOD).
**`sdi_customer_holding_event`** (event_id PK; customer_id, si_id, ticker, business_date, qty_delta, source ENUM(EXEC, CA, CLOSE)) — sổ cái lot, dựng từ execution feed + CA (derive holdings).

### 3.4 Chuỗi daily SI-level (nhỏ, materialize)

**`sdi_asset_snapshot_daily`** (business_date, si_id PK) — đủ trường FR-06: cash_balance, stock_value, cash_available, withdrawable_asset, buying_power, pending_buy, pending_sell, cash_dividend, custody_fee, mgmt_fee_accrued, payable_fee, total_asset, nav. (Gộp lại 2 phiên bản mâu thuẫn trong plan gốc §3 vs §8.)
**`sdi_holding_daily`** (business_date, si_id, ticker PK; quantity, market_price, market_value, weight) — top 20 + "mã khác" (BR-05.3).
**`sdi_si_performance_daily`** (business_date, si_id PK; nav, unit, unit_price, daily_pnl, daily_return) — "Hiệu suất SI" (NAV per share, gồm cash).
**`sdi_si_index_daily`** (business_date, si_id PK; index_value, daily_return) — danh mục mẫu (price/total-return theo #2).
**`sdi_benchmark_daily`** — đã nêu §3.2.

### 3.5 Customer-level

**`sdi_unit_ledger`** (customer_id, si_id, business_date PK; cf_net, delta_unit, unit) — **chỉ ghi dòng ngày có thay đổi unit** (cashflow). Unit lưu full precision.
> **NAV/Unit Price/%PnL của KH theo từng ngày = DERIVE on read** từ holding lots × giá + unit ledger (không materialize daily → §7). Có thể cache lazy.

### 3.6 Partition & retention

| Bảng | Partition | Retention |
|---|---|---|
| sdi_cashflow_event, sdi_execution_feed, sdi_customer_holding_event, sdi_unit_ledger | **HASH(customer_id)** + range YEAR | 10 năm online |
| sdi_*_daily (SI-level) | range YEAR | 10 năm online |
| sdi_holding_daily | range YEAR | 2 năm online + 8 năm archive |
| sdi_price_daily, sdi_benchmark_daily | range YEAR | 10 năm |

Index bắt buộc: `(customer_id, si_id, business_date)` trên các bảng customer-level; `(si_id, business_date)` trên SI-level.

---

## 4. Công thức engine (tham chiếu glossary §13 — đã chốt)

```
NAV (per KH×SI)   = Tổng tài sản − phí phải trả        (Tổng tài sản = Tiền + CP, gồm cash)
Mgmt fee          = thu THEO THÁNG tại ngày cố định    (#F TREO — chốt sau: F1 base snapshot/bình quân? F2 accrue daily?)
                    [khuyến nghị: accrue daily vào phí phải trả + thu tháng; thu = cash↓ + payable↓ = NAV neutral]
PnL ngày          = NAV cuối − NAV đầu + NAV ra − NAV vào
Cổ tức            = income → vào Tiền (accrue ngày EX, #6), KHÔNG vào NAV vào

# UNIT per KH — historic pricing (t-1), EOD (#B chốt: giá hôm trước)
Unit Price_0=10000 ; Unit_0 = NAV_0/10000
CF_t = NAV vào − NAV ra        # ⚠️ CHỈ từ event nhãn DEPOSIT/SIP/WITHDRAW (FO có nhãn); KHÔNG lấy Δ(tổng tiền) — cổ tức/bán CP không phải CF
ΔUnit_t = CF_t / Unit Price_(t-1)              # giá HÔM TRƯỚC (historic)
Unit_t = Unit_(t-1) + ΔUnit_t                  # lưu full precision
Unit Price_t = NAV cuối_t / Unit_t
# hệ quả: net_cashflow = ΔUnit_t × Unit Price_(t-1)

# %PnL (TWR) — compound theo range, lấy 2 đầu mút
%PnL(range) = UnitPrice[cuối]/UnitPrice[đầu] − 1        (cùng mốc với PnL tiền, #5)

# MWR (#3, E chốt implement) — "lợi suất của bạn", per KH×SI per range
# Modified Dietz (mặc định):
MWR(range) = (NAV_cuối − NAV_đầu − CF_ròng) / (NAV_đầu + Σ_i w_i·CF_i)
   w_i = (T − t_i)/T   (t_i = ngày từ đầu kỳ; T = độ dài kỳ); tử = PnL tiền
# XIRR (tùy chọn, chính xác): giải r: NAV_đầu·(1+r)^T + Σ CF_i·(1+r)^(T−t_i) = NAV_cuối
# derive-on-read (NAV 2 đầu mút + cashflow events có ngày); hiển thị period (không annualize)

# SI tổng hợp
SI NAV = Σ Customer NAV ; SI Unit Price = SI NAV / Σ Customer Unit

# SI INDEX (danh mục mẫu) — EOD, weights net cuối ngày (#8)
Index_0=1000 ; Index_t = Index_(t-1) × Σ_i (w_i × P_i,t / P_ref_i)
  P_ref_i = close hôm trước | ref_price_adjusted khi có quyền
  [+ w_cash × 1 nếu model_holds_cash = true, #7]
  # PRICE RETURN — #D chốt GIỮ PR (KHÔNG reinvest cổ tức); so với VN-Index (PR) cùng cơ sở
```

---

## 5. Batch EOD (pipeline)

```
B1  SYNC: FO (tài sản/holdings/tiền/phí), Market (giá, ref adj, VN-Index, CA), Allocation, Model weight
B2  ÁP CORPORATE ACTION: cổ tức accrue ngày EX (#6); cập nhật holding lot khi CA cổ phiếu
B3  DỰNG HOLDINGS per (KH×SI) từ lot events; mark-to-market = Σ(qty×close)
B4  TÍNH NAV per (KH×SI): tài sản − phí phải trả; xử lý tiền pending [mgmt fee: #F treo — O9]
B5  PnL ngày per KH
B6  UNIT (historic #B): gom CF ngày → ΔUnit=CF/UnitPrice_(t-1) → Unit=Unit_(t-1)+ΔUnit (full) → UnitPrice_t=NAV/Unit → ghi sdi_unit_ledger nếu có thay đổi
B7  SI TỔNG HỢP: SI NAV=ΣNAV, SI Unit=ΣUnit, SI Unit Price, daily_pnl/return → sdi_si_performance_daily
B8  SI INDEX (#8) + BENCHMARK: Index_t theo weights net cuối ngày → sdi_si_index_daily; ingest VN-Index PR (#11)
B9  PUSH → ASSET: asset_snapshot, holding_daily, si_performance, si_index, benchmark, unit_ledger
```

**Tính chất:**
- **Idempotent**: chạy lại 1 business_date → cùng kết quả (upsert theo PK).
- **Recompute/replay**: xóa daily series từ ngày X → replay từ event ledger (cashflow/execution feed/CA/price).
- **Thứ tự phụ thuộc**: B6 cần B4; B7 cần B6; B8 độc lập B4-B7 (chỉ cần model weight + price).
- **Reconciliation** (§8): sau B3, đối chiếu Σ lots per ticker vs holdings thật FO ở mức tài khoản → cảnh báo break.

---

## 6. API cho Asset/SMO (mapping FR)

| FR | API | Nguồn |
|---|---|---|
| FR-01 Tổng quan đa SI | GET /customer/{id}/si-overview | sum sdi_si_performance + derive customer NAV |
| FR-02 Chi tiết 1 SI | GET /customer/{id}/si/{si} | derive customer NAV/PnL + **TWR (chiến lược) & MWR (lợi suất của bạn)** + sdi_asset_snapshot |
| FR-03 Chart so sánh | GET /customer/{id}/si/{si}/performance?range= | sdi_si_performance (TR) + sdi_si_index (PR) + sdi_benchmark VN-Index (PR) — 2 đầu mút/range |
| FR-04 Thông tin đầu tư | GET /customer/{id}/si/{si}/info | sdi_customer_si |
| FR-05 Holdings | GET /customer/{id}/si/{si}/holdings | sdi_holding_daily (top20 + mã khác) |
| FR-06 Báo cáo tài sản | GET /customer/{id}/si/{si}/asset-report | sdi_asset_snapshot_daily |

> ⚠️ FR-03 trả **3 đường cùng kỳ** (BR-03.4): SI (NAV per share, TR), danh mục mẫu (index, PR), VN-Index (PR). Mỗi đường = `(điểm cuối/điểm đầu −1)`.

---

## 7. Scale & hiệu năng (100 SI, 200K KH × 5 SI, 10 năm)

| Bảng | Ước lượng | Chiến lược |
|---|---|---|
| sdi_si_*_daily | ~250K mỗi loại | materialize |
| sdi_cashflow/execution_feed/holding event | ~100M+ | event-sourced, partition hash(customer_id) |
| sdi_unit_ledger | ~120M (chỉ ngày thay đổi unit) | event-sourced |
| **Customer NAV/unit_price daily** | ~2,5 tỷ nếu materialize | ❌ **DERIVE on read** (không lưu) |

**Khóa của thiết kế scale:**
- **SI NAV/Index/benchmark tính ở mức SI-aggregate** (Σ holdings per ticker × giá, từ execution feed) → ~100×25 phép tính, không iterate 1M KH.
- **Customer NAV/% derive khi mở app**: lots KH (~20) × giá tại 2 đầu mút → rẻ per-request; cache lazy cho KH hot.
- Tránh hoàn toàn bảng 2,5 tỷ dòng.

---

## 8. Edge cases & data quality

1. **Làm tròn unit**: lưu full precision; reconcile `SI Unit = Σ Customer Unit` (định nghĩa, không tính 2 đường).
2. **Reconciliation FO** (#9): Σ lots per ticker (SDI) vs holdings thật FO → break detection.
3. **Lô lẻ / cash drag** (O5 ✅): lệnh trực tiếp trên TK KH (không phân bổ) → **không có bài toán lô lẻ**. Phần không khớp/không mua đủ → **tiền KH** → cash drag tự phản ánh qua NAV. SDI không cần logic riêng.
4. **Độ trễ giải ngân** (O6 — methodology ✅ chốt): tiền chờ giải ngân **vào NAV + phát unit NGAY tại ngày nộp** (giá t-1, historic); clock từ ngày nộp; cash drag nằm trong tiểu khoản KH (segregated, công bằng). **Trade-date accounting** (ghi nhận tại ngày khớp MP, không đợi settle T+2; tiền mua chờ khớp / bán chờ về ở cash sub-ledger). ⚠️ Độ lớn trễ + cadence = hỏi FO; clock-start = xác nhận BO.
5. **Thiếu/đến trễ giá**: thiếu close → dùng giá liền trước, log; backfill → trigger recompute.
6. **Ngày không giao dịch**: range lấy điểm liền trước.
7. **KH/SI khởi tạo giữa range**: gốc = join_date / inception (BR-03.3).
8. **SI CLOSED / KH rút hết**: ngừng phát unit, giữ lịch sử.
9. **Cổ tức**: phân biệt nguồn (holdings → income) vs KH nạp (cashflow) — KHÔNG tag nhầm (rủi ro thật của SMA).
10. **%PnL vs PnL tiền** (#5): cùng mốc kỳ.
11. **Cash sub-ledger typed** (O8): TỔNG tiền chỉ để tính NAV; **CF (unit) lấy từ event nhãn DEPOSIT/SIP/WITHDRAW**, income từ nhãn DIVIDEND/INTEREST, FR-06 từ components. KHÔNG decompose CF từ Δ tổng tiền. Reconcile: `Δ tổng tiền = Σ(nạp/rút) + Σ(cổ tức/lãi) + (bán − mua khớp)`.
12. **MWR** (E): mẫu số Modified Dietz ≈ 0 (không vốn trong kỳ) → trả null/n.a., không chia 0. XIRR: đổi dấu nhiều lần / không hội tụ → fallback Modified Dietz. Hiển thị TWR & MWR **gán nhãn rõ** (chiến lược vs của bạn) tránh KH hiểu nhầm.

---

## 9. Phasing

| Phase | Nội dung | Phụ thuộc |
|---|---|---|
| **P0** | Chốt open items §10 với BO/FO; finalize schema | — |
| **P1** | Master + Market + Event ingest (B1); reconciliation FO | P0 |
| **P2** | NAV + Unit per KH (historic, B2-B6); sdi_unit_ledger | P1 |
| **P3** | SI tổng hợp + SI Index + Benchmark (B7-B8) | P2 |
| **P4** | Derive customer NAV/%, MWR (#3); API FR-01..06 (B9) | P3 |
| **P5** | Recompute/replay, partition/archive, cache | P4 |
| **P6** | Eval đối chiếu file mẫu (historic, khớp ô) + backtest 10 năm 1 SI | P5 |

---

## 10. Open items — CẦN BO/FO CHỐT TRƯỚC KHI BUILD (không tự quyết)

| # | Câu hỏi | Ảnh hưởng |
|---|---|---|
| ~~O1~~ | ~~**[D]** Benchmark TR/PR?~~ → ✅ **ĐÃ CHỐT: giữ VN-Index (PR)**, không dùng VN30TRI. Chấp nhận gap KH(TR)-vs-benchmark(PR) vì UX. | (đóng) |
| ~~O2~~ | ~~**[E]** MWR cạnh TWR hay chỉ TWR?~~ → ✅ **ĐÃ CHỐT: implement CẢ HAI** (TWR=chiến lược/chart, MWR=lợi suất của bạn; Modified Dietz mặc định). §5.4 glossary, §4 plan. | (đóng) |
| O3 | **[#7]** Danh mục mẫu có giữ tiền theo chiến lược không? | Công thức index |
| ~~O4~~ | ~~**[#9]** SDI sinh tập lệnh hay tiêu thụ?~~ → ✅ **ĐÃ CHỐT: SDI gửi yêu cầu rebalance; FO đặt lệnh MP trực tiếp trên TK từng KH (không gom/phân bổ); SDI tiêu thụ execution feed.** (§9b) | (đóng) |
| ~~O5~~ | ~~Lô lẻ: mua lô lẻ hay để dư tiền?~~ → ✅ **ĐÃ CHỐT: lệnh trực tiếp trên TK KH, không phân bổ → không có lô lẻ phân bổ; không khớp = tiền KH (cash drag tự phản ánh).** (§9b) | (đóng) |
| O6 | Methodology ✅ **đã chốt** (phát unit tại ngày nộp + trade-date — §8 #4). Còn hỏi: **FO** nộp T→khớp T+? & cadence batch; **BO** clock từ ngày nộp hay ngày khớp (khuyến nghị: ngày nộp). | Cash drag, mốc hiệu suất |
| O7 | **[#5]** Mốc kỳ chuẩn cho %PnL & PnL tiền (đầu/cuối ngày biên)? | Báo cáo |
| ~~O8~~ | ~~Phân loại nguồn tiền — FO có gắn nhãn?~~ → ✅ **FO có nhãn ĐỦ.** Action: sửa công thức — TỔNG tiền chỉ tính NAV; **CF lấy từ event nhãn DEPOSIT/SIP/WITHDRAW, KHÔNG từ Δ tổng tiền**; giữ cash sub-ledger typed (§3 glossary, §8 #11). | (đóng, đã sửa công thức) |
| O9 | **[F]** Phí quản lý: (F1) base = AUM snapshot ngày thu hay AUM bình quân ngày? (F2) accrue daily vào payable hay chỉ lump tại ngày thu? Thu theo tháng tại ngày cố định đã rõ. | NAV/unit price, công bằng mid-month |

> Ghi chú tuân thủ: các điểm #1-#8 (điều chỉnh) và #9-#12 (bổ sung) trong [glossary §12] đã được phản ánh vào schema/batch ở trên. Không cut item nào; phần phụ thuộc quyết định nghiệp vụ để ở §10 chờ BO, KHÔNG tự decide.
```
