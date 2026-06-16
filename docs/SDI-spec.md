# SDI — Asset & Performance Engine — Specification

Engine tính tài sản, hiệu suất danh mục SI và từng khách hàng cho sản phẩm **SMA chỉ số** (separately managed account). Cấp dữ liệu cho Asset → SMO.

---

## 1. Mô hình & ranh giới

**SMA — segregated custody, lệnh đặt trực tiếp trên tài khoản khách:**

```
KH chuyển tiền vào tiểu khoản (mỗi tiểu khoản = 1 SI)
        │ SDI gửi yêu cầu rebalance (trigger)
        ▼
FO: tính tỷ trọng danh mục mẫu  +  đặt lệnh MP TRỰC TIẾP trên TK từng KH
    (không gom + phân bổ; khớp → cổ phiếu, không khớp → tiền của KH)
        │ feed: model_weight + execution + asset/holdings
        ▼
SDI: tính NAV, Unit/Unit Price, PnL, TWR, MWR, SI Index  →  push Asset  →  SMO (read-only)
```

| Việc | Chủ |
|---|---|
| Gửi yêu cầu rebalance (trigger, không chứa weights) | SDI |
| Tính tỷ trọng danh mục mẫu (luôn 100% cổ phiếu) | **FO** |
| Đặt & khớp lệnh MP trên TK từng KH | **FO** |
| Tính NAV / Unit / PnL / TWR / MWR / SI Index | SDI |
| Đọc & hiển thị | SMO (qua Asset, không tính toán) |

- **Custody = segregated**: tiền & cổ phiếu nằm thật trong tiểu khoản KH (KH sở hữu hợp pháp).
- **SDI = engine tính thuần**: không quyết tỷ trọng, không sinh/khớp lệnh.
- **EOD** là đơn vị tính; chốt 1 lần cuối ngày. **Event-sourced** để tính lại được.

---

## 2. Khái niệm nền — hai thế giới

| | Thế giới THẬT (per KH × SI) | Thế giới BENCHMARK (lý thuyết) |
|---|---|---|
| Đo | Tiền thật của KH: NAV → Unit → hiệu suất | Chỉ số: SI Index, VN-Index |
| Công cụ | Unit price (NAV per share) | Index (weights × giá) |
| Hiển thị | "Lợi suất của bạn" / "Hiệu suất SI" | "Danh mục mẫu", "VN-Index" trên chart FR-03 |

- **Tiểu khoản** = đơn vị nhỏ nhất = một **(customer × SI)**. Một KH có nhiều tiểu khoản.
- Hiệu suất tính **per (KH × SI)**; SI-level = tổng hợp các KH.

---

## 3. Tài sản & NAV

```
Chứng khoán   = Σ (KL nắm giữ × market price)
Tiền          = tiền mặt + tiền bán chờ về (T0+T1+T2) + cổ tức tiền
Tổng tài sản   = Tiền + Chứng khoán
Phí quản lý/ngày = NAV_t × mgmt_fee_rate / 365      (accrue THEO NGÀY trên NAV thật ngày đó)
NAV           = Tổng tài sản − phí lưu ký (FO) − phí quản lý lũy kế (SDI accrue)
Tổng vốn đầu tư = Σ NAV vào − Σ NAV ra              (net cashflow lũy kế)
```

- **NAV do SDI tính** — không đọc thô số dư tiểu khoản (số thô trộn tiền chờ giải ngân, tiền mua chờ khớp, tiền bán chờ về T+, và phí quản lý SDI chưa có trong số FO).
- **Phí quản lý**: accrue mỗi ngày `NAV_t × rate/365` (mỗi ngày một số theo NAV ngày đó, KHÔNG chia đều) → cộng dồn vào "phí phải trả". **Thu theo tháng tại ngày cố định**: lúc thu `tiền↓ + phí phải trả↓` → NAV neutral (không tụt bậc). Ngày nghỉ dùng NAV phiên gần nhất.
- **Phí lưu ký** từ FO; **phí quản lý** do SDI cộng thêm — không để trùng.
- Phí phạt rút sớm: chi phí, KHÔNG tính vào cashflow.

### Cash sub-ledger (typed)
Tổng tiền chỉ để tính NAV. Các thành phần tiền lưu **theo loại** (FO cấp nhãn) phục vụ 3 mục đích:

| Dùng cho | Lấy từ |
|---|---|
| NAV | tổng tiền |
| Cashflow (đổi unit) | event nhãn DEPOSIT/SIP/WITHDRAW — **không** từ Δ tổng tiền |
| Income | event nhãn DIVIDEND/INTEREST |
| FR-06 (sức mua, tài sản có thể rút) | components: chờ giải ngân, phong tỏa, mua chờ khớp, bán chờ về |

Đối chiếu mỗi ngày: `Δ tổng tiền = Σ(nạp/rút) + Σ(cổ tức/lãi) + (tiền bán − tiền mua khớp lệnh)`.

---

## 4. Cashflow vs Income

| Loại | Là gì | Vào đâu | Đổi Unit? |
|---|---|---|---|
| **NAV vào** (`cash_in`) | tiền KH bơm vào: nộp lần đầu, nộp thêm, SIP, lãi Infy | external cashflow | ✅ |
| **NAV ra** (`cash_out`) | tiền KH rút | external cashflow | ✅ |
| **Income** | cổ tức/lãi do tài sản quỹ sinh ra | PnL (qua NAV) | ❌ |
| **Chi phí** | phí quản lý, phí phạt rút sớm | giảm NAV | ❌ |

- `net_cashflow (CF_t) = cash_in − cash_out` — **external, per (KH×SI), per ngày**, lấy từ event có nhãn.
- **Cổ tức tiền mặt**: income — vào NAV qua thành phần Tiền, **accrue tại ngày EX** (ghi phải thu), tự động vào PnL. KHÔNG tag là cash_in.

---

## 5. Unit & Unit Price (historic pricing)

Mỗi tiểu khoản có chuỗi NAV & cashflow riêng → **Unit & Unit Price riêng**.

```
T0 (ngày tham gia):
   Unit Price_0 = 10.000
   Unit_0       = NAV_0 / 10.000

Tn (chốt EOD):
   CF_t        = cash_in − cash_out                 (net, gom trong ngày)
   ΔUnit_t     = CF_t / Unit Price_(t-1)            (giá ngày HÔM TRƯỚC)
   Unit_t      = Unit_(t-1) + ΔUnit_t
   Unit Price_t = NAV cuối_t / Unit_t
```

- **Unit lưu full precision** (`NUMERIC(38,10)`); chỉ làm tròn khi hiển thị.
- Hệ quả: `net_cashflow = ΔUnit × Unit Price_(t-1)`.
- **Đóng & mở lại vị thế**: khi `Unit` về 0 (rút toàn bộ) → vị thế đóng. Lần nộp mới khởi tạo lại như T0 (`Unit = CF/10.000`, `Unit Price = 10.000`). Hiệu suất tính theo từng vị thế.

---

## 6. Hiệu suất (per KH × SI)

### PnL (tiền)
```
PnL ngày  = NAV cuối − NAV đầu + NAV ra − NAV vào      (cashflow-neutral)
PnL cả kỳ = Σ PnL các ngày trong kỳ
```

### TWR — "hiệu suất chiến lược" (qua Unit Price)
```
Daily return = Unit Price_t / Unit Price_(t-1) − 1
%PnL(range)  = Unit Price[ngày cuối] / Unit Price[NGÀY MỐC] − 1
```
- **Ngày mốc (base)** = gốc 0%; return phủ các ngày **SAU** ngày mốc. Cả %PnL và PnL tiền dùng **cùng ngày mốc** (cùng span).
- Telescoping: %PnL = tích các daily return của các ngày sau ngày mốc.

**Ngày mốc theo filter:**

| Filter | Ngày mốc (base) |
|---|---|
| YTD | close phiên cuối năm trước (~31/12) |
| 1M / 3M / 6M / 1Y / 3Y | close ngày tương ứng N về trước |
| Inception | close ngày khởi tạo (= 10.000) |
| KH/SI tham gia sau mốc filter | close ngày tham gia |

### MWR — "lợi suất của bạn" (money-weighted)
```
Modified Dietz (mặc định):
   MWR = (NAV cuối − NAV đầu − CF_ròng) / (NAV đầu + Σ_i w_i · CF_i)
   w_i = (T − t_i)/T   (t_i = số phiên từ ngày mốc tới flow i; T = độ dài kỳ tính bằng phiên)
   tử số = PnL tiền cả kỳ

XIRR (tùy chọn, chính xác):  giải r:  NAV_đầu·(1+r)^T + Σ CF_i·(1+r)^(T−t_i) = NAV_cuối
```
- Hiển thị period return (không annualize trừ khi yêu cầu). Mẫu số ≈ 0 → trả null.
- Derive on read: NAV 2 đầu mút + cashflow events trong range.

### SI tổng hợp (đường "Hiệu suất SI" trên chart)
```
SI NAV        = Σ Customer NAV
SI Unit Price = SI NAV / Σ Customer Unit
```

---

## 7. SI Index — danh mục mẫu (benchmark)

```
Index_0 = 1000
Index_t = Index_(t-1) × Σ_i ( w_i^(t) × P_i,t / P_ref_i )

   w_i^(t) = bộ weights có effective_date ≤ t MỚI NHẤT (bộ chi phối ngày t)
   Σ w_i^(t) = 100%  (LUÔN 100% cổ phiếu — không có thành phần tiền)
   P_ref_i   = giá đóng cửa (t-1)  |  giá tham chiếu điều chỉnh khi có quyền (corporate action)
```

- **Weights do FO tính & feed về** (`model_weight`), version theo `effective_date` (mức ngày). Bộ weights `effective_date = D` chi phối return ngày D (đo từ close D-1 → close D).
- **Rebalance**: tính EOD-only, close-to-close; 1–2 lần/ngày chỉ lấy **trạng thái weights net cuối ngày**, không cần giá intraday. Sai lệch giữa SI thật và index = **tracking error thực thi** (hợp lệ — đúng mục đích FR-03).
- Mã mới vào rổ: `P_ref` = giá đóng cửa ngày trước khi vào. Mã halt không có giá: dùng giá gần nhất, gắn cờ.
- Corporate action: xử lý điều chỉnh `P_ref` trước, rồi áp weights.

### So sánh FR-03 (3 đường, đều `(điểm cuối/điểm mốc − 1)` cùng kỳ)

| Đường | Nguồn | Phương pháp |
|---|---|---|
| Hiệu suất SI | SI Unit Price | NAV-per-share (TWR), total return |
| Danh mục mẫu | SI Index | Index, price return |
| VN-Index | Market data | Index, price return |

> SI là total-return (NAV ăn cổ tức), benchmark là price-return → SI nhỉnh hơn ~mức cổ tức một cách hệ thống; đây là hệ quả cơ sở (PR vs TR), được chấp nhận (user đối chiếu VN-Index là chuẩn phổ quát).

---

## 8. Kiến trúc dữ liệu

Prefix `sdi_`. Tiền `BIGINT` (VND); tỷ lệ/giá `NUMERIC`; unit `NUMERIC(38,10)`. Entity = `si` (`sdi_` chỉ là prefix hệ thống).

### Master / cấu hình
- **`sdi_strategy`** (si_id PK; si_code, si_name, status[ACTIVE|CLOSED], inception_date, mgmt_fee_rate, benchmark_id)
- **`sdi_model_weight`** (si_id, effective_date, ticker PK; target_weight) — **FO tính & feed**; Σ = 100% cổ phiếu/eff_date.
- **`sdi_customer_si`** (customer_id, si_id PK; sub_account_no, join_date, status, initial_amount, sip_amount, sip_schedule, mgmt_fee_rate, min_invest) — cấu hình đầu tư KH (FR-04).

### Market data
- **`sdi_price_daily`** (ticker, business_date PK; close_price, adjusted_ref_price)
- **`sdi_corporate_action`** (ticker, ex_date, ca_type PK; ratio, cash_div_per_share, adjusted_ref_price)
- **`sdi_benchmark_daily`** (benchmark_id, business_date PK; index_value) — VN-Index (price return)

### Event / ledger (nguồn sự thật)
- **`sdi_rebalance_request`** (request_id PK; si_id, business_date, type[REBALANCE|DEPLOY|REDEEM], status) — **SDI → FO**, trigger (không chứa weights).
- **`sdi_execution_feed`** (exec_id PK; customer_id, si_id, ticker, side, qty, exec_price, business_date) — **FO → SDI**, kết quả khớp MP per tiểu khoản (giá MP biết EOD).
- **`sdi_cashflow_event`** (event_id PK; customer_id, si_id, business_date, event_type[INITIAL|TOPUP|SIP|INTEREST_IN|WITHDRAW], amount, created_time) — external cashflow (không chứa income).
- **`sdi_customer_holding_event`** (event_id PK; customer_id, si_id, ticker, business_date, qty_delta, source[EXEC|CA|CLOSE]) — sổ cái lot, dựng từ execution feed + CA.
- **`sdi_unit_ledger`** (customer_id, si_id, business_date PK; cf_net, delta_unit, unit) — ghi dòng khi unit thay đổi (cashflow). Unit full precision.

### Chuỗi daily SI-level (materialize, nhỏ)
- **`sdi_asset_snapshot_daily`** (business_date, si_id PK; cash_balance, stock_value, cash_available, withdrawable_asset, buying_power, pending_buy, pending_sell, cash_dividend, custody_fee, mgmt_fee_accrued, payable_fee, total_asset, nav)
- **`sdi_holding_daily`** (business_date, si_id, ticker PK; quantity, market_price, market_value, weight) — top 20 + "mã khác"
- **`sdi_si_performance_daily`** (business_date, si_id PK; nav, unit, unit_price, daily_pnl, daily_return)
- **`sdi_si_index_daily`** (business_date, si_id PK; index_value, daily_return)

### Control / orchestration
- **`sdi_eod_run`** (business_date, job PK; status[PENDING|RUNNING|DONE|FAILED], rows, started_at, ended_at, message) — theo dõi & resume batch EOD (§9.2).

### Customer-level: DERIVE on read
NAV / Unit Price / TWR / MWR của KH theo ngày **không materialize** — derive từ holding lots × giá + unit ledger + cash events. Tiền KH(t) = Σ(cashflow + mua/bán execution + cổ tức + phí) tới ngày t. **Snapshot cuối tháng** để giới hạn replay; cache lazy cho KH hot.

### Partition & retention

| Bảng | Partition | Retention |
|---|---|---|
| event/ledger customer-level | HASH(customer_id) + range YEAR | 10 năm online |
| daily SI-level, market | range YEAR | 10 năm |
| holding_daily | range YEAR | 2 năm online + 8 năm archive |

Index: `(customer_id, si_id, business_date)` cho customer-level; `(si_id, business_date)` cho SI-level.

---

## 9. Batch EOD

### 9.1 Công thức pipeline (mức tính toán)
```
NAV          = stock_value + cash − custody_fee − mgmt_fee_accrued
PnL ngày     = NAV cuối − NAV đầu + NAV ra − NAV vào
ΔUnit        = net CF / UnitPrice_(t-1) ; Unit = Unit_(t-1)+ΔUnit (full) ; UnitPrice = NAV/Unit
SI NAV/Unit  = Σ per si ; SI UnitPrice = SI NAV / SI Unit
SI Index_t   = Index_(t-1) × Σ w^(t)·P_t/P_ref
```

### 9.2 Danh sách JOB chạy tuần tự cuối ngày

Mỗi job **idempotent** (chạy lại 1 ngày → cùng kết quả), ghi trạng thái vào `sdi_eod_run`. "SB" = set-based (không RBAR). "‖" = song song theo SI/hash.

| # | Job | Phụ thuộc | Đọc | Ghi | SB | ‖ | Halt nếu lỗi |
|---|---|---|---|---|---|---|---|
| **J0** | `GATE` chờ nguồn sẵn sàng | — | cờ sẵn sàng FO/Market/model_weight @d | `sdi_eod_run` | – | – | ✅ (timeout→alert) |
| **J1** | `STAGE` bulk load input | J0 | FO (holdings, execution), giá, CA, model_weight, VN-Index, cashflow | staging tables (minimal logging) | ✅ | ‖ | ✅ |
| **J2** | `VALIDATE` chất lượng input | J1 | staging | log lỗi | ✅ | – | ✅ (thiếu giá/trùng key/qty âm/thiếu nhãn nguồn) |
| **J3** | `APPLY_CA` corporate action | J2 | staging CA | position_holding (split/quyền), state.cash (cổ tức ex-date), holding_event | ✅ | ‖ | ✅ |
| **J4** | `APPLY_EXEC` khớp lệnh | J3 | staging execution | position_holding (qty), state.cash (mua/bán trade-date), holding_event | ✅ | ‖ | ✅ |
| **J5** | `APPLY_CASHFLOW` nạp/rút | J4 | cashflow event | state.cash, CF_t per vị thế | ✅ | ‖ | – |
| **J6** | `ACCRUE_FEE` phí quản lý | J5 | state (NAV_prev) | state.payable += NAV_prev×rate/365 | ✅ | ‖ | – |
| **J7** | `MTM` định giá lại toàn bộ | J4 | position_holding + giá @d | stock_value per vị thế (#nav_today) | ✅ | ‖ | – |
| **J8** | `CALC_NAV` | J6, J7 | stock_value, state.cash, phí | NAV per vị thế | ✅ | ‖ | – |
| **J9** | `CALC_PNL` | J8 | NAV, NAV_prev, CF | daily_pnl per vị thế | ✅ | ‖ | – |
| **J10** | `CALC_UNIT` | J8 | CF_t, UnitPrice_prev | ΔUnit/Unit/UnitPrice; sdi_unit_ledger | ✅ | ‖ | – |
| **J11** | `SI_AGG` tổng hợp SI | J8, J10 | NAV/unit per vị thế | sdi_si_performance_daily | ✅ | ‖ | – |
| **J12** | `SI_INDEX` + benchmark | J2 | model_weight, giá, VN-Index | sdi_si_index_daily, sdi_benchmark_daily | ✅ | ‖ | – |
| **J13** | `RECONCILE` đối soát | J11 | SDI holdings/NAV vs FO; Σ customer NAV vs SI NAV; Σ unit | bảng break | ✅ | – | ✅ (break > ngưỡng → chặn publish) |
| **J14** | `BUILD_SNAPSHOT` | J8 | NAV, holdings, phí, components | sdi_asset_snapshot_daily, sdi_holding_daily (top20+mã khác) | ✅ | ‖ | – |
| **J15** | `PUBLISH` | J13, J14 | staging/đích | commit position_state; SWITCH/MERGE SI-level; push current snapshot + SI series → Asset | ✅ | – | ✅ |
| **J16** | `FINALIZE` | J15 | — | mark eod_run done; (cuối tháng) build snapshot KH; update stats; alert success | – | – | – |

### 9.3 Thứ tự, song song & orchestration

```
J0 → J1 → J2 ─┬─ J3 → J4 ─┬─ J5 → J6 ─┐
              │            └─ J7 ──────┴─ J8 → J9
              │                              └─ J10 ─┐
              │                                      └─ J11 → J13 ─┐
              └─ J12 (độc lập, chạy song song) ───────────────────┤
                                          J8 → J14 ───────────────┴─ J15 → J16
```
- **J12 (SI Index)** chỉ cần giá + model_weight → chạy **song song** toàn bộ nhánh customer (J3–J11).
- **J3→J4→J5→J6 tuần tự** (cùng ghi `state` → tránh tranh chấp). **J7** chỉ cần holdings (sau J4) → chạy song song J5/J6.
- **J7/J8/J9/J10/J14** chia **dải SI hoặc hash(customer_id)** chạy nhiều luồng.
- **J13 RECONCILE là cổng**: lệch quá ngưỡng → **dừng, KHÔNG publish dữ liệu sai**, alert.
- **Thực thi ALL-IN-DB**: mỗi job = **1 stored proc** (set-based); **master proc `usp_eod_run @business_date`** gọi tuần tự + ghi `sdi_eod_run(business_date, job, status, rows, started, ended, message)`. **App/SQL Agent chỉ kích hoạt master proc** — không tính toán ở app. Fail giữa chừng → **resume từ job lỗi** (idempotent). Ingestion = proc `BULK INSERT`; API đọc = stored proc.
- **RCSI** bật → app đọc current snapshot không bị batch chặn; **J15 PUBLISH** (switch-in) là thao tác ngắn duy nhất ảnh hưởng đích.
- **Roll-forward**: J3–J6 áp **delta** (chỉ vị thế có biến động); J7–J10 chạm toàn bộ ~1M (giá đổi) nhưng đều **set-based**. Không replay lịch sử.

> Chi tiết kỹ thuật (columnstore, partition switch, runtime ~vài phút–15 phút, anti-patterns): [SDI-db-architecture.md](./SDI-db-architecture.md).

---

## 10. API cho Asset/SMO

> Mỗi API = **app gọi 1 stored proc** (`usp_get_*`) — tính toán/derive trong DB; app chỉ trả JSON, không tính.

| FR | API | Nguồn |
|---|---|---|
| FR-01 Tổng quan đa SI | GET /customer/{id}/si-overview | sum sdi_si_performance + derive customer NAV |
| FR-02 Chi tiết 1 SI | GET /customer/{id}/si/{si} | derive customer NAV/PnL + TWR + MWR + asset_snapshot |
| FR-03 Chart so sánh | GET /customer/{id}/si/{si}/performance?range= | si_performance (TR) + si_index (PR) + benchmark VN-Index (PR), 2 đầu mút/range |
| FR-04 Thông tin đầu tư | GET /customer/{id}/si/{si}/info | sdi_customer_si |
| FR-05 Holdings | GET /customer/{id}/si/{si}/holdings | sdi_holding_daily (top20 + mã khác) |
| FR-06 Báo cáo tài sản | GET /customer/{id}/si/{si}/asset-report | sdi_asset_snapshot_daily |

---

## 11. Scale (100 SI, 200K KH × 5 SI, 10 năm)

> Chi tiết kiến trúc DB + EOD ở quy mô lớn cho SQL Server: [SDI-db-architecture.md](./SDI-db-architecture.md) (roll-forward state, set-based, columnstore, partitioning).

| Dữ liệu | Ước lượng |
|---|---|
| SI Index / Performance / Asset snapshot | ~250K dòng/loại |
| Cashflow / execution / holding event | ~100M+ |
| Unit ledger | ~120M |
| Customer NAV/UP/% daily | **derive-on-read** (tránh ~2,5 tỷ dòng) |

Customer NAV: derive từ lots (~20/KH) × giá tại 2 đầu mút → rẻ per-request. SI-level tính aggregate. Không materialize daily per-customer.

---

## 12. Edge cases

1. **Đóng/mở lại vị thế** (unit=0 rồi nộp lại): khởi tạo lại T0 (UP=10.000).
2. **Phí > tài sản** (NAV ≤ 0): hiếm (long-only + tiền); định floor 0 + cảnh báo.
3. **Reconcile FO**: Σ lots per ticker (SDI) vs holdings thật FO → break detection.
4. **Cash drag**: phần không khớp = tiền KH → tự phản ánh qua NAV (không logic riêng).
5. **Độ trễ giải ngân**: tiền chờ = cash 0% (không biến động) → ngày phẳng ×1.0, không ảnh hưởng tích lũy.
6. **Trade-date accounting**: mua/bán ghi nhận tại ngày khớp MP (không đợi settle T+2); pending ở cash sub-ledger.
7. **Thiếu/đến trễ giá**: dùng giá phiên trước, log; backfill → trigger recompute.
8. **Ngày không giao dịch**: lấy điểm gần nhất trước đó.
9. **Cổ tức**: phân biệt nguồn (holdings = income vs KH nạp = cashflow); income không tag cash_in.
10. **Rounding**: unit full precision; `SI Unit ≡ Σ Customer Unit` (định nghĩa, không tính 2 đường).
11. **MWR**: mẫu số ≈ 0 → null; XIRR không hội tụ → fallback Modified Dietz.
12. **SI không có KH** (Σ unit = 0): SI unit price = null.

---

## 13. Quy ước đặt tên (canonical — code phải theo)

snake_case. Một khái niệm = một code.

| Nhóm | code |
|---|---|
| Entity | `si_id`, `si_code`, `sub_account` (=customer_id+si_id), `customer_id`, `business_date` |
| Tài sản | `total_asset`, `stock_value`, `cash`, `cash_dividend`, `custody_fee`, `management_fee`, `payable_fee`, `nav`, `buying_power`, `withdrawable_asset`, `net_invested_capital` |
| Cashflow | `cash_in`, `cash_out`, **`net_cashflow`** (=cash_in−cash_out, dùng trong công thức), `income` (≠ cashflow) |
| Hiệu suất | `unit` (full precision), `unit_price`, `delta_unit`, `pnl`/`daily_pnl`, `return_pct` (=%PnL=TWR), `daily_return`, `mwr` |
| Index | `si_index`/`index_value`, `target_weight`, `weight` (holdings), `ref_price`, `adjusted_ref_price`, `benchmark`, `close_price` |
| Khái niệm | `cash_drag`, `tracking_error`, `trade_date_accounting`, `segregated`, `price_return` |

Quy tắc: trong công thức dùng `net_cashflow` (rõ "net"); `income` tách khỏi cashflow; `cash` (tổng) chỉ cho NAV; % lưu dạng thập phân.

---

## 14. Worked example — per KH (khớp file mẫu)

| Ngày | NAV | ra | vào | PnL | ΔUnit | Unit | Unit Price |
|---|---|---|---|---|---|---|---|
| KT | 10,000,000 | | 10,000,000 | – | – | 1,000.000 | 10,000 |
| 2 | 15,000,000 | | | 5,000,000 | – | 1,000.000 | 15,000 |
| 3 | 18,000,000 | 100,000 | 2,000,000 | 1,100,000 | 126.667 | 1,126.667 | 15,976 |
| 4 | 20,000,000 | 150,000 | 3,500,000 | (1,350,000) | 209.69 | 1,336.36 | 14,966 |
| 5 | 20,000,000 | | | – | – | 1,336.36 | 14,966 |
| 6 | 25,000,000 | | | 5,000,000 | – | 1,336.36 | 18,708 |
| 7 | 25,500,000 | | | 500,000 | – | 1,336.36 | 19,082 |

- Ngày 3: `ΔUnit = (2tr−0.1tr)/15,000 = 126.667`; `UP = 18tr/1,126.667 = 15,976`; `net_cashflow = 126.667×15,000 = 1,900,000` ✓
- **%PnL range ngày 3→7** (ngày mốc = cuối ngày 2): `19,082/15,000 − 1 = +27.21%`; PnL tiền (ngày 3→7) = `5,250,000`. Cùng ngày mốc.

### SI Index — khớp ví dụ

| Ngày | Tỷ trọng (eff) | P_ref | Close | Index |
|---|---|---|---|---|
| 0 | A40 B35 C25 | — | 100/50/80 | 1000 |
| 1 | A40 B35 C25 | 100/50/80 | 102/49/82 | 1007 |
| 2 | A40 B35 C25 | 102/49/82 | 101/50.5/81 | 1011 |
| 3 (rebalance −C +D) | A45 B35 D20 | 101/50.5/60 | 103/52/62 | 1037 |

`Index_3 = 1011 × (0.45·103/101 + 0.35·52/50.5 + 0.20·62/60) = 1037` (weights eff_date=3 chi phối return ngày 3).
