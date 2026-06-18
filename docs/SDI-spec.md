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
        │ feed EOD: model_weight + ĐỒNG BỘ holdings+cash toàn bộ TK (SDI không quản lý từng lệnh khớp)
        ▼
SDI: holdings (FO nạp thẳng current) + cash → tính NAV, Unit/Unit Price, PnL, TWR, MWR, SI Index  →  push Asset  →  SMO (read-only)
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

- **Hai cấp:** **MASTER** = danh mục mẫu/chiến lược (mã `C_MASTER_CODE`). **SUB-ACCOUNT (tiểu khoản)** = KH đầu tư 1 master → cấp 1 sub-account, mã `C_SI_ACCOUNT` (= CUST_CODE+đuôi, customer-level). **Close+reopen master ⇒ sub-account MỚI** (mã khác, KHÔNG tái dùng) → 1 KH có nhiều sub-account/master theo thời gian (tối đa 1 ACTIVE/lúc). Mọi bảng customer-level khóa theo `C_SI_ACCOUNT`.
- **Tiểu khoản** = đơn vị nhỏ nhất = 1 sub-account (`C_SI_ACCOUNT`) của một (customer × MASTER). Reopen → khởi tạo T0 mới (UP=10.000) trên sub-account mới.
  > ⚠️ Mọi tham chiếu **schema/khóa/pipeline** trong docs đã sweep về convention hiện tại (`T_MASTER_*` key `C_MASTER_CODE` / `T_SI_*` key `C_SI_ACCOUNT`). Phần prose còn dùng **"SI" như thuật ngữ feature/khái niệm** ("SI Index" = danh mục mẫu benchmark, "Hiệu suất SI", "SI NAV/Unit Price" = tổng hợp cấp master) — giữ nguyên, chờ quyết định đổi tên vocabulary sản phẩm "SI".
- Hiệu suất tính **per tiểu khoản (KH × master)**; cấp master = tổng hợp các KH.

---

## 3. Tài sản & NAV

```
Chứng khoán   = Σ (KL nắm giữ × market price)
Tiền (FO sync) = available cash FO đồng bộ EOD — ĐÃ NET phí QL + thuế GD + ghi nhận SIP
NAV           = Chứng khoán + Tiền (FO sync)
Tổng vốn đầu tư = Σ NAV vào − Σ NAV ra              (net cashflow lũy kế)
```

- **NAV = stock_value + FO cash.** FO đồng bộ available cash cuối ngày đã trừ sẵn mọi khoản FO hạch toán (phí quản lý danh mục, thuế/phí giao dịch) và đã phản ánh lệnh SIP. **SDI tuyệt đối KHÔNG accrue/trừ lại** các khoản này → tránh double-count (phương án A).
- **FO cash là nguồn tiền DUY NHẤT.** SDI mirror số FO đẩy về, không tự cộng/trừ điều chỉnh; không suy cash từ Δ holdings.
- **Phí quản lý + thuế GD**: do FO sở hữu & trừ vào cash khi book. SDI không có job accrue phí (J06 đã bỏ).
- **`CF_t` chỉ lấy từ cashflow event** (nhãn DEPOSIT/SIP/WITHDRAW) — **không** suy từ Δ tổng tiền. Event phải khớp đúng ngày + số tiền với thời điểm FO phản ánh vào cash.
- Phí phạt rút sớm: do FO trừ vào cash, KHÔNG tính vào cashflow.

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
| **Chi phí** | phí quản lý, thuế GD, phí phạt rút sớm | FO trừ vào cash (SDI không re-apply) | ❌ |

- `net_cashflow (CF_t) = cash_in − cash_out` — **external, per (KH×SI), per ngày**, lấy từ event có nhãn.
- **Cổ tức tiền mặt**: income — vào NAV qua thành phần Tiền, **accrue tại ngày EX** (ghi phải thu), tự động vào PnL. KHÔNG tag là cash_in.

---

## 5. Unit & Unit Price

Mỗi tiểu khoản có chuỗi NAV & cashflow riêng → **Unit & Unit Price riêng**.

**Giả định khử dòng tiền:** nạp/rút coi như **phát sinh ĐẦU ngày** và **tham gia đầu tư trong ngày**. Khi đó giá quy đổi tại thời điểm tiền vào = NAV/unit đầu ngày = **Unit Price ngày hôm trước (t-1)**.

```
T0 (ngày tham gia):
   Unit Price_0 = 10.000
   Unit_0       = NAV_0 / 10.000

Tn (chốt EOD):
   CF_t        = cash_in − cash_out                 (net, gom trong ngày)
   ΔUnit_t     = CF_t / Unit Price_(t-1)            (giá ĐẦU ngày = cuối ngày trước)
   Unit_t      = Unit_(t-1) + ΔUnit_t
   Unit Price_t = NAV cuối_t / Unit_t
```

- **Vì sao chia UP_(t-1) là đúng (không bias):** dưới giả định trên, rút gọn cho `Unit Price_t = Unit Price_(t-1) × (1 + r_t)` → **daily return = r_t (lợi suất tài sản thật), độc lập cashflow** = TWR sạch. Cashflow chỉ đổi **số unit** (ΔUnit), không đổi **tỷ lệ giá unit** giữa 2 ngày.
- **Hệ quả (telescoping):** `%PnL = Π(UP_t/UP_(t-1)) − 1 = UP_cuối/UP_đầu − 1` → tính bằng **nhân dồn daily return** hay **tỷ lệ 2 đầu mút** đều **cho cùng kết quả, kể cả có nạp/rút** (chính nhờ khử cashflow vào unit).
- **Unit lưu `DECIMAL(18,6)`** (6 lẻ đủ cho TWR; unit = NAV/unit_price nên lẻ); chỉ làm tròn khi hiển thị.
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
- Tính on-read: NAV 2 đầu mút (từ `T_SI_NAV_BALANCE`) + cashflow events trong range.

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

Prefix bảng `T_`, cột `C_`. **Quy chuẩn kiểu:** Tiền VND & quantity = `DECIMAL(20,0)` (không thập phân); giá = `DECIMAL(18,4)`; % / return / fee_rate = `DECIMAL(10,6)`; unit & unit_price = `DECIMAL(18,6)`; weight = `DECIMAL(12,8)`. **Hai cấp:** master (`C_MASTER_CODE`, bảng `T_MASTER_*`) / sub-account = tiểu khoản (`C_SI_ACCOUNT`, bảng `T_SI_*`).

### Master / cấu hình
- **`T_MASTER_PORTFOLIO`** (**`C_MASTER_CODE` PK** — mã danh mục MASTER, khóa chính + khóa public, KHÔNG surrogate; name, status[ACTIVE|CLOSED], inception_date, mgmt_fee_rate, benchmark_code) — các bảng khác tham chiếu master theo `C_MASTER_CODE`; bảng tổng hợp master-level: `T_MASTER_NAV_BALANCE`/`T_MASTER_HOLDING_BALANCE`/`T_MASTER_INDEX_DAILY`/`T_MASTER_NAV_CURRENT`. **Sub-account** (`T_SI_PORTFOLIO`): `C_SI_ACCOUNT` (mã sub-account, UNIQUE) + `C_MASTER_CODE` + `C_CUST_CODE` + close_date; filtered-unique 1 ACTIVE/(cust,master). Customer-level tables khóa theo `C_SI_ACCOUNT`.
- **`T_MASTER_PORTFOLIO_TICKER`** (`C_MASTER_CODE`, effective_date, ticker; target_weight) — **FO tính & feed**; Σ = 100% cổ phiếu/eff_date.
- **`T_SI_PORTFOLIO`** (`C_SI_ACCOUNT` UNIQUE; `C_CUST_CODE`, `C_MASTER_CODE`, sub_account_no, join_date, status, close_date, initial_amount, sip_amount, sip_schedule, mgmt_fee_rate, min_invest) — registry tiểu khoản + cấu hình đầu tư KH (FR-04).

### Market data
- **`T_PRICE_DAILY`** (ticker, business_date PK; close_price, adjusted_ref_price)
- **`T_CORPORATE_ACTION`** (ticker, ex_date, ca_type PK; ratio, cash_div_per_share, adjusted_ref_price)
- **`T_BENCHMARK_DAILY`** (benchmark_code, business_date PK; index_value) — chỉ số thị trường ngoài (VN-Index, price return), **nạp từ market data** (không do SDI tính). Key = code tự mô tả (giống ticker), không cần dimension riêng.

### FO sync (EOD) & cashflow
- **`T_REBALANCE_REQUEST`** (request_id PK; `C_MASTER_CODE`, business_date, type[REBALANCE|DEPLOY|REDEEM], status) — **SDI → FO**, trigger (không chứa weights).
- **`T_SI_HOLDING_HIST`** (`C_SI_ACCOUNT`, ticker, valid_from; valid_to, quantity, avg_cost) — **HISTORY holdings theo KHOẢNG (INTERVAL / SCD-2)**: **FULL history BẮT BUỘC (compliance), KHÔNG trùng lặp** — holding bất biến N năm = **1 dòng** (`valid_to=NULL` = đang mở). Maintain bằng **DIFF** current vs dòng open **TẠI INGEST (per-event Kafka)** (đóng dòng đổi/biến mất → mở dòng mới). Reconstruct ngày D: `valid_from≤D AND (valid_to>D OR valid_to IS NULL)`. FO ingest holdings THẲNG `T_SI_PORTFOLIO_HOLDING` (current); **EOD core KHÔNG đọc hist**.
- **`T_SI_CASH_HIST`** (`C_SI_ACCOUNT`, valid_from; valid_to, cash) — **HISTORY cash theo INTERVAL** (full, no-dup; đối xứng holding_hist). DIFF state.cash vs dòng open **tại INGEST**. (FO cash ingest thẳng `T_SI_NAV_CURRENT.C_CASH` — không còn bảng feed transient riêng.)
- **`T_SI_CASHFLOW_EVENT`** (event_id PK; `C_SI_ACCOUNT`, business_date, event_type[INITIAL|TOPUP|SIP|INTEREST_IN|WITHDRAW], amount, created_time) — external cashflow; dùng cho **CF_t** (PnL/unit), KHÔNG cộng lại cash (cash từ FO sync).
- **`T_SI_FEE_INCOME`** (event_id PK; business_date, `C_SI_ACCOUNT`, type[DIVIDEND|CUSTODY_FEE|MGMT_FEE], ticker, amount, source, created_time) — **FO đẩy cổ tức + phí per-KH (sparse)**. Dòng tiền/sự kiện ngoài, KHÔNG derive được → capture lúc phát sinh cho **báo cáo tài sản FR-06**. J11 SUM lên `cash_dividend`/`custody_fee`/`mgmt_fee_accrued` của `T_MASTER_NAV_BALANCE`. **KHÔNG ảnh hưởng NAV** (phương án A).
- **`T_SI_UNIT_LEDGER`** (`C_SI_ACCOUNT`, business_date; cf_net, delta_unit, unit) — ghi dòng khi unit thay đổi (cashflow). Unit full precision.

### Per-KH daily performance (LỊCH SỬ — materialize)
- **`T_SI_NAV_BALANCE`** (business_date, `C_SI_ACCOUNT`; nav, unit, unit_price, daily_pnl, daily_return) — **BẮT BUỘC**: vì FO sync snapshot (overwrite) → holdings không event-source → không derive được NAV/unit_price quá khứ → phải lưu để vẽ chart FR-03. ~2,5 tỷ dòng/10 năm → CCI + partition (có thể lấy điểm thưa để giảm tải).

### Chuỗi daily master-level (materialize, nhỏ)
- **`T_MASTER_NAV_BALANCE`** (business_date, `C_MASTER_CODE`; cash, stock_value, cash_dividend, custody_fee, mgmt_fee_accrued, payable_fee, total_asset, nav, unit, unit_price, daily_pnl, daily_return) — **NGUỒN NAV master-level DUY NHẤT** (gộp snapshot composition + hiệu suất master — cùng grain, NAV trùng). `nav = stock_value + cash`; `mgmt_fee_accrued`/`payable_fee` để FO báo cáo tham khảo (SDI không tự accrue).
- **`T_MASTER_NAV_CURRENT`** (`C_MASTER_CODE`; cash, stock_value, total_asset, last_nav, unit, last_unit_price, last_business_date) — NAV/state **current cấp master** (1 dòng/master, overwrite mỗi EOD bởi J11). Phục vụ đọc nhanh "toàn bộ quỹ hiện tại" (FR-01 overview, monitor AUM) khỏi `WHERE date=MAX`. Đối xứng `T_SI_NAV_CURRENT`. **Không** dùng cho tính EOD (agg lại tươi mỗi ngày).
- **`T_MASTER_HOLDING_BALANCE`** (business_date, `C_MASTER_CODE`, ticker; quantity, market_price, market_value, weight) — top 20 + "mã khác"
- **`T_MASTER_INDEX_DAILY`** (business_date, `C_MASTER_CODE`; index_value, daily_return)

### Control / orchestration
- **`T_EOD_RUN`** (business_date, job PK; status[PENDING|RUNNING|DONE|FAILED], rows, started_at, ended_at, message) — theo dõi & resume batch EOD (§9.2).

### Customer-level: MATERIALIZE (do FO-sync)
NAV/Unit Price/PnL theo ngày của KH được **lưu vào `T_SI_NAV_BALANCE`** mỗi EOD (J10). Vì FO sync **overwrite** holdings (không event-source) → KHÔNG derive được quá khứ → phải materialize. TWR/MWR theo range = đọc 2 đầu mút từ bảng này (TWR) hoặc dùng cashflow events (MWR). Giảm tải: điểm thưa / chỉ unit_price.

### Partition & retention

| Bảng | Partition | Retention |
|---|---|---|
| event/ledger customer-level | HASH(C_SI_ACCOUNT) + range YEAR | 10 năm online |
| daily master-level, market | range YEAR | 10 năm |
| holding_hist / cash_hist (interval) | range YEAR(valid_from) | full history (no-dup) — 2 năm online + cũ archive HDD |

Index: `(C_SI_ACCOUNT, business_date)` cho customer-level; `(C_MASTER_CODE, business_date)` cho master-level.

---

## 9. Batch EOD

### 9.1 Công thức pipeline (mức tính toán)
```
NAV          = stock_value + cash (FO cash đã NET phí QL + thuế GD; SDI không trừ lại)
PnL ngày     = NAV cuối − NAV đầu + NAV ra − NAV vào
ΔUnit        = net CF / UnitPrice_(t-1) ; Unit = Unit_(t-1)+ΔUnit (full) ; UnitPrice = NAV/Unit
SI NAV/Unit  = Σ per si ; SI UnitPrice = SI NAV / SI Unit
SI Index_t   = Index_(t-1) × Σ w^(t)·P_t/P_ref
```

### 9.2 Danh sách JOB chạy tuần tự cuối ngày

Mỗi job **idempotent** (chạy lại 1 ngày → cùng kết quả), ghi trạng thái vào `T_EOD_RUN`. "SB" = set-based (không RBAR). "‖" = song song theo master/hash(C_SI_ACCOUNT).

| # | Job | Phụ thuộc | Đọc | Ghi | SB | ‖ | Halt nếu lỗi |
|---|---|---|---|---|---|---|---|
| **INGEST** | `SP_INGEST_CUSTOMER` (Kafka per-KH, realtime, KHÔNG trong batch) | — | event 1 KH (JSON): cash + holdings + cổ tức/phí | cash→state + holdings→current + **interval CASH_HIST/HOLDING_HIST** + fee (dedup) + watermark `C_LAST_SYNC_DATE` | ✅ | ‖ per-cust | ✅ (forward-only: quá khứ→THROW) |
| **J0** | `GATE` chờ đủ FO ingest | INGEST | received (watermark=@d) vs expected (tiểu khoản ACTIVE) | chặn EOD nếu thiếu | – | – | ✅ (thiếu→alert) |
| ~~J1/J1b~~ | ~~`STAGE`/`SYNC_FO`~~ **(CHUYỂN sang INGEST realtime)** | — | FO sync giờ qua Kafka per-KH, không batch STAGE/SYNC | — | – | – | – |
| **J2** | `VALIDATE` (giá/market) | J0 | giá/CA/model_weight (market feed) | log lỗi | ✅ | – | ✅ (thiếu giá/trùng key/qty âm) |
| ~~J6~~ | ~~`ACCRUE_FEE`~~ **(ĐÃ BỎ)** | — | phí QL + thuế GD do FO trừ vào cash khi book; SDI không accrue lại (tránh double-count) | — | – | – | – |
| **J7** | `MTM` định giá lại toàn bộ | J1b | indexing_portfolio_ticker + giá @d | stock_value per vị thế (#nav_today) | ✅ | ‖ | – |
| **J8** | `CALC_NAV` | J7 | stock_value, state.cash (FO) | NAV = stock_value + cash | ✅ | ‖ | – |
| **J9** | `CALC_PNL` | J8 | NAV, NAV_prev, CF | daily_pnl per vị thế | ✅ | ‖ | – |
| **J10** | `CALC_UNIT` | J8 | CF_t (cashflow event), UnitPrice_prev | ΔUnit/Unit/UnitPrice; T_SI_UNIT_LEDGER; **T_SI_NAV_BALANCE** (lịch sử per-KH) | ✅ | ‖ | – |
| **J11** | `SI_AGG` tổng hợp SI | J8, J10 | cash/stock/NAV/unit per vị thế + T_SI_FEE_INCOME | **T_MASTER_NAV_BALANCE** (composition + NAV + hiệu suất + cổ tức/phí Σ từ ledger) + **T_MASTER_NAV_CURRENT** (upsert) | ✅ | ‖ | – |
| **J12** | `SI_INDEX` + benchmark | J2 | model_weight, giá, VN-Index | T_MASTER_INDEX_DAILY, T_BENCHMARK_DAILY | ✅ | ‖ | – |
| **J13** | `RECONCILE` đối soát | J11 | SDI holdings/NAV vs FO; Σ customer NAV vs master NAV; Σ unit | bảng break | ✅ | – | ✅ (break > ngưỡng → chặn publish) |
| **J14** | `BUILD_SNAPSHOT` | J8 | holdings | T_MASTER_HOLDING_BALANCE (top20+mã khác) | ✅ | ‖ | – |
| ~~J14b~~ | ~~`HISTORY`~~ **(CHUYỂN sang INGEST realtime)** | — | interval CASH_HIST/HOLDING_HIST maintain TẠI ingest per-event; `SP_EOD_HISTORY` chỉ còn utility bulk-backfill | — | – | – | – |
| **J15** | `PUBLISH` | J13, J14 | staging/đích | commit `T_SI_NAV_CURRENT`; SWITCH/MERGE master-level; push current snapshot + master series → Asset | ✅ | – | ✅ |
| **J16** | `FINALIZE` | J15 | — | mark eod_run done; (cuối tháng) build snapshot KH; update stats; alert success | – | – | – |

### 9.3 Thứ tự, song song & orchestration

```
INGEST (Kafka per-KH realtime: cash/holdings/phí + interval history) ──┐
                                                                       ▼
J0 GATE → J7 ─ J8 → J9
                    └─ J10 ─┐
                            └─ J11 → J13 ─┐
   J2 ──► J12 (độc lập, song song) ───────┤
                          J8 → J14 ───────┴─ J15 → J16
```
(J6 ACCRUE_FEE đã bỏ — FO cash đã NET phí; NAV = stock + cash.)
- **INGEST (thay J1/J1b/J14b)**: FO bắn Kafka per-KH (1 event=1 KH) cuối ngày trước EOD → `SP_INGEST_CUSTOMER` xử lý NGAY: cash→state + holdings→current + **interval history** + cổ tức/phí (dedup), set watermark. Idempotent (so-trạng-thái / fee dedup). **Forward-only** (event quá khứ→THROW; history sẽ làm sau: FO resync full D→nay + replay). Cashflow nạp/rút SDI-side (ghi thẳng, không Kafka). CA chỉ dùng cho **J12 index**; cashflow dùng cho **CF_t** (J9/J10).
- **J0 GATE**: đếm received (state có watermark=@d) vs expected (tiểu khoản ACTIVE) → đủ mới chạy, thiếu thì alert.
- **J12 (SI Index)** chỉ cần giá + model_weight → song song nhánh customer.
- **J7 sau INGEST** (state cash + holdings đã sync qua Kafka); J8 NAV = stock + cash (không trừ phí).
- **J7/J8/J9/J10/J14** chia **dải master hoặc hash(C_SI_ACCOUNT)** chạy nhiều luồng.
- **J13 RECONCILE là cổng**: lệch quá ngưỡng → **dừng, KHÔNG publish dữ liệu sai**, alert.
- **Thực thi ALL-IN-DB**: mỗi job = **1 stored proc** (set-based); **master proc `SP_EOD_RUN @business_date`** gọi tuần tự + ghi `T_EOD_RUN(business_date, job, status, rows, started, ended, message)`. **App/SQL Agent chỉ kích hoạt master proc** — không tính toán ở app. Fail giữa chừng → **resume từ job lỗi** (idempotent). Ingestion = proc `BULK INSERT`; API đọc = stored proc.
- **RCSI** bật → app đọc current snapshot không bị batch chặn; **J15 PUBLISH** (switch-in) là thao tác ngắn duy nhất ảnh hưởng đích.
- **Roll-forward**: holdings = FO snapshot full vào current; **state** (cash/NAV/unit) roll-forward tại chỗ; J7–J10 chạm toàn bộ ~1M (giá đổi) nhưng đều **set-based**. Không replay lịch sử.

> Chi tiết kỹ thuật (columnstore, partition switch, runtime ~vài phút–15 phút, anti-patterns): [SDI-db-architecture.md](./SDI-db-architecture.md).

---

## 10. API cho Asset/SMO

> Mỗi API = **app gọi 1 stored proc** (`SP_GET_*`, xem `db/05_API.sql`) — tính/derive trong DB; app chỉ trả JSON, không tính. Định danh: KH=`C_CUST_CODE`, đơn vị = `C_SI_ACCOUNT` (sub-account); master suy từ sub-account.

| FR | API | Proc | Nguồn |
|---|---|---|---|
| FR-01 Tổng quan đa SI | GET /customer/{id}/si-overview | `SP_GET_SI_OVERVIEW` | sum T_MASTER_NAV_BALANCE + derive customer NAV (current từ customer_nav_current) |
| FR-02 Chi tiết 1 SI | GET /customer/{id}/si/{si} | `SP_GET_SI_DETAIL` | derive customer NAV/PnL + TWR + MWR + T_MASTER_NAV_BALANCE |
| FR-03 Chart so sánh | GET /customer/{id}/si/{si}/performance?range= | `SP_GET_SI_PERFORMANCE` | T_MASTER_NAV_BALANCE (TR) + si_index (PR) + benchmark VN-Index (PR), chuỗi [mốc..cuối] |
| FR-04 Thông tin đầu tư | GET /customer/{id}/si/{si}/info | `SP_GET_SI_INFO` | T_SI_PORTFOLIO + master |
| FR-05 Holdings | GET /customer/{id}/si/{si}/holdings | `SP_GET_SI_HOLDINGS` | **holdings CURRENT của KH** (indexing_portfolio_ticker × giá mới nhất) top20 + "OTHER" — sản phẩm segregated nên đọc holdings KH (≠ SI-aggregate T_MASTER_HOLDING_BALANCE) |
| FR-06 Báo cáo tài sản | GET /customer/{id}/si/{si}/asset-report | `SP_GET_ASSET_REPORT` | T_SI_NAV_BALANCE (NAV) + T_SI_FEE_INCOME (cổ tức/phí) + cash/stock reconstruct (customer_cash_hist + holding_hist×giá theo interval) |

---

## 11. Scale (100 SI, 200K KH × 5 SI, 10 năm)

> Chi tiết kiến trúc DB + EOD ở quy mô lớn cho SQL Server: [SDI-db-architecture.md](./SDI-db-architecture.md) (roll-forward state, set-based, columnstore, partitioning).
> Hợp đồng trao đổi dữ liệu EOD FO↔SDI↔Asset (payload từng bên + định lượng small/medium/large): [SDI-eod-data-exchange.md](./SDI-eod-data-exchange.md).
> Dự phóng tăng trưởng dữ liệu 1M/1Q/1Y (KH tăng đều/nóng): [SDI-data-growth-projection.md](./SDI-data-growth-projection.md).

| Dữ liệu | Ước lượng |
|---|---|
| SI Index / Performance / Asset snapshot | ~250K dòng/loại |
| Cashflow / execution / holding event | ~100M+ |
| Unit ledger | ~120M |
| Customer NAV/UP/% daily | **materialize** `T_SI_NAV_BALANCE` (~2,5 tỷ, CCI) — bắt buộc do FO-sync |

Customer NAV/UP/% daily: **BẮT BUỘC materialize** `T_SI_NAV_BALANCE` mỗi EOD — vì FO sync overwrite holdings (không event-source) → KHÔNG derive được lịch sử per-customer. Master-level agg tươi mỗi ngày + lưu `T_MASTER_NAV_BALANCE`.

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
| Entity | `C_MASTER_CODE` (master), `C_SI_ACCOUNT` (sub-account/tiểu khoản = cust_code+đuôi), `C_CUST_CODE`, `business_date` |
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
