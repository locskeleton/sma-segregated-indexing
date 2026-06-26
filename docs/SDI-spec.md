# SDI — Asset & Performance Engine — Specification

Engine tính tài sản, hiệu suất danh mục master và từng khách hàng cho sản phẩm **SMA chỉ số** (separately managed account). Phục vụ **giao diện riêng của SDI** (NAV/index/perf) qua read API.

> 📖 Tra cứu nhanh thuật ngữ (VN/EN) + mọi công thức kèm ví dụ: [SDI-thuat-ngu-cong-thuc.md](./SDI-thuat-ngu-cong-thuc.md).
>
> **⚠️ THIN-LAYER (2026-06-26):** SDI **KHÔNG tự tính hiệu suất**. Asset gửi per-KH/ngày **`aum` (= NAV ròng) + `daily_return` (TWR, đã khử dòng tiền)** + `cash` + `cash_in/out`; SDI chỉ **LƯU + SERVE** (compound `daily_return` on-read → %PnL kỳ; AUM-weighted → master/PM; TE prefix-sum). **ĐÃ GỠ:** `unit`/`unit_price`(NAVPS)/PnL-tiền-ngày(`C_DAILY_PNL`)/`T_SI_UNIT_LEDGER`/MWR(Modified-Dietz·XIRR)/master pooled unit price/`stock_value`/J06·J08·J09·J10-derive. **GIỮ:** Master Index (SDI tự tính từ giá×weight), TE, Deviation, Benchmark, Cash drag, AUM growth, reconcile. Các §5 (Unit&UP), §6 (PnL/MWR/master pooled) dưới đánh dấu **"[thin-layer] ĐÃ GỠ"** = lịch sử. Bảng rename: `T_SI_BALANCE`/`T_SI_CURRENT`/`T_MASTER_BALANCE`/`T_MASTER_CURRENT`; `C_NAV`→`C_AUM`.
>
> **⚠️ BRD 2026-06-22:** SDI **KHÔNG còn đồng bộ tài sản KH/NAV-perf master sang Asset** (BO/FO/Market đẩy thẳng, Asset tự tính). Đã GỠ khỏi `db/05_API.sql` **2 producer**: `SP_GET_ASSET_SNAPSHOT` (per-SI tài sản), `SP_GET_ASSET_MASTER_SNAPSHOT` (master NAV/perf). **VẪN GIỮ `SP_GET_ASSET_INDEX_SNAPSHOT`** — SDI vẫn đẩy Master Index sang Asset theo luồng RIÊNG khi BO price-ready (`SP_EOD_RUN_INDEX`); đây là luồng SDI→Asset DUY NHẤT còn lại. **[BRD asset-sync] Asset gửi NAV RÒNG trực tiếp về SDI** (đã trừ phí QL — model realized; BO KHÔNG gửi số phí lũy kế accrued) → SDI **KHÔNG accrue phí QL** nữa, **AUM = NAV** (không tách payable). Engine read API (FR-01..06, PM) GIỮ NGUYÊN. Còn lại = điểm reconcile (NAV_CONSISTENCY, cashflow 2 nguồn, TWR) — xem [SDI-asset-gap.md](./SDI-asset-gap.md).

---

## 1. Mô hình & ranh giới

**SMA — segregated custody, lệnh đặt trực tiếp trên tài khoản khách:**

```
KH chuyển tiền vào tiểu khoản (mỗi tiểu khoản = 1 master)
        │ SDI gửi yêu cầu rebalance (trigger)
        ▼
FO: tính tỷ trọng danh mục mẫu  +  đặt lệnh MP TRỰC TIẾP trên TK từng KH
    (không gom + phân bổ; khớp → cổ phiếu, không khớp → tiền của KH)
        │ feed EOD: model_weight + ĐỒNG BỘ holdings toàn bộ TK (composition; SDI không quản lý từng lệnh khớp)
        ▼
SDI: [thin-layer] LƯU aum + daily_return (Asset gửi) + holdings (FO, composition) → SERVE %PnL kỳ (compound daily_return), Master Index (tự tính), TE/deviation  →  read API (UI riêng SDI)
```

> **BRD 2026-06-22:** SDI **không còn push tài sản KH/NAV-perf master sang Asset** (Asset nhận BO/FO/Market trực tiếp & tự tính); **vẫn GIỮ push Master Index** (`SP_GET_ASSET_INDEX_SNAPSHOT`, khi BO price-ready). Xem [SDI-asset-gap.md](./SDI-asset-gap.md). SDI phục vụ giao diện riêng qua read API.

| Việc | Chủ |
|---|---|
| Gửi yêu cầu rebalance (trigger, không chứa weights) | SDI |
| Tính tỷ trọng danh mục mẫu (luôn 100% cổ phiếu) | **FO** |
| Đặt & khớp lệnh MP trên TK từng KH | **FO** |
| Tính NAV ròng + `daily_return` (TWR) per-KH | **Asset** (thin-layer) |
| LƯU aum/daily_return + SERVE (%PnL compound, AUM-weighted), tính Master Index | SDI |
| Đọc & hiển thị (UI riêng của SDI) | SDI read API (FR-01..06, PM) |

- **Custody = segregated**: tiền & cổ phiếu nằm thật trong tiểu khoản KH (KH sở hữu hợp pháp).
- **[thin-layer] SDI = consumer + index engine + serve layer**: không quyết tỷ trọng, không sinh/khớp lệnh, **không tự định giá NAV / không tự tính hiệu suất** (Asset cấp `aum`+`daily_return`). SDI tự tính Master Index (giá×weight) + serve TE/deviation.
- **EOD** là đơn vị tính; chốt 1 lần cuối ngày. Sửa quá khứ = **re-ingest** (Asset gửi lại ngày cũ).

---

## 2. Khái niệm nền — hai thế giới

| | Thế giới THẬT (per KH × master) | Thế giới BENCHMARK (lý thuyết) |
|---|---|---|
| Đo | Tiền thật của KH: AUM + `daily_return` (Asset gửi) | Chỉ số: Master Index, VN-Index |
| Công cụ | `daily_return` (TWR, Asset khử dòng tiền) → compound | Index (weights × giá) |
| Hiển thị | "Lợi suất của bạn" / "Hiệu suất master" | "Danh mục mẫu", "VN-Index" trên chart FR-03 |

- **Hai cấp:** **MASTER** = danh mục mẫu/chiến lược (mã `C_MASTER_CODE`). **SUB-ACCOUNT (tiểu khoản)** = KH đầu tư 1 master → cấp 1 sub-account, mã `C_SI_ACCOUNT` (= CUST_CODE+đuôi, customer-level). **Close+reopen master ⇒ sub-account MỚI** (mã khác, KHÔNG tái dùng) → 1 KH có nhiều sub-account/master theo thời gian (tối đa 1 ACTIVE/lúc). Mọi bảng customer-level khóa theo `C_SI_ACCOUNT`.
- **Tiểu khoản** = đơn vị nhỏ nhất = 1 sub-account (`C_SI_ACCOUNT`) của một (customer × MASTER). Reopen → sub-account mới, chuỗi `daily_return` (Asset gửi) bắt đầu lại từ ngày tham gia. *([thin-layer] không còn khởi tạo UP=10.000.)*
  > **Vocabulary:** dùng **"master"** (= danh mục mẫu/chiến lược, cấp `C_MASTER_CODE`, bảng `T_MASTER_*`) và **"tiểu khoản"** (sub-account `C_SI_ACCOUNT`, bảng `T_SI_*`). Thuật ngữ "SI" cũ đã đổi hết: "SI Index" → **"Master Index"** (benchmark danh mục mẫu), "Hiệu suất SI" → "Hiệu suất master". *([thin-layer] "SI NAV/Unit Price" → "Master AUM"; unit price đã gỡ.)*
- Hiệu suất tính **per tiểu khoản (KH × master)**; cấp master = tổng hợp các KH.

---

## 3. Tài sản & NAV

```
AUM = NAV     = Asset GỬI TRỰC TIẾP per-KH (đã trừ phí QL — tài sản RÒNG)   [thin-layer]
Tiền (cash)   = TỔNG tiền dư (1 số: gộp tiền mặt + bán chờ về + cổ tức tiền) — Asset gửi kèm
daily_return  = Asset GỬI (TWR ngày, đã khử dòng tiền) — SDI compound on-read
Tổng vốn đầu tư = Σ cash_in − Σ cash_out             (net cashflow lũy kế)
```

> **[thin-layer]** SDI **KHÔNG còn tự định giá / accrue phí QL / derive unit-UP-PnL**. **Asset gửi per-KH/ngày GD**: `aum` (NAV ròng — model REALIZED, phí QL đã trừ) + `daily_return` (TWR) + `cash` (tổng) + `cash_in/cash_out`. SDI **ingest thẳng** (`SP_INGEST_ASSET_NAV` → `T_SI_ASSET_DAILY`) rồi **LƯU** `aum`+`daily_return` vào `T_SI_BALANCE` (KHÔNG tính thêm gì).

- **AUM = NAV = Asset gửi trực tiếp** (không còn `total_asset − payable`, không còn `stock + cash`). `cash` gửi kèm để hiển thị (FR-06) + cash drag. `cash` = **TỔNG tiền dư** (1 số — Asset không chia nhỏ). **[thin-layer]** Asset KHÔNG gửi `stock_value` ⇒ reconcile NAV_CONSISTENCY (`nav` vs `stock+cash`) đã GỠ.
- **Asset là nguồn NAV/tiền/hiệu suất DUY NHẤT**. SDI mirror số Asset đẩy về, không tự cộng/trừ; không suy cash/NAV/return từ holdings.
- **Phí QL: model REALIZED** — phí chỉ giảm tài sản khi BO cắt thật (qua cash). **BO KHÔNG gửi số phí lũy kế (accrued)** cho Asset; Asset đã trừ phí sẵn trong NAV. SDI **KHÔNG accrue, KHÔNG quản payable**. Thuế GD do FO/BO net vào cash khi khớp (ngoài SDI).
- **`CF_t` chỉ lấy từ cashflow event** (nhãn DEPOSIT/SIP/WITHDRAW) — **không** suy từ Δ tổng tiền. Event phải khớp đúng ngày + số tiền với thời điểm FO phản ánh vào cash.
- Phí phạt rút sớm: do FO trừ vào cash, KHÔNG tính vào cashflow.

### Cash sub-ledger (typed)
Tổng tiền chỉ để tính NAV. Các thành phần tiền lưu **theo loại** (FO cấp nhãn) phục vụ 3 mục đích:

| Dùng cho | Lấy từ |
|---|---|
| Cash drag / FR-06 | `cash` (tổng) Asset gửi |
| Cashflow (đối soát 2 nguồn) | event nhãn DEPOSIT/SIP/WITHDRAW (SDI originator) vs `cash_in/out` Asset gửi |
| Income | đã nằm trong `aum`/`cash` Asset gửi (không tag cash_in) |

> **[thin-layer]** Cashflow SDI nhập KHÔNG còn dùng để "đổi unit" (unit đã gỡ) — chỉ để **đối soát** với `cash_in/out` Asset gửi (reconcile CASHFLOW). `daily_return` Asset gửi đã khử dòng tiền sẵn.

---

## 4. Cashflow vs Income

| Loại | Là gì | Vào đâu | Khử khỏi return? |
|---|---|---|---|
| **NAV vào** (`cash_in`) | tiền KH bơm vào: nộp lần đầu, nộp thêm, SIP, lãi Infy | external cashflow | ✅ (Asset khử trong `daily_return`) |
| **NAV ra** (`cash_out`) | tiền KH rút | external cashflow | ✅ (Asset khử trong `daily_return`) |
| **Income** | cổ tức/lãi do tài sản quỹ sinh ra | đã trong `aum` Asset gửi | ❌ (vào hiệu suất) |
| **Chi phí** | phí quản lý/thuế/perf, thuế GD, phí phạt rút sớm | **[thin-layer]** phí QL đã trừ sẵn trong NAV Asset gửi (model realized); thuế GD/phạt do FO/BO trừ vào cash (SDI không re-apply) | ❌ |

- `net_cashflow (CF_t) = cash_in − cash_out` — **external, per (KH×master) = per tiểu khoản, per ngày**, lấy từ event có nhãn. **[thin-layer]** dùng để **đối soát** với `cash_in/out` Asset gửi (KHÔNG còn để tính unit/return — Asset cấp `daily_return` đã khử dòng tiền).
- **Cổ tức tiền mặt**: income — đã nằm trong `aum`/`cash` Asset gửi, tự động vào hiệu suất. KHÔNG tag là cash_in.

---

## 5. ~~Unit & Unit Price~~ — **[thin-layer] ĐÃ GỠ (historical)**

> **ĐÃ GỠ.** Unit/Unit Price là cơ chế SDI tự *derive* TWR từ NAV+cashflow (khử dòng tiền qua số unit). **Thin-layer: Asset gửi thẳng `daily_return` (TWR đã khử dòng tiền)** ⇒ SDI không phát hành unit / không tính unit price. Bảng `T_SI_UNIT_LEDGER`, cột `C_UNIT`/`C_UNIT_PRICE` đã bỏ. Tính chất khử dòng tiền `UPₜ = UP₍ₜ₋₁₎·(1+rₜ)` nay là **trách nhiệm của Asset** khi cấp `daily_return`.
>
> *(Công thức lịch sử, để truy nguồn — KHÔNG còn dùng:)*
> ```
> T0:  UP₀ = 10.000 ;  Unit₀ = NAV₀/10.000
> Tn:  CFₜ = cash_in−cash_out ;  ΔUnitₜ = CFₜ/UP₍ₜ₋₁₎ ;  Unitₜ = Unit₍ₜ₋₁₎+ΔUnitₜ ;  UPₜ = NAVₜ/Unitₜ
>      telescoping: %PnL = Π(UPₜ/UP₍ₜ₋₁₎)−1 = UP_cuối/UP_đầu−1
> ```

---

## 6. Hiệu suất (per KH × master)

> **[thin-layer]** SDI **LƯU** `aum` + `daily_return` (Asset gửi) per-KH/ngày, rồi **SERVE** %PnL kỳ = **compound** chuỗi `daily_return`. KHÔNG còn PnL-tiền/MWR/master pooled (cần unit).

### %PnL kỳ — TWR (compound daily_return)
```
%PnL(range) = ∏(1 + daily_returnₜ) − 1 = EXP( Σ ln(1 + daily_returnₜ) ) − 1     (t SAU ngày mốc)
```
- **Ngày mốc (base)** = gốc 0%; return phủ các ngày **SAU** ngày mốc.
- Compound on-read các `daily_return` Asset gửi; bỏ ngày `daily_return` NULL; guard `daily_return ≤ −1` (kẹp để LOG không vỡ).

**Ngày mốc theo filter:**

| Filter | Ngày mốc (base) |
|---|---|
| YTD | close phiên cuối năm trước (~31/12) |
| 1M / 3M / 6M / 1Y / 3Y | close ngày tương ứng N về trước |
| Inception | close ngày tiểu khoản khởi tạo (return cộng dồn từ đó) |
| KH/tiểu khoản tham gia sau mốc filter | close ngày tham gia |

### ~~PnL (tiền) / MWR~~ — **[thin-layer] ĐÃ GỠ (historical)**
> **ĐÃ GỠ.** PnL-tiền-ngày (`NAV cuối − NAV đầu + ra − vào`) và MWR (Modified Dietz / XIRR) cần NAV đầu/cuối + cashflow + unit để SDI tự tính. Thin-layer chỉ giữ `aum` + `daily_return` (Asset cấp) ⇒ chỉ phục vụ **TWR** (compound). Chênh AUM 2 mốc tự suy nếu UI cần con số tiền.

### Tổng hợp master ("Hiệu suất master" trên chart)
```
Master daily return = AUM-weighted  Σ(AUMᵢ·rᵢ) / Σ AUMᵢ      (engine ghi T_MASTER_BALANCE.C_DAILY_RETURN)
Master %PnL kỳ      = ∏(1 + master_daily_returnₜ) − 1         (compound; US3 composite)
```
> **[thin-layer]** KHÔNG còn `Master Unit Price = Σ NAV / Σ Unit` (unit đã gỡ). Master daily return = bình quân gia quyền AUM các `daily_return` KH; compound lên thành đường "Hiệu suất master".

---

## 7. Master Index — danh mục mẫu (benchmark)

```
Index_0 = 1000
Index_t = Index_(t-1) × Σ_i ( w_i^(t) × P_i,t / P_ref_i )

   w_i^(t) = bộ weights có effective_date ≤ t MỚI NHẤT (bộ chi phối ngày t)
   Σ w_i^(t) = 100%  (LUÔN 100% cổ phiếu — không có thành phần tiền)
   P_ref_i   = giá đóng cửa (t-1)  |  giá tham chiếu điều chỉnh khi có quyền (corporate action)
```

- **Weights do FO tính & feed về** (`model_weight`), version theo `effective_date` (mức ngày). Bộ weights `effective_date = D` chi phối return ngày D (đo từ close D-1 → close D).
- **Rebalance**: tính EOD-only, close-to-close; 1–2 lần/ngày chỉ lấy **trạng thái weights net cuối ngày**, không cần giá intraday. Sai lệch giữa hiệu suất master thật và index = **tracking error thực thi** (hợp lệ — đúng mục đích FR-03).
- Mã mới vào rổ: `P_ref` = giá đóng cửa ngày trước khi vào. Mã halt không có giá: dùng giá gần nhất, gắn cờ.
- Corporate action: xử lý điều chỉnh `P_ref` trước, rồi áp weights.

### So sánh FR-03 (3 đường, đều `(điểm cuối/điểm mốc − 1)` cùng kỳ)

| Đường | Nguồn | Phương pháp |
|---|---|---|
| Hiệu suất master | Master daily return (AUM-weighted `daily_return` Asset gửi) | compound (TWR), total return |
| Danh mục mẫu | Master Index | Index, price return |
| VN-Index | Market data | Index, price return |

> Master (hiệu suất thật) là total-return (NAV ăn cổ tức), benchmark là price-return → master nhỉnh hơn ~mức cổ tức một cách hệ thống; đây là hệ quả cơ sở (PR vs TR), được chấp nhận (user đối chiếu VN-Index là chuẩn phổ quát).

---

## 8. Kiến trúc dữ liệu

Prefix bảng `T_`, cột `C_`. **Quy chuẩn kiểu:** Tiền VND & quantity = `DECIMAL(20,0)` (không thập phân); giá = `DECIMAL(18,4)`; % / return / fee_rate = `DECIMAL(10,6)`; unit & unit_price = `DECIMAL(18,6)`; weight = `DECIMAL(12,8)`. **[BRD asset-sync]** Phí QL đã trừ sẵn trong NAV Asset gửi (model realized) ⇒ **không còn cột phí lũy kế (payable/accrued) ở SDI**. **Hai cấp:** master (`C_MASTER_CODE`, bảng `T_MASTER_*`) / sub-account = tiểu khoản (`C_SI_ACCOUNT`, bảng `T_SI_*`).

### Master / cấu hình
- **`T_MASTER_PORTFOLIO`** (**`C_MASTER_CODE` PK** — mã danh mục MASTER, khóa chính + khóa public, KHÔNG surrogate; name, status[ACTIVE|CLOSED], inception_date, benchmark_code) — các bảng khác tham chiếu master theo `C_MASTER_CODE`; bảng tổng hợp master-level: `T_MASTER_BALANCE`/`T_MASTER_HOLDING_BALANCE`/`T_MASTER_INDEX_DAILY`/`T_MASTER_CURRENT`. **[BRD asset-sync] Không còn cột phí ở đây** (phí QL đã trừ sẵn trong NAV Asset gửi — SDI không cấu hình/accrue phí). **Sub-account** (`T_SI_PORTFOLIO`): `C_SI_ACCOUNT` (mã sub-account, UNIQUE) + `C_MASTER_CODE` + `C_CUST_CODE` + close_date; filtered-unique 1 ACTIVE/(cust,master). Customer-level tables khóa theo `C_SI_ACCOUNT`.
- **`T_FEE_CONFIG`** — **[BRD asset-sync] ĐÃ GỠ.** SDI không còn accrue/cấu hình phí (phí QL đã trừ sẵn trong NAV Asset gửi — model realized). Catalog chính sách phí giờ thuộc Asset/BO, không phải SDI.
- **`T_MASTER_PORTFOLIO_TICKER`** (`C_MASTER_CODE`, effective_date, ticker; target_weight) — **FO tính & feed**; Σ = 100% cổ phiếu/eff_date.
- **`T_SI_PORTFOLIO`** (`C_SI_ACCOUNT` UNIQUE; `C_CUST_CODE`, `C_MASTER_CODE`, sub_account_no, join_date, status, close_date, initial_amount, sip_amount, sip_schedule, min_invest) — registry tiểu khoản + cấu hình đầu tư KH (FR-04). **[BRD asset-sync] Phí không thuộc SDI** (đã trừ sẵn trong NAV Asset gửi).

### Market data
- **`T_PRICE_DAILY`** (ticker, business_date PK; **ref_price** NOT NULL, close_price, **is_ex_rights** [1=ngày có sự kiện quyền gây chia giá / 0=phiên thường]) — **gộp corporate action vào bảng giá**: `ref_price` = giá tham chiếu đầu phiên sở publish MỖI ngày (phiên thường = close hôm trước; ex-rights = giá sau chia), là mẫu số daily-return J12 → engine self-contained, KHÔNG tra bản ghi ngày trước. `is_ex_rights` = metadata. Bỏ bảng `T_CORPORATE_ACTION` riêng (type/ratio/cash_div không tham gia tính; cổ tức/quyền vào NAV qua FO sync).
- **`T_BENCHMARK_DAILY`** (benchmark_code, business_date PK; index_value) — chỉ số thị trường ngoài (VN-Index, price return), **nạp từ market data** (không do SDI tính). Key = code tự mô tả (giống ticker), không cần dimension riêng.
- **`T_TICKER_INDUSTRY`** (ticker PK; industry_code, industry_name) — dimension mã→ngành cho industryWeight alert (`SP_GET_MASTER_ALERTS`). Nguồn nạp thật (FO/market data) chưa làm — hiện seed ở smoke.

### FO sync (EOD) & cashflow
- **`T_REBALANCE_REQUEST`** (request_id PK; `C_MASTER_CODE`, business_date, type[REBALANCE|DEPLOY|REDEEM], status) — **SDI → FO**, trigger (không chứa weights).
- **`T_SI_HOLDING_HIST`** (`C_SI_ACCOUNT`, ticker, valid_from; valid_to, quantity, avg_cost) — **HISTORY holdings theo KHOẢNG (INTERVAL / SCD-2)**: **FULL history BẮT BUỘC (compliance), KHÔNG trùng lặp** — holding bất biến N năm = **1 dòng** (`valid_to=NULL` = đang mở). Maintain bằng **DIFF** current vs dòng open **TẠI INGEST (per-event Kafka)** (đóng dòng đổi/biến mất → mở dòng mới). Reconstruct ngày D: `valid_from≤D AND (valid_to>D OR valid_to IS NULL)`. FO ingest holdings THẲNG `T_SI_PORTFOLIO_HOLDING` (current); **EOD core KHÔNG đọc hist**.
- **`T_SI_CASH_HIST`** — **[BRD asset-sync] ĐÃ GỠ.** History cash interval không còn (tiền/NAV nay từ Asset; sửa quá khứ = re-ingest, không reconstruct cash từ history).
- **State per-KH `T_SI_CURRENT`** (roll-forward) — **[thin-layer]** giữ `C_AUM` (= `C_LAST_AUM`, NAV ròng Asset gửi) + `cash` (tổng). **Không còn** `C_PAYABLE_FEE`/`C_STOCK_VALUE`/`C_UNIT`/`C_LAST_UNIT_PRICE` (Asset cấp `aum`+`daily_return`; AUM = NAV).
- **`T_SI_CASHFLOW_EVENT`** (event_id PK; `C_SI_ACCOUNT`, business_date, event_type[INITIAL|TOPUP|SIP|INTEREST_IN|WITHDRAW], amount, created_time) — external cashflow (SDI originator); **[thin-layer]** dùng để **đối soát** với `cash_in/out` Asset gửi (KHÔNG còn để tính unit — Asset cấp `daily_return` đã khử dòng tiền).
- **`T_SI_INCOME_FEE`** — **[BRD asset-sync] ĐÃ GỠ.** Sổ cái phí/thu nhập per-KH (cổ tức + phí lưu ký + phí ACCRUE cắt) không còn ở SDI: phí QL đã trừ sẵn trong NAV Asset gửi (model realized), cổ tức/income đã nằm trong NAV/`cash` Asset gửi. Cùng đó gỡ proc `SP_INGEST_FEE_CHARGE` (net-off payable).
- **`T_SI_FEE_ACCRUAL` / payable / Option B breakdown per-type** — **[BRD asset-sync] ĐÃ GỠ TOÀN BỘ.** Không còn `C_PAYABLE_FEE`, không accrue, không breakdown per-type. Phí QL chỉ giảm tài sản khi BO cắt thật (qua cash, đã phản ánh trong NAV Asset gửi) — SDI không lưu/dựng lại số phí lũy kế.
- **`T_SI_UNIT_LEDGER`** — **[thin-layer] ĐÃ GỠ.** Sổ cái unit thay đổi (cf_net/delta_unit/unit) không còn — SDI không phát hành unit (Asset cấp `daily_return`).

### Per-KH daily performance (LỊCH SỬ — materialize)
- **`T_SI_BALANCE`** (business_date, `C_SI_ACCOUNT`; **`C_AUM`** (NAV ròng Asset gửi), **`C_DAILY_RETURN`** (TWR Asset gửi), **cash_in, cash_out**, **accum_active_ret, accum_active_ret_sq, ret_day_count**) — **BẮT BUỘC**: chuỗi AUM + daily_return per-ngày để serve %PnL compound + vẽ chart FR-03. ~2,5 tỷ dòng/10 năm → CCI + partition (có thể lấy điểm thưa để giảm tải). **[thin-layer]** `C_AUM` = NAV ròng (= NAV); **đã GỠ cột** `unit`/`unit_price`/`daily_pnl`/`payable_fee`/`nav_gross`.
  - **`accum_active_ret` / `accum_active_ret_sq` / `ret_day_count`** (FLOAT/INT) — **lũy kế TE prefix-sum** (active return = `daily_return` KH − `daily_return` master index), maintain bởi **J12B** (xem §9.2). Cho phép serve-layer PM tính Tracking Error qua range BẤT KỲ bằng HIỆU 2 mốc base/end (đọc 2 lát, không quét lịch sử): `Var=(ΣA²−(ΣA)²/n)/(n−1)`, `TE=√Var×√min(n,252)`. Tiêu thụ ở [SDI-pm-tool-spec.md](./SDI-pm-tool-spec.md) (US1/US2). Index `IX_SI_NAV_BALANCE_MASTER (C_MASTER_CODE,C_BUSINESS_DATE)` INCLUDE 3 cột này + `daily_return`/`aum` để phủ đọc-2-lát.

### Chuỗi daily master-level (materialize, nhỏ)
- **`T_MASTER_BALANCE`** (business_date, `C_MASTER_CODE`; cash, **`C_AUM`**, **`C_DAILY_RETURN`**, **cash_in**, **cash_out**, **total_account**) — **NGUỒN AUM master-level DUY NHẤT**. **[thin-layer]** `aum = Σ aum` per-SI (= Σ NAV); **`C_DAILY_RETURN` master = AUM-weighted `Σ(AUMᵢ·rᵢ)/ΣAUMᵢ`** (engine ghi); **đã GỠ** `unit`/`unit_price`/`daily_pnl`/`payable_fee`/`stock_value`. **[PM]** `cash_in`/`cash_out` = Σ cashflow master/ngày; `total_account` = #tiểu khoản ACTIVE → AUM-growth, net-flow, #KH (US1/US2).
- **`T_MASTER_CURRENT`** (`C_MASTER_CODE`; cash, **`C_AUM`** (= `C_LAST_AUM`), **total_account**, last_business_date) — state **current cấp master** (overwrite mỗi EOD bởi J11). **[thin-layer]** `aum = Σ aum` per-SI (= NAV); cash = tổng tiền 1 số; **đã GỠ** `unit`/`last_unit_price`/`stock_value`. Phục vụ đọc nhanh AUM/cash-drag/#KH **hiện tại** (US1/US2 snapshot, FR-01). Đối xứng `T_SI_CURRENT`.
- **`T_MASTER_HOLDING_BALANCE`** (business_date, `C_MASTER_CODE`, ticker; quantity, market_price, market_value, weight) — top 20 + "mã khác"
- **`T_MASTER_INDEX_DAILY`** (business_date, `C_MASTER_CODE`; index_value, daily_return)

### Control / orchestration
- **`T_EOD_RUN`** (business_date, job PK; status[PENDING|RUNNING|DONE|FAILED], rows, started_at, ended_at, message) — theo dõi & resume batch EOD (§9.2).
- **`T_EOD_PIPELINE`** (business_date PK; mkt_data/fo_ingest [+total/received cust_code]/index/eod/reconcile status + overall) — **control toàn pipeline /ngày**. `SP_EOD_SET_SOURCE_READY`: **MKT_DATA** (BO báo ready → SDI pull API BO 1 lần, KHÔNG Kafka/không đếm → cờ READY); **FO_INGEST** (`@p_total_record`=tổng cust_code break event; SDI đếm received distinct, READY khi received>=total). **Master index TÁCH luồng riêng** `SP_EOD_RUN_INDEX` (BO ready → tính+lưu index, INDEX=DONE, app đẩy index sang Asset). `SP_EOD_RUN` chỉ chạy khi MKT/FO=READY + INDEX=DONE; reconcile gate; **trạng thái CUỐI = `EOD_DONE`** khi reconcile PASS. `SP_EOD_RESET` chạy lại. *(BRD 2026-06-22: bỏ stage asset-sync/`SP_EOD_SET_ASSET_SYNCED`/COMPLETED — SDI không push asset sang Asset.)*
- **`T_EOD_RECON_BREAK`** (business_date, check_name, master/si, value_sdi/value_check/diff) — chi tiết dòng lệch đối soát (J13 GHI, không throw); nghiệp vụ tra cứu. Có break ⇒ SP_EOD_RUN chặn publish.
- *(Đã BỎ `T_SDI_CONFIG`)* — **[BRD asset-sync]** SDI không còn accrue/cấu hình phí (phí QL đã trừ sẵn trong NAV Asset gửi); catalog phí `T_FEE_CONFIG` cũng đã gỡ.

### Customer-level: MATERIALIZE (LƯU số Asset gửi)
**[thin-layer]** `aum` + `daily_return` per-ngày của KH (Asset gửi) **lưu vào `T_SI_BALANCE`** mỗi EOD (ingest). Vì Asset gửi snapshot per-ngày (không tái dựng được on-read) → phải materialize để serve chart FR-03. %PnL theo range = **compound** chuỗi `daily_return` (quét lát [base,end]); AUM 2 mốc đọc trực tiếp. Giảm tải: điểm thưa.

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
AUM = NAV         = Asset GỬI TRỰC TIẾP per-KH (đã trừ phí QL — model realized)   [thin-layer]
daily_return      = Asset GỬI (TWR ngày, đã khử dòng tiền) — SDI LƯU, KHÔNG tính
%PnL kỳ (serve)   = ∏(1+daily_returnₜ)−1 = EXP(Σ ln(1+rₜ))−1   (compound ON-READ)
Master daily ret  = AUM-weighted Σ(AUMᵢ·rᵢ)/ΣAUMᵢ   (engine ghi T_MASTER_BALANCE)
Master Index_t    = Index_(t-1) × Σ w^(t)·P_t/P_ref   (SDI tự tính từ giá×weight)
```

### 9.2 Danh sách JOB chạy tuần tự cuối ngày

Mỗi job **idempotent** (chạy lại 1 ngày → cùng kết quả), ghi trạng thái vào `T_EOD_RUN`. "SB" = set-based (không RBAR). "‖" = song song theo master/hash(C_SI_ACCOUNT).

> **[thin-layer] Core mới:** EOD **ingest `aum` + `daily_return` từ Asset** (`SP_INGEST_ASSET_NAV` per-SI) rồi **LƯU thẳng** vào `T_SI_BALANCE` (KHÔNG derive gì), SUM lên master (J11, master daily return = AUM-weighted). **Không còn J06 accrue / J07 MTM / J08 NAV-from-holdings / J09 PnL / J10 Unit** (Asset cấp NAV + return). **Sửa quá khứ = RE-INGEST** (Asset gửi lại ngày cũ → ghi lại từ ngày đó) — `SP_EOD_RECOMPUTE_RANGE` đã bỏ.

| # | Job | Phụ thuộc | Đọc | Ghi | SB | ‖ | Halt nếu lỗi |
|---|---|---|---|---|---|---|---|
| **INGEST** | `SP_INGEST_CUSTOMER` (Kafka per-KH, realtime, KHÔNG trong batch) | — | event 1 KH (JSON): cash + holdings + cổ tức/phí | cash→state + holdings→current + **interval CASH_HIST/HOLDING_HIST** + fee (dedup) + watermark `C_LAST_SYNC_DATE` | ✅ | ‖ per-cust | ✅ (forward-only: quá khứ→THROW) |
| **J0** | `GATE` chờ đủ FO ingest | INGEST | received (watermark=@d) vs expected (tiểu khoản ACTIVE) | chặn EOD nếu thiếu | – | – | ✅ (thiếu→alert) |
| ~~J1/J1b~~ | ~~`STAGE`/`SYNC_FO`~~ **(CHUYỂN sang INGEST realtime)** | — | FO sync giờ qua Kafka per-KH, không batch STAGE/SYNC | — | – | – | – |
| **J2** | `VALIDATE` (giá/market) | J0 | giá/CA/model_weight (market feed) | log lỗi | ✅ | – | ✅ (thiếu giá/trùng key/qty âm) |
| ~~J6~~ | ~~`ACCRUE_FEE`~~ **[BRD asset-sync] ĐÃ GỠ** | — | phí QL đã trừ sẵn trong NAV Asset gửi (model realized) → SDI không accrue | — | – | – | – |
| ~~J6b~~ | ~~`LOG_ACCRUAL`~~ **(ĐÃ BỎ)** | — | không còn payable/breakdown per-type | — | – | – | – |
| ~~INGEST-FEE~~ | ~~`SP_INGEST_FEE_CHARGE`~~ **[BRD asset-sync] ĐÃ GỠ** | — | BO không gửi số phí lũy kế; phí cắt phản ánh qua cash trong NAV Asset | — | – | – | – |
| ~~J7~~ | ~~`MTM`~~ **[BRD asset-sync] ĐÃ GỠ khỏi EOD NAV** (NAV từ Asset; MTM giữ cho composition/near-realtime) | — | — | — | – | – | – |
| **INGEST-NAV** | `SP_INGEST_ASSET_NAV` (Asset gửi per-SI/ngày GD) | — | JSON `{si_account, aum, daily_return, cash, cash_in, cash_out}` | `T_SI_ASSET_DAILY` → **LƯU thẳng** `aum`+`daily_return` vào `T_SI_BALANCE` (KHÔNG derive); `AUM = NAV` | ✅ | ‖ | – |
| ~~J9~~ | ~~`CALC_PNL`~~ **[thin-layer] ĐÃ GỠ** | — | SDI không tính PnL-tiền (cần NAV đầu/cuối+flow) | — | – | – | – |
| ~~J10~~ | ~~`CALC_UNIT`~~ **[thin-layer] ĐÃ GỠ** | — | SDI không phát hành unit (Asset cấp `daily_return`); `T_SI_UNIT_LEDGER` bỏ | — | – | – | – |
| **J11** | `SI_AGG` tổng hợp master | INGEST-NAV | `aum`/`daily_return` per tiểu khoản + T_SI_CASHFLOW_EVENT | **T_MASTER_BALANCE** (`aum` Σ + `daily_return` **AUM-weighted Σ(AUMᵢ·rᵢ)/ΣAUMᵢ** + **[PM] cash_in/cash_out Σ + total_account**) + **T_MASTER_CURRENT** (upsert) | ✅ | ‖ | – |
| **J12** | `SI_INDEX` master index — **LUỒNG RIÊNG `SP_EOD_RUN_INDEX`** (BO ready, KHÔNG trong pipeline customer) | giá+weight | model_weight, giá | T_MASTER_INDEX_DAILY | ✅ | ‖ | – |
| **J12B** | `TE_ACCUM` lũy kế active return *(PM)* | J11, J12 | `daily_return` KH (Asset gửi) − `daily_return` index (J12); accum @prev | **T_SI_BALANCE** cập nhật `accum_active_ret/_sq + ret_day_count` (TE prefix-sum cho serve-layer PM). Idempotent: accum@d=accum@prev+a@d | ✅ | – | – |
| **J13** | `RECONCILE` đối soát (RECORDER) | J11 | NAV âm (`aum<0`); Σ customer AUM vs master AUM; **[thin-layer]** cashflow 2 nguồn (SDI vs Asset `cash_in/out`) | **GHI `T_EOD_RECON_BREAK`** (KHÔNG throw). *([thin-layer] gỡ NAV_CONSISTENCY + HOLDINGS_MISMATCH — Asset không gửi `stock_value`)* | ✅ | – | ✅ (có break → SP_EOD_RUN chặn publish, RECONCILE=BREAK) |
| **J14** | `BUILD_SNAPSHOT` | INGEST-NAV | holdings (FO) | T_MASTER_HOLDING_BALANCE (top20+mã khác) | ✅ | ‖ | – |
| ~~J14b~~ | ~~`HISTORY`~~ **(CHUYỂN sang INGEST realtime)** | — | interval CASH_HIST/HOLDING_HIST maintain TẠI ingest per-event; `SP_EOD_HISTORY` chỉ còn utility bulk-backfill | — | – | – | – |
| **J15** | `PUBLISH` | J13, J14 | staging/đích | commit `T_SI_CURRENT`; SWITCH/MERGE master-level (publish nội bộ cho read API). ~~push tài sản KH + master NAV/perf → Asset~~ **ĐÃ GỠ (BRD 2026-06-22)** — BO/FO/Market đẩy thẳng Asset. **Master Index VẪN đẩy Asset** qua luồng RIÊNG `SP_EOD_RUN_INDEX` (`SP_GET_ASSET_INDEX_SNAPSHOT`, BO price-ready), KHÔNG trong J15. Xem [SDI-asset-gap.md](./SDI-asset-gap.md) | ✅ | – | ✅ |
| **J16** | `FINALIZE` | J15 | — | mark T_EOD_RUN done; (cuối tháng) build snapshot KH; update stats; alert success | – | – | – |

### 9.3 Thứ tự, song song & orchestration

```
INGEST-NAV (Asset gửi per-SI/ngày GD: aum ròng + daily_return + cash + cash_in/out) ──┐
FO holdings ingest (Kafka per-KH, cho composition) ───────────────────────────────────┤
                                                                                      ▼
J0 GATE → INGEST-NAV (LƯU aum+daily_return, KHÔNG derive)
                    └─ J11 ─┬─ J13 ─┐
                            └─ J12B ─┤   (J12B cần J11 + J12)
   J2 ──► J12 (độc lập, song song) ─┬┤
                                    └ J12B
              INGEST-NAV → J14 ──────┴─ J15 → J16
```
> **[thin-layer]** Không còn J6 ACCRUE / J7 MTM / J8 NAV-from-holdings / J9 PnL / J10 Unit: **Asset cấp thẳng `aum` (NAV ròng) + `daily_return` (TWR)** → SDI LƯU. `AUM = NAV`, không tách payable, không derive.
- **INGEST-NAV**: Asset gửi per-SI/ngày GD `{si_account, aum, daily_return, cash, cash_in, cash_out}` → `SP_INGEST_ASSET_NAV` ghi `T_SI_ASSET_DAILY` + LƯU thẳng `aum`+`daily_return` vào `T_SI_BALANCE`. Idempotent (DELETE+INSERT theo date,si). **FO holdings ingest GIỮ** (per-KH, cho composition/near-realtime — KHÔNG dùng cho EOD NAV). Cashflow nạp/rút SDI-side (ghi thẳng) + đối soát với `cash_in/out` Asset.
- **J0 GATE**: chờ đủ nguồn (`ASSET_NAV` per-SI, FO holdings, MKT, INDEX) → đủ mới chạy, thiếu thì alert (err=12 nếu thiếu Asset NAV per-SI).
- **J12 (Master Index)** chỉ cần giá + model_weight → song song nhánh customer, **chạy độc lập NAV Asset** (chỉ cần BO price-ready).
- **INGEST-NAV/J11/J14** chia **dải master hoặc hash(C_SI_ACCOUNT)** chạy nhiều luồng.
- **J13 RECONCILE là cổng**: lệch quá ngưỡng → **dừng, KHÔNG publish dữ liệu sai**, alert.
- **Thực thi ALL-IN-DB**: mỗi job = **1 stored proc** (set-based); **master proc `SP_EOD_RUN @business_date`** gọi tuần tự + ghi `T_EOD_RUN(business_date, job, status, rows, started, ended, message)`. **App/SQL Agent chỉ kích hoạt master proc** — không tính toán ở app. Fail giữa chừng → **resume từ job lỗi** (idempotent). Ingestion = proc `BULK INSERT`; API đọc = stored proc.
- **RCSI** bật → app đọc current snapshot không bị batch chặn; **J15 PUBLISH** (switch-in) là thao tác ngắn duy nhất ảnh hưởng đích.
- **[thin-layer] LƯU thẳng**: `aum`/`daily_return` per-SI ingest từ Asset, roll-forward `T_SI_CURRENT`. Set-based ~1M. Không replay lịch sử (sửa quá khứ = re-ingest).

> Chi tiết kỹ thuật (columnstore, partition switch, runtime ~vài phút–15 phút, anti-patterns): [SDI-db-architecture.md](./SDI-db-architecture.md).

### 9.4 Sửa quá khứ — RE-INGEST (không reconstruct-from-history)

> **[thin-layer]** `SP_EOD_RECOMPUTE_RANGE` (reconstruct NAV từ history holdings×giá − payable) **đã GỠ**. Vì `aum`+`daily_return` là số **Asset gửi** (không phải SDI tự dựng), sửa quá khứ = **RE-INGEST**.

- **Use case:** `aum`/`daily_return` một ngày quá khứ SAI → **Asset gửi lại `T_SI_ASSET_DAILY` ngày đó** (`SP_INGEST_ASSET_NAV` idempotent DELETE+INSERT theo date,si) → SDI **ghi lại** `aum`+`daily_return` + lũy kế TE **từ ngày sửa trở đi** (J12B accum forward).
- **Index master** sửa riêng (độc lập NAV): `SP_EOD_RECOMPUTE_INDEX_RANGE` (chỉ cần giá BO + target weight).

---

## 10. API cho UI riêng của SDI

> Mỗi API = **app gọi 1 stored proc** (`SP_GET_*`, xem `db/05_API.sql`) — tính/derive trong DB; app chỉ trả JSON, không tính. Định danh: KH=`C_CUST_CODE`, đơn vị = `C_SI_ACCOUNT` (sub-account); master suy từ sub-account.
>
> **BRD 2026-06-22:** các read API dưới phục vụ **giao diện riêng của SDI** (KHÔNG còn serve Asset/SMO — Asset tự tính từ BO/FO/Market trực tiếp). Toàn bộ FR-01..06 + PM API GIỮ NGUYÊN. **FR-06 `SP_GET_ASSET_REPORT` = read API báo cáo tài sản KH (GIỮ)** — đừng nhầm với `SP_GET_ASSET_SNAPSHOT` (producer Kafka đã gỡ). Xem [SDI-asset-gap.md](./SDI-asset-gap.md).

| FR | API | Proc | Nguồn |
|---|---|---|---|
| FR-01 Tổng quan đa tiểu khoản | GET /customer/{id}/si-overview | `SP_GET_SI_OVERVIEW` | sum T_MASTER_BALANCE + customer AUM (current từ T_SI_CURRENT) |
| FR-02 Chi tiết 1 tiểu khoản | GET /customer/{id}/si/{si} | `SP_GET_SI_DETAIL` | **[thin-layer]** customer AUM + %PnL kỳ (compound `daily_return`) + T_MASTER_BALANCE. *(MWR đã gỡ — không còn unit.)* |
| FR-03 Chart so sánh | GET /customer/{id}/si/{si}/performance?range= | `SP_GET_SI_PERFORMANCE` | T_MASTER_BALANCE (TR, compound `daily_return`) + T_MASTER_INDEX_DAILY (PR) + benchmark VN-Index (PR), chuỗi [mốc..cuối] |
| FR-04 Thông tin đầu tư | GET /customer/{id}/si/{si}/info | `SP_GET_SI_INFO` | T_SI_PORTFOLIO + master. **[thin-layer]** phí QL không thuộc SDI (đã trừ trong NAV Asset) — không đọc rate phí từ SDI |
| FR-05 Holdings | GET /customer/{id}/si/{si}/holdings | `SP_GET_SI_HOLDINGS` | **holdings CURRENT của KH** (T_SI_PORTFOLIO_HOLDING × giá mới nhất) top20 + "OTHER" — sản phẩm segregated nên đọc holdings KH (≠ master-aggregate T_MASTER_HOLDING_BALANCE) |
| FR-06 Báo cáo tài sản | GET /customer/{id}/si/{si}/asset-report | `SP_GET_ASSET_REPORT` | **[thin-layer]** T_SI_BALANCE (`aum` ròng @asOf + %PnL compound) + `cash` (tổng) Asset gửi; `AUM = NAV` + holdings chi tiết per-mã (FO @asOf × giá). **Bỏ unit/UP + payable + breakdown per-type** (SDI không tính unit / không quản phí) |

> **FR-06 result sets [thin-layer]:** **RS1** summary (`aum` ròng + `cash` + %PnL compound; `AUM = NAV`), **RS2** holdings chi tiết per-mã @asOf (FO×giá; composition). **Đã BỎ RS3/RS4/RS5** (income/phí breakdown) + cột unit/UP — phí QL đã trừ trong NAV Asset, không còn unit price. *(Asset không gửi `stock_value` ⇒ không còn so RS2 Σ vs Asset stock.)*

---

## 11. Scale (100 master, 200K KH × 5 master, 10 năm)

> Chi tiết kiến trúc DB + EOD ở quy mô lớn cho SQL Server: [SDI-db-architecture.md](./SDI-db-architecture.md) (roll-forward state, set-based, columnstore, partitioning).
> Hợp đồng trao đổi dữ liệu EOD FO/Market/BO→SDI (payload từng bên + định lượng small/medium/large): [SDI-eod-data-exchange.md](./SDI-eod-data-exchange.md). *(SDI→Asset: gỡ 2 producer tài sản KH + master NAV/perf, GIỮ Master Index — BRD 2026-06-22; đối chiếu + reconcile (NAV_CONSISTENCY / cashflow 2 nguồn / holdings): [SDI-asset-gap.md](./SDI-asset-gap.md)).*
> Dự phóng tăng trưởng dữ liệu 1M/1Q/1Y (KH tăng đều/nóng): [SDI-data-growth-projection.md](./SDI-data-growth-projection.md).
> Dashboard PM quản lý master (US1–US5, SP serve, TE/deviation/cash-drag, config ngưỡng): [SDI-pm-tool-spec.md](./SDI-pm-tool-spec.md).

| Dữ liệu | Ước lượng |
|---|---|
| Master Index / Performance / master snapshot (nội bộ) | ~250K dòng/loại |
| Cashflow / execution / holding event | ~100M+ |
| Unit ledger | ~120M |
| Customer NAV/UP/% daily | **materialize** `T_SI_BALANCE` (~2,5 tỷ, CCI) — bắt buộc do FO-sync |

Customer NAV/UP/% daily: **BẮT BUỘC materialize** `T_SI_BALANCE` mỗi EOD — vì FO sync overwrite holdings (không event-source) → KHÔNG derive được lịch sử per-customer. Master-level agg tươi mỗi ngày + lưu `T_MASTER_BALANCE`.

---

## 12. Edge cases

1. **Đóng/mở lại vị thế**: sub-account mới, chuỗi `daily_return` (Asset gửi) bắt đầu lại từ ngày tham gia. *([thin-layer] không còn UP=10.000.)*
2. **NAV ≤ 0** (Asset gửi `aum` ròng âm, hiếm): cảnh báo (reconcile NAV_NEGATIVE).
3. **Reconcile FO**: Σ lots per ticker (SDI holdings) vs holdings thật FO → break detection (composition).
4. **Cash drag**: tiền KH = `cash` Asset gửi / AUM (không logic riêng).
5. **Độ trễ giải ngân**: phản ánh trong `aum`/`daily_return` Asset gửi.
6. **Trade-date accounting**: mua/bán ghi nhận tại ngày khớp MP (không đợi settle T+2) — phía Asset.
7. **`daily_return` NULL** (ngày đầu / Asset chưa gửi): bỏ khỏi compound (`∏` không nhân ngày NULL).
8. **Ngày không giao dịch**: Asset chỉ gửi ngày GD; T7/CN/lễ carry-forward.
9. **Cổ tức**: income — đã nằm trong `aum` Asset gửi; KH nạp = cashflow (không tag income).
10. **`daily_return ≤ −1`** (mất hết vốn): kẹp để `LOG(1+r)` không vỡ (serve compound).
11. **Master không có KH**: `aum`=0 ⇒ master daily return = NULL (mẫu số `ΣAUM`=0).

---

## 13. Quy ước đặt tên (canonical — code phải theo)

snake_case. Một khái niệm = một code.

| Nhóm | code |
|---|---|
| Entity | `C_MASTER_CODE` (master), `C_SI_ACCOUNT` (sub-account/tiểu khoản = cust_code+đuôi), `C_CUST_CODE`, `business_date` |
| Tài sản | `aum` (= nav, Asset gửi trực tiếp), `cash` (tổng), `buying_power`, `withdrawable_asset`, `net_invested_capital` *([thin-layer] bỏ `stock_value`/`payable_fee`/`custody_fee`/`management_fee` — Asset gửi NAV ròng + cash tổng)* |
| Cashflow | `cash_in`, `cash_out`, **`net_cashflow`** (=cash_in−cash_out, để đối soát Asset), `income` (≠ cashflow) |
| Hiệu suất | `daily_return` (Asset gửi, TWR ngày), `return_pct` (=%PnL kỳ = compound `daily_return`) *([thin-layer] bỏ `unit`/`unit_price`/`delta_unit`/`pnl`/`daily_pnl`/`mwr` — SDI không tính)* |
| Index | `master_index`/`index_value`, `target_weight`, `weight` (holdings), `ref_price` (giá tham chiếu đầu phiên), `is_ex_rights`, `benchmark`, `close_price` |
| Khái niệm | `cash_drag`, `tracking_error`, `trade_date_accounting`, `segregated`, `price_return` |

Quy tắc: trong công thức dùng `net_cashflow` (rõ "net"); `income` tách khỏi cashflow; `cash` (tổng) chỉ cho NAV; % lưu dạng thập phân.

**Tham số stored procedure:**
- Mọi tham số SP **BẮT BUỘC prefix `@p_`** (vd `@p_si_account`, `@p_d`, `@p_range`) — phân biệt với biến cục bộ `@local`.
- SP phục vụ **API** (`SP_GET_*`, `SP_SET_*`) **BẮT BUỘC** thêm: `@p_user` (định danh người gọi), `@p_err_code INT OUTPUT`, `@p_err_msg NVARCHAR(400) OUTPUT` (trả lỗi về app, `@p_err_code=0`=OK; KHÔNG THROW ra ngoài). Pattern: validate trong thân → set err_code/err_msg → `THROW` để `TRY/CATCH` bắt; `CATCH` gán `err_code=-1` cho lỗi runtime (guard `IF @p_err_code=0`). **Đã áp dụng toàn bộ** `SP_GET_SI_*` (05_API) + `SP_GET_MASTER_*`/`SP_GET_PM_*`/`SP_SET_MASTER_PM_CONFIG`/`SP_GET_MASTER_ALERTS` (06_PM_API).
- SP **engine** nội bộ (`SP_EOD_STEP`, các job `SP_EOD_*`, `SP_INGEST_*`) — lỗi → `THROW` chặn EOD publish (chỉ theo rule prefix `@p_`). **Ngoại lệ `SP_EOD_RUN`** (orchestrator app gọi): bọc TRY/CATCH, trả `@p_err_code`/`@p_err_msg` OUT (0=OK, -1=lỗi), KHÔNG THROW; step nội bộ vẫn THROW + log FAILED, orchestrator bắt lại.

---

## 14. Worked example — per KH **[thin-layer]** (Asset gửi `aum` + `daily_return`)

> **[thin-layer]** SDI nhận `aum` + `daily_return` (Asset đã khử dòng tiền), LƯU. KHÔNG còn cột Unit/Unit Price/PnL (Asset chịu trách nhiệm khử nạp/rút khi cấp `daily_return`). %PnL kỳ = **compound** chuỗi `daily_return`.

| Ngày | aum (Asset gửi) | cash_in | cash_out | daily_return (Asset gửi) |
|---|---|---|---|---|
| KT | 10,000,000 | 10,000,000 | | NULL (ngày đầu) |
| 2 | 15,000,000 | | | 0.500000 |
| 3 | 18,000,000 | 2,000,000 | 100,000 | 0.063333 |
| 4 | 20,000,000 | 3,500,000 | 150,000 | (0.063253) |
| 5 | 20,000,000 | | | 0.000000 |
| 6 | 25,000,000 | | | 0.250000 |
| 7 | 25,500,000 | | | 0.020000 |

- `daily_return` Asset gửi đã **khử dòng tiền**: vd ngày 3 nạp 2tr rút 0.1tr (CF ròng 1.9tr) nhưng `daily_return=0.063333` chỉ phản ánh lãi tài sản, KHÔNG gồm phần nạp.
- **%PnL range ngày 3→7** (ngày mốc = cuối ngày 2) = compound `daily_return` ngày 3..7:
  `∏ = 1.063333 × 0.936747 × 1.000000 × 1.250000 × 1.020000 = 1.2700` → **+27.00%**. Tương đương `EXP(Σ ln(1+rₜ)) − 1`. *(SDI compound on-read, KHÔNG cần UP_cuối/UP_mốc.)*

### Master Index — khớp ví dụ

| Ngày | Tỷ trọng (eff) | P_ref | Close | Index |
|---|---|---|---|---|
| 0 | A40 B35 C25 | — | 100/50/80 | 1000 |
| 1 | A40 B35 C25 | 100/50/80 | 102/49/82 | 1007 |
| 2 | A40 B35 C25 | 102/49/82 | 101/50.5/81 | 1011 |
| 3 (rebalance −C +D) | A45 B35 D20 | 101/50.5/60 | 103/52/62 | 1037 |

`Index_3 = 1011 × (0.45·103/101 + 0.35·52/50.5 + 0.20·62/60) = 1037` (weights eff_date=3 chi phối return ngày 3).
