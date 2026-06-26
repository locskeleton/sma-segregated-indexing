# SDI — Asset & Performance Engine — Specification

Engine tính tài sản, hiệu suất danh mục master và từng khách hàng cho sản phẩm **SMA chỉ số** (separately managed account). Phục vụ **giao diện riêng của SDI** (NAV/index/perf) qua read API.

> 📖 Tra cứu nhanh thuật ngữ (VN/EN) + mọi công thức kèm ví dụ: [SDI-thuat-ngu-cong-thuc.md](./SDI-thuat-ngu-cong-thuc.md).
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
        │ feed EOD: model_weight + ĐỒNG BỘ holdings+cash toàn bộ TK (SDI không quản lý từng lệnh khớp)
        ▼
SDI: holdings (FO nạp thẳng current) + cash → tính NAV, Unit/Unit Price, PnL, TWR, MWR, Master Index  →  read API (UI riêng SDI)
```

> **BRD 2026-06-22:** SDI **không còn push tài sản KH/NAV-perf master sang Asset** (Asset nhận BO/FO/Market trực tiếp & tự tính); **vẫn GIỮ push Master Index** (`SP_GET_ASSET_INDEX_SNAPSHOT`, khi BO price-ready). Xem [SDI-asset-gap.md](./SDI-asset-gap.md). SDI phục vụ giao diện riêng qua read API.

| Việc | Chủ |
|---|---|
| Gửi yêu cầu rebalance (trigger, không chứa weights) | SDI |
| Tính tỷ trọng danh mục mẫu (luôn 100% cổ phiếu) | **FO** |
| Đặt & khớp lệnh MP trên TK từng KH | **FO** |
| Tính NAV / Unit / PnL / TWR / MWR / Master Index | SDI |
| Đọc & hiển thị (UI riêng của SDI) | SDI read API (FR-01..06, PM) |

- **Custody = segregated**: tiền & cổ phiếu nằm thật trong tiểu khoản KH (KH sở hữu hợp pháp).
- **SDI = engine tính thuần**: không quyết tỷ trọng, không sinh/khớp lệnh.
- **EOD** là đơn vị tính; chốt 1 lần cuối ngày. **Event-sourced** để tính lại được.

---

## 2. Khái niệm nền — hai thế giới

| | Thế giới THẬT (per KH × master) | Thế giới BENCHMARK (lý thuyết) |
|---|---|---|
| Đo | Tiền thật của KH: NAV → Unit → hiệu suất | Chỉ số: Master Index, VN-Index |
| Công cụ | Unit price (NAV per share) | Index (weights × giá) |
| Hiển thị | "Lợi suất của bạn" / "Hiệu suất master" | "Danh mục mẫu", "VN-Index" trên chart FR-03 |

- **Hai cấp:** **MASTER** = danh mục mẫu/chiến lược (mã `C_MASTER_CODE`). **SUB-ACCOUNT (tiểu khoản)** = KH đầu tư 1 master → cấp 1 sub-account, mã `C_SI_ACCOUNT` (= CUST_CODE+đuôi, customer-level). **Close+reopen master ⇒ sub-account MỚI** (mã khác, KHÔNG tái dùng) → 1 KH có nhiều sub-account/master theo thời gian (tối đa 1 ACTIVE/lúc). Mọi bảng customer-level khóa theo `C_SI_ACCOUNT`.
- **Tiểu khoản** = đơn vị nhỏ nhất = 1 sub-account (`C_SI_ACCOUNT`) của một (customer × MASTER). Reopen → khởi tạo T0 mới (UP=10.000) trên sub-account mới.
  > **Vocabulary:** dùng **"master"** (= danh mục mẫu/chiến lược, cấp `C_MASTER_CODE`, bảng `T_MASTER_*`) và **"tiểu khoản"** (sub-account `C_SI_ACCOUNT`, bảng `T_SI_*`). Thuật ngữ "SI" cũ đã đổi hết: "SI Index" → **"Master Index"** (benchmark danh mục mẫu), "Hiệu suất SI" → "Hiệu suất master", "SI NAV/Unit Price" → "Master NAV/Unit Price".
- Hiệu suất tính **per tiểu khoản (KH × master)**; cấp master = tổng hợp các KH.

---

## 3. Tài sản & NAV

```
Chứng khoán   = Σ (KL nắm giữ × market price)      (Asset gửi stock_value đã định giá)
Tiền          = TỔNG tiền dư (1 số: gộp tiền mặt + bán chờ về + cổ tức tiền)
NAV           = Asset GỬI TRỰC TIẾP (đã trừ phí QL — tài sản RÒNG)
AUM           = stock_value + cash = NAV           (không tách payable)
Tổng vốn đầu tư = Σ NAV vào − Σ NAV ra              (net cashflow lũy kế)
```

> **[BRD asset-sync]** SDI **KHÔNG còn tự định giá / accrue phí QL**. **Asset gửi NAV RÒNG trực tiếp** (đã trừ phí QL sẵn — model REALIZED) + components (`stock_value`, `cash` tổng) + `cash_in/cash_out` per-SI/ngày. SDI **ingest thẳng** (`SP_INGEST_ASSET_NAV`) rồi derive unit/UP/PnL/return.

- **NAV = Asset gửi trực tiếp (đã trừ phí QL); AUM = NAV** (không còn `total_asset − payable`). Components Asset gửi kèm để hiển thị (FR-06) + reconcile NAV_CONSISTENCY (`nav` vs `stock + cash`). `cash` = **TỔNG tiền dư** (1 số, gộp tiền mặt + bán chờ về + cổ tức tiền — Asset không chia nhỏ).
- **Asset là nguồn NAV/tiền DUY NHẤT**. SDI mirror số Asset đẩy về, không tự cộng/trừ; không suy cash/NAV từ holdings.
- **Phí QL: model REALIZED** — phí chỉ giảm tài sản khi BO cắt thật (qua cash). **BO KHÔNG gửi số phí lũy kế (accrued)** cho Asset; Asset đã trừ phí sẵn trong NAV. SDI **KHÔNG accrue, KHÔNG quản payable**. Thuế GD do FO/BO net vào cash khi khớp (ngoài SDI).
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
| **Chi phí** | phí quản lý/thuế/perf, thuế GD, phí phạt rút sớm | **[BRD asset-sync]** phí QL đã trừ sẵn trong NAV Asset gửi (model realized); thuế GD/phạt do FO/BO trừ vào cash (SDI không re-apply) | ❌ |

- `net_cashflow (CF_t) = cash_in − cash_out` — **external, per (KH×master) = per tiểu khoản, per ngày**, lấy từ event có nhãn.
- **Cổ tức tiền mặt**: income — đã nằm trong NAV/`cash` Asset gửi, tự động vào PnL. KHÔNG tag là cash_in.

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

## 6. Hiệu suất (per KH × master)

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
| KH/tiểu khoản tham gia sau mốc filter | close ngày tham gia |

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

### Tổng hợp master (đường "Hiệu suất master" trên chart)
```
Master NAV        = Σ Customer NAV
Master Unit Price = Master NAV / Σ Customer Unit
```

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
| Hiệu suất master | Master Unit Price | NAV-per-share (TWR), total return |
| Danh mục mẫu | Master Index | Index, price return |
| VN-Index | Market data | Index, price return |

> Master (hiệu suất thật) là total-return (NAV ăn cổ tức), benchmark là price-return → master nhỉnh hơn ~mức cổ tức một cách hệ thống; đây là hệ quả cơ sở (PR vs TR), được chấp nhận (user đối chiếu VN-Index là chuẩn phổ quát).

---

## 8. Kiến trúc dữ liệu

Prefix bảng `T_`, cột `C_`. **Quy chuẩn kiểu:** Tiền VND & quantity = `DECIMAL(20,0)` (không thập phân); giá = `DECIMAL(18,4)`; % / return / fee_rate = `DECIMAL(10,6)`; unit & unit_price = `DECIMAL(18,6)`; weight = `DECIMAL(12,8)`. **[BRD asset-sync]** Phí QL đã trừ sẵn trong NAV Asset gửi (model realized) ⇒ **không còn cột phí lũy kế (payable/accrued) ở SDI**. **Hai cấp:** master (`C_MASTER_CODE`, bảng `T_MASTER_*`) / sub-account = tiểu khoản (`C_SI_ACCOUNT`, bảng `T_SI_*`).

### Master / cấu hình
- **`T_MASTER_PORTFOLIO`** (**`C_MASTER_CODE` PK** — mã danh mục MASTER, khóa chính + khóa public, KHÔNG surrogate; name, status[ACTIVE|CLOSED], inception_date, benchmark_code) — các bảng khác tham chiếu master theo `C_MASTER_CODE`; bảng tổng hợp master-level: `T_MASTER_NAV_BALANCE`/`T_MASTER_HOLDING_BALANCE`/`T_MASTER_INDEX_DAILY`/`T_MASTER_NAV_CURRENT`. **[BRD asset-sync] Không còn cột phí ở đây** (phí QL đã trừ sẵn trong NAV Asset gửi — SDI không cấu hình/accrue phí). **Sub-account** (`T_SI_PORTFOLIO`): `C_SI_ACCOUNT` (mã sub-account, UNIQUE) + `C_MASTER_CODE` + `C_CUST_CODE` + close_date; filtered-unique 1 ACTIVE/(cust,master). Customer-level tables khóa theo `C_SI_ACCOUNT`.
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
- **`T_SI_CASH_HIST`** (`C_SI_ACCOUNT`, valid_from; valid_to, cash) — **HISTORY cash theo INTERVAL** (full, no-dup; đối xứng holding_hist). DIFF state.cash vs dòng open **tại INGEST**.
- **State per-KH `T_SI_NAV_CURRENT`** (roll-forward) — **[BRD asset-sync]** giữ `C_NAV` + components (`stock_value`, `cash` tổng) Asset gửi; `AUM = stock + cash = NAV`. **Không còn cột `C_PAYABLE_FEE`** (phí QL đã trừ sẵn trong NAV ⇒ `NAV = AUM`, không trừ payable).
- **`T_SI_CASHFLOW_EVENT`** (event_id PK; `C_SI_ACCOUNT`, business_date, event_type[INITIAL|TOPUP|SIP|INTEREST_IN|WITHDRAW], amount, created_time) — external cashflow; dùng cho **CF_t** (PnL/unit), KHÔNG cộng lại cash (cash từ FO sync).
- **`T_SI_INCOME_FEE`** — **[BRD asset-sync] ĐÃ GỠ.** Sổ cái phí/thu nhập per-KH (cổ tức + phí lưu ký + phí ACCRUE cắt) không còn ở SDI: phí QL đã trừ sẵn trong NAV Asset gửi (model realized), cổ tức/income đã nằm trong NAV/`cash` Asset gửi. Cùng đó gỡ proc `SP_INGEST_FEE_CHARGE` (net-off payable).
- **`T_SI_FEE_ACCRUAL` / payable / Option B breakdown per-type** — **[BRD asset-sync] ĐÃ GỠ TOÀN BỘ.** Không còn `C_PAYABLE_FEE`, không accrue, không breakdown per-type. Phí QL chỉ giảm tài sản khi BO cắt thật (qua cash, đã phản ánh trong NAV Asset gửi) — SDI không lưu/dựng lại số phí lũy kế.
- **`T_SI_UNIT_LEDGER`** (`C_SI_ACCOUNT`, business_date; cf_net, delta_unit, unit) — ghi dòng khi unit thay đổi (cashflow). Unit full precision.

### Per-KH daily performance (LỊCH SỬ — materialize)
- **`T_SI_NAV_BALANCE`** (business_date, `C_SI_ACCOUNT`; nav, unit, unit_price, daily_pnl, daily_return, **cash_in, cash_out**, **accum_active_ret, accum_active_ret_sq, ret_day_count**) — **BẮT BUỘC**: NAV/unit/unit_price per-ngày không derive được on-read → phải lưu để vẽ chart FR-03. ~2,5 tỷ dòng/10 năm → CCI + partition (có thể lấy điểm thưa để giảm tải). **[BRD asset-sync]** `nav` = NAV RÒNG Asset gửi (đã trừ phí QL); **không còn cột `payable_fee`/`nav_gross`** — `AUM = nav`.
  - **`accum_active_ret` / `accum_active_ret_sq` / `ret_day_count`** (FLOAT/INT) — **lũy kế TE prefix-sum** (active return = `daily_return` KH − `daily_return` master index), maintain bởi **J12B** (xem §9.2). Cho phép serve-layer PM tính Tracking Error qua range BẤT KỲ bằng HIỆU 2 mốc base/end (đọc 2 lát, không quét lịch sử): `Var=(ΣA²−(ΣA)²/n)/(n−1)`, `TE=√Var×√min(n,252)`. Tiêu thụ ở [SDI-pm-tool-spec.md](./SDI-pm-tool-spec.md) (US1/US2). Index `IX_SI_NAV_BALANCE_MASTER (C_MASTER_CODE,C_BUSINESS_DATE)` INCLUDE 3 cột này + unit_price/daily_return/nav để phủ đọc-2-lát.

### Chuỗi daily master-level (materialize, nhỏ)
- **`T_MASTER_NAV_BALANCE`** (business_date, `C_MASTER_CODE`; cash, aum, nav, unit, unit_price, daily_pnl, daily_return, **cash_in**, **cash_out**, **total_account**) — **NGUỒN NAV master-level DUY NHẤT**. **[BRD asset-sync]** `aum = Σ (stock + cash)` per-SI `= Σ nav = nav` (không tách payable; không còn cột `payable_fee`). **[PM tool +]** `cash_in`/`cash_out` = Σ cashflow nạp/rút master/ngày; `total_account` = #tiểu khoản ACTIVE → phục vụ AUM-growth, net-flow, #KH ở dashboard PM (US1/US2).
- **`T_MASTER_NAV_CURRENT`** (`C_MASTER_CODE`; cash, aum, last_nav, unit, last_unit_price, **total_account**, last_business_date) — NAV/state **current cấp master** (overwrite mỗi EOD bởi J11). **[BRD asset-sync]** `aum = stock + cash = last_nav` (không tách payable; cash = tổng tiền 1 số). Phục vụ đọc nhanh AUM/cash-drag/#KH **hiện tại** (US1/US2 snapshot, FR-01). Đối xứng `T_SI_NAV_CURRENT`.
- **`T_MASTER_HOLDING_BALANCE`** (business_date, `C_MASTER_CODE`, ticker; quantity, market_price, market_value, weight) — top 20 + "mã khác"
- **`T_MASTER_INDEX_DAILY`** (business_date, `C_MASTER_CODE`; index_value, daily_return)

### Control / orchestration
- **`T_EOD_RUN`** (business_date, job PK; status[PENDING|RUNNING|DONE|FAILED], rows, started_at, ended_at, message) — theo dõi & resume batch EOD (§9.2).
- **`T_EOD_PIPELINE`** (business_date PK; mkt_data/fo_ingest [+total/received cust_code]/index/eod/reconcile status + overall) — **control toàn pipeline /ngày**. `SP_EOD_SET_SOURCE_READY`: **MKT_DATA** (BO báo ready → SDI pull API BO 1 lần, KHÔNG Kafka/không đếm → cờ READY); **FO_INGEST** (`@p_total_record`=tổng cust_code break event; SDI đếm received distinct, READY khi received>=total). **Master index TÁCH luồng riêng** `SP_EOD_RUN_INDEX` (BO ready → tính+lưu index, INDEX=DONE, app đẩy index sang Asset). `SP_EOD_RUN` chỉ chạy khi MKT/FO=READY + INDEX=DONE; reconcile gate; **trạng thái CUỐI = `EOD_DONE`** khi reconcile PASS. `SP_EOD_RESET` chạy lại. *(BRD 2026-06-22: bỏ stage asset-sync/`SP_EOD_SET_ASSET_SYNCED`/COMPLETED — SDI không push asset sang Asset.)*
- **`T_EOD_RECON_BREAK`** (business_date, check_name, master/si, value_sdi/value_check/diff) — chi tiết dòng lệch đối soát (J13 GHI, không throw); nghiệp vụ tra cứu. Có break ⇒ SP_EOD_RUN chặn publish.
- *(Đã BỎ `T_SDI_CONFIG`)* — **[BRD asset-sync]** SDI không còn accrue/cấu hình phí (phí QL đã trừ sẵn trong NAV Asset gửi); catalog phí `T_FEE_CONFIG` cũng đã gỡ.

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
NAV          = Asset GỬI TRỰC TIẾP (đã trừ phí QL — model realized)   [BRD asset-sync]
AUM          = stock_value + cash = NAV   (không tách payable)
PnL ngày     = NAV cuối − NAV đầu + NAV ra − NAV vào
ΔUnit        = net CF / UnitPrice_(t-1) ; Unit = Unit_(t-1)+ΔUnit (full) ; UnitPrice = NAV/Unit
Master NAV/Unit  = Σ per master ; Master UnitPrice = Master NAV / Master Unit
Master Index_t   = Index_(t-1) × Σ w^(t)·P_t/P_ref
```

### 9.2 Danh sách JOB chạy tuần tự cuối ngày

Mỗi job **idempotent** (chạy lại 1 ngày → cùng kết quả), ghi trạng thái vào `T_EOD_RUN`. "SB" = set-based (không RBAR). "‖" = song song theo master/hash(C_SI_ACCOUNT).

> **[BRD asset-sync] Core mới:** EOD **ingest NAV ròng từ Asset** (`SP_INGEST_ASSET_NAV` per-SI) rồi **derive** Unit/UnitPrice/PnL/return (ghi `T_SI_UNIT_LEDGER` + `T_SI_NAV_BALANCE`), SUM lên master (J11). **Không còn J06 accrue / J07 MTM / J08 NAV-from-holdings** (NAV lấy thẳng từ Asset). **Sửa quá khứ = RE-INGEST** (Asset gửi lại ngày cũ → derive lại từ ngày đó) — `SP_EOD_RECOMPUTE_RANGE` đã bỏ.

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
| **INGEST-NAV** | `SP_INGEST_ASSET_NAV` (Asset gửi per-SI/ngày GD) | — | JSON `{si_account, nav, stock_value, cash, cash_in, cash_out}` | `T_SI_ASSET_DAILY` + derive Unit/UP/PnL/return → `T_SI_NAV_BALANCE`; `AUM = stock + cash = NAV` | ✅ | ‖ | – |
| **J9** | `CALC_PNL` *(trong INGEST-NAV)* | INGEST-NAV | NAV, NAV_prev, CF | daily_pnl per vị thế | ✅ | ‖ | – |
| **J10** | `CALC_UNIT` *(trong INGEST-NAV)* | INGEST-NAV | CF_t (cashflow event), UnitPrice_prev | ΔUnit/Unit/UnitPrice; T_SI_UNIT_LEDGER; **T_SI_NAV_BALANCE** (lịch sử per-KH) | ✅ | ‖ | – |
| **J11** | `SI_AGG` tổng hợp master | INGEST-NAV, J10 | NAV/AUM/unit per tiểu khoản + T_SI_CASHFLOW_EVENT | **T_MASTER_NAV_BALANCE** (NAV + AUM(=stock+cash) + hiệu suất + **[PM] cash_in/cash_out Σ + total_account**) + **T_MASTER_NAV_CURRENT** (upsert) | ✅ | ‖ | – |
| **J12** | `SI_INDEX` master index — **LUỒNG RIÊNG `SP_EOD_RUN_INDEX`** (BO ready, KHÔNG trong pipeline customer) | giá+weight | model_weight, giá | T_MASTER_INDEX_DAILY | ✅ | ‖ | – |
| **J12B** | `TE_ACCUM` lũy kế active return *(PM)* | J10, J12 | `daily_return` KH (J10) − `daily_return` index (J12); accum @prev | **T_SI_NAV_BALANCE** cập nhật `accum_active_ret/_sq + ret_day_count` (TE prefix-sum cho serve-layer PM). Idempotent: accum@d=accum@prev+a@d | ✅ | – | – |
| **J13** | `RECONCILE` đối soát (RECORDER) | J11 | NAV âm/unit≤0; Σ customer NAV vs master NAV; **[BRD asset-sync]** NAV_CONSISTENCY (`nav` vs `stock+cash`); cashflow 2 nguồn; holdings (Σ FO×giá vs Asset stock) | **GHI `T_EOD_RECON_BREAK`** (KHÔNG throw) | ✅ | – | ✅ (có break → SP_EOD_RUN chặn publish, RECONCILE=BREAK) |
| **J14** | `BUILD_SNAPSHOT` | INGEST-NAV | holdings | T_MASTER_HOLDING_BALANCE (top20+mã khác) | ✅ | ‖ | – |
| ~~J14b~~ | ~~`HISTORY`~~ **(CHUYỂN sang INGEST realtime)** | — | interval CASH_HIST/HOLDING_HIST maintain TẠI ingest per-event; `SP_EOD_HISTORY` chỉ còn utility bulk-backfill | — | – | – | – |
| **J15** | `PUBLISH` | J13, J14 | staging/đích | commit `T_SI_NAV_CURRENT`; SWITCH/MERGE master-level (publish nội bộ cho read API). ~~push tài sản KH + master NAV/perf → Asset~~ **ĐÃ GỠ (BRD 2026-06-22)** — BO/FO/Market đẩy thẳng Asset. **Master Index VẪN đẩy Asset** qua luồng RIÊNG `SP_EOD_RUN_INDEX` (`SP_GET_ASSET_INDEX_SNAPSHOT`, BO price-ready), KHÔNG trong J15. Xem [SDI-asset-gap.md](./SDI-asset-gap.md) | ✅ | – | ✅ |
| **J16** | `FINALIZE` | J15 | — | mark T_EOD_RUN done; (cuối tháng) build snapshot KH; update stats; alert success | – | – | – |

### 9.3 Thứ tự, song song & orchestration

```
INGEST-NAV (Asset gửi per-SI/ngày GD: nav ròng + stock/cash + cash_in/out) ──┐
FO holdings ingest (Kafka per-KH, cho composition) ──────────────────────────┤
                                                                             ▼
J0 GATE → INGEST-NAV derive (NAV→PnL→Unit, J9/J10)
                    └─ J11 ─┬─ J13 ─┐
                            └─ J12B ─┤   (J12B cần J10 + J12)
   J2 ──► J12 (độc lập, song song) ─┬┤
                                    └ J12B
              INGEST-NAV → J14 ──────┴─ J15 → J16
```
> **[BRD asset-sync]** Không còn J6 ACCRUE / J7 MTM / J8 NAV-from-holdings: NAV **ingest thẳng từ Asset** (đã trừ phí QL — model realized; BO không gửi số phí lũy kế). `AUM = stock + cash = NAV`, không tách payable.
- **INGEST-NAV**: Asset gửi per-SI/ngày GD `{si_account, nav, stock_value, cash, cash_in, cash_out}` → `SP_INGEST_ASSET_NAV` ghi `T_SI_ASSET_DAILY` + derive Unit/UP/PnL/return. Idempotent (MERGE theo date,si). **FO holdings ingest GIỮ** (per-KH, cho composition/near-realtime — KHÔNG dùng cho EOD NAV). Cashflow nạp/rút SDI-side (ghi thẳng) + đối soát với `cash_in/out` Asset.
- **J0 GATE**: chờ đủ nguồn (`ASSET_NAV` per-SI, FO holdings, MKT, INDEX) → đủ mới chạy, thiếu thì alert (err=12 nếu thiếu Asset NAV per-SI).
- **J12 (Master Index)** chỉ cần giá + model_weight → song song nhánh customer, **chạy độc lập NAV Asset** (chỉ cần BO price-ready).
- **derive/J9/J10/J14** chia **dải master hoặc hash(C_SI_ACCOUNT)** chạy nhiều luồng.
- **J13 RECONCILE là cổng**: lệch quá ngưỡng → **dừng, KHÔNG publish dữ liệu sai**, alert.
- **Thực thi ALL-IN-DB**: mỗi job = **1 stored proc** (set-based); **master proc `SP_EOD_RUN @business_date`** gọi tuần tự + ghi `T_EOD_RUN(business_date, job, status, rows, started, ended, message)`. **App/SQL Agent chỉ kích hoạt master proc** — không tính toán ở app. Fail giữa chừng → **resume từ job lỗi** (idempotent). Ingestion = proc `BULK INSERT`; API đọc = stored proc.
- **RCSI** bật → app đọc current snapshot không bị batch chặn; **J15 PUBLISH** (switch-in) là thao tác ngắn duy nhất ảnh hưởng đích.
- **Roll-forward**: NAV/state per-SI ingest từ Asset; unit derive roll-forward (UP_{t-1}). Derive chạm toàn bộ ~1M nhưng **set-based**. Không replay lịch sử (sửa quá khứ = re-ingest).

> Chi tiết kỹ thuật (columnstore, partition switch, runtime ~vài phút–15 phút, anti-patterns): [SDI-db-architecture.md](./SDI-db-architecture.md).

### 9.4 Sửa quá khứ — RE-INGEST (không reconstruct-from-history)

> **[BRD asset-sync]** `SP_EOD_RECOMPUTE_RANGE` (reconstruct NAV từ history holdings×giá − payable) **đã GỠ**. Vì NAV là số **Asset gửi** (không phải SDI tự dựng từ holdings), sửa quá khứ = **RE-INGEST**.

- **Use case:** NAV một ngày quá khứ SAI → **Asset gửi lại `T_SI_ASSET_DAILY` ngày đó** (`SP_INGEST_ASSET_NAV` idempotent MERGE theo date,si) → SDI **derive lại** unit/UP/PnL/return + lũy kế TE **từ ngày sửa trở đi** (đã có NAV+flow history per (date,si) ở `T_SI_NAV_BALANCE`).
- **Index master** sửa riêng (độc lập NAV): `SP_EOD_RECOMPUTE_INDEX_RANGE` (chỉ cần giá BO + target weight).

---

## 10. API cho UI riêng của SDI

> Mỗi API = **app gọi 1 stored proc** (`SP_GET_*`, xem `db/05_API.sql`) — tính/derive trong DB; app chỉ trả JSON, không tính. Định danh: KH=`C_CUST_CODE`, đơn vị = `C_SI_ACCOUNT` (sub-account); master suy từ sub-account.
>
> **BRD 2026-06-22:** các read API dưới phục vụ **giao diện riêng của SDI** (KHÔNG còn serve Asset/SMO — Asset tự tính từ BO/FO/Market trực tiếp). Toàn bộ FR-01..06 + PM API GIỮ NGUYÊN. **FR-06 `SP_GET_ASSET_REPORT` = read API báo cáo tài sản KH (GIỮ)** — đừng nhầm với `SP_GET_ASSET_SNAPSHOT` (producer Kafka đã gỡ). Xem [SDI-asset-gap.md](./SDI-asset-gap.md).

| FR | API | Proc | Nguồn |
|---|---|---|---|
| FR-01 Tổng quan đa tiểu khoản | GET /customer/{id}/si-overview | `SP_GET_SI_OVERVIEW` | sum T_MASTER_NAV_BALANCE + derive customer NAV (current từ T_SI_NAV_CURRENT) |
| FR-02 Chi tiết 1 tiểu khoản | GET /customer/{id}/si/{si} | `SP_GET_SI_DETAIL` | derive customer NAV/PnL + TWR + MWR + T_MASTER_NAV_BALANCE |
| FR-03 Chart so sánh | GET /customer/{id}/si/{si}/performance?range= | `SP_GET_SI_PERFORMANCE` | T_MASTER_NAV_BALANCE (TR) + T_MASTER_INDEX_DAILY (PR) + benchmark VN-Index (PR), chuỗi [mốc..cuối] |
| FR-04 Thông tin đầu tư | GET /customer/{id}/si/{si}/info | `SP_GET_SI_INFO` | T_SI_PORTFOLIO + master. **[BRD asset-sync]** phí QL không thuộc SDI (đã trừ trong NAV Asset) — không đọc rate phí từ SDI |
| FR-05 Holdings | GET /customer/{id}/si/{si}/holdings | `SP_GET_SI_HOLDINGS` | **holdings CURRENT của KH** (T_SI_PORTFOLIO_HOLDING × giá mới nhất) top20 + "OTHER" — sản phẩm segregated nên đọc holdings KH (≠ master-aggregate T_MASTER_HOLDING_BALANCE) |
| FR-06 Báo cáo tài sản | GET /customer/{id}/si/{si}/asset-report | `SP_GET_ASSET_REPORT` | **[BRD asset-sync]** T_SI_NAV_BALANCE (NAV ròng + unit/UP derive @asOf) + components Asset (`stock_value`, `cash` tổng); `AUM = stock + cash = NAV` + holdings chi tiết per-mã (FO @asOf × giá). **Bỏ payable / `C_FEE_ACCRUED_TOTAL` / breakdown per-type** (SDI không quản chi tiết phí) |

> **FR-06 result sets [BRD asset-sync]:** **RS1** summary (NAV ròng + unit/UP + `AUM = stock + cash = NAV`), **RS2** holdings chi tiết per-mã @asOf (FO×giá; ⚠️ Σ có thể lệch Asset `stock_value` — đo ở reconcile HOLDINGS_MISMATCH, RS1 dùng số Asset authoritative). **Đã BỎ RS3/RS4/RS5** (chi tiết income/phí + breakdown per-type) — phí QL đã trừ trong NAV Asset, SDI không quản sổ phí.

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
| Customer NAV/UP/% daily | **materialize** `T_SI_NAV_BALANCE` (~2,5 tỷ, CCI) — bắt buộc do FO-sync |

Customer NAV/UP/% daily: **BẮT BUỘC materialize** `T_SI_NAV_BALANCE` mỗi EOD — vì FO sync overwrite holdings (không event-source) → KHÔNG derive được lịch sử per-customer. Master-level agg tươi mỗi ngày + lưu `T_MASTER_NAV_BALANCE`.

---

## 12. Edge cases

1. **Đóng/mở lại vị thế** (unit=0 rồi nộp lại): khởi tạo lại T0 (UP=10.000).
2. **NAV ≤ 0** (Asset gửi NAV ròng âm, hiếm): floor 0 + cảnh báo (reconcile NAV_NEGATIVE).
3. **Reconcile FO**: Σ lots per ticker (SDI) vs holdings thật FO → break detection.
4. **Cash drag**: phần không khớp = tiền KH → tự phản ánh qua NAV (không logic riêng).
5. **Độ trễ giải ngân**: tiền chờ = cash 0% (không biến động) → ngày phẳng ×1.0, không ảnh hưởng tích lũy.
6. **Trade-date accounting**: mua/bán ghi nhận tại ngày khớp MP (không đợi settle T+2); pending ở cash sub-ledger.
7. **Thiếu/đến trễ giá**: dùng giá phiên trước, log; backfill → trigger recompute.
8. **Ngày không giao dịch**: lấy điểm gần nhất trước đó.
9. **Cổ tức**: phân biệt nguồn (holdings = income vs KH nạp = cashflow); income không tag cash_in.
10. **Rounding**: unit full precision; `Master Unit ≡ Σ Customer Unit` (định nghĩa, không tính 2 đường).
11. **MWR**: mẫu số ≈ 0 → null; XIRR không hội tụ → fallback Modified Dietz.
12. **Master không có KH** (Σ unit = 0): Master unit price = null.

---

## 13. Quy ước đặt tên (canonical — code phải theo)

snake_case. Một khái niệm = một code.

| Nhóm | code |
|---|---|
| Entity | `C_MASTER_CODE` (master), `C_SI_ACCOUNT` (sub-account/tiểu khoản = cust_code+đuôi), `C_CUST_CODE`, `business_date` |
| Tài sản | `aum` (=stock+cash=nav), `stock_value`, `cash` (tổng), `nav`, `buying_power`, `withdrawable_asset`, `net_invested_capital` *([BRD asset-sync] bỏ `payable_fee`/`custody_fee`/`management_fee` — phí đã trừ trong NAV Asset)* |
| Cashflow | `cash_in`, `cash_out`, **`net_cashflow`** (=cash_in−cash_out, dùng trong công thức), `income` (≠ cashflow) |
| Hiệu suất | `unit` (full precision), `unit_price`, `delta_unit`, `pnl`/`daily_pnl`, `return_pct` (=%PnL=TWR), `daily_return`, `mwr` |
| Index | `master_index`/`index_value`, `target_weight`, `weight` (holdings), `ref_price` (giá tham chiếu đầu phiên), `is_ex_rights`, `benchmark`, `close_price` |
| Khái niệm | `cash_drag`, `tracking_error`, `trade_date_accounting`, `segregated`, `price_return` |

Quy tắc: trong công thức dùng `net_cashflow` (rõ "net"); `income` tách khỏi cashflow; `cash` (tổng) chỉ cho NAV; % lưu dạng thập phân.

**Tham số stored procedure:**
- Mọi tham số SP **BẮT BUỘC prefix `@p_`** (vd `@p_si_account`, `@p_d`, `@p_range`) — phân biệt với biến cục bộ `@local`.
- SP phục vụ **API** (`SP_GET_*`, `SP_SET_*`) **BẮT BUỘC** thêm: `@p_user` (định danh người gọi), `@p_err_code INT OUTPUT`, `@p_err_msg NVARCHAR(400) OUTPUT` (trả lỗi về app, `@p_err_code=0`=OK; KHÔNG THROW ra ngoài). Pattern: validate trong thân → set err_code/err_msg → `THROW` để `TRY/CATCH` bắt; `CATCH` gán `err_code=-1` cho lỗi runtime (guard `IF @p_err_code=0`). **Đã áp dụng toàn bộ** `SP_GET_SI_*` (05_API) + `SP_GET_MASTER_*`/`SP_GET_PM_*`/`SP_SET_MASTER_PM_CONFIG`/`SP_GET_MASTER_ALERTS` (06_PM_API).
- SP **engine** nội bộ (`SP_EOD_STEP`, các job `SP_EOD_*`, `SP_INGEST_*`) — lỗi → `THROW` chặn EOD publish (chỉ theo rule prefix `@p_`). **Ngoại lệ `SP_EOD_RUN`** (orchestrator app gọi): bọc TRY/CATCH, trả `@p_err_code`/`@p_err_msg` OUT (0=OK, -1=lỗi), KHÔNG THROW; step nội bộ vẫn THROW + log FAILED, orchestrator bắt lại.

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

### Master Index — khớp ví dụ

| Ngày | Tỷ trọng (eff) | P_ref | Close | Index |
|---|---|---|---|---|
| 0 | A40 B35 C25 | — | 100/50/80 | 1000 |
| 1 | A40 B35 C25 | 100/50/80 | 102/49/82 | 1007 |
| 2 | A40 B35 C25 | 102/49/82 | 101/50.5/81 | 1011 |
| 3 (rebalance −C +D) | A45 B35 D20 | 101/50.5/60 | 103/52/62 | 1037 |

`Index_3 = 1011 × (0.45·103/101 + 0.35·52/50.5 + 0.20·62/60) = 1037` (weights eff_date=3 chi phối return ngày 3).
