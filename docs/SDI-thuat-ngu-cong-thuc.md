# SDI — Thuật ngữ & Công thức (Glossary & Formulas)

Tài liệu **tổng hợp** mọi thuật ngữ (tiếng Việt / tiếng Anh) và công thức tính toán dùng trong module SDI (Asset & Performance Engine). Mục tiêu: một nơi tra cứu duy nhất, giải thích dễ hiểu kèm ví dụ.

> **⚠️ THIN-LAYER (2026-06-26):** SDI KHÔNG tự tính hiệu suất — Asset gửi per-KH/ngày **`aum` (= NAV ròng) + `daily_return` (TWR, Asset đã khử dòng tiền)** + `cash` + `cash_in/out`. SDI chỉ **LƯU + SERVE**. Đã GỠ: `unit`/`unit_price`(NAVPS)/PnL-tiền-ngày/`T_SI_UNIT_LEDGER`/MWR/master pooled unit price/`stock_value`. %PnL kỳ = compound `∏(1+daily_return)−1 = EXP(Σ ln(1+r))−1`. GIỮ NGUYÊN: Tracking Error, Deviation, Master Index, Benchmark, Cash drag, AUM growth. Các mục §3/§7 đánh dấu **"[thin-layer] ĐÃ GỠ"** = lịch sử (giữ để truy nguồn). Bảng đổi tên: `T_SI_BALANCE`/`T_SI_CURRENT`/`T_MASTER_BALANCE`/`T_MASTER_CURRENT`; `C_NAV`→`C_AUM`.

> Bổ trợ: [SDI-spec.md](./SDI-spec.md) (đặc tả chi tiết + job EOD), [SDI-db-architecture.md](./SDI-db-architecture.md) (kiến trúc DB), [SDI-eod-data-exchange.md](./SDI-eod-data-exchange.md) (luồng dữ liệu), [SDI-pm-tool-spec.md](./SDI-pm-tool-spec.md) (dashboard PM). Khi lệch, **doc đặc tả gốc là chuẩn**; tài liệu này chỉ tổng hợp.

---

> **Viết tắt cột "BRD tham chiếu":** `spec` = [SDI-spec.md](./SDI-spec.md) · `pm` = [SDI-pm-tool-spec.md](./SDI-pm-tool-spec.md) · `eod` = [SDI-eod-data-exchange.md](./SDI-eod-data-exchange.md) · `db-arch` = [SDI-db-architecture.md](./SDI-db-architecture.md). Số sau là mục (§) trong doc đó.

## 1. Mô hình & định danh (Model & identifiers)

| Tiếng Việt | English | Ký hiệu / cột | Giải thích | BRD tham chiếu |
|---|---|---|---|---|
| Danh mục mẫu / Master | Master portfolio / Strategy | `C_MASTER_CODE` | Danh mục chiến lược chuẩn (rổ cổ phiếu + trọng số mục tiêu) do FO quản lý. PM theo dõi ở cấp này. PK bảng `T_MASTER_PORTFOLIO`. | spec §1 |
| Tiểu khoản | Sub-account | `C_SI_ACCOUNT` | Sinh khi 1 khách hàng (KH) đầu tư vào 1 master. Mã = `CUST_CODE` + đuôi, duy nhất toàn cục. Đóng rồi mở lại master ⇒ tiểu khoản MỚI. Tối đa 1 ACTIVE / (KH×master). Đơn vị tính NAV/hiệu suất customer-level. | spec §1 |
| Khách hàng | Customer | `C_CUST_CODE` | Định danh KH (VARCHAR10), xuyên các sub-system. 1 KH có nhiều tiểu khoản (nhiều master / theo thời gian). | spec §1 |
| Mã cổ phiếu | Ticker / Symbol | `C_TICKER` | Mã chứng khoán trong rổ. | spec §2 |
| Trọng số mục tiêu | Target weight | `C_TARGET_WEIGHT`, `wᵢ` | Tỷ trọng mã trong danh mục mẫu; Σ theo (master, ngày hiệu lực) = 1.0 (100% cổ phiếu, không có thành phần tiền). | spec §7 |
| Ngày hiệu lực (trọng số) | Effective date | `C_EFFECTIVE_DATE` | Ngày bộ trọng số bắt đầu áp dụng = **mốc rebalance**. Bộ "mới nhất ≤ ngày D" chi phối ngày D. | spec §7 |
| Tái cân bằng | Rebalance | — | FO đổi bộ trọng số (thêm/bớt mã, đổi tỷ trọng). Tính EOD, close-to-close. | spec §7 ; pm §3 (US3) |
| Ngày giao dịch | Business date | `C_BUSINESS_DATE` | Ngày làm việc thị trường (có giá đóng cửa). | spec §8 |

---

## 2. Tiền & tài sản (Cash & assets)

| Tiếng Việt | English | Ký hiệu / cột | Giải thích | BRD tham chiếu |
|---|---|---|---|---|
| Tiền (tổng) | Cash (total) | `C_CASH` | **[thin-layer] Asset gửi — là TỔNG, ĐÃ GỘP 2 khoản chờ về.** `C_CASH = tiền mặt + C_DIVIDEND_PENDING + C_SELL_PENDING`. Đây là số dùng cho cash drag. ⚠️ Đừng đọc `C_CASH` là "tiền mặt" — quan hệ NGƯỢC với bản cũ (trước 2026-08-03 `C_CASH` là tiền mặt và tổng phải CỘNG thêm 2 khoản chờ). | spec §3 |
| Tiền mặt | Cash on hand | — (suy ra) | **KHÔNG có cột riêng** — Asset không gửi. `Tiền mặt = C_CASH − C_DIVIDEND_PENDING − C_SELL_PENDING` (phần CÒN LẠI). Nhờ vậy 4 cột tiền của báo cáo AUM luôn cộng khớp theo định nghĩa. Ingest chặn `div+sell > cash` (err=22) nên không ra âm. | spec §3 ; `SP_GET_REPORT_AUM_TOTAL` |
| Tiền khả dụng | Available cash | `C_CASH_AVAILABLE` | Asset gửi. Số **thật sự rút/cắt được** — loại thêm cả tiền PHONG TOẢ / CHỜ KHỚP, nên `C_CASH − C_CASH_AVAILABLE ≥ C_DIVIDEND_PENDING + C_SELL_PENDING`. ⚠️ **KHÁC "tiền mặt"** — đừng dùng thay nhau (lấy khả dụng làm tiền mặt ⇒ 4 cột báo cáo lệch). Là nguồn thu phí. | spec §3 ; fee |
| Tiền bán chờ về | Pending settlement cash | `C_SELL_PENDING` | Tiền bán cổ phiếu chưa về tài khoản (chu kỳ T+2). **Khoản phải thu (receivable)** — vẫn tính vào tài sản → NAV không hụt giả khi bán. Asset gửi (`sell_pending`), **đã gộp trong `C_CASH`**. | spec §3 |
| Cổ tức tiền chờ về | Dividend receivable | `C_DIVIDEND_PENDING` | Tiền cổ tức đã chia nhưng chưa về. Receivable. Asset gửi (`dividend_pending`), **đã gộp trong `C_CASH`**. | spec §3 |
| ~~Giá trị cổ phiếu~~ **[thin-layer] ĐÃ GỠ** | ~~Stock value (MTM)~~ | ~~`C_STOCK_VALUE`~~ | **ĐÃ GỠ khỏi feed Asset** — thin-layer Asset gửi `aum` (NAV ròng) + `cash` (tổng) + `daily_return`, KHÔNG gửi `stock_value`. Composition per-mã vẫn có từ FO holdings (`T_MASTER_HOLDING_BALANCE`), nhưng định giá tổng cổ phiếu không còn là trường feed riêng. | — |
| AUM (= NAV ròng) | AUM / Net Asset Value | `C_AUM`, `C_LAST_AUM` | **[thin-layer] Asset gửi trực tiếp** (NAV ròng per-KH, đã trừ phí QL). SDI lưu, KHÔNG tự cộng từ stock+cash. Ở cấp master = Σ AUM tiểu khoản (Assets Under Management). | spec §3 ; eod (4) |
| ~~Phí phải trả~~ **[thin-layer] ĐÃ GỠ** | ~~Payable (accrued) fee~~ | ~~`C_PAYABLE_FEE`~~ | **ĐÃ GỠ** — SDI không accrue phí; Asset gửi NAV ròng (đã trừ phí QL sẵn). | — |
| AUM = NAV | — | — | Phí QL đã trừ trong NAV Asset gửi ⇒ **AUM (gross) = NAV (net)**, KHÔNG tách payable. | spec §3 |
| Số lượng | Quantity | `C_QUANTITY` | Số cổ phiếu nắm giữ. | spec §8 |
| Giá vốn bình quân | Average cost | `C_AVG_COST` | Tham chiếu lãi/lỗ; **KHÔNG** vào NAV (NAV theo giá thị trường). | spec §8 |
| Dòng tiền vào/ra | Cashflow in/out | `C_CF_IN`/`C_CF_OUT`, `CF` | Nạp/rút của KH (external). **Không** tính vào lãi/lỗ (PnL khử dòng tiền). | spec §4 |
| Nạp ban đầu / Nạp thêm / SIP | Initial / Top-up / SIP | `C_EVENT_TYPE` | Các loại tiền VÀO. SIP = nạp định kỳ (Systematic Investment Plan). | spec §4 |
| Rút | Withdraw | `WITHDRAW` | Tiền RA. | spec §4 |

---

## 3. Đơn vị quỹ & hiệu suất (Units & performance)

> **[thin-layer] ĐỔI MÔ HÌNH (2026-06-26):** SDI **KHÔNG tự tính hiệu suất** nữa. Asset gửi per-KH/ngày **`aum` (= NAV ròng) + `daily_return` (TWR, Asset ĐÃ khử dòng tiền)**; SDI chỉ **LƯU + SERVE**. Vì thế **Unit / Unit Price (NAVPS) / PnL-tiền-ngày đã GỠ** (chúng là công cụ để SDI *derive* TWR từ NAV+flow — nay Asset cấp thẳng `daily_return` nên không cần). %PnL kỳ = **compound** `∏(1+daily_return)−1` (xem §7.4). Các dòng "ĐÃ GỠ" dưới giữ lại để truy nguồn lịch sử.

| Tiếng Việt | English | Ký hiệu / cột | Giải thích | BRD tham chiếu |
|---|---|---|---|---|
| Lợi suất ngày | Daily return | `C_DAILY_RETURN`, `rₜ` | **[thin-layer] Asset GỬI** (TWR ngày, đã khử dòng tiền). SDI nhận + lưu, KHÔNG tự tính. (Trước: SDI derive `UPₜ/UP₍ₜ₋₁₎−1`.) | spec §6 ; eod (4) |
| Hiệu suất theo thời gian | Time-Weighted Return (TWR) | — | Lợi suất "chiến lược", miễn nhiễm thời điểm/khối lượng nạp-rút. **[thin-layer] Asset tính** (khử dòng tiền); SDI chỉ **compound** chuỗi `daily_return` để ra %PnL kỳ. | spec §6 |
| %Lãi lỗ kỳ | %PnL (range return) | `C_PNL_PCT` | **[thin-layer]** `= ∏(1+daily_return) − 1 = EXP(Σ ln(1+rₜ))−1` (compound các `daily_return` Asset gửi sau mốc). (Trước: `UP(cuối)/UP(mốc) − 1`.) | spec §6 |
| Ngày mốc (đầu kỳ) | Base date | `@base` | Gốc 0% của kỳ xem (theo filter 1D/1M/YTD/INCEPTION…). KH tham gia sau mốc → mốc = ngày tham gia. | spec §6 ; pm §2 |
| ~~Đơn vị quỹ~~ **[thin-layer] ĐÃ GỠ** | ~~Unit~~ | ~~`C_UNIT`~~ | **ĐÃ GỠ** (+ `T_SI_UNIT_LEDGER`). Unit là công cụ để derive TWR từ NAV+cashflow; nay Asset cấp thẳng `daily_return` nên SDI không còn cần phát hành/giữ unit. | — |
| ~~Giá đơn vị quỹ~~ **[thin-layer] ĐÃ GỠ** | ~~Unit Price (NAVPS)~~ | ~~`C_UNIT_PRICE`, `UP`~~ | **ĐÃ GỠ.** Gốc 10.000 không còn. NAVPS = NAV/Unit chỉ cần khi SDI tự tính return từ unit — model thin-layer dùng `daily_return` Asset gửi nên bỏ. | — |
| ~~Lãi/lỗ (tiền)~~ **[thin-layer] ĐÃ GỠ** | ~~PnL (money)~~ | ~~`C_DAILY_PNL`~~ | **ĐÃ GỠ.** SDI không tính PnL-tiền-ngày nữa (cần NAV đầu/cuối + flow để derive). AUM theo ngày vẫn lưu (`C_AUM`); chênh tiền tự suy nếu cần. | — |
| ~~Lợi suất theo dòng tiền~~ **[thin-layer] ĐÃ GỠ** | ~~Money-Weighted Return (MWR)~~ | — | **ĐÃ GỠ** (xem §7.5). MWR/Modified-Dietz cần chuỗi unit + cashflow nội bộ để tính; thin-layer không giữ unit ⇒ bỏ. SDI chỉ phục vụ TWR (compound `daily_return`). | — |

---

## 4. Chỉ số, benchmark & chỉ số PM (Index, benchmark & PM metrics)

| Tiếng Việt | English | Ký hiệu / cột | Giải thích | BRD tham chiếu |
|---|---|---|---|---|
| Chỉ số danh mục mẫu | Master Index | `C_INDEX_VALUE` (`T_MASTER_INDEX_DAILY`) | Chỉ số mô phỏng hiệu suất rổ mẫu (price-return), tái cân bằng hằng ngày về trọng số mục tiêu. Gốc 1000. | spec §7 |
| Lợi nhuận giá | Price Return (PR) | — | Chỉ tính biến động giá, KHÔNG gồm cổ tức. (Master Index, VN-Index là PR.) | spec §7 |
| Lợi nhuận tổng | Total Return (TR) | — | Gồm cả cổ tức. (Hiệu suất KH/master = TWR từ `daily_return` Asset gửi là TR vì NAV ăn cổ tức.) | spec §7 |
| Benchmark | Benchmark | `C_BENCHMARK_CODE` | Chỉ số tham chiếu ngoài (VN-Index, VN30…). PR. | spec §7 |
| Hiệu suất DM tổng KH | Aggregate customer return (AUM-weighted) | — | Lợi suất bình quân gia quyền theo AUM của toàn bộ KH trong master (end-weight). | pm §2 (US3) |
| Độ lệch hiệu suất | Performance deviation | `C_DEVIATION_BPS` | Chênh giữa hiệu suất KH và chỉ số master, tính bằng **điểm cơ bản (BPS)**. | pm §2 (US2) |
| Điểm cơ bản | Basis point (BPS) | — | `1 BPS = 0.01% = 0.0001`. (1% = 100 BPS.) | pm §2 |
| Sai số theo dõi | Tracking Error (TE) | `C_TE_AUMW` | Độ biến động của chênh lệch lợi suất KH vs chỉ số master (độ "bám" danh mục mẫu). | pm §2/§5 (US2) |
| Lợi suất chủ động | Active return | `aₜ` | `= r_KH,t − r_masterIndex,t` (chênh lợi suất ngày). TE = độ lệch chuẩn của chuỗi này. | pm §5 ; spec §8 (J12B) |
| Tỷ lệ tiền nhàn rỗi | Cash drag | `C_CASH_DRAG` | `= Tiền / Tổng tài sản`. Tiền nhiều ⇒ "ghì" hiệu suất so với rổ 100% cổ phiếu. | pm §2 |
| Dòng tiền ròng | Net flow | `C_NET_IN`/`C_NET_OUT`/`C_NET_FLOW` | Tiền vào − ra trong kỳ ở cấp master. | pm §2 ; spec §8 (J11) |
| Tăng trưởng AUM | AUM growth | `C_AUM_GROWTH_PCT` | `= AUM_hiện tại / AUM_đầu kỳ − 1`. | pm §2 |
| Số tiểu khoản | Account count | `C_TOTAL_ACCOUNT` | Số tiểu khoản ACTIVE của master. | pm §2 ; spec §8 (J11) |

---

## 5. Phí (Fees)

| Tiếng Việt | English | Ký hiệu / cột | Giải thích | BRD tham chiếu |
|---|---|---|---|---|
| Phí quản lý | Management fee | — | [BRD asset-sync] SDI **KHÔNG còn accrue**. Asset tính & trừ sẵn trong NAV ròng gửi sang (phí chỉ effect khi BO cắt thật — model realized). | — |
| (ĐÃ GỠ) Catalog phí, accrue, net-off | Fee catalog / accrue (removed) | ~~`T_FEE_CONFIG`~~, ~~`C_FEE_TYPE`~~, ~~`SP_INGEST_FEE_CHARGE`~~ | **ĐÃ GỠ** toàn bộ cơ chế accrue phí của SDI (catalog rate, J06 accrue, net-off khi BO cắt). Phí do Asset xử lý. | — |
| Phí lưu ký | Custody fee | `CUSTODY_FEE` | Phí lưu ký chứng khoán — đã gộp trong tiền/NAV ròng Asset gửi (SDI không quản chi tiết). | spec §3 |
| Phí giao dịch / thuế | Trading fee / tax | — | FO đã NET vào tiền mặt khi khớp lệnh — SDI không tính lại. | spec §3 ; eod (B) |

---

## 6. Kỹ thuật / EOD (Technical / End-of-day)

| Tiếng Việt | English | Ký hiệu | Giải thích | BRD tham chiếu |
|---|---|---|---|---|
| Cuối ngày | End-of-day (EOD) | `SP_EOD_RUN` | Batch tính toán chốt ngày (định giá → NAV → hiệu suất → index → đối soát → snapshot). | spec §9 ; eod §2 |
| Định giá thị trường | Mark-to-market (MTM) | J07 | Định giá lại toàn bộ cổ phiếu theo giá đóng cửa. | spec §9 |
| Cuốn trạng thái | Roll-forward | — | EOD lấy trạng thái hôm trước (current) + delta ngày → tính ngày mới, ghi đè current. | db-arch ; spec §9 |
| Đối soát | Reconcile | J13 | Cổng kiểm tra lệch (SDI vs FO, Σ KH vs master) — lệch quá ngưỡng thì chặn publish. | spec §9 ; eod §5 |
| Ảnh chụp | Snapshot | J14 | Chốt holdings/NAV cấp master để phục vụ đọc. | spec §9 |
| Idempotent | Idempotent | — | Chạy lại cho cùng kết quả (không cộng đôi). | eod ; db-arch |
| Tổng tích lũy | Prefix-sum / cumulative | `C_ACCUM_ACTIVE_RET`… | Lũy kế để tính nhanh thống kê qua khoảng bất kỳ bằng hiệu 2 mốc. | spec §8 (J12B) ; pm §5 |
| Lịch sử theo khoảng | Interval / SCD-2 | `C_VALID_FROM/TO` | Lưu lịch sử không trùng lặp (1 dòng/khoảng bất biến). | eod ; db-arch |

---

## 7. Công thức chi tiết (Formulas)

Quy ước: `t` = ngày; `(t-1)` = ngày giao dịch trước; `@base`/`@end` = mốc đầu/cuối kỳ.

### 7.1 Tài sản & AUM (= NAV ròng)
```
Tiền          = C_CASH                               (Asset gửi 1 SỐ tổng tiền dư)
AUM = NAV     = Asset GỬI TRỰC TIẾP per-KH (NAV ròng, đã trừ phí QL)   [thin-layer]
```
**[thin-layer]** SDI KHÔNG tự cộng `stock + tiền` để ra NAV — Asset gửi thẳng `aum` (NAV ròng). `cash` gửi kèm để hiển thị (FR-06) + cash-drag. Phí QL đã trừ ⇒ AUM = NAV (không tách payable).
**Ví dụ:** Asset gửi `aum = 1.000tr`, `cash = 100tr` → AUM = NAV = 1.000tr; cash drag = 100/1.000 = 10%.

### 7.2 ~~Unit & Unit Price (TWR sạch)~~ — **[thin-layer] ĐÃ GỠ (historical)**
> **ĐÃ GỠ.** Unit/Unit Price là cơ chế để SDI tự *derive* TWR từ NAV+cashflow (khử dòng tiền qua số unit). Thin-layer: **Asset gửi thẳng `daily_return` (TWR đã khử dòng tiền)** nên SDI không còn phát hành unit / tính unit price. Bảng `T_SI_UNIT_LEDGER` đã bỏ.
>
> *(Công thức lịch sử, để truy nguồn: `UP₀=10.000; Unit₀=NAV₀/10.000; ΔUnitₜ=CFₜ/UP₍ₜ₋₁₎; Unitₜ=Unit₍ₜ₋₁₎+ΔUnitₜ; UPₜ=NAVₜ/Unitₜ`. Tính chất khử dòng tiền `UPₜ=UP₍ₜ₋₁₎×(1+rₜ)` nay do Asset đảm bảo khi cấp `daily_return`.)*

### 7.3 ~~PnL tiền (cashflow-neutral)~~ — **[thin-layer] ĐÃ GỠ (historical)**
> **ĐÃ GỠ** (`C_DAILY_PNL`). PnL-tiền-ngày `= NAV cuối − NAV đầu + tiền RA − tiền VÀO` cần NAV đầu/cuối + flow để SDI tự tính. Thin-layer SDI chỉ lưu `aum` + `daily_return` (Asset cấp), không derive PnL tiền. Chênh AUM 2 mốc tự suy nếu UI cần.

### 7.4 TWR — %lãi lỗ kỳ (compound daily_return)
```
%PnL(kỳ) = ∏(1 + daily_returnₜ) − 1 = EXP( Σ ln(1 + daily_returnₜ) ) − 1     (t sau mốc, daily_return Asset gửi)
```
**[thin-layer]** SDI **compound on-read** chuỗi `daily_return` Asset gửi (KHÔNG còn `UP_cuối/UP_mốc − 1`). Bỏ ngày `daily_return` NULL; guard `daily_return ≤ −1` (mất hết vốn) → kẹp để LOG không vỡ.
**Ví dụ:** 2 ngày `daily_return = 0,05` rồi `0,028571` ⇒ ∏ = `1,05 × 1,028571 = 1,08` → %PnL = **+8,0%**. Tương đương `EXP(ln 1,05 + ln 1,028571) − 1 = 0,08`.

### 7.5 ~~MWR — money-weighted (Modified Dietz / XIRR)~~ — **[thin-layer] ĐÃ GỠ (historical)**
> **ĐÃ GỠ.** MWR (Modified Dietz, XIRR) cần chuỗi **unit + cashflow nội bộ** để giải lợi suất theo dòng tiền. Thin-layer không giữ unit (Asset cấp `daily_return` = TWR) ⇒ SDI chỉ phục vụ **TWR** (compound, §7.4), không tính MWR.
>
> *(Công thức lịch sử: `MWR = (NAV cuối − NAV đầu − CF_ròng)/(NAV đầu + Σᵢ wᵢ·CFᵢ)`, `wᵢ=(T−tᵢ)/T`; hoặc XIRR giải `NAV_đầu·(1+r)^T + Σ CFᵢ·(1+r)^(T−tᵢ) = NAV_cuối`.)*

### 7.6 Master Index — danh mục mẫu (price return, tái cân bằng ngày)
```
Index₀ = 1000
Indexₜ = Index₍ₜ₋₁₎ × Σᵢ ( wᵢ⁽ᵗ⁾ × Pᵢ,ₜ / P_refᵢ )
   wᵢ⁽ᵗ⁾  = bộ trọng số có effective_date ≤ t MỚI NHẤT ; Σ wᵢ⁽ᵗ⁾ = 100%
   P_refᵢ = C_REF_PRICE (giá tham chiếu đầu phiên, sở publish): phiên thường = close (t-1); ex-rights = giá sau chia
Daily return (index) = Σᵢ wᵢ⁽ᵗ⁾·Pᵢ,ₜ/P_refᵢ − 1   (= FACTOR − 1)
```
**Ví dụ (rebalance ngày 3 −C +D):** Index₂=1011, trọng số eff_date=3 A45 B35 D20:
`Index₃ = 1011 × (0,45·103/101 + 0,35·52/50,5 + 0,20·62/60) = 1037`.

### 7.7 ~~Tổng hợp cấp master (pooled unit price)~~ — **[thin-layer] ĐÃ GỠ (historical)**
> **ĐÃ GỠ.** Pooled `Master Unit Price = Σ NAV / Σ Unit` cần unit per-KH. Thin-layer không giữ unit. Thay bằng:
> - **Master daily return** = AUM-weighted `Σ(AUMᵢ·rᵢ) / Σ AUMᵢ` (engine ghi `T_MASTER_BALANCE.C_DAILY_RETURN` mỗi EOD, rᵢ = `daily_return` Asset gửi từng KH).
> - **Master %PnL kỳ** = compound chuỗi master daily return: `∏(1 + master_daily_returnₜ) − 1` (US3 composite làm việc này).
>
> *(Historical: `Master NAV = Σ NAV; Master Unit = Σ Unit; Master Unit Price = Master NAV/Master Unit`.)*

### 7.8 Hiệu suất DM tổng KH — AUM-weighted (end-weight, dùng cho PM)
```
Wᵢ   = AUMᵢ(cuối kỳ) / Σ AUM(cuối kỳ)                  (trọng số = AUM cuối kỳ = C_LAST_AUM)
Rᵢ   = ∏(1 + daily_returnₜ) − 1 = EXP(Σ ln(1+rₜ))−1    [thin-layer] compound daily_return KH i (TWR)
Return_DM_tổng_KH = Σᵢ Wᵢ · Rᵢ
```
**Ví dụ (3 KH):** Rᵢ S1=8% S2=6% S3=12% (mỗi Rᵢ = compound `daily_return` của KH đó); AUM 100tr/200tr/100tr (Σ=400tr) →
`(0,08·100 + 0,06·200 + 0,12·100)/400 = 0,08 = +8,0%`.
> Lưu ý: đây là **end-weight** (trọng số AUM cuối kỳ). Weight = AUM (`C_LAST_AUM`); AUM = NAV (phí QL đã trừ trong NAV Asset).
>
> **Vì sao `Rᵢ` dùng TWR (compound `daily_return`) chứ KHÔNG dùng `AUM_cuối/AUM_đầu − 1`:** KH nạp định kỳ hằng tháng (lãi tự chuyển vào) ⇒ AUM tăng do *nạp tiền*, không phải do lãi. Tỷ số `AUM_cuối/AUM_đầu − 1` (kiểu "ending/beginning − 1") gộp luôn phần nạp vào → **thổi phồng hiệu suất**; chỉ đúng khi không có dòng tiền giữa kỳ. `daily_return` Asset gửi đã khử dòng tiền (TWR) nên compound chuỗi đó mới là hiệu suất đầu tư thật. Đây cũng là điều kiện để so KH vs Master Index và tính Tracking Error có nghĩa.
>
> **[thin-layer] 1 API** `SP_GET_MASTER_RETURN_COMPOUND` (`Rᵢ = ∏(1+rₜ) − 1 = EXP(Σ ln(1+rₜ))−1`, quét `daily_return` qua (base,end]). *(Bản 2-lát `SP_GET_MASTER_RETURN_RETINDEX` dựa trên `UP_cuối/UP_mốc` đã GỠ — không còn unit price.)*

### 7.9 Độ lệch hiệu suất (Deviation, BPS)
```
Deviation (BPS) = (Return_DM_tổng_KH − Return_master_index) × 10000
```
**Ví dụ:** 0,08 − 0,071 = 0,009 → ×10000 = **90 BPS**.

### 7.10 Tracking Error (TE)
**Định nghĩa:** độ lệch chuẩn của chuỗi active return, quy năm.
```
aₜ        = r_KH,t − r_masterIndex,t                  (active return ngày)
TEᵢ (per KH) = STDEV(aₜ qua kỳ) × √X        X = số ngày GD trong kỳ (cap 252)
TE master = Σᵢ (TEᵢ · AUMᵢ) / Σ AUMᵢ                  (AUM-weighted)
```
**Tính nhanh on-read bằng prefix-sum** (lưu lũy kế ở `T_SI_BALANCE`, maintain EOD J12B):
```
Trên đoạn (base, end]:
   n   = ret_day_count(end)     − ret_day_count(base)
   ΣA  = accum_active_ret(end)    − accum_active_ret(base)
   ΣA² = accum_active_ret_sq(end) − accum_active_ret_sq(base)
   Var = (ΣA² − (ΣA)²/n) / (n − 1)              (n ≥ 2 ; Var<0 do làm tròn → 0)
   TEᵢ = √Var × √min(n, 252)
```
**Ví dụ (KH có 2 ngày, a = [0 ; 0,008571]):** ΣA=0,008571; ΣA²=0,00007346; n=2 →
Var = (0,00007346 − 0,008571²/2)/1 = 0,00003673 → STDEV=0,006061 → TE = 0,006061×√2 = **0,008571**.
*AUM-weighted 3 KH (S1 .008571, S2 .029126, S3 .051818; AUM 100/200/100):* TE master ≈ **0,029660** (≈2,97%).

### 7.11 Cash drag
```
Cash drag = Tiền / Tổng tài sản                       (Tiền = cash + pending + div)
```
**Ví dụ:** Tiền 16tr / AUM 400tr = **0,04 = 4%**.

### 7.12 Dòng tiền ròng & tăng trưởng AUM (cấp master, theo kỳ)
```
Net in   = Σ C_CASH_IN  trong kỳ
Net out  = Σ C_CASH_OUT trong kỳ
Net flow = Net in − Net out
AUM growth % = AUM(hiện tại) / AUM(đầu kỳ) − 1
```

### 7.13 Phí quản lý — [BRD asset-sync] SDI KHÔNG còn accrue
```
SDI KHÔNG tự tính/trích phí QL nữa. Asset gửi NAV RÒNG (đã trừ phí QL sẵn) → SDI dùng trực tiếp.
BO KHÔNG tính số phí lũy kế (accrued) gửi Asset; phí chỉ giảm tài sản khi BO cắt THẬT (qua cash,
   model REALIZED — Asset phản ánh, SDI nhận qua NAV). ⇒ AUM = NAV (gross = net), KHÔNG tách payable.
Đã gỡ: cột C_PAYABLE_FEE (mọi bảng) + C_FEE_ACCUM (T_SI_ASSET_DAILY) + J06 accrue + catalog
   T_FEE_CONFIG + SP_INGEST_FEE_CHARGE. Thuế GD: FO net thẳng vào cash (như cũ).
```

---

## 8. Quy ước số (Numeric conventions)

| Loại | Kiểu DECIMAL | Ghi chú |
|---|---|---|
| Tiền, Số lượng | `(20,0)` | đồng (VND) / cổ phiếu, không lẻ |
| Giá | `(18,4)` | giá đóng cửa, giá vốn |
| % / return / fee_rate / daily_return | `(10,6)` | tỷ lệ (0,01 = 1%); `daily_return` Asset gửi |
| Giá trị đối chiếu/chênh lệch (reconcile) | `(20,6)` | giữ thập phân khi so 2 nguồn |
| ~~Unit & Unit Price~~ **[thin-layer] ĐÃ GỠ** | ~~`(18,6)`~~ | **ĐÃ GỠ** — không còn unit/unit_price (Asset cấp `daily_return`). |
| Trọng số (weight) | `(12,8)` | Σ = 1.0 |
| Index value | `(18,x)` | gốc 1000 |
| Lũy kế TE (accum active) | `FLOAT` | double, tránh mất số khi cộng dồn |
| Deviation | BPS (`DECIMAL(12,2)`) | 1% = 100 BPS |

> **Đơn vị mặc định:** tiền = VND; lợi suất/return = tỷ lệ thập phân (0,08 = 8%); `daily_return` Asset gửi cùng đơn vị; deviation = BPS; Index/benchmark gốc tương ứng 1000/điểm thị trường. *([thin-layer] Unit Price gốc 10.000 đã GỠ — không còn đơn vị quỹ.)*
