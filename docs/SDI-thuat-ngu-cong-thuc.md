# SDI — Thuật ngữ & Công thức (Glossary & Formulas)

Tài liệu **tổng hợp** mọi thuật ngữ (tiếng Việt / tiếng Anh) và công thức tính toán dùng trong module SDI (Asset & Performance Engine). Mục tiêu: một nơi tra cứu duy nhất, giải thích dễ hiểu kèm ví dụ.

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
| Tiền mặt | Cash | `C_CASH` | Tiền khả dụng (FO sync). **Đã NET** phí giao dịch + thuế (FO trừ khi khớp lệnh). | spec §3 |
| Tiền bán chờ về | Pending settlement cash | `C_PENDING_CASH` | Tiền bán cổ phiếu chưa về tài khoản (chu kỳ T+2, tổng T0+T1+T2). Là **khoản phải thu (receivable)** — vẫn tính vào tài sản → NAV không hụt giả khi bán. | spec §3 |
| Cổ tức tiền chờ về | Dividend receivable | `C_DIV_CASH` | Tiền cổ tức đã chia nhưng chưa về. Receivable. | spec §3 |
| Tiền | Cash (tổng) | — | `Tiền = C_CASH + C_PENDING_CASH + C_DIV_CASH` (tiền mặt + 2 khoản chờ về). | spec §3 |
| Giá trị cổ phiếu | Stock value (MTM) | `C_STOCK_VALUE` | Định giá theo thị trường = `Σ (số lượng × giá đóng cửa)`. | spec §3 ; eod (J07) |
| Tổng tài sản | Total asset / AUM | `C_TOTAL_ASSET` | `= Giá trị cổ phiếu + Tiền` (gồm receivable). Ở cấp master = AUM (Assets Under Management). | spec §3 ; pm §2 |
| Phí phải trả | Payable (accrued) fee | `C_PAYABLE_FEE` | **TỔNG** phí đã tính dồn (accrue) nhưng **chưa thu** của MỌI loại phí accrue (QL/thuế/perf…). | spec §3 ; §9 (J06) |
| NAV ròng | Net Asset Value (net) | `C_NAV` | `= Tổng tài sản − Phí phải trả`. Giá trị thực thuộc về nhà đầu tư. | spec §3 |
| NAV gộp | Gross NAV | — | `= Tổng tài sản = NAV ròng + Phí phải trả`. | spec §3 |
| Số lượng | Quantity | `C_QUANTITY` | Số cổ phiếu nắm giữ. | spec §8 |
| Giá vốn bình quân | Average cost | `C_AVG_COST` | Tham chiếu lãi/lỗ; **KHÔNG** vào NAV (NAV theo giá thị trường). | spec §8 |
| Dòng tiền vào/ra | Cashflow in/out | `C_CF_IN`/`C_CF_OUT`, `CF` | Nạp/rút của KH (external). **Không** tính vào lãi/lỗ (PnL khử dòng tiền). | spec §4 |
| Nạp ban đầu / Nạp thêm / SIP | Initial / Top-up / SIP | `C_EVENT_TYPE` | Các loại tiền VÀO. SIP = nạp định kỳ (Systematic Investment Plan). | spec §4 |
| Rút | Withdraw | `WITHDRAW` | Tiền RA. | spec §4 |

---

## 3. Đơn vị quỹ & hiệu suất (Units & performance)

| Tiếng Việt | English | Ký hiệu / cột | Giải thích | BRD tham chiếu |
|---|---|---|---|---|
| Đơn vị quỹ | Unit | `C_UNIT` | Số "phần" của tiểu khoản. Chỉ thay đổi do dòng tiền (nạp/rút), KHÔNG do biến động giá → tách bạch hiệu suất khỏi dòng tiền. | spec §5 |
| Giá đơn vị quỹ | Unit Price (NAVPS) | `C_UNIT_PRICE`, `UP` | `= NAV ròng / Unit`. Gốc tại ngày tham gia (T0) = **10.000**. | spec §5 |
| Lợi suất ngày | Daily return | `C_DAILY_RETURN`, `rₜ` | `= UPₜ / UP₍ₜ₋₁₎ − 1`. Độc lập dòng tiền. | spec §6 |
| Lãi/lỗ (tiền) | PnL (money) | `C_DAILY_PNL` | Lãi/lỗ bằng tiền trong ngày/kỳ, đã khử dòng tiền. | spec §6 |
| Hiệu suất theo thời gian | Time-Weighted Return (TWR) | — | Lợi suất "chiến lược", miễn nhiễm thời điểm/khối lượng nạp-rút. Tính qua Unit Price. | spec §6 |
| Lợi suất theo dòng tiền | Money-Weighted Return (MWR) | — | Lợi suất "của bạn", chịu ảnh hưởng thời điểm nạp-rút. Dùng Modified Dietz / XIRR. | spec §6 |
| %Lãi lỗ kỳ | %PnL (range return) | — | `= UP(cuối kỳ)/UP(mốc) − 1` (TWR). | spec §6 |
| Ngày mốc (đầu kỳ) | Base date | `@base` | Gốc 0% của kỳ xem (theo filter 1D/1M/YTD/INCEPTION…). KH tham gia sau mốc → mốc = ngày tham gia. | spec §6 ; pm §2 |

---

## 4. Chỉ số, benchmark & chỉ số PM (Index, benchmark & PM metrics)

| Tiếng Việt | English | Ký hiệu / cột | Giải thích | BRD tham chiếu |
|---|---|---|---|---|
| Chỉ số danh mục mẫu | Master Index | `C_INDEX_VALUE` (`T_MASTER_INDEX_DAILY`) | Chỉ số mô phỏng hiệu suất rổ mẫu (price-return), tái cân bằng hằng ngày về trọng số mục tiêu. Gốc 1000. | spec §7 |
| Lợi nhuận giá | Price Return (PR) | — | Chỉ tính biến động giá, KHÔNG gồm cổ tức. (Master Index, VN-Index là PR.) | spec §7 |
| Lợi nhuận tổng | Total Return (TR) | — | Gồm cả cổ tức. (Unit Price của KH/master là TR vì NAV ăn cổ tức.) | spec §7 |
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
| Phí quản lý | Management fee | `C_RATE` (type MGMT_FEE) | Phí %/**năm** trên tài sản. SDI tính dồn (accrue) hằng ngày; BO thực hiện cắt tiền. Rate khai trong `T_FEE_CONFIG` (type MGMT_FEE, group PAYABLE). | spec §9 (J06) |
| Catalog chính sách phí/thuế | Fee/tax policy catalog | `T_FEE_CONFIG` | **Catalog chính sách phí/thuế chung TOÀN HỆ** (GLOBAL, PK clustered (C_FEE_TYPE) — 1 dòng/loại áp mọi master, KHÔNG có master_code; per-master defer): cột `C_FEE_TYPE` + `C_FEE_GROUP`[INCOME\|PAYABLE] (DÙNG CHUNG vocabulary, khớp `T_SI_INCOME_FEE`), `C_RATE` NULL-able (NULL = không accrue), `C_DAY_COUNT`. J06 chỉ accrue dòng group=PAYABLE & rate>0. Thêm chính sách phí mới = INSERT 1 dòng, không sửa schema/SP. | spec §8 ; db-arch §3 |
| Loại phí | Fee type | `C_FEE_TYPE` | MGMT_FEE \| TAX \| PERF_FEE \| … — phân loại phí accrue (config) + dòng cắt trong ledger. | spec §8 |
| Tính dồn (phí) | Accrue | — | Cộng dồn phí phải trả mỗi ngày dương lịch (chưa thu tiền), ĐA-LOẠI theo config. | spec §9 (J06) |
| Cắt phí (net-off) | Fee charge / net-off | `T_SI_INCOME_FEE` (group PAYABLE) | BO cắt tiền phí 1 cục/tháng (mang `fee_type`) → báo về → SDI trừ vào khoản phải trả (trừ tổng mọi loại). | spec §9 ; eod (B) |
| Phí lưu ký | Custody fee | `CUSTODY_FEE` | Phí lưu ký chứng khoán (FO đẩy về, ghi `T_SI_INCOME_FEE` group PAYABLE). Point-event, KHÔNG accrue config. | spec §3 ; eod (B) |
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

### 7.1 Tài sản & NAV
```
Tiền          = C_CASH + C_PENDING_CASH + C_DIV_CASH
Tổng tài sản  = Giá trị cổ phiếu + Tiền                       (= AUM ở cấp master)
NAV (ròng)    = Tổng tài sản − Phí phải trả
NAV gộp       = Tổng tài sản                                  (= NAV ròng + Phí phải trả)
```
**Ví dụ:** stock 900tr, cash 60tr, pending 40tr, div 0, payable 0.85tr →
Tiền = 100tr; Tổng tài sản = 1.000tr; NAV ròng = 999.15tr.
*Bán cổ phiếu 40tr (chờ về T+2):* stock 860tr, pending 40tr → Tổng tài sản vẫn 1.000tr (NAV không hụt giả).

### 7.2 Unit & Unit Price (TWR sạch)
```
T0 (ngày tham gia):  UP₀ = 10.000 ;  Unit₀ = NAV₀ / 10.000
CFₜ      = cash_in − cash_out                  (ròng, gom trong ngày)
ΔUnitₜ   = CFₜ / UP₍ₜ₋₁₎                        (giá quy đổi = UP cuối ngày trước)
Unitₜ    = Unit₍ₜ₋₁₎ + ΔUnitₜ
UPₜ      = NAVₜ / Unitₜ
```
Vì sao chia `UP₍ₜ₋₁₎`: rút gọn ra `UPₜ = UP₍ₜ₋₁₎ × (1 + rₜ)` ⇒ daily return = lợi suất tài sản thật, **độc lập dòng tiền** (cashflow chỉ đổi số Unit, không đổi tỷ lệ giá Unit).

**Ví dụ (ngày 3, từ worked example §8.6):** đầu ngày Unit=1000, UP₍₂₎=15.000; trong ngày nạp 2tr rút 0.1tr → CF=1.9tr; NAV cuối 18tr.
`ΔUnit = 1.900.000 / 15.000 = 126,667` → Unit = 1.126,667 → `UP = 18.000.000 / 1.126,667 = 15.976`.

### 7.3 PnL tiền (cashflow-neutral)
```
PnL ngày  = NAV cuối − NAV đầu + tiền RA − tiền VÀO
PnL kỳ    = Σ PnL các ngày trong kỳ
```

### 7.4 TWR — %lãi lỗ kỳ (time-weighted)
```
Daily return = UPₜ / UP₍ₜ₋₁₎ − 1
%PnL(kỳ)     = UP[ngày cuối] / UP[ngày mốc] − 1     (= tích các daily return sau mốc)
```
**Ví dụ:** UP mốc 15.000 → UP cuối 19.082 ⇒ %PnL = 19.082/15.000 − 1 = **+27,21%** (bất kể nạp/rút giữa kỳ).

### 7.5 MWR — lợi suất của bạn (money-weighted, Modified Dietz)
```
MWR = (NAV cuối − NAV đầu − CF_ròng) / (NAV đầu + Σᵢ wᵢ · CFᵢ)
   wᵢ = (T − tᵢ) / T     (tᵢ = số phiên từ mốc tới flow i ; T = số phiên cả kỳ)
   tử số = PnL tiền cả kỳ
XIRR (tùy chọn, chính xác): giải r trong  NAV_đầu·(1+r)^T + Σ CFᵢ·(1+r)^(T−tᵢ) = NAV_cuối
```
Mẫu số ≈ 0 → trả null. Hiển thị period return (không quy năm trừ khi yêu cầu).

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

### 7.7 Tổng hợp cấp master (pooled)
```
Master NAV          = Σ NAV các tiểu khoản
Master Unit         = Σ Unit các tiểu khoản
Master Unit Price   = Master NAV / Master Unit          (pooled; total-return)
```

### 7.8 Hiệu suất DM tổng KH — AUM-weighted (end-weight, dùng cho PM)
```
Wᵢ   = AUMᵢ(cuối kỳ) / Σ AUM(cuối kỳ)          (trọng số = AUM cuối kỳ)
PnLᵢ = UPᵢ(cuối) / UPᵢ(mốc) − 1                (TWR từng KH)
Return_DM_tổng_KH = Σᵢ Wᵢ · PnLᵢ
```
**Ví dụ (3 KH):** PnL S1=8% S2=6% S3=12%; AUM 100tr/200tr/100tr (Σ=400tr) →
`(0,08·100 + 0,06·200 + 0,12·100)/400 = 0,08 = +8,0%`.
> Lưu ý: đây là **end-weight** (trọng số AUM cuối kỳ), KHÁC pooled Master Unit Price (§7.7, ~begin-weight). Hai số đo khác nhau có chủ đích.

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
**Tính nhanh on-read bằng prefix-sum** (lưu lũy kế ở `T_SI_NAV_BALANCE`, maintain EOD J12B):
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

### 7.13 Phí ACCRUE đa-loại — accrue & net-off (BO-driven, config-driven)
```
Accrue (EOD J06, mỗi ngày) — đọc T_FEE_CONFIG (WHERE C_FEE_GROUP='PAYABLE' AND C_RATE>0):
   Phí phải trả += AUM gộp × (số NGÀY DƯƠNG LỊCH kể từ lần tính trước) × Σ_loại (C_RATE / C_DAY_COUNT)
   (mỗi loại phí có rate + day_count riêng [default 365]; không khai config / rate NULL / group INCOME → loại đó không accrue)
Net-off (khi BO cắt, ngoài EOD):
   Phí phải trả −= số tiền BO báo đã cắt        (trừ TỔNG mọi loại; thiếu/đủ cứ trừ; phần dư treo tiếp)
NAV ròng = Tổng tài sản − Phí phải trả          (Phí phải trả = tổng accrued mọi loại)
```
**Ví dụ (1 loại MGMT_FEE):** AUM 1.000tr, rate 1%/năm (day_count 365), 31 ngày dương lịch →
`accrue = 1.000.000.000 × 31 × 0,01/365 = 849.315 đ`. BO cắt 800.000 → phải trả còn treo 49.315 đ.
**Đa-loại:** thêm TAX 0,1%/năm → `Σ(rate/dc) = (0,01+0,001)/365`; accrue/ngày = AUM × ngày × tổng đó.

---

## 8. Quy ước số (Numeric conventions)

| Loại | Kiểu DECIMAL | Ghi chú |
|---|---|---|
| Tiền, Số lượng | `(20,0)` | đồng (VND) / cổ phiếu, không lẻ |
| Giá | `(18,4)` | giá đóng cửa, giá vốn |
| % / return / fee_rate | `(10,6)` | tỷ lệ (0,01 = 1%) |
| Phí lũy kế ngày (payable/accrued) | `(20,6)` | giữ thập phân, không round VND/ngày |
| Unit & Unit Price | `(18,6)` | Unit Price gốc 10.000 |
| Trọng số (weight) | `(12,8)` | Σ = 1.0 |
| Index value | `(18,x)` | gốc 1000 |
| Lũy kế TE (accum active) | `FLOAT` | double, tránh mất số khi cộng dồn |
| Deviation | BPS (`DECIMAL(12,2)`) | 1% = 100 BPS |

> **Đơn vị mặc định:** tiền = VND; lợi suất/return = tỷ lệ thập phân (0,08 = 8%); deviation = BPS; Unit Price gốc = 10.000; Index/benchmark gốc tương ứng 1000/điểm thị trường.
