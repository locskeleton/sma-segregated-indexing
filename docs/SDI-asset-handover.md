# SDI ⇄ Asset — bàn giao tính NAV/phí (BRD 2026-06-24)

> **⚠️ SUPERSEDED bởi THIN-LAYER (2026-06-26):** tài liệu này chốt model **2026-06-24** (Asset gửi NAV ròng, **SDI tự derive unit/UP/PnL/TWR** từ NAV+flow). Model HIỆN TẠI: **Asset gửi CẢ `daily_return` (TWR)** → **SDI KHÔNG derive gì** (LƯU + SERVE compound). Gỡ unit/unit_price/PnL-tiền/T_SI_UNIT_LEDGER/MWR. Các §3/§4/§5 "SDI derive unit/UP" dưới = LỊCH SỬ design — xem [SDI-spec.md](SDI-spec.md) §3/§6 (thin-layer) là chuẩn hiện hành.
>
> Trạng thái: **ĐÃ IMPLEMENT 2026-06-24** (P0→P5, build + all tests GREEN). Chi tiết: [SDI-asset-impl-plan.md](SDI-asset-impl-plan.md).
> **Nhánh `feat/brd-asset-nav-sync` — ĐỘC LẬP, KHÔNG merge vào `main`.** Main giữ luồng hiện tại (SDI tự tính NAV/phí).
> Liên quan: [SDI-asset-gap.md](SDI-asset-gap.md).

## 0. Lưu ý quan điểm (ghi lại để team cân nhắc)

Có quan ngại kiến trúc: **SDI là nơi phát sinh nghiệp vụ chính** → lẽ ra nên là nơi quản lý & tính toán số liệu, thay vì nhận đồng bộ NAV/phí từ Asset. "Dù hệ thống nào tính thì số liệu phải cân nhau" — nếu cân thì việc đổi nguồn không bắt buộc; nếu không cân thì đổi nguồn chỉ **dời** chỗ lệch. BRD vẫn yêu cầu đổi → tách nhánh này để thử, **không động luồng main đã ổn**.

## 0b. Quyết định đã CHỐT (2026-06-24)

| # | Quyết định | Chốt |
|---|---|---|
| 1 | `fee` Asset gửi dạng nào | **[BRD asset-sync] SUPERSEDED 2026-06-25: Asset KHÔNG gửi số phí lũy kế.** Asset gửi **NAV RÒNG** (phí QL đã trừ sẵn — model realized); `AUM = NAV` (gross = net, không tách payable). |
| 2 | Granularity | **Chỉ per-SI**. SDI **tự SUM lên master** cuối ngày (`aum` Σ + `daily_return` AUM-weighted). **[thin-layer] KHÔNG còn "tính lại unit/unit_price"** — Asset cấp `daily_return`. |
| 3 | Index↔NAV giá BO | **Cùng giá BO close + cùng cutoff.** MỞ RỘNG: **MỌI hệ (BO/Asset/SDI) tham chiếu CÙNG 1 bộ giá đóng cửa** → không deviation giả. |
| 4 | Ngày gửi NAV | **Chỉ ngày GD**; T7/CN/lễ SDI **carry-forward**. |
| 5 | Originator cashflow | **SDI VẪN nhập cashflow** (giữ `T_SI_CASHFLOW_EVENT`) + Asset cũng gửi `cash_in/out` → **đối soát 2 nguồn**. Unit/UP tính theo cashflow SDI (authoritative); **reconcile phải PASS** để NAV(Asset) & unit(SDI) nhất quán (cùng dòng tiền). |

**Bổ sung — giá cuối ngày + thành phần tài sản (chốt 2026-06-24):** ~~Asset gửi ĐỦ thành phần tài sản dạng SỐ TỔNG cấp SI (`stock_value + ... + cash`)~~ **[thin-layer 2026-06-26] cập nhật:** Asset gửi `aum` (NAV ròng) + `daily_return` + `cash` (tổng), **KHÔNG `stock_value`**. SDI **không tự định giá EOD**, chỉ **LƯU**. **Mọi hệ (BO/Asset/SDI) tham chiếu CÙNG 1 bộ giá đóng cửa.**

**FO holdings (per-mã): TẠM GIỮ ingest.** Không phục vụ EOD NAV (AUM từ Asset), mà cho: composition `T_MASTER_HOLDING_BALANCE`, US3-click RS2 (delta holdings thực per-mã), alert drift/ngành, và **tính tài sản CẬN REAL-TIME (future)**. *([thin-layer] đối soát `Σ(FO×giá) vs Asset stock_value` GỠ — Asset không gửi stock_value.)*

> Lưu ý scope PM BRD: core **US1–US5 KHÔNG cần per-mã** (chạy trên NAV/unit cấp SI/master). Per-mã chỉ nuôi US3-click RS2 + alert (alert: nguồn ngành `T_TICKER_INDUSTRY` thật chưa nạp).

## 1. Thay đổi BRD

1. **Phí quản lý không tính theo AUM trung bình** mà **theo từng ngày**.
2. **Asset là nguồn DUY NHẤT** của NAV, dòng tiền, phí — Asset đã tính hết để hiển thị SMO → **đồng bộ về SDI**.
3. SDI **bỏ quản lý chi tiết giao dịch phí**, **không tự định giá NAV** nữa; chỉ nhận **con số tổng per-SI** từ Asset.

Hệ quả: SDI chuyển từ *engine định giá* → **consumer + index engine + PM serve layer + validator**.

## 2. Nguyên tắc single-source

| Đại lượng | Nguồn sự thật | SDI làm gì |
|---|---|---|
| AUM (=NAV) per-SI | **Asset** (gửi) | lưu (`T_SI_BALANCE`/`_CURRENT`) |
| **[thin-layer] daily_return (TWR)** | **Asset** (gửi) | lưu (KHÔNG derive) |
| cash_in / cash_out per-SI | **Asset** (gửi) | lưu (đối soát cashflow SDI) |
| ~~phí (lũy kế/ngày)~~ | **KHÔNG gửi** | phí QL đã trừ sẵn trong NAV ròng Asset (model realized); SDI không lưu/hiển thị số phí |
| ~~units / unit_price / PnL-tiền / MWR~~ | **[thin-layer] ĐÃ GỠ** | Asset cấp `daily_return` ⇒ SDI không còn derive unit/UP/PnL/MWR |
| %PnL kỳ (TWR) | **SDI serve** | compound `daily_return` (`∏(1+r)−1`) on-read |
| Index danh mục mẫu | **SDI** (giá BO × target weight) | tính (J12, giữ nguyên) |
| Composition holdings (alert/drift) | **FO holdings × giá BO** | tính (`T_MASTER_HOLDING_BALANCE`, giữ) |

**[thin-layer]** AUM + daily_return đều là số **Asset gửi** → SDI chỉ LƯU + SERVE (compound), **không tự tính** ⇒ không lệch nguồn.

## 3. Data contract Asset → SDI (CHỐT 2026-06-24)

Mỗi `(business_date, si_account)` — **chỉ ngày GD** (QĐ4), **per-SI** (QĐ2). **[thin-layer 2026-06-26] cập nhật contract:**
```
aum            -- NAV RÒNG cuối ngày — Asset GỬI TRỰC TIẾP (đã trừ phí QL; AUM = NAV).
daily_return   -- [thin-layer] TWR ngày (Asset ĐÃ khử dòng tiền). NULL ngày đầu. SDI LƯU, KHÔNG tự tính.
cash           -- TỔNG tiền dư (1 SỐ: gộp tiền mặt + bán chờ về T+ + cổ tức tiền) — KHÔNG chia nhỏ.
cash_in        -- nạp trong ngày (ĐỐI SOÁT với cashflow SDI tự nhập — QĐ5)
cash_out       -- rút trong ngày (ĐỐI SOÁT)
-- [thin-layer] KHÔNG còn `stock_value` (Asset không gửi) / `fee`/`fee_accum` (phí QL đã trừ trong aum — model realized).
```
- **[thin-layer] AUM = NAV = Asset gửi trực tiếp** (`aum`, đã RÒNG phí QL) + **`daily_return` (TWR)**. SDI KHÔNG lắp/trừ/derive. `cash` gửi kèm để hiển thị (FR-06) + cash drag. *(Bản 2026-06-24 gửi `nav`+`stock_value` để SDI derive unit/UP — đã thay: Asset gửi luôn `daily_return`, bỏ `stock_value` + reconcile NAV_CONSISTENCY.)*
- **GIÁ EOD THỐNG NHẤT (QĐ3 mở rộng):** Asset định giá `aum` bằng **đúng giá BO close** mà SDI dùng cho index → index ↔ NAV apples-to-apples.
- **Per-SI** (QĐ2): PM tool toàn bộ per-KH; SDI tự SUM lên master.
- **QĐ5:** SDI dùng `cash_in/cash_out` **của chính no** (originator) để **đối soát**; `cash_in/out` Asset gửi để bắt lệch. **[thin-layer]** dòng tiền không còn dùng tính unit (Asset cấp `daily_return` đã khử dòng tiền) — reconcile lệch ⇒ `daily_return` Asset & flow SDI không cùng dòng tiền → ghi break, chặn publish.

## 4. ~~Quy ước định giá đơn vị quỹ~~ — **[thin-layer 2026-06-26] ĐÃ GỠ (historical)**

> **ĐÃ GỠ.** Quy ước unit/UP (init 10k + prior-day historic) là để **SDI tự derive TWR** từ NAV+flow. Thin-layer: **Asset gửi thẳng `daily_return` (TWR đã khử dòng tiền)** ⇒ SDI không tính unit/UP/PnL nữa. Việc "khử dòng tiền theo UP_{t-1}" nay là **trách nhiệm của Asset** khi cấp `daily_return`. %PnL kỳ (SDI serve) = `∏(1+daily_return)−1`.
>
> *(Công thức lịch sử — KHÔNG còn dùng:)*
> ```
> Δunits = (cash_in−cash_out)/UP_{t-1} ; units_t = units_{t-1}+Δunits ; UP_t = NAV_t/units_t
> return_t = UP_t/UP_{t-1}−1 ; PnL_t = NAV_t−NAV_{t-1}+cash_out−cash_in ; init UP=10.000
> ```
> ⚠️ Thỏa thuận **phương pháp** (init 10k + prior-day historic khử dòng tiền) vẫn quan trọng — nhưng nay **Asset thực thi** khi tính `daily_return`, KHÔNG còn ở SDI.

## 5. Vai trò SDI sau cắt

**GIỮ:**
- Index danh mục mẫu (J12 `SP_EOD_SI_INDEX`) — chỉ cần giá BO + target weight, độc lập NAV.
- PM serve layer (06_PM_API) — đọc `aum`/`daily_return`/TE đã ingest; **[thin-layer]** %PnL = compound `daily_return`.
- TE accum (J12B) — đọc `daily_return` (Asset gửi) − `index daily_return` từ J12.
- **FO holdings ingest (per-mã) — TẠM GIỮ**: composition `T_MASTER_HOLDING_BALANCE`, US3-click RS2, alert drift/ngành, **near-realtime asset (future)**. KHÔNG dùng cho EOD NAV.
- **`T_SI_CASHFLOW_EVENT` (QĐ5): SDI VẪN là originator** nạp/rút → giữ; **[thin-layer]** chỉ để **đối soát** với `cash_in/out` Asset (KHÔNG còn tính unit).
- **SI_AGG (J11): SUM per-SI → master** (`aum` Σ + `daily_return` AUM-weighted; SUM số ingest).

**BỎ:**
- `SP_EOD_COMPUTE` J06 accrue, **J07 MTM cho EOD NAV** (GIỮ năng lực MTM holdings cho near-realtime future), J08 NAV-from-holdings, **[thin-layer] J09 PnL + J10 derive unit/UP** (Asset cấp `daily_return`).
- `SP_INGEST_FEE_CHARGE`, `T_SI_INCOME_FEE` (chi tiết phí), `T_FEE_CONFIG`, `C_PAYABLE_FEE`.
- **[thin-layer] `T_SI_UNIT_LEDGER`** + cột `unit`/`unit_price`/`daily_pnl` + **MWR** (cần unit).
- `T_SI_CASH_HIST` (interval cash cho tính NAV). **Holdings GIỮ** (FO ingest — xem GIỮ).
- `SP_EOD_RECOMPUTE_RANGE` (recompute NAV từ history) → thay bằng **re-ingest** (§7.D).

**ĐỔI THÀNH INGEST:**
- `T_SI_BALANCE` / `T_SI_CURRENT` ← `SP_INGEST_ASSET_NAV` (per-SI: **[thin-layer]** `{aum, daily_return, cash, cash_in, cash_out}` — KHÔNG `stock_value`/`fee`; SDI **LƯU thẳng**, KHÔNG derive).

## 6. Cơ chế giảm vênh: NAV-bridge reconcile (SDI làm validator)

**MỤC ĐÍCH KÉP (chốt):** reconcile vừa **chặn publish** khi lệch lớn, vừa **GHI LẠI độ lệch (giá trị diff per-SI/ngày) KỂ CẢ khi trong ngưỡng** → dùng để **đo mức vênh SDI-derive vs Asset theo thời gian**, đánh giá thực tế thiết kế 2-nguồn lệch tới đâu (đúng quan ngại §0). Lưu diff (không chỉ pass/fail) để báo cáo/giám sát.

3 check, ghi vào `T_EOD_RECON_BREAK` (mở rộng + cột lưu diff):
1. **NAV_NEGATIVE:** `aum < 0` (Asset gửi NAV ròng âm bất thường).
2. **SI_NAV_MISMATCH:** Σ `aum` per-SI vs `aum` master (master do SDI tự SUM — bắt lỗi agg).
3. **CASHFLOW_MISMATCH (QĐ5):** `cash_in/out (SDI nhập)` vs `cash_in/out (Asset gửi)`. Lệch → `daily_return` (Asset) & cashflow (SDI) không cùng dòng tiền.
> **[thin-layer]** Đã GỠ **NAV-bridge/NAV_CONSISTENCY** (`Δ_định_giá` cần stock — Asset không gửi) + **HOLDINGS_MISMATCH** (so Asset `stock_value` — không còn). Còn 3 check trên.

## 7. Điểm vênh còn lại + xử lý

- **A. Granularity**: bắt buộc per-SI (đã chốt ràng buộc §3).
- **B. Index (SDI) vs NAV (Asset) — so sánh chéo nguồn**: deviation PM = return KH (Asset-NAV) − return index (SDI). 🔶 Phải chốt index & NAV **cùng giá BO + cùng thời điểm chốt** để không sinh "deviation giả".
- **C. Timing/dependency**: thêm 1 hop — BO/FO → Asset tính → Asset gửi SDI. PM/TE chờ NAV Asset về; **Index chạy độc lập** (chỉ cần BO). Thêm nguồn `ASSET_NAV` vào `T_EOD_PIPELINE` (gate chờ trước khi serve/TE).
- **D. Sửa quá khứ → RE-INGEST, không recompute**: Asset sửa `aum`/`daily_return` ngày cũ → SDI nhận lại → **ghi lại** `aum`+`daily_return`/lũy kế TE **từ ngày sửa trở đi**. `SP_EOD_RECOMPUTE_RANGE` (reconstruct NAV) thành obsolete.
- ~~**E. Ngày dương lịch vs ngày GD cho phí**~~ **[BRD asset-sync] MOOT**: SDI không nhận/nội suy phí (phí QL đã trừ trong NAV ròng Asset — model realized).
- ~~**F. Làm tròn phí**~~ **[BRD asset-sync] MOOT**: không còn field `fee` ở SDI.

## 8. Trạng thái quyết định

**ĐÃ CHỐT:** QĐ1–5 (§0b) · cash/div = Asset gửi đủ thành phần số tổng SI · Asset không per-mã · **FO holdings TẠM GIỮ** (composition + US3-RS2 + alert + near-realtime future).

**CÒN LẠI (chi tiết implement, quyết khi code):**
🔶 **(b) Thiết kế reconcile cashflow 2 nguồn (QĐ5)**: ngưỡng lệch, xử lý khi lệch (chặn publish? cảnh báo?), thời điểm đối soát. SDI-cashflow để bắt lệch vs Asset `cash_in/out`.
🔶 ~~**(c) Reconcile holdings**: `Σ(FO×giá BO)` vs Asset `stock_value`~~ **[thin-layer] GỠ** (Asset không gửi `stock_value`).
🔶 **(d) Near-realtime asset valuation (future)**: giữ năng lực MTM holdings cho intraday — phạm vi/định nghĩa để sau.

## 9. Tác động migration (high-level, sau khi chốt §8)

- Bỏ phần lớn `SP_EOD_COMPUTE`/`_CORE`, fee subsystem, recompute NAV.
- Thêm `SP_INGEST_ASSET_NAV` (**[thin-layer]** LƯU `aum`+`daily_return`, KHÔNG derive units/UP).
- Pipeline: thêm nguồn `ASSET_NAV` + tách rõ luồng Index (BO) vs luồng NAV-consume (Asset).
- Smoke: thay test EOD-compute bằng test ingest+derive; giữ test index, PM, composition.
- Memory cập nhật KHI implement (CHỈ trên nhánh này, KHÔNG đụng memory chung của main): `mgmt-fee-accrual-decision`, `sdi-asset-sync-architecture`, `eod-pipeline-control`, `recompute-granularity-limits`.
