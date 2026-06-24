# SDI ⇄ Asset — bàn giao tính NAV/phí (BRD 2026-06-24)

> Trạng thái: **ĐÃ IMPLEMENT 2026-06-24** (P0→P5, build + all tests GREEN). Chi tiết: [SDI-asset-impl-plan.md](SDI-asset-impl-plan.md).
> **Nhánh `feat/brd-asset-nav-sync` — ĐỘC LẬP, KHÔNG merge vào `main`.** Main giữ luồng hiện tại (SDI tự tính NAV/phí).
> Liên quan: [SDI-asset-gap.md](SDI-asset-gap.md).

## 0. Lưu ý quan điểm (ghi lại để team cân nhắc)

Có quan ngại kiến trúc: **SDI là nơi phát sinh nghiệp vụ chính** → lẽ ra nên là nơi quản lý & tính toán số liệu, thay vì nhận đồng bộ NAV/phí từ Asset. "Dù hệ thống nào tính thì số liệu phải cân nhau" — nếu cân thì việc đổi nguồn không bắt buộc; nếu không cân thì đổi nguồn chỉ **dời** chỗ lệch. BRD vẫn yêu cầu đổi → tách nhánh này để thử, **không động luồng main đã ổn**.

## 0b. Quyết định đã CHỐT (2026-06-24)

| # | Quyết định | Chốt |
|---|---|---|
| 1 | `fee` Asset gửi dạng nào | **LŨY KẾ đến ngày** (cumulative). SDI lưu thẳng (khớp SMO); phí/ngày = hiệu 2 ngày. |
| 2 | Granularity | **Chỉ per-SI**. SDI **tự SUM lên master** cuối ngày + **tính lại unit/unit_price per-SI**. (Không cross-check Σ vs master-Asset vì master do SDI tự gộp.) |
| 3 | Index↔NAV giá BO | **Cùng giá BO close + cùng cutoff.** MỞ RỘNG: **MỌI hệ (BO/Asset/SDI) tham chiếu CÙNG 1 bộ giá đóng cửa** → không deviation giả. |
| 4 | Ngày gửi NAV | **Chỉ ngày GD**; T7/CN/lễ SDI **carry-forward**. |
| 5 | Originator cashflow | **SDI VẪN nhập cashflow** (giữ `T_SI_CASHFLOW_EVENT`) + Asset cũng gửi `cash_in/out` → **đối soát 2 nguồn**. Unit/UP tính theo cashflow SDI (authoritative); **reconcile phải PASS** để NAV(Asset) & unit(SDI) nhất quán (cùng dòng tiền). |

**Bổ sung — giá cuối ngày + thành phần tài sản (chốt):** Asset gửi **ĐỦ thành phần tài sản dạng SỐ TỔNG cấp SI** (`stock_value + pending + cash + div_cash`), **KHÔNG chi tiết từng mã** → SDI **không tự định giá EOD** (bỏ MTM cho EOD NAV), chỉ **lắp** NAV. **Mọi hệ (BO/Asset/SDI) tham chiếu CÙNG 1 bộ giá đóng cửa.**

**FO holdings (per-mã): TẠM GIỮ ingest.** Không phục vụ EOD NAV (lấy từ Asset), mà cho: composition `T_MASTER_HOLDING_BALANCE`, US3-click RS2 (delta holdings thực per-mã), alert drift/ngành, và **tính tài sản CẬN REAL-TIME (future)**. Thêm đối soát `Σ(FO holdings × giá BO) ≈ Asset stock_value`.

> Lưu ý scope PM BRD: core **US1–US5 KHÔNG cần per-mã** (chạy trên NAV/unit cấp SI/master). Per-mã chỉ nuôi US3-click RS2 + alert (alert: nguồn ngành `T_TICKER_INDUSTRY` thật chưa nạp).

## 1. Thay đổi BRD

1. **Phí quản lý không tính theo AUM trung bình** mà **theo từng ngày**.
2. **Asset là nguồn DUY NHẤT** của NAV, dòng tiền, phí — Asset đã tính hết để hiển thị SMO → **đồng bộ về SDI**.
3. SDI **bỏ quản lý chi tiết giao dịch phí**, **không tự định giá NAV** nữa; chỉ nhận **con số tổng per-SI** từ Asset.

Hệ quả: SDI chuyển từ *engine định giá* → **consumer + index engine + PM serve layer + validator**.

## 2. Nguyên tắc single-source

| Đại lượng | Nguồn sự thật | SDI làm gì |
|---|---|---|
| NAV per-SI | **Asset** (gửi) | lưu (`T_SI_NAV_BALANCE`/`_CURRENT`) |
| cash_in / cash_out per-SI | **Asset** (gửi) | lưu + derive PnL |
| phí (lũy kế/ngày) | **Asset** (gửi) | lưu, hiển thị |
| units / unit_price / TWR | **SDI derive** từ NAV+flow+history | tính (quy ước §4) |
| PnL / daily_return | **SDI derive** từ NAV+flow | tính |
| Index danh mục mẫu | **SDI** (giá BO × target weight) | tính (J12, giữ nguyên) |
| Composition holdings (alert/drift) | **FO holdings × giá BO** | tính (`T_MASTER_HOLDING_BALANCE`, giữ) |

Vì units/UP/PnL/TWR đều derive từ **NAV+flow của Asset** (chỉ dùng UP kỳ trước SDI tự lưu) → **không lệch nguồn**.

## 3. Data contract Asset → SDI (CHỐT 2026-06-24)

Mỗi `(business_date, si_account)` — **chỉ ngày GD** (QĐ4), **per-SI** (QĐ2):
```
stock_value    -- tổng tiền CP nắm giữ (Asset ĐÃ định giá; SDI KHÔNG MTM) -- SỐ TỔNG cấp SI, KHÔNG chi tiết từng mã
pending        -- tiền bán chờ về (T+) / receivables
cash           -- số dư tiền (Asset gửi — đã chốt)
div_cash       -- cổ tức tiền chờ/đã về (Asset gửi — đã chốt)
fee            -- phí LŨY KẾ đến ngày (QĐ1)
cash_in        -- nạp trong ngày (ĐỐI SOÁT với cashflow SDI tự nhập — QĐ5)
cash_out       -- rút trong ngày (ĐỐI SOÁT)
```
- **NAV = stock_value + cash + pending + div_cash − fee** — SDI **lắp**, không tự định giá.
- **GIÁ EOD THỐNG NHẤT (QĐ3 mở rộng):** Asset định giá `stock_value` bằng **đúng giá BO close** mà SDI dùng cho index → index ↔ NAV apples-to-apples.
- **Per-SI** (QĐ2): PM tool toàn bộ per-KH; SDI tự SUM lên master.
- **QĐ5:** SDI dùng `cash_in/cash_out` **của chính nó** (originator) để tính unit/UP; `cash_in/out` Asset gửi chỉ để **đối soát**. Reconcile lệch → ghi break, chặn publish (vì NAV Asset & unit SDI phải cùng dòng tiền).

## 4. Quy ước định giá đơn vị quỹ — **ĐÃ CHỐT (LOCKED)**

- **Khởi tạo `unit_price = 10.000`** (par) cho SI mới / ngày đầu.
- **Historic pricing**: dòng tiền trong ngày định giá theo **`unit_price` NGÀY TRƯỚC** (`UP_{t-1}`).
- Công thức (SDI tự tính, single-source):
```
Δunits   = (cash_in − cash_out) / UP_{t-1}
units_t  = units_{t-1} + Δunits
UP_t     = NAV_t / units_t
return_t = UP_t / UP_{t-1} − 1            (TWR per-SI/ngày)
PnL_t    = NAV_t − NAV_{t-1} + cash_out − cash_in
```
- "Ngày trước" = ngày **có NAV gần nhất** (qua T7/CN lấy phiên trước). Ngày đầu chưa có prior → `UP = 10.000`.
- ⚠️ **Asset/SMO PHẢI dùng CÙNG quy ước này** (init 10k + prior-day historic). Nếu Asset định giá flow theo `UP` ngày hiện tại (forward) → units/UP/TWR **lệch SMO đúng ngày có nạp/rút**. Đây là thỏa thuận **phương pháp**, không phải field — bỏ qua = vênh.

## 5. Vai trò SDI sau cắt

**GIỮ:**
- Index danh mục mẫu (J12 `SP_EOD_SI_INDEX`) — chỉ cần giá BO + target weight, độc lập NAV.
- PM serve layer (06_PM_API) — đọc NAV/UP/return/TE đã ingest+derive.
- TE accum (J12B) — đọc `daily_return` derive, `index daily_return` từ J12.
- **FO holdings ingest (per-mã) — TẠM GIỮ**: composition `T_MASTER_HOLDING_BALANCE`, US3-click RS2, alert drift/ngành, **near-realtime asset (future)**. KHÔNG dùng cho EOD NAV.
- **`T_SI_CASHFLOW_EVENT` (QĐ5): SDI VẪN là originator** nạp/rút → giữ; dùng tính unit/UP + đối soát với Asset.
- **SI_AGG (J11): SUM per-SI → master** (giờ SUM số ingest, không phải số tự tính).
- **Tính unit/unit_price/PnL/return per-SI** từ NAV(ingest) + cashflow(SDI) + UP history (§4).
- **Validator** đối soát NAV-bridge + cashflow 2 nguồn (§6).

**BỎ:**
- `SP_EOD_COMPUTE` J06 accrue, **J07 MTM cho EOD NAV** (EOD lấy `stock_value` từ Asset — nhưng GIỮ năng lực MTM holdings cho near-realtime future), J08 NAV-from-holdings (EOD chỉ lắp NAV từ Asset), J09/J10 derive unit/PnL nhưng nguồn = NAV ingest.
- `SP_INGEST_FEE_CHARGE`, `T_SI_INCOME_FEE` (chi tiết phí), `T_FEE_CONFIG`, `C_PAYABLE_FEE`.
- `T_SI_CASH_HIST` (interval cash cho tính NAV). **Holdings GIỮ** (FO ingest — xem GIỮ).
- `SP_EOD_RECOMPUTE_RANGE` (recompute NAV từ history) → thay bằng **re-ingest** (§7.D).

**ĐỔI THÀNH INGEST:**
- `T_SI_NAV_BALANCE` / `T_SI_NAV_CURRENT` ← `SP_INGEST_ASSET_NAV` (per-SI: NAV + cash_in + cash_out + fee; SDI derive units/UP/PnL/return rồi ghi).

## 6. Cơ chế giảm vênh: NAV-bridge reconcile (SDI làm validator)

**MỤC ĐÍCH KÉP (chốt):** reconcile vừa **chặn publish** khi lệch lớn, vừa **GHI LẠI độ lệch (giá trị diff per-SI/ngày) KỂ CẢ khi trong ngưỡng** → dùng để **đo mức vênh SDI-derive vs Asset theo thời gian**, đánh giá thực tế thiết kế 2-nguồn lệch tới đâu (đúng quan ngại §0). Lưu diff (không chỉ pass/fail) để báo cáo/giám sát.

3 check, ghi vào `T_EOD_RECON_BREAK` (mở rộng + cột lưu diff):
1. **NAV-bridge per-SI:** `NAV_t ≈ NAV_{t-1} + (cash_in − cash_out) + Δ_định_giá` — bắt Asset gửi thiếu/sai SI.
2. **Cashflow 2 nguồn (QĐ5):** `cash_in/out (SDI nhập)` vs `cash_in/out (Asset gửi)`. Lệch → unit/UP (SDI) và NAV (Asset) không cùng dòng tiền.
3. **Holdings (QĐ FO-giữ):** `Σ(FO holdings × giá BO)` vs Asset `stock_value`.
(Không check `Σ(SI)=master` vì master do SDI tự SUM — QĐ2.)

## 7. Điểm vênh còn lại + xử lý

- **A. Granularity**: bắt buộc per-SI (đã chốt ràng buộc §3).
- **B. Index (SDI) vs NAV (Asset) — so sánh chéo nguồn**: deviation PM = return KH (Asset-NAV) − return index (SDI). 🔶 Phải chốt index & NAV **cùng giá BO + cùng thời điểm chốt** để không sinh "deviation giả".
- **C. Timing/dependency**: thêm 1 hop — BO/FO → Asset tính → Asset gửi SDI. PM/TE chờ NAV Asset về; **Index chạy độc lập** (chỉ cần BO). Thêm nguồn `ASSET_NAV` vào `T_EOD_PIPELINE` (gate chờ trước khi serve/TE).
- **D. Sửa quá khứ → RE-INGEST, không recompute**: Asset sửa NAV ngày cũ → SDI nhận lại → tính lại units/UP/TWR/lũy kế **từ ngày sửa trở đi** (cần lưu **NAV+flow history per (date,si)** — đã có `T_SI_NAV_BALANCE`). `SP_EOD_RECOMPUTE_RANGE` (reconstruct NAV) thành obsolete.
- **E. Ngày dương lịch vs ngày GD cho phí**: nếu Asset gửi `fee` sẵn thì SDI khỏi lo day-count; nếu SDI phải nội suy phí ngày nghỉ → cần quy ước carry-forward khớp Asset.
- **F. Làm tròn**: nếu `fee` per-day, giữ precision cao, chỉ round khi hiển thị.

## 8. Trạng thái quyết định

**ĐÃ CHỐT:** QĐ1–5 (§0b) · cash/div = Asset gửi đủ thành phần số tổng SI · Asset không per-mã · **FO holdings TẠM GIỮ** (composition + US3-RS2 + alert + near-realtime future).

**CÒN LẠI (chi tiết implement, quyết khi code):**
🔶 **(b) Thiết kế reconcile cashflow 2 nguồn (QĐ5)**: ngưỡng lệch, xử lý khi lệch (chặn publish? cảnh báo?), thời điểm đối soát. SDI-cashflow authoritative cho unit/UP; Asset-cash_in/out để bắt lệch.
🔶 **(c) Reconcile holdings**: `Σ(FO×giá BO)` vs Asset `stock_value` — ngưỡng + xử lý.
🔶 **(d) Near-realtime asset valuation (future)**: giữ năng lực MTM holdings cho intraday — phạm vi/định nghĩa để sau.

## 9. Tác động migration (high-level, sau khi chốt §8)

- Bỏ phần lớn `SP_EOD_COMPUTE`/`_CORE`, fee subsystem, recompute NAV.
- Thêm `SP_INGEST_ASSET_NAV` + derive units/UP.
- Pipeline: thêm nguồn `ASSET_NAV` + tách rõ luồng Index (BO) vs luồng NAV-consume (Asset).
- Smoke: thay test EOD-compute bằng test ingest+derive; giữ test index, PM, composition.
- Memory cập nhật KHI implement (CHỈ trên nhánh này, KHÔNG đụng memory chung của main): `mgmt-fee-accrual-decision`, `sdi-asset-sync-architecture`, `eod-pipeline-control`, `recompute-granularity-limits`.
