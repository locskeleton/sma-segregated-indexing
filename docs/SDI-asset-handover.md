# SDI ⇄ Asset — bàn giao tính NAV/phí (BRD 2026-06-24)

> Trạng thái: **DESIGN — đang chốt**. Một số quyết định CÒN MỞ (đánh dấu 🔶). Chưa implement.
> **Nhánh `feat/brd-asset-nav-sync` — ĐỘC LẬP, KHÔNG merge vào `main`.** Main giữ luồng hiện tại (SDI tự tính NAV/phí).
> Liên quan: [SDI-asset-gap.md](SDI-asset-gap.md).

## 0. Lưu ý quan điểm (ghi lại để team cân nhắc)

Có quan ngại kiến trúc: **SDI là nơi phát sinh nghiệp vụ chính** → lẽ ra nên là nơi quản lý & tính toán số liệu, thay vì nhận đồng bộ NAV/phí từ Asset. "Dù hệ thống nào tính thì số liệu phải cân nhau" — nếu cân thì việc đổi nguồn không bắt buộc; nếu không cân thì đổi nguồn chỉ **dời** chỗ lệch. BRD vẫn yêu cầu đổi → tách nhánh này để thử, **không động luồng main đã ổn**.

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

## 3. Data contract Asset → SDI

Mỗi `(business_date, si_account)`:
```
NAV            -- giá trị tài sản ròng cuối ngày (đã trừ phí)
cash_in        -- nạp trong ngày
cash_out       -- rút trong ngày
fee            -- 🔶 lũy kế HAY phát sinh/ngày (chốt §8)
```
🔶 **Master roll-up**: Asset có gửi kèm bản tổng cấp master để SDI đối soát `Σ(SI)=master` không? (chốt §8)

Ràng buộc cứng:
- **Per-SI** (theo `C_SI_ACCOUNT`) — PM tool toàn bộ là per-KH (top-N, phân phối PnL, deviation, TE). Per-master là KHÔNG đủ.
- **NAV gross/net**: NAV gửi về là **net (đã trừ phí)**; `fee` gửi riêng để hiển thị. (Tránh vòng lặp: Asset không cần payable của SDI.)
- Gửi đủ **mọi ngày dương lịch** hay chỉ ngày GD? → nếu chỉ ngày GD, SDI carry-forward; phí theo ngày dương lịch cần làm rõ (§7.E).

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
- Composition `T_MASTER_HOLDING_BALANCE` (alert drift/ngành) — cần FO holdings.
- **Validator** đối soát NAV-bridge (§6).

**BỎ:**
- `SP_EOD_COMPUTE` J06 accrue, J07 MTM, J08 NAV, J09 PnL, J10 unit (tự tính NAV).
- `SP_INGEST_FEE_CHARGE`, `T_SI_INCOME_FEE` (chi tiết phí), `T_FEE_CONFIG`, `C_PAYABLE_FEE`.
- `T_SI_CASHFLOW_EVENT` (SDI là nguồn) — cashflow về Asset.
- `T_SI_CASH_HIST` (interval cash) — không còn tự tính NAV.
- `SP_EOD_RECOMPUTE_RANGE` (recompute NAV từ history) → thay bằng **re-ingest** (§7.D).

**ĐỔI THÀNH INGEST:**
- `T_SI_NAV_BALANCE` / `T_SI_NAV_CURRENT` ← `SP_INGEST_ASSET_NAV` (per-SI: NAV + cash_in + cash_out + fee; SDI derive units/UP/PnL/return rồi ghi).

## 6. Cơ chế giảm vênh: NAV-bridge reconcile (SDI làm validator)

Có NAV + cashflow → SDI tự kiểm tra dữ liệu Asset:
```
NAV_t  ≈  NAV_{t-1} + (cash_in − cash_out) + Δ_định_giá
```
Lệch quá ngưỡng → ghi break (mở rộng `T_EOD_RECON_BREAK`), chặn publish — như J13 hiện tại nhưng đối tượng là **dữ liệu Asset gửi**. Nếu Asset gửi cả master roll-up → thêm check `Σ(SI)=master`.

## 7. Điểm vênh còn lại + xử lý

- **A. Granularity**: bắt buộc per-SI (đã chốt ràng buộc §3).
- **B. Index (SDI) vs NAV (Asset) — so sánh chéo nguồn**: deviation PM = return KH (Asset-NAV) − return index (SDI). 🔶 Phải chốt index & NAV **cùng giá BO + cùng thời điểm chốt** để không sinh "deviation giả".
- **C. Timing/dependency**: thêm 1 hop — BO/FO → Asset tính → Asset gửi SDI. PM/TE chờ NAV Asset về; **Index chạy độc lập** (chỉ cần BO). Thêm nguồn `ASSET_NAV` vào `T_EOD_PIPELINE` (gate chờ trước khi serve/TE).
- **D. Sửa quá khứ → RE-INGEST, không recompute**: Asset sửa NAV ngày cũ → SDI nhận lại → tính lại units/UP/TWR/lũy kế **từ ngày sửa trở đi** (cần lưu **NAV+flow history per (date,si)** — đã có `T_SI_NAV_BALANCE`). `SP_EOD_RECOMPUTE_RANGE` (reconstruct NAV) thành obsolete.
- **E. Ngày dương lịch vs ngày GD cho phí**: nếu Asset gửi `fee` sẵn thì SDI khỏi lo day-count; nếu SDI phải nội suy phí ngày nghỉ → cần quy ước carry-forward khớp Asset.
- **F. Làm tròn**: nếu `fee` per-day, giữ precision cao, chỉ round khi hiển thị.

## 8. 🔶 QUYẾT ĐỊNH CÒN MỞ (cần chốt trước khi code)

1. **`fee`**: Asset gửi **lũy kế** hay **phát sinh/ngày**? (cho cột "pql lũy kế").
2. **Master roll-up**: Asset gửi kèm tổng cấp master (để SDI đối soát `Σ(SI)=master`) hay chỉ per-SI?
3. **Index ↔ NAV**: thống nhất **giá BO nào + thời điểm chốt** giữa index (SDI) và NAV (Asset) để deviation PM chuẩn.
4. **Ngày gửi NAV**: chỉ ngày GD (SDI carry-forward) hay mọi ngày dương lịch?
5. **Cashflow gốc**: ai là originator nghiệp vụ (Asset/BO/banking) — xác nhận SDI thôi vai nguồn.

## 9. Tác động migration (high-level, sau khi chốt §8)

- Bỏ phần lớn `SP_EOD_COMPUTE`/`_CORE`, fee subsystem, recompute NAV.
- Thêm `SP_INGEST_ASSET_NAV` + derive units/UP.
- Pipeline: thêm nguồn `ASSET_NAV` + tách rõ luồng Index (BO) vs luồng NAV-consume (Asset).
- Smoke: thay test EOD-compute bằng test ingest+derive; giữ test index, PM, composition.
- Memory cập nhật KHI implement (CHỈ trên nhánh này, KHÔNG đụng memory chung của main): `mgmt-fee-accrual-decision`, `sdi-asset-sync-architecture`, `eod-pipeline-control`, `recompute-granularity-limits`.
