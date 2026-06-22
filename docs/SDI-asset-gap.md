# SDI ↔ Asset — Đối chiếu dữ liệu (BRD 2026-06-22)

> **BRD mới (chốt 2026-06-22):** BO, FO, Market đẩy dữ liệu **THẲNG sang Asset**; **Asset tự tính**.
> SDI **KHÔNG còn đồng bộ tài sản/NAV/perf sang Asset**, **GIỮ NGUYÊN** engine + read API (FR-01..06, PM)
> phục vụ **giao diện riêng của SDI**. Việc Asset xử lý BO/FO để không lệch = **việc của Asset**.
> Tài liệu này = **đối chiếu nhanh: ai cấp gì + điểm cần reconcile** giữa 2 hệ.
>
> **Cập nhật quan trọng (bổ sung từ user):**
> - **BO trả phí QL lũy kế theo ngày (accrued mgmt fee/ngày) THẲNG cho Asset** → Asset có payable, tính được NAV ròng.
> - **Index snapshot: SDI VẪN đẩy riêng** (khi nhận tín hiệu price-ready từ BO) → Asset có master index. Producer
>   `SP_GET_ASSET_INDEX_SNAPSHOT` (db/05_API.sql) **GIỮ**.
> - **Đã gỡ** (db/05_API.sql): `SP_GET_ASSET_SNAPSHOT` (per-SI tài sản), `SP_GET_ASSET_MASTER_SNAPSHOT` (master NAV/perf).
>
> ⇒ **Asset ĐỦ thông tin** để tự tính. Xem [[sdi-asset-sync-architecture]] (memory).

---

## 1. Ai cấp gì cho Asset

| Nguồn | Cấp cho Asset | Ghi chú |
|---|---|---|
| **FO** | holdings, tiền 3 khoản (cash/pending/div), cổ tức + phí lưu ký, cashflow, **model_weight** (target weight) | đẩy thẳng |
| **BO** | event CẮT phí + **phí QL LŨY KẾ theo ngày (accrued)** | accrued/ngày là MỚI — đóng gap payable |
| **Market** | giá EOD (ref/close/ex-rights), benchmark (VNINDEX) | universe mã |
| **SDI** | **Master Index (danh mục mẫu)** qua `SP_GET_ASSET_INDEX_SNAPSHOT` — đẩy riêng khi BO price-ready | CHỈ còn luồng index này SDI→Asset |

---

## 2. Asset tính được (đủ data)

| Đại lượng | Nguồn |
|---|---|
| Tài sản GỘP = `Σ(holdings×close) + cash + pending + div` | FO + Market |
| **Payable (phí QL accrued)** | **BO (lũy kế/ngày)** |
| **NAV ròng = gross − payable** | FO + Market + BO |
| **Master Index (danh mục mẫu)** | **SDI đẩy riêng** (price-ready) |
| Benchmark | Market |
| **Unit/UnitPrice/TWR/return/PnL** | Asset TỰ TÍNH từ NAV series + cashflow (FO) |

---

## 3. Điểm CẦN RECONCILE (để 2 hệ không lệch)

> Không còn "gap thiếu data" — nhưng có **điểm dễ lệch** khi mỗi hệ tự tính. Khi đối chiếu, soi đúng các điểm này:

| # | Điểm | Rủi ro lệch | Đối chiếu |
|---|---|---|---|
| **R1** | **Payable (phí QL accrued)** — BO accrue vs SDI accrue | Khác cách accrue (ngày dương lịch? day_count? AUM_gross gồm pending/div?) → NAV ròng lệch. SDI: `AUM_gross × ngày-dương-lịch × Σ(rate/day_count)` theo `T_FEE_CONFIG`, net-off cắt. [[mgmt-fee-accrual-decision]] | rate + day_count + base AUM + quy ước ngày phải KHỚP giữa BO ↔ SDI |
| **R2** | **Master Index** | SDI đẩy → cùng nguồn, nhưng nếu Asset cache/replay sai mode | dùng đúng `SP_GET_ASSET_INDEX_SNAPSHOT` payload (index PR + benchmark) |
| **R3** | **Unit/TWR/return** — Asset tự tính vs SDI | Methodology phải khớp: `T0=10.000`, `ΔUnit=CF_net/UP_prev`, `UP=NAV/Unit`, return=`UP_t/UP_{t-1}−1`, PnL=`NAV−NAV_prev+CF_out−CF_in`. [[master-index-methodology]] cho index | seed + công thức TWR phải khớp SDI; NAV ròng (R1) khớp trước |

---

## 4. Tóm tắt

| Đại lượng ở Asset | Có đủ data? | Nguồn / lưu ý |
|---|---|---|
| Tài sản gộp, composition, benchmark | ✅ | FO + Market |
| Payable (phí QL accrued) | ✅ | **BO lũy kế/ngày** (reconcile R1) |
| NAV ròng | ✅ | gross − payable(BO) |
| Master index | ✅ | **SDI đẩy riêng** (price-ready) |
| Unit/TWR/return/PnL | ✅ data | Asset tự tính (reconcile R3 methodology) |

**Kết luận:** với BO cấp phí QL accrued + SDI giữ luồng index + FO/Market cấp phần còn lại → **Asset đủ thông tin** tự tính NAV ròng/perf. SDI chỉ còn 1 luồng sang Asset = **index snapshot**. Khi reconcile, soi **R1 (payable), R3 (TWR methodology)** — 2 chỗ dễ lệch nhất.

Liên quan: [SDI-eod-data-exchange.md](./SDI-eod-data-exchange.md), [SDI-spec.md](./SDI-spec.md), memory [[mgmt-fee-accrual-decision]] · [[master-index-methodology]] · [[sdi-asset-sync-architecture]].
