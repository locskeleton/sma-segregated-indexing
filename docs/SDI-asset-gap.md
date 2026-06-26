# SDI ↔ Asset — Đối chiếu dữ liệu (BRD 2026-06-22)

> **BRD mới (chốt 2026-06-22):** BO, FO, Market đẩy dữ liệu **THẲNG sang Asset**; **Asset tự tính**.
> SDI **KHÔNG còn đồng bộ tài sản/NAV/perf sang Asset**, **GIỮ NGUYÊN** engine + read API (FR-01..06, PM)
> phục vụ **giao diện riêng của SDI**. Việc Asset xử lý BO/FO để không lệch = **việc của Asset**.
> Tài liệu này = **đối chiếu nhanh: ai cấp gì + điểm cần reconcile** giữa 2 hệ.
>
> **Cập nhật quan trọng (chốt 2026-06-26):**
> - **[THIN-LAYER] Asset GỬI CẢ `daily_return` (TWR) về SDI** (cùng `aum`/`cash`/`cash_in/out`). SDI **KHÔNG tự tính** unit/UP/PnL/TWR — chỉ LƯU + SERVE (compound `daily_return`). ⇒ R3 (TWR methodology 2 hệ phải khớp) **GỠ**: không còn 2 hệ derive độc lập. Asset KHÔNG gửi `stock_value`.
> - **BO KHÔNG tính/gửi phí QL lũy kế (accrued).** Asset chạy model **REALIZED**: phí QL chỉ giảm tài sản khi BO **cắt THẬT** (qua cash). ⇒ **AUM = NAV (gross = net, không tách payable)**; SDI cũng đã **GỠ accrual** (cột `C_PAYABLE_FEE` + `C_FEE_ACCUM` + J06 + `T_FEE_CONFIG` + `SP_INGEST_FEE_CHARGE`) để khớp Asset. Xem [[aum-weighted-return-twr-decision]].
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
| **BO** | event CẮT phí (realized → giảm cash, phản ánh qua FO) | KHÔNG gửi số accrued — phí chỉ effect khi cắt THẬT |
| **Market** | giá EOD (ref/close/ex-rights), benchmark (VNINDEX) | universe mã |
| **SDI** | **Master Index (danh mục mẫu)** qua `SP_GET_ASSET_INDEX_SNAPSHOT` — đẩy riêng khi BO price-ready | CHỈ còn luồng index này SDI→Asset |

---

## 2. Asset tính được (đủ data)

| Đại lượng | Nguồn |
|---|---|
| Tài sản = `Σ(holdings×close) + cash` (cash gồm pending/div) | FO + Market |
| **NAV = Tài sản** (phí QL đã trừ khi BO cắt thật; KHÔNG có accrued payable) ⇒ AUM = NAV | FO + Market |
| **Master Index (danh mục mẫu)** | **SDI đẩy riêng** (price-ready) |
| Benchmark | Market |
| **`daily_return` (TWR per-KH/ngày)** | **[thin-layer]** Asset TỰ TÍNH (khử dòng tiền) rồi **GỬI về SDI** — SDI LƯU, không derive |

---

## 3. Điểm CẦN RECONCILE (để 2 hệ không lệch)

> Không còn "gap thiếu data" — nhưng có **điểm dễ lệch** khi mỗi hệ tự tính. Khi đối chiếu, soi đúng các điểm này:

| # | Điểm | Rủi ro lệch | Đối chiếu |
|---|---|---|---|
| ~~R1~~ | ~~Payable (phí QL accrued)~~ — **ĐÃ GỠ** | Không còn: BO không accrue, SDI không accrue, Asset NAV = realized ⇒ KHÔNG có payable 2 bên để lệch. AUM = NAV. [[aum-weighted-return-twr-decision]] | — (gỡ) |
| **R2** | **Master Index** | SDI đẩy → cùng nguồn, nhưng nếu Asset cache/replay sai mode | dùng đúng `SP_GET_ASSET_INDEX_SNAPSHOT` payload (index PR + benchmark) |
| ~~R3~~ | ~~**Unit/TWR/return** — Asset tự tính vs SDI~~ **[thin-layer] GỠ điểm lệch** | **Asset GỬI `daily_return` (TWR) thẳng cho SDI** ⇒ KHÔNG còn 2 hệ tự tính TWR độc lập (trước: cả Asset & SDI đều derive `UP=NAV/Unit; return=UP_t/UP_{t-1}−1` → phải khớp methodology). SDI chỉ compound `daily_return` Asset gửi. Còn lại = `CASHFLOW_MISMATCH` (cash_in/out SDI vs Asset). | cashflow 2 nguồn khớp ⇒ `daily_return` & flow cùng cơ sở |

---

## 4. Tóm tắt

| Đại lượng ở Asset | Có đủ data? | Nguồn / lưu ý |
|---|---|---|
| Tài sản gộp, composition, benchmark | ✅ | FO + Market |
| ~~Payable (phí QL accrued)~~ | — | **ĐÃ GỠ** — BO không accrue; phí chỉ realized khi cắt thật |
| NAV (= AUM) | ✅ | tài sản (stock + cash); phí QL đã trừ khi cắt thật, KHÔNG tách payable |
| Master index | ✅ | **SDI đẩy riêng** (price-ready) |
| `daily_return` (TWR)/PnL | ✅ | **[thin-layer]** Asset tự tính + **GỬI về SDI** (SDI không derive) |

**Kết luận:** BO/FO/Market cấp đủ data → **Asset tự tính** NAV (= AUM, model realized không accrue) + `daily_return` (TWR) rồi **gửi cả về SDI** (thin-layer). SDI chỉ còn 1 luồng sang Asset = **index snapshot**. Khi reconcile, soi **CASHFLOW_MISMATCH** (cash_in/out 2 nguồn) — *(R3 TWR-methodology đã GỠ: Asset gửi `daily_return`, SDI không tự derive; R1 payable đã gỡ — 2 hệ không accrue).*

Liên quan: [SDI-eod-data-exchange.md](./SDI-eod-data-exchange.md), [SDI-spec.md](./SDI-spec.md), memory [[aum-weighted-return-twr-decision]] · [[master-index-methodology]] · [[sdi-asset-sync-architecture]].
