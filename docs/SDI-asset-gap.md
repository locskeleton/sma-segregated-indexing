# SDI ↔ Asset — Gap đối chiếu dữ liệu (BRD 2026-06-22)

> **BRD mới (chốt 2026-06-22):** BO, FO (và Market data) **đẩy dữ liệu THẲNG sang Asset**; **Asset tự tính**.
> SDI **KHÔNG còn đồng bộ (tài sản/perf) sang Asset** nữa. SDI **giữ nguyên engine + read API (FR-01..06, PM)**
> phục vụ **giao diện riêng của SDI** (vẫn hiển thị NAV/index/perf). Việc Asset làm sao với BO/FO để **không lệch**
> hoặc lấy số gộp là **việc của Asset — SDI không quan tâm**. Tài liệu này chỉ để **đối chiếu nhanh GAP** giữa 2 hệ.
>
> Đã gỡ khỏi SDI: `SP_GET_ASSET_SNAPSHOT`, `SP_GET_ASSET_MASTER_SNAPSHOT`, `SP_GET_ASSET_INDEX_SNAPSHOT`
> (3 producer Kafka SDI→Asset). Xem [[sdi-asset-sync-architecture]] (memory, đã đánh dấu superseded).

---

## 1. Ai sở hữu dữ liệu (nguồn sự thật) — đẩy thẳng Asset

| Nguồn | Đẩy sang Asset | Grain |
|---|---|---|
| **FO** | holdings (KH×master×mã), **tiền 3 khoản** (cash mặt / pending tiền bán chờ về / div cổ tức tiền), cổ tức + phí lưu ký, cashflow (INITIAL/TOPUP/SIP/INTEREST_IN/WITHDRAW), **model_weight** (target weight danh mục mẫu) | per-KH / per-mã |
| **BO** | **event CẮT phí** (phí QL/thuế/perf — 1 cục/tháng): `{si_account, amount, charge_date, fee_type?}` | sparse, chỉ khi cắt |
| **Market** | giá EOD (`ref_price`/`close_price`/`is_ex_rights`), benchmark (VNINDEX…) | universe mã |

---

## 2. Asset TÍNH ĐƯỢC (đủ data từ BO+FO+Market)

| Đại lượng | Công thức | Nguồn |
|---|---|---|
| **Tài sản GỘP (gross)** | `Σ(holdings × close_price) + cash + pending + div` | FO (holdings/tiền) + Market (giá) |
| Stock value (MTM) | `Σ qty × close` | FO + Market |
| Composition / tỷ trọng holdings | từ holdings + giá | FO + Market |
| Benchmark value | trực tiếp | Market |
| Biến động holdings/ngày | `qty(D) − qty(D-1)` | FO (snapshot 2 ngày) |

---

## 3. ❌ GAP — Asset KHÔNG tính được nếu chỉ có BO+FO+Market

> Đây là 3 thứ **SDI-unique**. SDI **không đẩy** sang Asset nữa ⇒ nếu Asset muốn có, **Asset phải tự dựng** (cần SDI giao config/methodology) — **trách nhiệm của Asset**. SDI vẫn tính các thứ này cho UI riêng của SDI.

### GAP 1 — Payable (phí ACCRUE) → NAV RÒNG  *(DATA gap, nặng nhất)*
- **Thiếu gì:** phần **accrue phí hằng ngày**. BO **chỉ gửi event CẮT**, KHÔNG gửi phần lũy kế chưa cắt.
- **SDI tính:** `payable += AUM_gross × (số NGÀY DƯƠNG LỊCH kể từ EOD trước) × Σ(C_RATE/C_DAY_COUNT)` cho mỗi loại phí `C_FEE_GROUP='PAYABLE' AND C_RATE>0` trong **`T_FEE_CONFIG`** (catalog SDI sở hữu); net-off khi BO cắt. Hiện 1 loại accrue = `MGMT_FEE`. Xem [[mgmt-fee-accrual-decision]].
- **Asset cần để bù:** `T_FEE_CONFIG` (rate/day_count mỗi loại) + thuật toán accrue ngày-dương-lịch + net-off cắt (dedup `source_event_id`).
- **Nếu không bù:** Asset chỉ ra **NAV GỘP**; **NAV ròng / UnitPrice / return SAI lệch đúng bằng phí tích lũy**.

### GAP 2 — Master Index (danh mục mẫu)  *(LOGIC gap)*
- **Data CÓ** (target weight FO + giá Market) nhưng **methodology riêng SDI**: daily-rebalanced (`P_ref = ref_price` đầu phiên, KHÔNG tra ngày trước), factor `= Σ wᵢ × close/ref`, xử lý **ex-rights** qua `ref_price`, base = 1000. Xem [[master-index-methodology]].
- **Asset cần để bù:** code lại Y HỆT methodology (rủi ro lệch nếu sai chi tiết ref/ex-rights/rebalance).

### GAP 3 — Unit / UnitPrice / TWR (perf)  *(LOGIC + seed gap, phụ thuộc GAP 1)*
- **SDI tính:** `T0 unit price = 10.000`; `ΔUnit = CF_net / UnitPrice_prev` (unit chỉ đổi do nạp/rút, KHÔNG do giá); `UnitPrice = NAV/Unit`; `PnL = NAV − NAV_prev + CF_out − CF_in`; daily return = `UP_t/UP_{t-1} − 1`. TWR/MWR, TE/deviation derive từ chuỗi này.
- **Asset cần để bù:** NAV ròng (→ phụ thuộc GAP 1) + cashflow (FO có) + **seed unit T0 + chuỗi unit đệ quy** + methodology TWR.
- **Nếu không bù:** không có hiệu suất chuẩn TWR ở Asset; PnL/return lệch theo GAP 1.

---

## 4. Tóm tắt đối chiếu

| Đại lượng ở Asset | Tự tính được? | Phụ thuộc |
|---|---|---|
| Tài sản gộp, composition, benchmark | ✅ | FO + Market |
| **NAV ròng phí** | ❌ | GAP 1 (T_FEE_CONFIG + accrue) |
| **Master index (danh mục mẫu)** | ❌ | GAP 2 (methodology) |
| **Unit / UnitPrice / TWR / return / PnL** | ❌ | GAP 3 (+ GAP 1) |
| **TE / deviation / PM analytics** | ❌ | derive từ GAP 1+2+3 |

**Kết luận:** Asset đủ cho **tài sản GỘP**; **thiếu NAV ròng / index / hiệu suất** (3 sản phẩm lõi SDI). Muốn có ở Asset → Asset tự dựng (SDI giao config + spec). SDI vẫn tính đủ cho UI SDI qua read API. Khi reconcile 2 hệ, **đối chiếu đúng 3 GAP trên** (nhất là GAP 1 — payable, dễ làm NAV lệch nhất).

Liên quan: [SDI-eod-data-exchange.md](./SDI-eod-data-exchange.md) (hợp đồng dữ liệu), [SDI-spec.md](./SDI-spec.md) (công thức J06–J12), memory [[mgmt-fee-accrual-decision]] · [[master-index-methodology]] · [[sdi-asset-sync-architecture]].
