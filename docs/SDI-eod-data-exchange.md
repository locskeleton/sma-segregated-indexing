# SDI — Hợp đồng trao đổi dữ liệu EOD: FO ↔ SDI ↔ Asset

Tài liệu tổng hợp **dữ liệu cuối ngày (EOD)** các hệ thống cần trao đổi: ai gửi gì cho ai, payload cụ thể, và **báo cáo định lượng** theo 3 kịch bản small / medium / large.

> Liên quan: [SDI-spec.md](./SDI-spec.md) (công thức, job EOD J0–J16), [SDI-db-architecture.md](./SDI-db-architecture.md) (kiến trúc DB, roll-forward, set-based).

---

## 1. Các hệ thống & vai trò

| Hệ thống | Vai trò trong luồng EOD |
|---|---|
| **FO** (Front Office) | Tính tỷ trọng danh mục mẫu (model_weight); **đặt & khớp lệnh MP trực tiếp trên TK từng KH**; sở hữu tiền (trừ phí QL + thuế GD vào cash). **Nguồn sự thật về holdings + cash + cổ tức/phí + cashflow.** |
| **Market data** | Cấp giá EOD, corporate action, chỉ số benchmark (VN-Index…). (Nguồn riêng, không phải FO.) |
| **SDI** | Nhận holdings (FO nạp THẲNG vào current) + cash từ FO → tính NAV, Unit/Unit Price, PnL, TWR, MWR, SI Index. KHÔNG quản lý từng lệnh khớp, KHÔNG accrue phí. → đẩy kết quả sang Asset. |
| **Asset** | Nhận current snapshot + chuỗi SI từ SDI; phục vụ **SMO** đọc/hiển thị (read-only, không tính). |
| **SMO** | Tầng hiển thị, đọc qua Asset. |

**Nguyên tắc nền:** FO đồng bộ **snapshot overwrite** mỗi EOD (không event-source từng lệnh). `NAV = stock_value + FO cash`; FO cash **đã NET** phí QL + thuế GD + SIP → SDI tuyệt đối không trừ lại (tránh double-count).

---

## 2. Sơ đồ luồng EOD

```
        ┌─────────────────────── trong ngày ───────────────────────┐
SDI ──(1) rebalance_request (trigger REBALANCE/DEPLOY/REDEEM) ──────▶ FO
        └────────────────────────────────────────────────────────────┘

        ┌──────────────────────── cuối ngày (EOD) ─────────────────────┐
FO  ──(2) model_weight ─────────────────────────────────────────────▶ SDI
FO  ──(3) holdings snapshot (KH×SI×mã)  ◀── volume chính ───────────▶ SDI
FO  ──(4) cash snapshot (KH×SI) ───────────────────────────────────▶ SDI
FO  ──(5) cổ tức/phí per-KH (sparse) ──────────────────────────────▶ SDI
FO  ──(6) cashflow nạp/rút/SIP (sparse) ───────────────────────────▶ SDI
Mkt ──(7) giá EOD + corporate action + benchmark ──────────────────▶ SDI
        │
        │  SDI: J1 STAGE (FO→current) → J1b SYNC_FO cash → J7 MTM → J8 NAV → J9 PnL
        │       → J10 Unit → J11 SI agg → J12 Index → J13 RECONCILE (cổng)
        │       → J14 snapshot → J15 PUBLISH
        ▼
SDI ──(8) current snapshot + SI series ────────────────────────────▶ Asset ──▶ SMO
SDI ──(9) (API pull) customer NAV/holdings lịch sử theo yêu cầu ◀───── Asset
        └────────────────────────────────────────────────────────────┘
```

Thứ tự: **(1) trong ngày** (SDI kích FO rebalance) → **(2–7) FO/Market đẩy EOD** → SDI tính + **J13 reconcile là cổng** (break > ngưỡng ⇒ chặn publish) → **(8) SDI push Asset**. (9) Asset/SMO đọc lịch sử qua API (pull), không nhận bulk.

---

## 3. Chi tiết từng luồng (payload cụ thể)

### A. SDI → FO

| # | Luồng | Bảng/payload | Trường | Tần suất |
|---|---|---|---|---|
| 1 | **Rebalance trigger** | `sdi_rebalance_request` | request_id, si_code, business_date, type[REBALANCE\|DEPLOY\|REDEEM], status | **SPARSE** — chỉ khi cần tái cân bằng/giải ngân/rút. KHÔNG chứa weights (FO tự tính). |

### B. FO → SDI (feed EOD)

| # | Luồng | Bảng/payload | Trường | Tính chất |
|---|---|---|---|---|
| 2 | **Model weight** | `sdi_master_portfolio_ticker` | si_code, effective_date, ticker, target_weight (Σ=100%) | Version theo effective_date; **chỉ đẩy khi đổi** rổ. |
| 3 | **Holdings snapshot** | `sdi_indexing_portfolio_ticker` (**current**) | cust_code, si_code, ticker, quantity, avg_cost | **DENSE — toàn bộ TK mỗi EOD**, FO **nạp THẲNG current** (overwrite), volume chính. **J14b droppable** DIFF current → `sdi_customer_holding_hist` (interval, full history, no-dup) — EOD core không phụ thuộc. |
| 4 | **Cash snapshot** | `sdi_fo_cash_sync` (feed @d) | business_date, cust_code, si_code, cash | **DENSE** — available cash đã NET phí/thuế/SIP. Nguồn tiền DUY NHẤT (transient feed); J14b DIFF state.cash → `sdi_customer_cash_hist` (interval, full history, no-dup). |
| 5 | **Cổ tức + phí** | `sdi_customer_fee_income` | business_date, cust_code, si_code, type[DIVIDEND\|CUSTODY_FEE\|MGMT_FEE], ticker, amount | **SPARSE** — chỉ ngày có sự kiện. Cho báo cáo FR-06; KHÔNG ảnh hưởng NAV. |
| 6 | **Cashflow** | `sdi_cashflow_event` | cust_code, si_code, business_date, event_type[INITIAL\|TOPUP\|SIP\|INTEREST_IN\|WITHDRAW], amount | **SPARSE** — chỉ KH có nạp/rút/SIP. Dùng cho CF_t (PnL/unit), KHÔNG cộng lại cash. |

### C. Market data → SDI (feed EOD)

| # | Luồng | Bảng/payload | Trường | Tính chất |
|---|---|---|---|---|
| 7a | **Giá EOD** | `sdi_price_daily` | ticker, business_date, close_price, adjusted_ref_price | Theo **universe mã** (không theo KH). |
| 7b | **Corporate action** | `sdi_corporate_action` | ticker, ex_date, ca_type, ratio, cash_div_per_share, adjusted_ref_price | **SPARSE** — chỉ ex-date. Chỉ dùng J12 index. |
| 7c | **Benchmark** | `sdi_benchmark_daily` | benchmark_code (VNINDEX…), business_date, index_value | 1 dòng/benchmark/ngày. |

### D. SDI → Asset (J15 PUBLISH, sau reconcile)

| # | Luồng | Bảng/payload | Trường | Tính chất |
|---|---|---|---|---|
| 8a | **Current snapshot KH** | `sdi_customer_nav_current` | cust_code, si_code, unit, cash, last_nav, last_unit_price, status, last_business_date | Push **current/delta** (1 dòng/tiểu khoản). |
| 8b | **Current snapshot SI** | `sdi_si_nav_current` | si_code, cash, stock_value, total_asset, last_nav, unit, last_unit_price | Push current toàn quỹ (overview/AUM). |
| 8c | **SI series ngày** | `sdi_si_nav_balance`, `sdi_si_index_daily` | nav/unit/up/pnl/return + index_value (PR) | Append dòng SI của ngày @d (nhỏ). |
| 9 | **Lịch sử KH (API pull)** | `sdi_customer_nav_balance`, `sdi_si_holding_balance`, `sdi_customer_fee_income` | NAV/UP/return chart, holdings top20, cổ tức/phí | Asset/SMO **đọc qua API** (`usp_get_*`) on-demand — **KHÔNG** push bulk lịch sử. |

> **Điểm mấu chốt:** FO→SDI nặng (per-mã, dense, nạp THẲNG current); SDI→Asset nhẹ (per-tiểu-khoản current). Lịch sử dài hạn = `customer_nav_balance` (~2,5 tỷ dòng) SDI giữ + serve API. Holdings/cash history = **interval (SCD-2) full history, KHÔNG trùng lặp** (holding bất biến = 1 dòng) qua J14b droppable — không trong EOD core.

---

## 4. Báo cáo định lượng (small / medium / large)

**Tham số kịch bản** (theo `db/bench.ps1`):

| Kịch bản | KH | SI/KH | Mã/SI | Tiểu khoản (KH×SI) | **Holdings (KH×SI×mã)** |
|---|---|---|---|---|---|
| small | 1.000 | 3 | 20 | 3.000 | **60.000** |
| medium | 10.000 | 5 | 25 | 50.000 | **1.250.000** |
| large | 50.000 | 5 | 25 | 250.000 | **6.250.000** |
| *(prod tham chiếu)* | *200.000* | *5* | *~20* | *~1.000.000* | *~20.000.000* |

### 4.1 Số bản ghi mỗi feed/ngày

| Feed | Grain | small | medium | large |
|---|---|---:|---:|---:|
| **(3) Holdings snapshot** FO→SDI | KH×SI×mã | 60.000 | 1.250.000 | 6.250.000 |
| **(4) Cash snapshot** FO→SDI | KH×SI | 3.000 | 50.000 | 250.000 |
| (2) Model weight FO→SDI | SI×mã (khi đổi) | 60 | 125 | 125 |
| (5) Cổ tức/phí FO→SDI | sparse | *0 → ~tiểu khoản nắm mã chia* | | |
| (6) Cashflow FO→SDI | sparse | *0 → ~tiểu khoản có SIP/nạp/rút* | | |
| (7a) Giá EOD Mkt→SDI | universe mã | 20 | 25 | 25 *(prod ~1.600)* |
| (7b/7c) CA + benchmark | sparse / 1-vài | nhỏ | nhỏ | nhỏ |
| **(8a) Current snapshot KH** SDI→Asset | KH×SI | 3.000 | 50.000 | 250.000 |
| (8b) Current snapshot SI SDI→Asset | SI | 3 | 5 | 5 |
| (8c) SI series/ngày SDI→Asset | SI | 3 | 5 | 5 |

**Sparse feeds (5)(6):** phụ thuộc sự kiện —
- *Ngày thường:* cashflow ≈ % nhỏ tiểu khoản (chỉ KH có SIP/nạp/rút); cổ tức/phí ≈ 0.
- *Ngày cao điểm:* SIP định kỳ → tới ~50% tiểu khoản; ngày chia cổ tức 1 mã phổ biến → ~tiểu khoản đang nắm mã đó. Ví dụ medium, mã chia được ~30% tiểu khoản nắm → ~15.000 dòng.

### 4.2 Ước lượng dung lượng (raw, chưa nén)

> Ước theo bytes/row xấp xỉ; prod nén PAGE/CCI thường giảm **~3–5×**.

| Feed | ~bytes/row | small | medium | large |
|---|---:|---:|---:|---:|
| (3) Holdings snapshot | ~60 | ~3,6 MB | ~75 MB | ~375 MB |
| (4) Cash snapshot | ~28 | ~0,08 MB | ~1,4 MB | ~7 MB |
| (8a) Current snapshot KH | ~50 | ~0,15 MB | ~2,5 MB | ~12,5 MB |
| **Tổng FO→SDI/ngày** (3+4) | | **~3,7 MB** | **~76 MB** | **~382 MB** |
| **Tổng SDI→Asset/ngày** (8a+b+c) | | **~0,15 MB** | **~2,5 MB** | **~12,5 MB** |

→ **FO→SDI lớn gấp ~25–30× SDI→Asset** (vì holdings theo từng mã; Asset chỉ nhận current cấp tiểu khoản). *(prod ~20M holdings ⇒ feed ~1,2 GB/ngày raw.)*

### 4.3 Thời gian xử lý EOD (đo thực, SQL Express 1 máy)

| Kịch bản | Holdings | TOTAL EOD | J07 MTM (nặng nhất) | Ghi chú |
|---|---:|---:|---:|---|
| small | 60.000 | ~0,9–1,2 s | ~0,5 s | — |
| medium | 1.250.000 | ~16–19 s | ~8–9 s | đo A/B `db/bench.ps1` |
| large | 6.250.000 | *chưa đo* | *~5× medium* | ~80–100 s ước lượng tuyến tính |

> Đo bằng `db/bench.ps1` → ghi `db/perf-history.csv`. Điểm nóng cố định = **J07 MTM** (định giá lại toàn bộ holdings) — bám cột này khi theo dõi regression. Lưu ý: `cust_code` VARCHAR(10) chậm hơn BIGINT ~13% total (xem perf note); chấp nhận vì là định danh KH xuyên hệ.

---

## 5. Quy tắc hợp đồng (contract)

1. **Snapshot overwrite, idempotent:** FO nạp toàn bộ holdings THẲNG vào current + cash mỗi EOD; chạy lại 1 ngày cho cùng kết quả (overwrite, không cộng dồn).
8. **Holdings/cash history tách rời (interval):** J14b droppable DIFF current → `T_CUSTOMER_HOLDING_HIST` & `T_CUSTOMER_CASH_HIST` (SCD-2 valid_from/valid_to, **full history BẮT BUỘC, no-dup**) — EOD core chỉ đọc current; bỏ J14b không ảnh hưởng EOD (history dừng cập nhật).
2. **FO cash là nguồn tiền duy nhất, đã NET** phí QL + thuế GD + SIP → SDI không re-apply.
3. **Biến động holdings/ngày** SDI suy ra on-demand (qty(D)−qty(D-1)), KHÔNG cần FO gửi delta.
4. **Cổ tức/phí & cashflow là sự kiện sparse** — FO chỉ gửi khi phát sinh; capture đúng ngày + số tiền khớp thời điểm FO ghi vào cash.
5. **J13 RECONCILE là cổng:** Σ holdings/NAV SDI vs FO, Σ customer NAV vs SI NAV, Σ unit — lệch > ngưỡng ⇒ **chặn J15 publish**.
6. **SDI→Asset chỉ push current/delta + chuỗi SI**; lịch sử KH SDI giữ và serve qua API (pull), không đổ bulk.
7. **J0 GATE** chờ đủ nguồn (FO holdings+cash, model_weight, giá, CA, benchmark, cashflow) sẵn sàng cho @d mới chạy.

---

## 6. Giả định & lưu ý
- Bytes/row là ước lượng raw để so sánh tương đối — số thật phụ thuộc kiểu cột/nén.
- Kịch bản small/medium/large theo `bench.ps1`; **prod thực** ≈ 200K KH (~20M holdings) — large (6,25M) ≈ 31% prod.
- Sparse feeds (cổ tức/phí, cashflow, CA, rebalance) biến động mạnh theo lịch sự kiện → cần đo riêng theo lịch SIP/chia cổ tức thực tế, không suy tuyến tính từ holdings.
- Thời gian EOD large chưa đo — chạy `./bench.ps1 -Scale large` để lấy số thật trước khi cam kết SLA cửa sổ EOD.
