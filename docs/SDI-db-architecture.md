# SDI — Kiến trúc DB & EOD ở quy mô lớn (SQL Server)

Bổ trợ cho [SDI-spec.md](./SDI-spec.md). Tập trung: chịu tải dữ liệu lớn + chạy batch chốt EOD trong cửa sổ đêm.

---

## 1. Khối lượng & điểm nóng

| Đại lượng | Số |
|---|---|
| SI | 100 |
| Tiểu khoản (KH × SI) active | ~1.000.000 |
| Lot holdings (≈20/tiểu khoản) | ~20.000.000 |
| Phiên/năm × 10 năm | ~2.500 |
| **NAV phải tính MỖI EOD** | **~1.000.000 vị thế × 20 lot = ~20M phép định giá/ngày** |
| Customer daily NAV nếu lưu hết | ~2,5 tỷ dòng |

**Điểm nóng = bước định giá lại (MTM) toàn bộ ~1M vị thế mỗi ngày.** Giá biến động hằng ngày → **mọi** vị thế đổi NAV → bắt buộc revalue tất cả. Đây là chỗ quyết định EOD chạy vài phút hay vài giờ.

> ⚠️ **Lỗi chết người:** xử lý kiểu **cursor / vòng lặp từng tiểu khoản** (RBAR). 1M vị thế × logic/dòng = hàng giờ→ngày, KHÔNG xong. Toàn bộ thiết kế dưới đây xoay quanh việc **né RBAR**.

---

## 2. Hai nguyên tắc nền (quyết định thành/bại)

### 2.1 ROLL-FORWARD STATE — không replay lịch sử mỗi ngày
Giữ **trạng thái hiện tại** (current state), mỗi EOD chỉ **áp delta của ngày** rồi định giá lại. KHÔNG dựng lại NAV từ đầu lịch sử mỗi ngày.

- `sdi_position_state` — 1 dòng/vị thế (unit, cash, last_nav, last_unit_price) → ~1M dòng, **update tại chỗ**.
- `sdi_position_holding` — 1 dòng/(vị thế × mã) (quantity, avg_cost) → ~20M dòng, update incremental.
- Event ledger (cashflow, execution, CA) chỉ **append**; dùng để recompute khi cần.

### 2.2 SET-BASED — không RBAR
Toàn bộ EOD = **một số ít câu lệnh tập hợp** (JOIN + GROUP BY + MERGE), chạy trên engine, KHÔNG cursor/loop. Bước MTM 20M dòng là **1 câu** `JOIN price + SUM GROUP BY`.

---

## 3. Mô hình bảng theo tải

| Bảng | Vai trò | Quy mô | Lưu trữ |
|---|---|---|---|
| `sdi_position_state` | trạng thái hiện tại/vị thế | ~1M | **rowstore**, clustered PK (customer_id, si_id), PAGE compression; cân nhắc memory-optimized |
| `sdi_position_holding` | holdings hiện tại | ~20M | **rowstore** clustered (customer_id, si_id, ticker) + **NCCI** (HTAP) cho MTM |
| `sdi_cashflow_event` | sổ cái nạp/rút | ~120M | **CCI** (clustered columnstore), partition theo năm |
| `sdi_execution_feed` | khớp lệnh MP từ FO | ~lớn | **CCI**, partition theo năm |
| `sdi_customer_holding_event` | sổ cái lot delta | ~lớn | **CCI**, partition theo năm |
| `sdi_unit_ledger` | unit thay đổi (cashflow) | ~120M | **CCI**, partition theo năm |
| `sdi_si_performance_daily` | SI-level daily | ~250K | rowstore, partition năm |
| `sdi_si_index_daily` / `sdi_benchmark_daily` | index daily | ~250K | rowstore |
| `sdi_asset_snapshot_daily` (SI) | snapshot SI | ~250K | rowstore |
| `sdi_price_daily` | giá EOD | ~4M | rowstore, index (business_date, ticker) — nhỏ, cache RAM |
| `sdi_position_daily` (TÙY CHỌN) | customer NAV daily lịch sử | ~2,5 tỷ | **CCI**, partition (xem §7) |

**Quyết định customer daily NAV:** **KHÔNG materialize 2,5 tỷ dòng mặc định.** Giữ `position_state` (current, ~1M) + event ledger; **derive lịch sử khi cần** (chart) + **snapshot cuối tháng** giới hạn replay. Chỉ bật `sdi_position_daily` (CCI) nếu đo thấy độ trễ đọc chart không đạt (xem §7.3).

---

## 4. Tính năng SQL Server bắt buộc dùng

| Tính năng | Dùng cho | Lợi ích |
|---|---|---|
| **Table Partitioning** (partition function/scheme) | mọi bảng lớn theo `business_date`/năm | quản lý, archive, partition elimination khi query |
| **Clustered Columnstore (CCI)** | event ledger, history, position_daily | nén ~10×, **batch-mode** → aggregate nhanh 10–100× |
| **Nonclustered Columnstore (NCCI)** trên rowstore | `sdi_position_holding` (HTAP) | vừa update OLTP vừa MTM analytic nhanh |
| **Partition SWITCH** | nạp & archive | nạp/đẩy partition **tức thời** (metadata-only), không ghi lại dữ liệu |
| **Batch-mode on rowstore** (2019+, compat 150) | aggregate rowstore không cần columnstore | tăng tốc GROUP BY |
| **RCSI / SNAPSHOT isolation** | batch không chặn app đọc | EOD chạy song song với SMO đọc |
| **PAGE compression** | rowstore lớn | giảm I/O |
| **Memory-Optimized table** (In-Memory OLTP) | `sdi_position_state` nếu update nóng | bỏ latch/lock contention |
| **Filegroups** nóng/lạnh | partition năm hiện tại (SSD) vs archive (HDD) | chi phí + tốc độ |
| **Resource Governor + MAXDOP** | giới hạn/đảm bảo tài nguyên batch | ổn định cửa sổ EOD |

---

## 5. Pipeline EOD — set-based (không RBAR)

Tất cả vào **staging** trước, validate, rồi **publish** (partition switch/MERGE). Idempotent + resumable.

```
B1  STAGE input ngày @d: bulk insert price, execution feed, model_weight, CA → bảng staging (minimal logging)
B2  ÁP DELTA vào state (incremental — chỉ vị thế có biến động):
      - CA: MERGE holding (split/quyền đổi qty); cash (cổ tức) → state.cash
      - Execution: MERGE holding (qty +/−); state.cash -/+ (mua/bán, trade-date)
      - Cashflow: state.cash +/−; tính CF_t cho từng vị thế có nạp/rút
      - Mgmt fee: state.payable += NAV_prev × rate/365   (1 UPDATE set-based)
B3  MTM TOÀN BỘ (câu lệnh nặng nhất — set-based):
      INSERT #nav_today (customer_id, si_id, stock_value)
      SELECT h.customer_id, h.si_id, SUM(h.quantity * p.close_price)
      FROM   sdi_position_holding h
      JOIN   sdi_price_daily p ON p.ticker=h.ticker AND p.business_date=@d
      GROUP BY h.customer_id, h.si_id;        -- batch-mode (NCCI) trên 20M dòng
B4  NAV = stock_value + state.cash − custody_fee − mgmt_fee_accrued     (1 UPDATE join)
B5  PnL ngày = NAV_today − NAV_prev + ra − vào                          (1 UPDATE)
B6  UNIT: chỉ vị thế có CF_t:  ΔUnit = CF/unit_price_prev; unit += ΔUnit  (1 UPDATE)
      unit_price = NAV / unit   (mọi vị thế — 1 UPDATE)
      → INSERT sdi_unit_ledger các dòng có ΔUnit ≠ 0
B7  SI AGGREGATE (set-based):
      SI NAV = Σ NAV, SI Unit = Σ unit per si_id → sdi_si_performance_daily
B8  SI INDEX: Index_t = Index_(t-1) × Σ w^(t)·P_t/P_ref  (100 SI × ~25 mã — nhẹ) → sdi_si_index_daily
B9  PUBLISH: cập nhật sdi_position_state (current); SWITCH/MERGE SI-level vào bảng đích;
      push delta sang Asset (current snapshot, không append toàn lịch sử)
```

- **B2 incremental**: chỉ vị thế có event (nạp/rút/khớp/CA) — vài chục–trăm nghìn/ngày, nhẹ.
- **B3 full revaluation**: chạm 20M dòng nhưng là **1 câu hash-aggregate batch-mode** → giây→phút.
- **B4–B6**: UPDATE set-based trên ~1M dòng → 1–2 phút.
- Không câu nào lặp từng vị thế.

---

## 6. Song song hóa & cửa sổ EOD

- **Độc lập theo SI**: vị thế của 1 SI không ảnh hưởng SI khác (chỉ chung giá). → chia batch theo **dải SI** hoặc **hash(customer_id)** chạy song song N luồng.
- **MAXDOP** cho câu MTM/aggregate (để engine parallel intra-query); **Resource Governor** cấp pool riêng cho batch đêm.
- **Partition-aligned**: xử lý từng partition độc lập → switch-in song song.

**Ước lượng runtime (server ~16–32 core, NVMe):**
| Bước | Ước tính |
|---|---|
| B3 MTM 20M dòng (CCI batch-mode) | ~10–60 giây |
| B4–B6 UPDATE ~1M dòng | ~1–3 phút |
| B7–B8 aggregate + index | ~giây |
| **Tổng core EOD** | **~vài phút – ~15 phút** |

→ Thừa sức trong cửa sổ đêm. **Cùng workload nếu làm bằng cursor → hàng giờ–ngày.** Khác biệt là set-based + columnstore.

---

## 7. Partitioning & vòng đời dữ liệu

### 7.1 Partition function/scheme (theo năm)
```sql
CREATE PARTITION FUNCTION pf_year (date)
  AS RANGE RIGHT FOR VALUES ('2017-01-01','2018-01-01', ... ,'2027-01-01');
CREATE PARTITION SCHEME ps_year AS PARTITION pf_year
  TO (fg_archive, ..., fg_2026, fg_2027, fg_next);   -- map năm cũ → filegroup HDD
```
- Bảng lớn tạo trên `ps_year(business_date)`. CCI tự chia rowgroup theo partition → **partition elimination** khi query theo ngày.

### 7.2 Sliding window (archive tự động)
- Cuối năm: `SPLIT` thêm partition năm mới; `SWITCH` partition cũ ra bảng archive (metadata-only, tức thời); `MERGE` nếu cần.
- Holdings: 2 năm online (SSD) + 8 năm archive (HDD/đọc nguội).
- Nạp EOD: bulk vào **staging cùng filegroup + cùng index + CHECK constraint khớp biên** → `SWITCH` vào partition đích → tức thời, không khóa bảng lớn.

### 7.3 Customer daily NAV — 2 phương án (chọn theo đo đạc)
| | A. Derive-on-read (mặc định) | B. Materialize CCI |
|---|---|---|
| Lưu | current snapshot + event + snapshot cuối tháng | `sdi_position_daily` CCI ~2,5 tỷ (nén ~10×) |
| EOD | nhẹ (không append 1M/ngày) | +bulk 1M/ngày vào CCI (rẻ) |
| Đọc chart KH | derive (replay trong tháng) — vài chục ms | đọc thẳng — nhanh |
| Khuyến nghị | **dùng trước**; chart lấy điểm thưa (tuần/tháng) | bật nếu đo thấy đọc không đạt SLA |

> Nếu chọn B: partition theo **hash(customer_id) + năm**, hoặc **ordered CCI** theo (customer_id, business_date) để segment-elimination khi đọc 1 KH. Tránh nonclustered rowstore index trên 2,5 tỷ (phình ~trăm GB).

---

## 8. Indexing & thiết kế khóa

- **Khóa clustering**: dùng khóa **hẹp, tăng dần, không GUID** (GUID ngẫu nhiên → page split, fragmentation). Dùng `BIGINT IDENTITY` hoặc khóa tự nhiên hẹp.
- `sdi_position_state`: clustered PK `(customer_id, si_id)` — point update/lookup.
- `sdi_position_holding`: clustered `(customer_id, si_id, ticker)` + **NCCI** (cho B3).
- Event ledger (CCI): partition `(business_date)`; CCI tự lo, thêm **nonclustered rowstore** `(customer_id, si_id, business_date)` nếu cần truy vết theo KH.
- SI-level daily: clustered `(si_id, business_date)`.
- `sdi_price_daily`: clustered `(business_date, ticker)` — nhỏ, cache buffer pool.
- **Avoid**: index thừa trên bảng ghi nóng (chậm insert); EAV; nvarchar(max) trong fact.

---

## 9. Idempotency, resume, đối soát

- **Idempotent**: mỗi bước ghi vào staging gắn `@d`; publish bằng SWITCH/MERGE theo PK → chạy lại 1 ngày ra cùng kết quả.
- **Resume**: bảng `sdi_eod_run(business_date, step, status, rows, ts)`; fail giữa chừng → tiếp từ step lỗi.
- **Recompute lịch sử**: xóa daily series từ ngày X + reset state về snapshot tháng → replay event ledger (CCI scan nhanh).
- **Đối soát (reconcile)** sau B3: `Σ holding qty per (KH,ticker)` (SDI) vs holdings thật FO → bảng break; chặn publish nếu lệch quá ngưỡng.
- **RCSI** bật → app đọc current snapshot không bị batch chặn; publish cuối cùng là thao tác ngắn.

---

## 10. Lưu trữ & nén (ước tính)

| Bảng | Dòng | Sau nén (ước) |
|---|---|---|
| position_state (rowstore PAGE) | 1M | ~vài trăm MB |
| position_holding (rowstore + NCCI) | 20M | ~vài GB |
| event ledger (CCI) | ~300M tổng | ~chục GB |
| SI-level daily (rowstore) | ~1M | ~nhỏ |
| position_daily nếu bật (CCI) | 2,5 tỷ | ~100–300 GB |

→ Không bật position_daily: tổng ~chục GB, rất gọn. Bật: thêm ~trăm GB (CCI nén) — vẫn trong tầm SQL Server, đặt phần lớn ở filegroup archive.

---

## 11. Anti-patterns (CẤM)

- ❌ **Cursor/WHILE loop** xử lý từng tiểu khoản trong EOD.
- ❌ Tính NAV bằng **replay toàn lịch sử mỗi ngày** (thay vì roll-forward state).
- ❌ **Scalar UDF** trong câu set-based (1 lần/dòng → giết batch; nếu phải, dùng inline TVF hoặc 2019+ scalar inlining).
- ❌ **GUID ngẫu nhiên làm clustered key** trên bảng lớn (fragmentation, page split).
- ❌ Nonclustered rowstore index nặng trên bảng tỷ-dòng (phình + chậm ghi) — ưu tiên columnstore + partition elimination.
- ❌ Materialize 2,5 tỷ dòng khi chưa cần.
- ❌ `MERGE` trên bảng cực lớn không partition-aligned (dễ chậm/deadlock) — tách INSERT/UPDATE hoặc switch.
- ❌ Index thừa trên bảng ghi nóng làm chậm bulk insert EOD.

---

## 12. Tóm tắt quyết định kiến trúc

1. **Roll-forward state** (`position_state` 1M + `position_holding` 20M), KHÔNG replay mỗi ngày.
2. **EOD set-based**: ~10 câu lệnh; nặng nhất = MTM 20M dòng (1 câu, CCI batch-mode).
3. **Columnstore** (CCI/NCCI) cho fact/history + **partition theo năm** + **partition switch** nạp/archive.
4. **Customer daily NAV: derive-on-read mặc định** (+ snapshot cuối tháng); materialize CCI chỉ khi cần.
5. **Song song theo SI/hash**, MAXDOP, Resource Governor; **RCSI** để không chặn app.
6. **Idempotent + resumable + reconcile** trước khi publish.
7. EOD ước ~vài phút–15 phút (vs hàng giờ nếu RBAR).
