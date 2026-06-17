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
- `sdi_indexing_portfolio_ticker` — 1 dòng/(vị thế × mã) (quantity, avg_cost) → ~20M dòng, update incremental.
- Event ledger (cashflow, execution, CA) chỉ **append**; dùng để recompute khi cần.

### 2.2 SET-BASED — không RBAR
Toàn bộ EOD = **một số ít câu lệnh tập hợp** (JOIN + GROUP BY + MERGE), chạy trên engine, KHÔNG cursor/loop. Bước MTM 20M dòng là **1 câu** `JOIN price + SUM GROUP BY`.

---

## 3. Mô hình bảng theo tải

| Bảng | Vai trò | Quy mô | Lưu trữ |
|---|---|---|---|
| `sdi_position_state` | trạng thái hiện tại/vị thế | ~1M | **rowstore**, clustered PK (customer_id, si_id), PAGE compression; cân nhắc memory-optimized |
| `sdi_indexing_portfolio_ticker` | holdings hiện tại (mirror FO) | ~20M | **rowstore** clustered (customer_id, si_id, ticker) + **NCCI** (HTAP) cho MTM |
| `sdi_si_position_holding_daily` | snapshot holdings DATED (per-KH) | ~20M/ngày | **CCI**, partition năm — nguồn current mirror + audit + tái dựng holdings (thay customer_holding_event; biến động suy ra on-demand) |
| `sdi_fo_cash_sync` | snapshot tiền FO đồng bộ EOD (per-KH) | ~20M/ngày staging | rowstore, partition theo ngày/năm |
| `sdi_cashflow_event` | sổ cái nạp/rút | ~120M | **CCI** (clustered columnstore), partition theo năm |
| `sdi_indexing_performance_daily` | **lịch sử perf per-KH** (materialize) | ~2,5 tỷ | **CCI** + partition (cần vì holdings không event-source) |
| `sdi_unit_ledger` | unit thay đổi (cashflow) | ~120M | **CCI**, partition theo năm |
| `sdi_si_performance_daily` | SI-level daily | ~250K | rowstore, partition năm |
| `sdi_si_index_daily` / `sdi_benchmark_daily` | index daily | ~250K | rowstore |
| `sdi_asset_snapshot_daily` (SI) | snapshot SI | ~250K | rowstore |
| `sdi_price_daily` | giá EOD | ~4M | rowstore, index (business_date, ticker) — nhỏ, cache RAM |
| `sdi_indexing_performance_daily` | customer perf daily (lịch sử) | ~2,5 tỷ | **CCI**, partition (xem §7) |

**Quyết định customer daily perf (đổi do FO-sync):** vì FO đồng bộ **snapshot overwrite** → holdings KHÔNG còn event-source → **KHÔNG derive được NAV/unit_price quá khứ** → **BẮT BUỘC materialize** `sdi_indexing_performance_daily` (nav/unit/unit_price/day) để vẽ chart FR-03. Giảm tải: lấy **điểm thưa (tuần/tháng)** hoặc chỉ lưu `unit_price`. Lưu CCI + partition (§7.3).

---

## 4. Tính năng SQL Server bắt buộc dùng

| Tính năng | Dùng cho | Lợi ích |
|---|---|---|
| **Table Partitioning** (partition function/scheme) | mọi bảng lớn theo `business_date`/năm | quản lý, archive, partition elimination khi query |
| **Clustered Columnstore (CCI)** | event ledger, history, position_daily | nén ~10×, **batch-mode** → aggregate nhanh 10–100× |
| **Nonclustered Columnstore (NCCI)** trên rowstore | `sdi_indexing_portfolio_ticker` (HTAP) | vừa update OLTP vừa MTM analytic nhanh |
| **Partition SWITCH** | nạp & archive | nạp/đẩy partition **tức thời** (metadata-only), không ghi lại dữ liệu |
| **Batch-mode on rowstore** (2019+, compat 150) | aggregate rowstore không cần columnstore | tăng tốc GROUP BY |
| **RCSI / SNAPSHOT isolation** | batch không chặn app đọc | EOD chạy song song với SMO đọc |
| **PAGE compression** | rowstore lớn | giảm I/O |
| **Memory-Optimized table** (In-Memory OLTP) | `sdi_position_state` nếu update nóng | bỏ latch/lock contention |
| **Filegroups** nóng/lạnh | partition năm hiện tại (SSD) vs archive (HDD) | chi phí + tốc độ |
| **Resource Governor + MAXDOP** | giới hạn/đảm bảo tài nguyên batch | ổn định cửa sổ EOD |

### 4.1 Cấu hình đặt ở cấp nào / ai thiết lập
Hỗn hợp 3 cấp — KHÔNG phải tất cả khi tạo bảng:

| Cấu hình | Cấp | Ai | Cách |
|---|---|---|---|
| MAXDOP, cost threshold | Instance / DB-scoped | DBA | `sp_configure` / `ALTER DATABASE SCOPED CONFIGURATION` / hint `OPTION(MAXDOP n)` |
| Resource Governor | Instance | DBA | `CREATE RESOURCE POOL/WORKLOAD GROUP` |
| Compatibility level 150+ | Database | DBA | `ALTER DATABASE SET COMPATIBILITY_LEVEL=150` |
| RCSI | Database | DBA | `ALTER DATABASE SET READ_COMMITTED_SNAPSHOT ON` |
| Filegroups nóng/lạnh | Database | DBA | `ALTER DATABASE ADD FILEGROUP/FILE` |
| Memory-optimized filegroup | Database | DBA | `ALTER DATABASE ADD FILEGROUP … MEMORY_OPTIMIZED_DATA` |
| Partition FUNCTION + SCHEME | DB-object (1 lần) | DBA + Dev | `CREATE PARTITION FUNCTION/SCHEME` |
| Tạo bảng trên scheme | Object | **Dev** | `CREATE TABLE … ON ps_year(business_date)` |
| Columnstore CCI/NCCI | Object | **Dev** | `CREATE [CLUSTERED] COLUMNSTORE INDEX` |
| PAGE compression | Object | **Dev** | `WITH (DATA_COMPRESSION=PAGE)` |
| Index thường | Object | **Dev** | DDL |
| Memory-optimized table | Object (cần FG từ DBA) | **Dev** | `WITH (MEMORY_OPTIMIZED=ON)` |
| Partition SWITCH/SPLIT/MERGE | Code (batch) | **Dev** | `ALTER TABLE … SWITCH` |

→ **DBA lo hạ tầng 1 lần** (instance + database: RCSI, filegroups, compat, resource governor, partition function/scheme). **Dev viết trong DDL/batch** (columnstore, compression, index, tạo bảng trên scheme, partition switch). Partitioning = phối hợp DBA+Dev.

### 4.2 Mô hình thực thi: ALL-IN-DB (app chỉ gọi proc)

**Toàn bộ engine = stored procedure T-SQL trong DB.** App KHÔNG tính toán — chỉ gọi proc (execute batch + đọc dữ liệu).

| Thành phần | Hiện thực |
|---|---|
| Mỗi job J0–J16 | 1 stored proc (set-based) |
| Orchestration | master proc `usp_eod_run @business_date` gọi tuần tự + ghi `sdi_eod_run` (resume); App/SQL Agent chỉ kích hoạt |
| Ingestion (feed FO/Market → staging) | proc `BULK INSERT` / `OPENROWSET` / external table — KHÔNG kéo qua app |
| API đọc (SMO/Asset) | stored proc (`usp_get_*`); app gọi & trả JSON, không tính |
| MWR Modified Dietz | set-based trong proc |
| MWR XIRR (nếu cần, iterative) | **SQL CLR** (trong DB) — không tính ở app |

→ App = thin client: `EXEC usp_eod_run` + `EXEC usp_get_*`. Không pull-compute-push.

---

## 5. Pipeline EOD — set-based (không RBAR)

Tất cả vào **staging** trước, validate, rồi **publish** (partition switch/MERGE). Idempotent + resumable.

```
B1  STAGE input ngày @d: bulk insert price, FO holdings+cash sync, model_weight, CA, cashflow → staging (minimal logging)
B1b SYNC_FO: mirror holdings (overwrite T_INDEXING_PORTFOLIO_TICKER) + cash (state) từ snapshot sdi_si_position_holding_daily
B2  ÁP DELTA vào state (incremental — chỉ vị thế có biến động):
      - SYNC_FO đã overwrite holdings + cash từ snapshot FO (đã phản ánh trade/CA/cổ tức/SIP)
      - Biến động holdings NET/ngày (audit/tái dựng) suy ra on-demand = qty(D)−qty(D-1) từ snapshot dated, KHÔNG lưu cột
      - Cashflow event: tính CF_t cho vị thế có nạp/rút (chỉ để đổi unit — KHÔNG cộng lại vào cash)
      - (KHÔNG accrue phí: FO cash đã NET phí QL + thuế GD — tránh double-count)
B3  MTM TOÀN BỘ (câu lệnh nặng nhất — set-based):
      INSERT #nav_today (customer_id, si_id, stock_value)
      SELECT h.customer_id, h.si_id, SUM(h.quantity * p.close_price)
      FROM   sdi_indexing_portfolio_ticker h
      JOIN   sdi_price_daily p ON p.ticker=h.ticker AND p.business_date=@d
      GROUP BY h.customer_id, h.si_id;        -- batch-mode (NCCI) trên 20M dòng
B4  NAV = stock_value + state.cash   (FO cash đã NET phí → không trừ lại)   (1 UPDATE join)
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

### 7.3 Customer daily perf — BẮT BUỘC materialize (do FO-sync)
FO sync overwrite holdings → không event-source → derive-on-read lịch sử **không khả thi** → **phải materialize** `sdi_indexing_performance_daily`:
- Lưu CCI, **partition hash(customer_id) + năm** (hoặc **ordered CCI** theo (customer_id, business_date)) để segment-elimination khi đọc 1 KH. Tránh nonclustered rowstore index trên 2,5 tỷ (phình ~trăm GB).
- EOD: bulk ~1M dòng/ngày vào CCI (rẻ). Đọc chart KH: đọc thẳng (nhanh).
- **Giảm tải**: lấy điểm **thưa (tuần/tháng)** thay vì daily, hoặc chỉ lưu cột `unit_price` (+nav) — narrow CCI nén rất tốt.

---

## 8. Indexing & thiết kế khóa

- **Khóa clustering**: dùng khóa **hẹp, tăng dần, không GUID** (GUID ngẫu nhiên → page split, fragmentation). Dùng `BIGINT IDENTITY` hoặc khóa tự nhiên hẹp.
- `sdi_position_state`: clustered PK `(customer_id, si_id)` — point update/lookup.
- `sdi_indexing_portfolio_ticker`: clustered `(customer_id, si_id, ticker)` + **NCCI** (cho B3).
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

> **Nén LÀM GIẢM dung lượng, không tăng** (CCI ~10×, PAGE ~2–4×). Mọi con số dưới là **sau nén**.

| Bảng | Dòng | Sau nén |
|---|---|---|
| position_state (rowstore PAGE) | 1M | ~vài trăm MB |
| indexing_portfolio_ticker (rowstore + NCCI) | 20M | ~vài GB |
| event ledger (cashflow/unit_ledger, CCI) | ~240M | ~chục GB |
| SI-level daily (rowstore) | ~1M | ~nhỏ |
| **`indexing_performance_daily` (CCI)** | **~2,5 tỷ** | **~100–300 GB** (bắt buộc — do FO-sync, xem §7.3) |

> Nén ~10× kéo bảng perf từ ~1–3 TB về ~100–300 GB. **Giảm**: lấy điểm thưa (tuần/tháng) hoặc chỉ lưu `unit_price` (+nav) → narrow CCI còn nhỏ hơn nhiều. Đặt partition cũ ở filegroup archive.
> Lưu ý: trước đây có thể derive-on-read (event-source); sau khi đổi sang **FO sync snapshot** thì **bắt buộc** materialize bảng này.

---

## 10b. Chiến lược Primary Key (chốt)

### Quy tắc
```
Fact/event lớn, hot, columnstore        → BIGINT IDENTITY (surrogate)
Master bị FK ref bởi bảng lớn           → BIGINT  (ĐỪNG GUID — sẽ lan 16B vào bảng lớn)
Reference/daily có tổ hợp tự nhiên      → composite natural key (cột TYPED; API compose từ field)
Nhạy cảm phơi id ra URL/API trực tiếp   → BIGINT nội bộ + cột GUID RANDOM nonclustered unique (public id)
Config nhỏ ĐỘC LẬP, không natural key    → (seq) GUID OK
```
- Composite natural key: để **các cột typed riêng** (DATE+BIGINT…), **KHÔNG nối chuỗi** (`"date_id"` → mất partition elimination, parse, nén kém). API cần token đơn thì **ghép/parse ở tầng API**, DB giữ cột riêng.
- Leftmost-prefix: query phải có **cột dẫn đầu** mới seek; pattern khác → thêm nonclustered index.

### Per-table
| Bảng | PK | Loại |
|---|---|---|
| T_MASTER_PORTFOLIO | C_SI_ID | BIGINT (bị ref rộng) |
| T_INDEXING_PORTFOLIO | (C_CUSTOMER_ID, C_SI_ID) | composite typed |
| T_MASTER_PORTFOLIO_TICKER | (C_SI_ID, C_EFFECTIVE_DATE, C_TICKER) | composite natural |
| T_PRICE_DAILY | (C_BUSINESS_DATE, C_TICKER) | composite natural |
| T_CORPORATE_ACTION | (C_TICKER, C_EX_DATE, C_CA_TYPE) | composite natural (optional: + C_CA_ID surrogate, composite→UNIQUE) |
| T_BENCHMARK_DAILY | (C_BENCHMARK_ID, C_BUSINESS_DATE) | composite |
| T_REBALANCE_REQUEST | C_REQUEST_ID | BIGINT IDENTITY |
| T_SI_POSITION_HOLDING_DAILY | (C_BUSINESS_DATE, C_CUSTOMER_ID, C_SI_ID, C_TICKER) | composite natural (snapshot FO dated) |
| T_FO_CASH_SYNC | (C_BUSINESS_DATE, C_CUSTOMER_ID, C_SI_ID) | composite natural (staging FO) |
| T_CASHFLOW_EVENT | C_EVENT_ID | BIGINT IDENTITY (fact/CCI) |
| T_INDEXING_PERFORMANCE_DAILY | (C_BUSINESS_DATE, C_CUSTOMER_ID, C_SI_ID) | composite natural (history, CCI) |
| T_UNIT_LEDGER | (C_CUSTOMER_ID, C_SI_ID, C_BUSINESS_DATE) | composite natural |
| T_POSITION_STATE | (C_CUSTOMER_ID, C_SI_ID) | composite typed (hot) |
| T_INDEXING_PORTFOLIO_TICKER | (C_CUSTOMER_ID, C_SI_ID, C_TICKER) | composite typed |
| T_EOD_WORK | (C_BUSINESS_DATE, C_CUSTOMER_ID, C_SI_ID) | composite (transient) |
| T_SI_PERFORMANCE_DAILY | (C_BUSINESS_DATE, C_SI_ID) | composite natural |
| T_SI_INDEX_DAILY | (C_BUSINESS_DATE, C_SI_ID) | composite natural |
| T_HOLDING_DAILY | (C_BUSINESS_DATE, C_SI_ID, C_TICKER) | composite natural |
| T_ASSET_SNAPSHOT_DAILY | (C_BUSINESS_DATE, C_SI_ID) | composite natural |
| T_EOD_RUN | (C_BUSINESS_DATE, C_JOB) | composite natural |

→ **Không bảng nào dùng GUID** vì master nhỏ đều bị bảng lớn FK-ref; bảng nhỏ còn lại đã có natural key.

### Benchmark GUID vs BIGINT (đo thật, 500.000 dòng, SQL Server Express)
| PK clustered | Insert (ms) | Size | Fragmentation | Page fill | NC index |
|---|---|---|---|---|---|
| BIGINT IDENTITY | 2,870 | 40.7 MB | 0.46% | 99.6% | 15.0 MB |
| **GUID ngẫu nhiên** | 4,268 (**+49%**) | 64.1 MB (**+57%**) | **99.16%** | 69.2% | 18.9 MB (**+26%**) |
| GUID tuần tự | 3,013 (+5%) | 44.4 MB (+9%) | 0.65% | 100% | 18.9 MB (+26%) |
→ Random GUID PK: insert +49%, storage +57%, fragmentation ~99%, mỗi NC index +26%. Trên tỷ-dòng/columnstore còn tệ hơn. Seq GUID nhẹ hơn nhưng vẫn dưới BIGINT.

### Bảo mật (IDOR / enumeration)
- Vấn đề id số tăng dần **bị enumerate** là chuyện **EXPOSURE ở API**, không phải PK trong DB.
- Giải: **BIGINT nội bộ (perf) + cột `C_PUBLIC_ID UNIQUEIDENTIFIER DEFAULT NEWID()` nonclustered unique** phơi ra API. Chỉ áp cho entity **lộ id ra URL**.
- **Seq GUID KHÔNG chống enumeration** (đoán được) → public id phải **random (NEWID)**.
- Root cause của IDOR = **authorization per-request** (OWASP); id opaque chỉ là defense-in-depth.

---

## 11. Anti-patterns (CẤM)

- ❌ **Cursor/WHILE loop** xử lý từng tiểu khoản trong EOD.
- ❌ Tính NAV bằng **replay toàn lịch sử mỗi ngày** (thay vì roll-forward state).
- ❌ **Scalar UDF** trong câu set-based (1 lần/dòng → giết batch; nếu phải, dùng inline TVF hoặc 2019+ scalar inlining).
- ❌ **GUID ngẫu nhiên làm clustered key** trên bảng lớn (fragmentation, page split).
- ❌ Nonclustered rowstore index nặng trên bảng tỷ-dòng (phình + chậm ghi) — ưu tiên columnstore + partition elimination.
- ❌ Lưu daily HOLDINGS snapshot per-KH (~50 tỷ) — thay vào đó materialize daily PERF (~2,5 tỷ, nhỏ hơn nhiều).
- ❌ `MERGE` trên bảng cực lớn không partition-aligned (dễ chậm/deadlock) — tách INSERT/UPDATE hoặc switch.
- ❌ Index thừa trên bảng ghi nóng làm chậm bulk insert EOD.

---

## 12. Tóm tắt quyết định kiến trúc

1. **Roll-forward state** (`position_state` 1M + `indexing_portfolio_ticker` 20M), KHÔNG replay mỗi ngày.
2. **EOD set-based**: ~10 câu lệnh; nặng nhất = MTM 20M dòng (1 câu, CCI batch-mode).
3. **Columnstore** (CCI/NCCI) cho fact/history + **partition theo năm** + **partition switch** nạp/archive.
4. **Customer daily perf: materialize** `indexing_performance_daily` (CCI, partition) — bắt buộc do FO-sync overwrite (không event-source được); giảm tải bằng điểm thưa / chỉ unit_price.
5. **Song song theo SI/hash**, MAXDOP, Resource Governor; **RCSI** để không chặn app.
6. **Idempotent + resumable + reconcile** trước khi publish.
7. EOD ước ~vài phút–15 phút (vs hàng giờ nếu RBAR).
