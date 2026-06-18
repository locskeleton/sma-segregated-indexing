# SDI — Kiến trúc DB & EOD ở quy mô lớn (SQL Server)

Bổ trợ cho [SDI-spec.md](./SDI-spec.md). Tập trung: chịu tải dữ liệu lớn + chạy batch chốt EOD trong cửa sổ đêm.

---

## 1. Khối lượng & điểm nóng

| Đại lượng | Số |
|---|---|
| Master (danh mục mẫu) | 100 |
| Tiểu khoản (KH × master) active | ~1.000.000 |
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

- `T_SI_NAV_CURRENT` — 1 dòng/vị thế (unit, cash, last_nav, last_unit_price) → ~1M dòng, **update tại chỗ**.
- `T_SI_PORTFOLIO_HOLDING` — 1 dòng/(vị thế × mã) (quantity, avg_cost) → ~20M dòng, update incremental.
- Event ledger (cashflow, execution, CA) chỉ **append**; dùng để recompute khi cần.

### 2.2 SET-BASED — không RBAR
Toàn bộ EOD = **một số ít câu lệnh tập hợp** (JOIN + GROUP BY + MERGE), chạy trên engine, KHÔNG cursor/loop. Bước MTM 20M dòng là **1 câu** `JOIN price + SUM GROUP BY`.

---

## 3. Mô hình bảng theo tải

| Bảng | Vai trò | Quy mô | Lưu trữ |
|---|---|---|---|
| `T_SI_NAV_CURRENT` | trạng thái hiện tại/tiểu khoản (gồm cash) | ~1M | **rowstore**, clustered PK (C_SI_ACCOUNT), PAGE compression; cân nhắc memory-optimized |
| `T_SI_PORTFOLIO_HOLDING` | holdings hiện tại — **đích FO ingest thẳng** | ~20M | **rowstore** clustered (C_SI_ACCOUNT, C_TICKER) + **NCCI** (HTAP) cho MTM. Nguồn EOD core. |
| `T_SI_HOLDING_HIST` | **HISTORY holdings INTERVAL** (valid_from..valid_to), full + no-dup | ~20M + Δ/ingest | **CCI**, partition năm(valid_from) — DIFF current→close/open **tại INGEST (per-event Kafka)**; holding bất biến = 1 dòng; KHÔNG trong EOD core |
| `T_SI_CASH_HIST` | **HISTORY cash INTERVAL**, full + no-dup | ~1M + Δ/ingest | rowstore/CCI, partition năm(valid_from) — DIFF state.cash→close/open **tại INGEST** (đối xứng holding_hist) |
| `T_SI_CASHFLOW_EVENT` | sổ cái nạp/rút | ~120M | **CCI** (clustered columnstore), partition theo năm |
| `T_SI_NAV_BALANCE` | **lịch sử perf per-tiểu-khoản** (materialize) | ~2,5 tỷ | **CCI** + partition (cần vì holdings không event-source) |
| `T_SI_UNIT_LEDGER` | unit thay đổi (cashflow) | ~120M | **CCI**, partition theo năm |
| `T_MASTER_NAV_BALANCE` | NAV master-level daily: composition + NAV + hiệu suất (gộp snapshot tài sản + hiệu suất master) | ~250K | rowstore, partition năm |
| `T_MASTER_NAV_CURRENT` | NAV/state current cấp master (1 dòng/master, overwrite EOD) — serving overview/AUM | ~100 | rowstore (nhỏ, cache RAM) |
| `T_MASTER_INDEX_DAILY` / `T_BENCHMARK_DAILY` | index daily | ~250K | rowstore |
| `T_PRICE_DAILY` | giá EOD | ~4M | rowstore, index (C_BUSINESS_DATE, C_TICKER) — nhỏ, cache RAM |
| `T_SI_FEE_INCOME` | cổ tức + phí per-tiểu-khoản (sparse, FO ingest) | ~triệu/năm | **CCI**, partition năm — nguồn FR-06; J11 Σ lên `T_MASTER_NAV_BALANCE` |

**Quyết định customer daily perf (đổi do FO-sync):** vì FO đồng bộ **snapshot overwrite** → holdings KHÔNG còn event-source → **KHÔNG derive được NAV/unit_price quá khứ** → **BẮT BUỘC materialize** `T_SI_NAV_BALANCE` (nav/unit/unit_price/day) để vẽ chart FR-03. Giảm tải: lấy **điểm thưa (tuần/tháng)** hoặc chỉ lưu `unit_price`. Lưu CCI + partition (§7.3).

---

## 4. Tính năng SQL Server bắt buộc dùng

| Tính năng | Dùng cho | Lợi ích |
|---|---|---|
| **Table Partitioning** (partition function/scheme) | mọi bảng lớn theo `business_date`/năm | quản lý, archive, partition elimination khi query |
| **Clustered Columnstore (CCI)** | event ledger, history, position_daily | nén ~10×, **batch-mode** → aggregate nhanh 10–100× |
| **Nonclustered Columnstore (NCCI)** trên rowstore | `T_SI_PORTFOLIO_HOLDING` (HTAP) | vừa update OLTP vừa MTM analytic nhanh |
| **Partition SWITCH** | nạp & archive | nạp/đẩy partition **tức thời** (metadata-only), không ghi lại dữ liệu |
| **Batch-mode on rowstore** (2019+, compat 150) | aggregate rowstore không cần columnstore | tăng tốc GROUP BY |
| **RCSI / SNAPSHOT isolation** | batch không chặn app đọc | EOD chạy song song với SMO đọc |
| **PAGE compression** | rowstore lớn | giảm I/O |
| **Memory-Optimized table** (In-Memory OLTP) | `T_SI_NAV_CURRENT` nếu update nóng | bỏ latch/lock contention |
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
| Orchestration | master proc `SP_EOD_RUN @business_date` gọi tuần tự + ghi `T_EOD_RUN` (resume); App/SQL Agent chỉ kích hoạt |
| Ingestion (feed FO/Market → staging) | proc `BULK INSERT` / `OPENROWSET` / external table — KHÔNG kéo qua app |
| API đọc (SMO/Asset) | stored proc (`SP_GET_*`); app gọi & trả JSON, không tính |
| MWR Modified Dietz | set-based trong proc |
| MWR XIRR (nếu cần, iterative) | **SQL CLR** (trong DB) — không tính ở app |

→ App = thin client: `EXEC SP_EOD_RUN` + `EXEC SP_GET_*`. Không pull-compute-push.

---

## 5. Pipeline EOD — set-based (không RBAR)

Hai pha tách rời: **INGEST** (liên tục, ngoài EOD — Kafka per-KH) duy trì state + interval history; **EOD batch** (đêm) đọc state set-based rồi publish. Idempotent + resumable.

```
INGEST (upstream, KHÔNG trong EOD) — FO bắn Kafka mỗi event = 1 KH → SP_INGEST_CUSTOMER (@json):
      - overwrite holdings → T_SI_PORTFOLIO_HOLDING ; cash → T_SI_NAV_CURRENT.C_CASH
      - cổ tức/phí → append T_SI_FEE_INCOME (dedup C_SOURCE_EVENT_ID)
      - DIFF current vs dòng open → maintain interval T_SI_HOLDING_HIST & T_SI_CASH_HIST (full history, no-dup)
      - set watermark C_LAST_SYNC_DATE=@d. FORWARD-ONLY (event quá khứ THROW).
      (Nạp/rút phát sinh SDI-side → ghi thẳng T_SI_CASHFLOW_EVENT, KHÔNG qua Kafka.)

EOD batch — SP_EOD_RUN @d, set-based, log/resume qua T_EOD_RUN:
J0_GATE       chờ đủ FO ingest: đếm tiểu khoản ACTIVE có C_LAST_SYNC_DATE=@d vs kỳ vọng → THROW nếu thiếu.
J07_COMPUTE   (câu nặng nhất) MTM TOÀN BỘ + NAV + PnL + Unit, roll-forward state:
      INSERT #nav_today (C_SI_ACCOUNT, stock_value)
      SELECT h.C_SI_ACCOUNT, SUM(h.C_QUANTITY * p.C_CLOSE_PRICE)
      FROM   T_SI_PORTFOLIO_HOLDING h
      JOIN   T_PRICE_DAILY p ON p.C_TICKER=h.C_TICKER AND p.C_BUSINESS_DATE=@d
      GROUP BY h.C_SI_ACCOUNT;                  -- batch-mode (NCCI) trên 20M dòng
      NAV = stock_value + state.cash (FO cash đã NET phí → KHÔNG trừ lại)
      PnL ngày = NAV_today − NAV_prev + ra − vào
      UNIT: vị thế có CF_t → ΔUnit = CF/unit_price_prev; unit_price = NAV/unit → INSERT T_SI_UNIT_LEDGER (ΔUnit≠0)
      (KHÔNG accrue phí: FO cash đã NET phí QL + thuế GD — tránh double-count)
J11_SI_AGG    Σ per master (C_MASTER_CODE) → T_MASTER_NAV_BALANCE (composition + NAV + hiệu suất); upsert T_MASTER_NAV_CURRENT
J12_SI_INDEX  Index_t = Index_(t-1) × Σ w^(t)·P_t/P_ref  (100 master × ~25 mã — nhẹ) → T_MASTER_INDEX_DAILY
J13_RECONCILE đối soát Σ holding qty (SDI) vs FO → bảng break; CHẶN snapshot nếu lệch quá ngưỡng
J14_SNAPSHOT  publish perf per-tiểu-khoản → T_SI_NAV_BALANCE; push delta Asset (current snapshot, không append toàn lịch sử)
```

- **INGEST per-event**: interval history maintain tại đây (DIFF current vs open-row), **KHÔNG** trong EOD → EOD core nhẹ hẳn (xem [growth-projection §7](./SDI-data-growth-projection.md)).
- **J07 delta incremental**: chỉ vị thế có event (nạp/rút/khớp/CA) → đổi unit; còn revaluation thì chạm toàn bộ.
- **J07 full revaluation**: chạm 20M dòng nhưng là **1 câu hash-aggregate batch-mode** → giây→phút.
- Không câu nào lặp từng vị thế.

---

## 6. Song song hóa & cửa sổ EOD

- **Độc lập theo master**: tiểu khoản của 1 master không ảnh hưởng master khác (chỉ chung giá). → chia batch theo **dải master (C_MASTER_CODE)** hoặc **hash(C_SI_ACCOUNT)** chạy song song N luồng.
- **MAXDOP** cho câu MTM/aggregate (để engine parallel intra-query); **Resource Governor** cấp pool riêng cho batch đêm.
- **Partition-aligned**: xử lý từng partition độc lập → switch-in song song.

**Ước lượng runtime (server ~16–32 core, NVMe):**
| Bước | Ước tính |
|---|---|
| J07 MTM 20M dòng (CCI batch-mode) | ~10–60 giây |
| J07 UPDATE NAV/PnL/Unit ~1M dòng | ~1–3 phút |
| J11–J12 aggregate + index | ~giây |
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
FO sync overwrite holdings → không event-source → derive-on-read lịch sử **không khả thi** → **phải materialize** `T_SI_NAV_BALANCE`:
- Lưu CCI, **partition hash(C_SI_ACCOUNT) + năm** (hoặc **ordered CCI** theo (C_SI_ACCOUNT, C_BUSINESS_DATE)) để segment-elimination khi đọc 1 tiểu khoản. Tránh nonclustered rowstore index trên 2,5 tỷ (phình ~trăm GB).
- EOD: bulk ~1M dòng/ngày vào CCI (rẻ). Đọc chart tiểu khoản: đọc thẳng (nhanh).
- **Giảm tải**: lấy điểm **thưa (tuần/tháng)** thay vì daily, hoặc chỉ lưu cột `unit_price` (+nav) — narrow CCI nén rất tốt.

---

## 8. Indexing & thiết kế khóa

- **Khóa clustering**: dùng khóa **hẹp, tăng dần, không GUID** (GUID ngẫu nhiên → page split, fragmentation). Dùng `BIGINT IDENTITY` hoặc khóa tự nhiên hẹp.
- `T_SI_NAV_CURRENT`: clustered PK `(C_SI_ACCOUNT)` — point update/lookup.
- `T_SI_PORTFOLIO_HOLDING`: clustered `(C_SI_ACCOUNT, C_TICKER)` + **NCCI** (cho J07 MTM).
- Event ledger (CCI): partition `(C_BUSINESS_DATE)`; CCI tự lo, thêm **nonclustered rowstore** `(C_SI_ACCOUNT, C_BUSINESS_DATE)` nếu cần truy vết theo tiểu khoản.
- Master-level daily: clustered `(C_MASTER_CODE, C_BUSINESS_DATE)`.
- `T_PRICE_DAILY`: clustered `(C_BUSINESS_DATE, C_TICKER)` — nhỏ, cache buffer pool.
- **Avoid**: index thừa trên bảng ghi nóng (chậm insert); EAV; nvarchar(max) trong fact.

---

## 9. Idempotency, resume, đối soát

- **Idempotent**: mỗi bước ghi vào staging gắn `@d`; publish bằng SWITCH/MERGE theo PK → chạy lại 1 ngày ra cùng kết quả.
- **Resume**: bảng `T_EOD_RUN(business_date, step, status, rows, ts)`; fail giữa chừng → tiếp từ step lỗi.
- **Recompute lịch sử**: xóa daily series từ ngày X + reset state về snapshot tháng → replay event ledger (CCI scan nhanh).
- **Đối soát (reconcile)** = job `J13` (sau J07 MTM): `Σ holding qty per (C_SI_ACCOUNT,C_TICKER)` (SDI) vs holdings thật FO → bảng break; chặn J14 snapshot nếu lệch quá ngưỡng.
- **RCSI** bật → app đọc current snapshot không bị batch chặn; publish cuối cùng là thao tác ngắn.

---

## 10. Lưu trữ & nén (ước tính)

> **Nén LÀM GIẢM dung lượng, không tăng** (CCI ~10×, PAGE ~2–4×). Mọi con số dưới là **sau nén**.

| Bảng | Dòng | Sau nén |
|---|---|---|
| T_SI_NAV_CURRENT (rowstore PAGE) | 1M | ~vài trăm MB |
| T_SI_PORTFOLIO_HOLDING (rowstore + NCCI) | 20M | ~vài GB |
| event ledger (T_SI_CASHFLOW_EVENT/T_SI_UNIT_LEDGER, CCI) | ~240M | ~chục GB |
| master-level daily (rowstore) | ~1M | ~nhỏ |
| **`T_SI_NAV_BALANCE` (CCI)** | **~2,5 tỷ** | **~100–300 GB** (bắt buộc — do FO-sync, xem §7.3) |

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
> **Chuẩn PK (chốt 2026-06-18):** mọi bảng có cột GUID `PK_<table>` (NEWID, random, IDOR-safe) = **khóa public API/UI**. Cách **cluster** chọn theo tải để giữ tốc độ EOD:
> - **Bảng lớn/ghi-nóng EOD** → **clustered = khóa perf** (BIGINT IDENTITY cho append-fact / natural cho point-access+join), GUID là **UNIQUE NONCLUSTERED** (`UQ_<table>_PKID`). Khóa natural giữ `UQ_<table>_NK` cho idempotency.
> - **Bảng nhỏ/ghi-thưa** → **GUID làm clustered PK luôn** (`PK_<table>`), natural `UQ_<table>_NK`. (volume thấp ⇒ fragmentation không đáng kể, gọn 1 surrogate.)
> - `T_MASTER_PORTFOLIO`: PK = `C_MASTER_CODE` (mã nghiệp vụ, đã là khóa public — không GUID). `T_EOD_WORK`: transient, natural PK, KHÔNG GUID.

| Bảng | Clustered PK | GUID public | UNIQUE natural `_NK` |
|---|---|---|---|
| T_MASTER_PORTFOLIO | C_MASTER_CODE (natural) | — | — |
| **T_SI_NAV_BALANCE** ~2,5 tỷ | C_NAV_BALANCE_ID (BIGINT) | PK_… (nc) | (C_BUSINESS_DATE,C_SI_ACCOUNT) |
| **T_SI_HOLDING_HIST** ~20M | C_HOLDING_HIST_ID (BIGINT) | PK_… (nc) | (C_SI_ACCOUNT,C_TICKER,C_VALID_FROM) +filtered open IX |
| **T_SI_CASH_HIST** | C_CASH_HIST_ID (BIGINT) | PK_… (nc) | (C_SI_ACCOUNT,C_VALID_FROM) +filtered open IX |
| **T_SI_CASHFLOW_EVENT** ~120M | C_EVENT_ID (BIGINT) | PK_… (nc) | — |
| **T_SI_UNIT_LEDGER** ~120M | C_SI_UNIT_LEDGER_ID (BIGINT) | PK_… (nc) | (C_SI_ACCOUNT,C_BUSINESS_DATE) |
| **T_SI_PORTFOLIO_HOLDING** ~20M | (C_SI_ACCOUNT,C_TICKER) natural | PK_… (nc) | — (PK là natural) |
| **T_SI_NAV_CURRENT** ~1M | (C_SI_ACCOUNT) natural | PK_… (nc) | — |
| **T_PRICE_DAILY** | (C_BUSINESS_DATE,C_TICKER) natural | PK_… (nc) | — |
| T_EOD_WORK (transient) | (C_BUSINESS_DATE,C_SI_ACCOUNT) natural | — | — |
| T_SI_PORTFOLIO | PK_SI_PORTFOLIO (GUID) | (clustered) | (C_SI_ACCOUNT) + filtered-unique ACTIVE (C_CUST_CODE,C_MASTER_CODE) |
| T_MASTER_PORTFOLIO_TICKER | PK_… (GUID) | (clustered) | (C_MASTER_CODE,C_EFFECTIVE_DATE,C_TICKER) |
| T_REBALANCE_REQUEST | PK_… (GUID) | (clustered) | (C_REQUEST_ID) |
| T_CORPORATE_ACTION | PK_… (GUID) | (clustered) | (C_TICKER,C_EX_DATE,C_CA_TYPE) |
| T_BENCHMARK_DAILY | PK_… (GUID) | (clustered) | (C_BENCHMARK_CODE,C_BUSINESS_DATE) |
| T_SI_FEE_INCOME | PK_… (GUID) | (clustered) | (C_EVENT_ID) + filtered-unique (C_SOURCE_EVENT_ID) |
| T_MASTER_NAV_BALANCE / _INDEX_DAILY / _HOLDING_BALANCE / _NAV_CURRENT | PK_… (GUID) | (clustered) | natural per bảng |
| T_EOD_RUN | PK_EOD_RUN (GUID) | (clustered) | (C_BUSINESS_DATE,C_JOB) |

→ **Đo thật (medium 1,25M, SQL Express — thời điểm interval-insert CÒN là job EOD `J14b`):** GUID-clustered MỌI bảng = **~40s**; mixed (perf-clustered cho 8 bảng nóng + GUID nonclustered) = **~32s**; baseline không GUID = **~20s**. Driver chi phí ~2× = **chỉ mục GUID nonclustered (NEWID random) phải maintain khi insert khối lớn** vào history. **Lưu ý:** interval-insert nay đã **chuyển sang INGEST (per-event Kafka)** → overhead GUID này áp ở **ingest-time, KHÔNG trong EOD core** (EOD nhẹ hơn nhiều). Tất cả vẫn << SLA 10 phút ⇒ chấp nhận để mọi row addressable qua API/UI.
→ Muốn giảm overhead GUID ở history: bỏ GUID ở `holding_hist`/`cash_hist` (API FR-06 địa chỉ theo `C_SI_ACCOUNT`+asOf, KHÔNG cần GUID per-row của history) — để ngỏ, chưa làm.
→ **Prod (nav_balance tỷ-dòng):** chuyển sang **CCI clustered + GUID nonclustered** + partition; FILLFACTOR + REORG/REBUILD định kỳ cho bảng GUID-clustered.

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
- ❌ **GUID ngẫu nhiên làm clustered key** trên **bảng lớn/ghi-nóng** (fragmentation, page split) → policy: bảng lớn cluster theo BIGINT IDENTITY/natural, GUID để nonclustered. GUID-clustered chỉ cho bảng nhỏ/ghi-thưa.
- ❌ Nonclustered rowstore index nặng trên bảng tỷ-dòng (phình + chậm ghi) — ưu tiên columnstore + partition elimination.
- ❌ Lưu daily HOLDINGS snapshot per-KH (~50 tỷ) — thay vào đó materialize daily PERF (~2,5 tỷ, nhỏ hơn nhiều).
- ❌ `MERGE` trên bảng cực lớn không partition-aligned (dễ chậm/deadlock) — tách INSERT/UPDATE hoặc switch.
- ❌ Index thừa trên bảng ghi nóng làm chậm bulk insert EOD.

---

## 12. Tóm tắt quyết định kiến trúc

1. **Roll-forward state** (`T_SI_NAV_CURRENT` 1M + `T_SI_PORTFOLIO_HOLDING` 20M), KHÔNG replay mỗi ngày.
2. **EOD set-based**: ~10 câu lệnh; nặng nhất = MTM 20M dòng (1 câu, CCI batch-mode).
3. **Columnstore** (CCI/NCCI) cho fact/history + **partition theo năm** + **partition switch** nạp/archive.
4. **Customer daily perf: materialize** `T_SI_NAV_BALANCE` (CCI, partition) — bắt buộc do FO-sync overwrite (không event-source được); giảm tải bằng điểm thưa / chỉ unit_price.
5. **Song song theo master/hash(C_SI_ACCOUNT)**, MAXDOP, Resource Governor; **RCSI** để không chặn app.
6. **Idempotent + resumable + reconcile** trước khi publish.
7. EOD ước ~vài phút–15 phút (vs hàng giờ nếu RBAR).
