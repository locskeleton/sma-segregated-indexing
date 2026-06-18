# SDI — Engine core (SQL Server)

Implement engine tính toán SDI **ALL-IN-DB** (set-based, no RBAR). App chỉ `EXEC` proc.

## Naming convention
| Đối tượng | Quy ước |
|---|---|
| Bảng | `T_` + UPPERCASE (vd `T_SI_NAV_CURRENT`) |
| Cột | `C_` + UPPERCASE. **Hai cấp:** `C_MASTER_CODE` = mã **MASTER** (danh mục mẫu/chiến lược, PK `T_MASTER_PORTFOLIO`); `C_SI_ACCOUNT` = mã **SUB-ACCOUNT** (= CUST_CODE+đuôi, customer-level, sinh khi KH đầu tư 1 master). Close+reopen master ⇒ sub-account MỚI (mã khác) → 1 KH có NHIỀU sub-account/master theo thời gian (tối đa 1 ACTIVE). Cùng `C_CUST_CODE`, `C_TICKER`, `C_BENCHMARK_CODE` |
| Hai cấp dữ liệu | **MASTER-level** (key `C_MASTER_CODE`): `T_MASTER_PORTFOLIO(_TICKER)`, `T_MASTER_NAV_BALANCE`, `T_MASTER_HOLDING_BALANCE`, `T_MASTER_INDEX_DAILY`, `T_MASTER_NAV_CURRENT`. **SUB-ACCOUNT/customer-level** (key `C_SI_ACCOUNT`, giữ `C_CUST_CODE`+`C_MASTER_CODE` denormalized): holdings/cash/nav/hist/cashflow/fee/work. `T_SI_PORTFOLIO` = bảng sub-account (`C_SI_ACCOUNT` UNIQUE + filtered-unique ACTIVE per (cust,master) + `C_CLOSE_DATE`) |
| Khóa public (GUID `PK_<table>`, NEWID, IDOR-safe) | Mọi bảng (trừ master + `T_EOD_WORK` transient) có cột GUID `PK_<table>` cho API/UI. **Bảng lớn/ghi-nóng:** GUID `UNIQUE NONCLUSTERED` (`UQ_<table>_PKID`), clustered theo khóa perf. **Bảng nhỏ:** GUID làm clustered PK luôn. `T_MASTER_PORTFOLIO`: `C_MASTER_CODE` là khóa public |
| Clustered PK theo tải | append-fact lớn → **BIGINT IDENTITY** `C_<table>_ID` (`PK_<table>_ID`); point-access/join → **natural** (`PK_<table>_NK`); nhỏ → GUID (`PK_<table>`). Natural giữ `UQ_<table>_NK` cho idempotency |
| Foreign key | **KHÔNG hard-set constraint** — đánh dấu qua tên cột (`C_MASTER_CODE` → master; `FK_<table>` → surrogate) |
| Stored procedure | `SP_` |
| Function | `UDF_` |

## Thứ tự chạy
```
01_TABLES.sql      -- DDL bảng (T_/C_/PK_, PAGE compression; prod: + partition/columnstore)
02_SP_ENGINE.sql   -- engine core: UDF + SP_EOD_* + master SP_EOD_RUN + dispatcher SP_EOD_STEP
05_API.sql         -- read API: UDF_RANGE_CUTOFF + SP_GET_* (FR-01..06) cho Asset/SMO
03_SMOKE.sql       -- smoke test (1 SI, 1 KH, 4 phiên) — verify số đúng
04_BENCH.sql       -- (benchmark) seed dataset lớn theo scale + chạy EOD — dùng qua bench.ps1
```
(`05_API.sql` chạy sau `02` — read-only, không cần cho EOD/bench; cần cho API.)
(Tùy chọn `00_INFRA.sql` — DBA: filegroups, partition function/scheme, RCSI, resource governor — xem `docs/SDI-db-architecture.md`.)

## Benchmark perf (theo dõi regression sau refactor)
Smoke (1 KH) KHÔNG đo được perf — chạy `bench.ps1` với dataset lớn để bắt regression:
```powershell
cd db
./bench.ps1                  # scale large (~50k KH × 5 SI × 25 mã ≈ 6.25M vị thế) — mặc định
./bench.ps1 -Scale small     # ~1k KH — kiểm tra harness chạy nhanh
./bench.ps1 -Scale medium    # ~10k KH
```
Mỗi lần chạy: tạo DB sạch `SDI_BENCH` → seed → `SP_EOD_RUN` → đo duration per job từ `T_EOD_RUN`
→ **APPEND 1 dòng vào `perf-history.csv`** (kèm git commit + subject). Drop DB sau khi xong (trừ `-KeepDb`).

**Quy trình:** sau mỗi refactor lớn, commit xong → `./bench.ps1` → so dòng mới với dòng trước trong
`perf-history.csv` (đặc biệt `J07_ms` — MTM, job nặng nhất). Chênh lớn = refactor ảnh hưởng perf.

> `perf-history.csv` cột: timestamp_utc, commit, subject, scale, n_customers, n_positions, total_ms,
> J01_ms, J07_ms, J11_ms, J12_ms, J13_ms, J14_ms. So sánh trong CÙNG scale (số chỉ có nghĩa khi cùng tải).

## Chạy
```powershell
sqlcmd -S .\SQLEXPRESS -E -Q "CREATE DATABASE SDI_TEST;"
sqlcmd -S .\SQLEXPRESS -E -d SDI_TEST -b -f 65001 -i 01_TABLES.sql
sqlcmd -S .\SQLEXPRESS -E -d SDI_TEST -b -f 65001 -i 02_SP_ENGINE.sql
sqlcmd -S .\SQLEXPRESS -E -d SDI_TEST -b -f 65001 -i 03_SMOKE.sql
```

## EOD: app gọi 1 proc
```sql
EXEC SP_EOD_RUN @C_BUSINESS_DATE = '2026-01-06';
```
Master gọi tuần tự (idempotent + transaction + log `T_EOD_RUN`, resume từ job lỗi):
`J0 gate (chờ đủ FO ingest) → J07 compute (MTM→NAV→PnL→Unit, roll-forward, perf per-KH) → J11 SI agg → J12 SI index → J13 reconcile (cổng) → J14 snapshot`.
(J06 phí QL = **TOGGLE** qua `T_SDI_CONFIG.C_ENABLE_MGMT_FEE_ACCRUAL`. **OFF mặc định**: FO cash đã NET phí QL + thuế GD → **NAV = stock + FO cash** (tránh double-count). **ON**: SDI accrue payable ngày trong J07 + lệnh thu cuối tháng `T_SI_FEE_SCHEDULE` → FO cắt → settle; **NAV = stock + cash − payable** (net phí). Thuế GD luôn FO net.)

## Ingest FO (Kafka per-KH) — `SP_INGEST_CUSTOMER`
FO đồng bộ EOD qua **Kafka, mỗi event = 1 KH** (gồm các sub-account: cash + holdings + cổ tức/phí). App đọc event → `EXEC SP_INGEST_CUSTOMER @json` (JSON). Xử lý **NGAY khi nhận** (forward):
- **Cash** → cập nhật `T_SI_NAV_CURRENT.C_CASH` + diff interval `T_SI_CASH_HIST`.
- **Holdings** → overwrite `T_SI_PORTFOLIO_HOLDING` (current) + diff interval `T_SI_HOLDING_HIST`.
- **Cổ tức/phí** → append `T_SI_FEE_INCOME` (dedup theo `C_SOURCE_EVENT_ID`).
- Set watermark `C_LAST_SYNC_DATE=@d` (J0 GATE đếm received vs expected = tiểu khoản ACTIVE).

→ **Interval history maintain TẠI INGEST** (per-event), KHÔNG còn job J14b trong EOD → EOD batch nhẹ hẳn (đo medium: ~11s vs ~32–48s trước). **Idempotent**: cash/holdings so-trạng-thái (redelivery=no-op); fee dedup. **FORWARD-ONLY**: event quá khứ (< watermark) bị THROW (history sẽ làm sau: FO resync full D→nay + replay). Cashflow nạp/rút **KHÔNG** qua Kafka (SDI là nguồn → ghi thẳng `T_SI_CASHFLOW_EVENT`). `SP_EOD_HISTORY` giữ lại làm **utility bulk-backfill** (không trong pipeline).

## Đã verify (SQL Server Express)
Smoke 1 KH / 3 phiên — khớp kỳ vọng (phương án A: NAV = stock + FO cash, không accrue phí):
| Phiên | NAV | Unit | Unit Price | Daily PnL | SI Index |
|---|---|---|---|---|---|
| 02-01 | 10,000,000 | 1000 | 10,000 | 0 | 1000 |
| 05-01 | 10,440,000 | 1000 | 10,440 | 440,000 | 1044 |
| 06-01 | 10,760,000 | 1000 | 10,760 | 320,000 | 1078.8 |

28/28 job DONE (7 job × 4 phiên — gồm J14b history), reconcile pass, re-run idempotent (không double-apply). Interval verify: holding bất biến 4 phiên = 1 dòng (no-dup); rebalance phiên 07 (BBB 80000→90000) đóng dòng cũ + mở dòng mới; reconstruct @05 & @07 đúng.

## Read API — `05_API.sql` (FR-01..06)
Mỗi API = app `EXEC` 1 proc; tính/derive trong DB, app chỉ serialize JSON. Định danh: KH = `C_CUST_CODE`; đơn vị = **`C_SI_ACCOUNT`** (sub-account). Master suy từ sub-account.

| FR | Proc | Tham số | Trả về |
|---|---|---|---|
| FR-01 | `SP_GET_SI_OVERVIEW` | cust | RS1 breakdown từng sub-account (si_account, master, current NAV/UP + %return inception); RS2 tổng KH |
| FR-02 | `SP_GET_SI_DETAIL` | cust, si_account, range | current + **TWR** (unit_price) + **MWR** (Modified Dietz) + PnL kỳ; RS2 master-level mới nhất |
| FR-03 | `SP_GET_SI_PERFORMANCE` | cust, si_account, range | chuỗi ngày: unit_price sub-account + master UP (TR) + master index (PR) + benchmark (PR) |
| FR-04 | `SP_GET_SI_INFO` | cust, si_account | config sub-account + master (mgmt fee effective) |
| FR-05 | `SP_GET_SI_HOLDINGS` | cust, si_account, top=20 | holdings current sub-account định giá mới nhất, top-N + `OTHER` |
| FR-06 | `SP_GET_ASSET_REPORT` | cust, si_account, asOf | RS1 summary (NAV + cash/stock **reconstruct interval** + cổ tức/phí lũy kế); RS2 holdings @asOf; RS3 chi tiết |

`range` ∈ {`1M`,`3M`,`6M`,`1Y`,`3Y`,`YTD`,`INCEPTION`} — ngày mốc = phiên gần nhất ≤ cutoff; KH tham gia sau mốc → ngày sớm nhất. Verify SQL Express (data smoke): FR-01..06 đúng; reconstruct interval FR-06 @05 ra BBB=80000 (trước rebalance); MWR mid-period cashflow = 0.075 khớp Modified Dietz tay (TWR=0.2, cf_net=5M).

> Chưa implement (mở rộng): ingestion file FO → `T_SI_PORTFOLIO_HOLDING` (current) + `T_FO_CASH_SYNC` (feed cash) + `T_SI_FEE_INCOME` (cổ tức/phí, `BULK INSERT`), J15 publish→Asset, XIRR (qua SQL CLR), partition/columnstore prod (gồm `T_SI_NAV_BALANCE` CCI + interval hist partition theo `valid_from`).
