# SDI — Engine core (SQL Server)

Implement engine tính toán SDI **ALL-IN-DB** (set-based, no RBAR). App chỉ `EXEC` proc.

## Naming convention
| Đối tượng | Quy ước |
|---|---|
| Bảng | `T_` + UPPERCASE (vd `T_CUSTOMER_NAV_CURRENT`) |
| Cột | `C_` + UPPERCASE (vd `C_BUSINESS_DATE`, `C_CUST_CODE`). Riêng entity-id `SI_ID`: `PK_SI_ID`@master / `FK_SI_ID` ở bảng khác |
| Primary key | constraint `PK_<table>` |
| Foreign key | **KHÔNG hard-set constraint** — FK SI_ID đánh dấu prefix `FK_` (+ comment `FK_<child>__<parent>` khi cần) |
| Stored procedure | `SP_` |
| Function | `UDF_` |

## Thứ tự chạy
```
01_TABLES.sql      -- DDL bảng (T_/C_/PK_, PAGE compression; prod: + partition/columnstore)
02_SP_ENGINE.sql   -- engine core: UDF + SP_EOD_* + master SP_EOD_RUN + dispatcher SP_EOD_STEP
03_SMOKE.sql       -- smoke test (1 SI, 1 KH, 3 phiên) — verify số đúng
04_BENCH.sql       -- (benchmark) seed dataset lớn theo scale + chạy EOD — dùng qua bench.ps1
```
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
`J01 sync_fo (cash từ T_FO_CASH_SYNC → state; holdings FO nạp THẲNG current) → J07 compute (MTM→NAV→PnL→Unit, roll-forward, perf per-KH) → J11 SI agg → J12 SI index → J13 reconcile (cổng) → J14 snapshot → J14b history (DIFF current → interval hist, DROPPABLE)`.
(FO nạp holdings THẲNG vào T_INDEXING_PORTFOLIO_TICKER (current) + cash vào T_FO_CASH_SYNC (feed @d) cuối ngày — EOD core chỉ đọc current. T_CUSTOMER_HOLDING_HIST & T_CUSTOMER_CASH_HIST = **full history theo INTERVAL** (valid_from/valid_to, SCD-2, no-dup) do J14b DIFF current vs dòng open → đóng/mở khoảng. KHÔNG trong luồng core — bỏ J14b thì core không đổi (history dừng cập nhật). Cashflow event chỉ dùng cho CF_t.)
(J06 ACCRUE_FEE đã bỏ — FO cash đã NET phí QL + thuế GD; **NAV = stock_value + FO cash**, SDI không accrue lại để tránh double-count.)

## Đã verify (SQL Server Express)
Smoke 1 KH / 3 phiên — khớp kỳ vọng (phương án A: NAV = stock + FO cash, không accrue phí):
| Phiên | NAV | Unit | Unit Price | Daily PnL | SI Index |
|---|---|---|---|---|---|
| 02-01 | 10,000,000 | 1000 | 10,000 | 0 | 1000 |
| 05-01 | 10,440,000 | 1000 | 10,440 | 440,000 | 1044 |
| 06-01 | 10,760,000 | 1000 | 10,760 | 320,000 | 1078.8 |

28/28 job DONE (7 job × 4 phiên — gồm J14b history), reconcile pass, re-run idempotent (không double-apply). Interval verify: holding bất biến 4 phiên = 1 dòng (no-dup); rebalance phiên 07 (BBB 80000→90000) đóng dòng cũ + mở dòng mới; reconstruct @05 & @07 đúng.

> Chưa implement (mở rộng): ingestion file FO → `T_INDEXING_PORTFOLIO_TICKER` (current) + `T_FO_CASH_SYNC` (feed cash) + `T_CUSTOMER_FEE_INCOME` (cổ tức/phí, `BULK INSERT`), J15 publish→Asset, read procs `SP_GET_*` (FR-01..06), MWR (Modified Dietz set-based / XIRR qua SQL CLR), partition/columnstore prod (gồm `T_CUSTOMER_NAV_DAILY` CCI + interval hist partition theo `valid_from`).
