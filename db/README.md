# SDI — Engine core (SQL Server)

Implement engine tính toán SDI **ALL-IN-DB** (set-based, no RBAR). App chỉ `EXEC` proc.

## Naming convention
| Đối tượng | Quy ước |
|---|---|
| Bảng | `T_` + UPPERCASE (vd `T_POSITION_STATE`) |
| Cột | `C_` + UPPERCASE (vd `C_BUSINESS_DATE`) |
| Primary key | `PK_<table>` |
| Foreign key | **KHÔNG hard-set constraint** — chỉ NAMING `FK_<child>__<parent>` trong comment để tham chiếu |
| Stored procedure | `SP_` |
| Function | `UDF_` |

## Thứ tự chạy
```
01_TABLES.sql      -- DDL bảng (T_/C_/PK_, PAGE compression; prod: + partition/columnstore)
02_SP_ENGINE.sql   -- engine core: UDF + SP_EOD_* + master SP_EOD_RUN + dispatcher SP_EOD_STEP
03_SMOKE.sql       -- smoke test (1 SI, 1 KH, 3 phiên) — verify số đúng
```
(Tùy chọn `00_INFRA.sql` — DBA: filegroups, partition function/scheme, RCSI, resource governor — xem `docs/SDI-db-architecture.md`.)

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
`J01 sync_fo (mirror holdings + cash per-KH từ snapshot T_SI_POSITION_HOLDING_DAILY; biến động/ngày suy ra on-demand) → J07 compute (MTM→NAV→PnL→Unit, roll-forward, perf per-KH) → J11 SI agg → J12 SI index → J13 reconcile (cổng) → J14 snapshot`.
(FO đồng bộ holdings+cash TỪNG KH cuối ngày — SDI không quản lý từng lệnh khớp. Snapshot holdings dated giữ trong T_SI_POSITION_HOLDING_DAILY (audit/tái dựng); biến động net/ngày suy ra on-demand khi cần. Cashflow event chỉ dùng cho CF_t.)
(J06 ACCRUE_FEE đã bỏ — FO cash đã NET phí QL + thuế GD; **NAV = stock_value + FO cash**, SDI không accrue lại để tránh double-count.)

## Đã verify (SQL Server Express)
Smoke 1 KH / 3 phiên — khớp kỳ vọng (phương án A: NAV = stock + FO cash, không accrue phí):
| Phiên | NAV | Unit | Unit Price | Daily PnL | SI Index |
|---|---|---|---|---|---|
| 02-01 | 10,000,000 | 1000 | 10,000 | 0 | 1000 |
| 05-01 | 10,440,000 | 1000 | 10,440 | 440,000 | 1044 |
| 06-01 | 10,760,000 | 1000 | 10,760 | 320,000 | 1078.8 |

18/18 job DONE (6 job × 3 phiên), reconcile pass, re-run idempotent (không double-apply).

> Chưa implement (mở rộng): ingestion file FO → `T_SI_POSITION_HOLDING_DAILY` + `T_FO_CASH_SYNC` (`BULK INSERT`), J15 publish→Asset, read procs `SP_GET_*` (FR-01..06), MWR (Modified Dietz set-based / XIRR qua SQL CLR), partition/columnstore prod (gồm `T_INDEXING_PERFORMANCE_DAILY` CCI).
