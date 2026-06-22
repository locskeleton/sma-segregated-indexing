# SDI — Engine core (SQL Server)

Implement engine tính toán SDI **ALL-IN-DB** (set-based, no RBAR). App chỉ `EXEC` proc.

> **⚠️ BRD 2026-06-22 — SDI→Asset gỡ 2 / giữ 1:** BO/FO/Market nay đẩy dữ liệu **THẲNG sang Asset** và **Asset tự tính** (tài sản gộp + NAV ròng + Unit/TWR). SDI **KHÔNG còn đồng bộ tài sản KH/NAV-perf master sang Asset**. Đã gỡ khỏi `05_API.sql` **2 producer**: `SP_GET_ASSET_SNAPSHOT` (per-SI tài sản), `SP_GET_ASSET_MASTER_SNAPSHOT` (master NAV/perf). **VẪN GIỮ `SP_GET_ASSET_INDEX_SNAPSHOT`** — SDI vẫn đẩy Master Index sang Asset theo luồng RIÊNG khi BO báo price-ready (path `SP_EOD_RUN_INDEX`); đây là luồng SDI→Asset DUY NHẤT còn lại. **BO còn đẩy phí QL accrued/ngày → Asset** (Asset tự tính NAV ròng). Read API (`SP_GET_SI_*` FR-01..06, PM `SP_GET_MASTER_*`) GIỮ NGUYÊN — phục vụ **giao diện riêng của SDI**. **`SP_GET_ASSET_REPORT` (FR-06) là read API báo cáo tài sản KH — GIỮ** (đừng nhầm với `SP_GET_ASSET_SNAPSHOT` đã gỡ). Bước asset-sync customer trong pipeline (ASSET_SYNC tài sản KH / `SP_EOD_SET_ASSET_SYNCED` / "publish sang Asset" bên dưới) phản ánh mô hình CŨ — không còn push tài sản KH/master NAV; nhưng luồng index (`SP_EOD_RUN_INDEX` → Asset) VẪN chạy. Còn lại = reconcile R1 payable + R3 TWR. Xem [docs/SDI-asset-gap.md](../docs/SDI-asset-gap.md).

## Naming convention
| Đối tượng | Quy ước |
|---|---|
| Bảng | `T_` + UPPERCASE (vd `T_SI_NAV_CURRENT`) |
| Cột | `C_` + UPPERCASE. **Hai cấp:** `C_MASTER_CODE` = mã **MASTER** (danh mục mẫu/chiến lược, PK `T_MASTER_PORTFOLIO`); `C_SI_ACCOUNT` = mã **SUB-ACCOUNT** (= CUST_CODE+đuôi, customer-level, sinh khi KH đầu tư 1 master). Close+reopen master ⇒ sub-account MỚI (mã khác) → 1 KH có NHIỀU sub-account/master theo thời gian (tối đa 1 ACTIVE). Cùng `C_CUST_CODE`, `C_TICKER`, `C_BENCHMARK_CODE` |
| Hai cấp dữ liệu | **MASTER-level** (key `C_MASTER_CODE`): `T_MASTER_PORTFOLIO(_TICKER)`, `T_MASTER_NAV_BALANCE`, `T_MASTER_HOLDING_BALANCE`, `T_MASTER_INDEX_DAILY`, `T_MASTER_NAV_CURRENT`. **SUB-ACCOUNT/customer-level** (key `C_SI_ACCOUNT`, giữ `C_CUST_CODE`+`C_MASTER_CODE` denormalized): holdings/cash/nav/hist/cashflow/fee/work. `T_SI_PORTFOLIO` = bảng sub-account (`C_SI_ACCOUNT` UNIQUE + filtered-unique ACTIVE per (cust,master) + `C_CLOSE_DATE`) |
| Khóa public (GUID `PK_<table>`, NEWID, IDOR-safe) | Mọi bảng (trừ master + `T_EOD_WORK` transient) có cột GUID `PK_<table>` cho API/UI. **Bảng lớn/ghi-nóng:** GUID `UNIQUE NONCLUSTERED` (`UQ_<table>_PKID`), clustered theo khóa perf. **Bảng nhỏ:** GUID làm clustered PK luôn. `T_MASTER_PORTFOLIO`: `C_MASTER_CODE` là khóa public |
| Clustered PK theo tải | append-fact lớn → **BIGINT IDENTITY** `C_<table>_ID` (`PK_<table>_ID`); point-access/join → **natural** (`PK_<table>_NK`); nhỏ → GUID (`PK_<table>`). Natural giữ `UQ_<table>_NK` cho idempotency |
| Foreign key | **KHÔNG hard-set constraint** — đánh dấu qua tên cột (`C_MASTER_CODE` → master; `FK_<table>` → surrogate) |
| Stored procedure | `SP_` + UPPERCASE (vd `SP_EOD_RUN`, `SP_GET_SI_DETAIL`) |
| Tham số SP | **BẮT BUỘC prefix `@p_`** (vd `@p_si_account`, `@p_d`, `@p_json`, `@p_range`) — phân biệt tham số với biến cục bộ (`@local`) trong proc |
| Tham số SP cho API | SP phục vụ API (`SP_GET_*`, `SP_SET_*`) **BẮT BUỘC** thêm 3 tham số chuẩn: `@p_user` (định danh người gọi — audit/authz), `@p_err_code INT OUTPUT` + `@p_err_msg NVARCHAR(400) OUTPUT` (trả mã/thông điệp lỗi về app, KHÔNG THROW ra ngoài). Quy ước: `@p_err_code=0` = OK, ≠0 = lỗi. **Engine SP nội bộ** (`SP_EOD_STEP`/`SP_EOD_COMPUTE`/`SP_EOD_*` job, `SP_INGEST_*`) lỗi → `THROW` (chỉ áp dụng rule prefix `@p_`). **NGOẠI LỆ — `SP_EOD_RUN`** (orchestrator app gọi trực tiếp): bọc TRY/CATCH, trả lỗi qua `@p_err_code`/`@p_err_msg` OUT (KHÔNG THROW); step nội bộ vẫn THROW + log FAILED `T_EOD_RUN`, orchestrator bắt lại |
| Function | `UDF_` |

## Thứ tự chạy
```
01_TABLES.sql      -- DDL bảng (T_/C_/PK_, PAGE compression; prod: + partition/columnstore)
02_SP_ENGINE.sql   -- engine core: UDF + SP_EOD_* + master SP_EOD_RUN + dispatcher SP_EOD_STEP
05_API.sql         -- read API KH: UDF_RANGE_CUTOFF + SP_GET_SI_* (FR-01..06) cho UI riêng SDI (BRD 2026-06-22: gỡ 2 producer SP_GET_ASSET_SNAPSHOT + _MASTER_SNAPSHOT; GIỮ SP_GET_ASSET_INDEX_SNAPSHOT — đẩy Master Index khi BO price-ready)
06_PM_API.sql      -- read API PM (master-keyed): UDF_PM_CONFIG + SP_GET_MASTER_*/PM_OVERVIEW_ALL + SP_SET_MASTER_PM_CONFIG
03_SMOKE.sql       -- smoke test core (1 SI, 1 KH, 4 phiên) — verify số đúng
07_PM_SMOKE.sql    -- smoke PM (1 master × 3 KH × 3 phiên) — verify AUM-weighted/TE/deviation/dist/top-N
04_BENCH.sql       -- (benchmark) seed dataset lớn theo scale + chạy EOD — dùng qua bench.ps1
```
(`05_API`/`06_PM_API` chạy sau `02` — read-only, không cần cho EOD/bench; cần cho API. `07_PM_SMOKE` cần `06`.)
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

## EOD: pipeline control (T_EOD_PIPELINE) + app gọi 1 proc
Toàn luồng /ngày track ở **`T_EOD_PIPELINE`** (1 dòng/ngày): upstream → EOD → đối soát → Asset.
```sql
DECLARE @ec INT, @em NVARCHAR(400);
-- 1) BO báo market data ready → app pull API BO 1 lần → mark READY (KHÔNG total/đếm — dữ liệu thị trường).
EXEC SP_EOD_SET_SOURCE_READY @p_business_date='2026-01-06', @p_source='MKT_DATA', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
-- 2) MASTER INDEX — LUỒNG RIÊNG (chỉ cần BO): tính + lưu → app publish index sang Asset luôn.
EXEC SP_EOD_RUN_INDEX @p_business_date='2026-01-06', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
-- 3) FO ready — completeness: @p_total_record = tổng cust_code FO gửi (break event); SDI đếm received, READY khi >=.
EXEC SP_EOD_SET_SOURCE_READY @p_business_date='2026-01-06', @p_source='FO_INGEST', @p_total_record=50000, @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
-- 4) EOD customer (CHẶN nếu chưa MKT/FO=READY + INDEX=DONE → @ec=10). @ec=-2 = reconcile BREAK.
EXEC SP_EOD_RUN @p_business_date='2026-01-06', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
-- 5) app publish Kafka sang Asset xong → báo lại
EXEC SP_EOD_SET_ASSET_SYNCED @p_business_date='2026-01-06', @p_status='DONE', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
-- sửa nguồn rồi chạy lại: EXEC SP_EOD_RESET @p_business_date='2026-01-06', ... (xóa job+break, recompute)
```
**Trạng thái `T_EOD_PIPELINE`**: MKT_DATA (PENDING|READY — pull API BO, không đếm) + FO_INGEST (PENDING|READY, kèm total/received cust_code) → INDEX (PENDING|DONE) →
EOD (PENDING|RUNNING|DONE|FAILED) → RECONCILE (PENDING|PASS|**BREAK**) → ASSET_SYNC (PENDING|DONE|FAILED);
overall WAITING_DATA→READY→EOD_RUNNING→(RECONCILE_BREAK | EOD_DONE)→COMPLETED. **Chỉ COMPLETED khi reconcile PASS + asset DONE.**

**Master index TÁCH khỏi pipeline customer** (`SP_EOD_RUN_INDEX`, trigger khi BO ready): chỉ cần giá + target
weight, KHÔNG cần FO → tính+lưu+sync Asset sớm, độc lập. Pipeline customer `SP_EOD_RUN` (cần MKT/FO READY +
INDEX DONE) gọi tuần tự:
`J0 gate → J07 compute (MTM→NAV→PnL→Unit) → J11 SI agg → J12B TE accum (đọc index đã tính) → J13 reconcile (recorder break) → [CỔNG: có break ⇒ chặn, KHÔNG chạy J14] → J14 snapshot`.
(J13 GHI chi tiết lệch vào **`T_EOD_RECON_BREAK`** — KHÔNG throw để break được commit; `SP_EOD_RUN` đọc bảng break sau J13 → có break ⇒ RECONCILE=BREAK + chặn publish. Data J07-J12B đã commit từng step → nghiệp vụ soi break trên data đó; sửa nguồn → `SP_EOD_RESET` → chạy lại.)
(J12B `SP_EOD_TE_ACCUM` [PM tool]: lũy kế per-KH `C_ACCUM_ACTIVE_RET/_SQ` + `C_RET_DAY_COUNT` vào `T_SI_NAV_BALANCE` — active return = KH return − master index return; chạy sau J12 vì cần index daily return. Cho phép tính TE qua range BẤT KỲ bằng HIỆU 2 mốc base/end (prefix-sum) → serve-layer PM đọc 2 lát thay vì quét lịch sử. Idempotent: accum@d = accum@prev + a@d. Bench medium ~676ms/phiên.)
(J06 phí ACCRUE = **BO-driven, ĐA-LOẠI config-driven**: SDI accrue payable hằng ngày theo NGÀY DƯƠNG LỊCH trong J07 cho MỌI dòng `C_FEE_GROUP='PAYABLE' AND C_RATE>0` khai trong catalog `T_FEE_CONFIG` (GLOBAL toàn hệ, 1 dòng/loại áp mọi master; per-master defer) (`payable += AUM_gross × DATEDIFF(ngày) × Σ(C_RATE/C_DAY_COUNT)`, mỗi loại có rate + day_count riêng [default 365]; INCOME / rate NULL không accrue). `C_FEE_TYPE` + `C_FEE_GROUP` khớp vocabulary `T_SI_INCOME_FEE`. Thêm chính sách phí mới = INSERT 1 dòng config, KHÔNG sửa schema/SP. BO cắt phí 1 cục/tháng → event Kafka → `SP_INGEST_FEE_CHARGE` net-off payable (log `T_SI_INCOME_FEE` group PAYABLE, type theo `fee_type` của charge [default MGMT_FEE], dedup `C_SOURCE_EVENT_ID`). **`C_PAYABLE_FEE` = TỔNG phí phải trả ACCRUED chưa cắt của MỌI loại; NAV = total_asset − payable**; total_asset = stock + cash + tiền bán chờ về + cổ tức tiền (gồm receivables). Phí point-event trừ thẳng cash (thuế GD, phí lưu ký) KHÔNG khai config → không accrue. Đã BỎ `T_SDI_CONFIG`/`T_SI_FEE_SCHEDULE`/`SP_EOD_FEE_CHARGE`.)

## Ingest FO (Kafka per-KH) — `SP_INGEST_CUSTOMER`
FO đồng bộ EOD qua **Kafka, mỗi event = 1 KH** (gồm các sub-account: cash + holdings + cổ tức/phí). App đọc event → `EXEC SP_INGEST_CUSTOMER @json` (JSON). Xử lý **NGAY khi nhận** (forward):
- **Cash** → cập nhật `T_SI_NAV_CURRENT.C_CASH` + diff interval `T_SI_CASH_HIST`.
- **Holdings** → overwrite `T_SI_PORTFOLIO_HOLDING` (current) + diff interval `T_SI_HOLDING_HIST`.
- **Cổ tức/phí** → append `T_SI_INCOME_FEE` (DIVIDEND group INCOME, CUSTODY_FEE group PAYABLE; dedup theo `C_SOURCE_EVENT_ID`). (Phí ACCRUE đa-loại khai trong catalog `T_FEE_CONFIG` GLOBAL toàn hệ, dòng group=PAYABLE & rate>0; type+group khớp `T_SI_INCOME_FEE`; BO cắt phí qua `SP_INGEST_FEE_CHARGE` mang `fee_type`.)
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
Mỗi API = app `EXEC` 1 proc; tính/derive trong DB, app chỉ serialize JSON. Định danh: KH = `C_CUST_CODE` (chỉ FR-01 list theo KH); đơn vị per-si = **`C_SI_ACCOUNT`** (UNIQUE toàn cục → đủ định danh, KHÔNG cần truyền cust; master suy từ sub-account).

| FR | Proc | Tham số | Trả về |
|---|---|---|---|
| FR-01 | `SP_GET_SI_OVERVIEW` | cust | RS1 breakdown từng sub-account (si_account, master, current NAV/UP + %return inception); RS2 tổng KH |
| FR-02 | `SP_GET_SI_DETAIL` | si_account, range | current + **TWR** (unit_price) + **MWR** (Modified Dietz) + PnL kỳ; RS2 master-level mới nhất |
| FR-03 | `SP_GET_SI_PERFORMANCE` | si_account, range | chuỗi ngày: unit_price sub-account + master UP (TR) + master index (PR) + benchmark (PR) |
| FR-04 | `SP_GET_SI_INFO` | si_account | config sub-account + master (mgmt fee effective) |
| FR-05 | `SP_GET_SI_HOLDINGS` | si_account, top=20 | holdings current sub-account định giá mới nhất, top-N + `OTHER` |
| FR-06 | `SP_GET_ASSET_REPORT` | si_account, asOf | RS1 summary (NAV + cash/stock **reconstruct interval** + cổ tức/phí lưu ký lũy kế + **phí: đã thu `C_ACCUM_MGMT_FEE_PAID` + accrued `C_FEE_ACCRUED_TOTAL`** [tổng phí phải trả accrued chưa cắt @asOf, mọi loại]); RS2 holdings @asOf; RS3 chi tiết cổ tức/phí lưu ký; **RS4 chi tiết lệnh thu phí**; **RS5 kê khoản phải trả per-type (Option B)** (`C_FEE_TYPE`, `C_FEE_PENDING`=`C_PAYABLE_FEE` đã lưu @asOf [exact, khớp NAV], `C_FEE_PAID`=Σ cắt loại đó, `C_FEE_ACCRUED`=pending+paid; payable = 1 tổng dồn, KHÔNG reconstruct, bảng dày `T_SI_FEE_ACCRUAL` đã bỏ; GUARD `err_code=4` nếu >1 loại accrue → cần nâng cấp JSON per-type) |

`range` ∈ {`1D`,`1W`,`MTD`,`1M`,`3M`/`3T`,`6M`/`6T`,`QTD`,`1Y`,`3Y`,`YTD`,`INCEPTION`} — ngày mốc = phiên gần nhất ≤ cutoff; KH tham gia sau mốc → ngày sớm nhất. Verify SQL Express (data smoke): FR-01..06 đúng; reconstruct interval FR-06 @05 ra BBB=80000 (trước rebalance); MWR mid-period cashflow = 0.075 khớp Modified Dietz tay (TWR=0.2, cf_net=5M).

## PM tool API — `06_PM_API.sql` (dashboard quản lý master)
Serve-layer **on-read** cho PM theo dõi cấp **master** (spec: `docs/SDI-pm-tool-spec.md`). master-keyed (`@C_MASTER_CODE`), KHÔNG trả định danh KH ngoài top-N (US5). 2 bản chất: snapshot (current) realtime-on-query · hiệu suất (T-1).

| US | Proc | Tham số | Trả về |
|---|---|---|---|
| cfg | `SP_SET_MASTER_PM_CONFIG` | master + 6 ngưỡng (NULL=default) | upsert `T_MASTER_PM_CONFIG`; RS cấu hình hiệu lực |
| US2 | `SP_GET_MASTER_OVERVIEW` | master, range | AUM+growth, net in/out, AUM-weighted TE+badge+#vượt, cash drag+#vượt, deviation+#dev>A/<B |
| US3 | `SP_GET_MASTER_PERFORMANCE` | master, range, resolution(auto) | RS1 chuỗi 3 đường (master index PR + KH AUM-weighted + benchmark); RS2 mốc rebalance |
| US3 | `SP_GET_MASTER_REBALANCE_DETAIL` | master, date | RS1 target weight cũ→mới; RS2 net delta holdings (`T_MASTER_HOLDING_BALANCE`) |
| US4 | `SP_GET_MASTER_PNL_DIST` | master, range | #lãi/#lỗ+tỷ lệ, avg AUM-weighted, trung vị, histogram %PnL |
| US5 | `SP_GET_MASTER_TOP_KH` | master, range, topN, dir | top-N mã KH theo %PnL (TWR) |
| US1 | `SP_GET_PM_OVERVIEW_ALL` | range, sort | RS1 #master/#KH; RS2 tổng (ΣAUM+growth, net in/out, cash drag, #master cash>ngưỡng); RS3 list master |

Công thức (spec §2): AUM = total_asset = `C_LAST_NAV+C_PAYABLE_FEE` (per KH); DM tổng KH = AUM-weighted end-weight (`ΣWᵢ·PnLᵢ`, PnL=TWR unit_price); deviation = (R_KH−R_master_index)×10000 BPS; TE per-KH = `STDEV(R_KH,t−R_master,t)×√X` (X=#ngày GD, cap 252), master = AUM-weighted. Ngưỡng per-master `T_MASTER_PM_CONFIG` (NULL→default `UDF_PM_CONFIG`). Verify: `07_PM_SMOKE.sql` (3 KH, số tính tay — KH_ret=.08/master=.071/dev=90BPS/TE≈.0297 MED/histogram/top-N đúng).

> Chưa implement (mở rộng): ingestion file FO → `T_SI_PORTFOLIO_HOLDING` (current) + `T_FO_CASH_SYNC` (feed cash) + `T_SI_INCOME_FEE` (cổ tức/phí, `BULK INSERT`), XIRR (qua SQL CLR), partition/columnstore prod (gồm `T_SI_NAV_BALANCE` CCI + interval hist partition theo `valid_from`). *(J15 publish tài sản KH + master NAV/perf → Asset đã bỏ — BRD 2026-06-22; Master Index VẪN đẩy Asset qua `SP_EOD_RUN_INDEX` (`SP_GET_ASSET_INDEX_SNAPSHOT`). Xem [docs/SDI-asset-gap.md](../docs/SDI-asset-gap.md).)*
