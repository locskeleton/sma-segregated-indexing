# SDI — Engine core (SQL Server)

Implement engine tính toán SDI **ALL-IN-DB** (set-based, no RBAR). App chỉ `EXEC` proc.

> **⚠️ BRD 2026-06-22 — SDI→Asset gỡ 2 / giữ 1:** BO/FO/Market nay đẩy dữ liệu **THẲNG sang Asset** và **Asset tự tính** (tài sản gộp + NAV ròng + Unit/TWR). SDI **KHÔNG còn đồng bộ tài sản KH/NAV-perf master sang Asset**. Đã gỡ khỏi `05_API.sql` **2 producer**: `SP_GET_ASSET_SNAPSHOT` (per-SI tài sản), `SP_GET_ASSET_MASTER_SNAPSHOT` (master NAV/perf). **VẪN GIỮ `SP_GET_ASSET_INDEX_SNAPSHOT`** — SDI vẫn đẩy Master Index sang Asset theo luồng RIÊNG khi BO báo price-ready (path `SP_EOD_RUN_INDEX`); đây là luồng SDI→Asset DUY NHẤT còn lại. **BO KHÔNG đẩy phí QL accrued** — Asset NAV theo model REALIZED (phí trừ khi BO cắt thật, qua cash); **AUM = NAV** (SDI đã gỡ accrual `C_PAYABLE_FEE`/`C_FEE_ACCUM`/J06/`T_FEE_CONFIG`/`SP_INGEST_FEE_CHARGE`). Read API (`SP_GET_SI_*` FR-01..06, PM `SP_GET_MASTER_*`) GIỮ NGUYÊN — phục vụ **giao diện riêng của SDI**. **`SP_GET_ASSET_REPORT` (FR-06) là read API báo cáo tài sản KH — GIỮ** (đừng nhầm với `SP_GET_ASSET_SNAPSHOT` đã gỡ). **Đã GỠ stage asset-sync trong pipeline** (`SP_EOD_SET_ASSET_SYNCED` + cột `C_ASSET_SYNC_*` + trạng thái `COMPLETED`) — trạng thái CUỐI pipeline nay = `EOD_DONE` (reconcile PASS); luồng index (`SP_EOD_RUN_INDEX` → Asset) VẪN chạy. Còn lại = reconcile R3 TWR (R1 payable đã gỡ — 2 hệ đều không accrue). Xem [docs/SDI-asset-gap.md](../docs/SDI-asset-gap.md).

> **⚠️ THIN-LAYER (chốt 2026-06-26):** SDI **KHÔNG tự tính hiệu suất** nữa. Asset gửi per-KH per-ngày **`aum` + `daily_return` (TWR)**; SDI chỉ **LƯU + SERVE** (compound `daily_return` on-read cho %PnL kỳ; AUM-weighted cho master/PM; TE từ prefix-sum). Đã GỠ: `unit`/`unit_price`/`T_SI_UNIT_LEDGER`/J09 PnL/J10 Unit/master pooled UP/`stock_value`+HOLDINGS_MISMATCH. Bảng đổi tên (drop `NAV_`): `T_SI_BALANCE`, `T_SI_CURRENT`, `T_MASTER_BALANCE`, `T_MASTER_CURRENT`; cột `C_NAV`→`C_AUM`. 2 API return gộp 1 (`SP_GET_MASTER_RETURN_COMPOUND`; bỏ RET_INDEX). **⚠️ Một số mô tả `unit`/`unit_price`/`TWR-do-SDI-tính` ở các doc bên dưới là LỊCH SỬ — đang cập nhật.**

## Naming convention
| Đối tượng | Quy ước |
|---|---|
| Bảng | `T_` + UPPERCASE (vd `T_SI_CURRENT`) |
| Cột | `C_` + UPPERCASE. **Hai cấp:** `C_MASTER_CODE` = mã **MASTER** (danh mục mẫu/chiến lược, PK `T_MASTER_PORTFOLIO`); `C_SI_ACCOUNT` = mã **SUB-ACCOUNT** (= CUST_CODE+đuôi, customer-level, sinh khi KH đầu tư 1 master). Close+reopen master ⇒ sub-account MỚI (mã khác) → 1 KH có NHIỀU sub-account/master theo thời gian (tối đa 1 ACTIVE). Cùng `C_CUST_CODE`, `C_TICKER`, `C_BENCHMARK_CODE` |
| Hai cấp dữ liệu | **MASTER-level** (key `C_MASTER_CODE`): `T_MASTER_PORTFOLIO(_TICKER)`, `T_MASTER_BALANCE`, `T_MASTER_HOLDING_BALANCE`, `T_MASTER_INDEX_DAILY`, `T_MASTER_CURRENT`. **SUB-ACCOUNT/customer-level** (key `C_SI_ACCOUNT`, giữ `C_CUST_CODE`+`C_MASTER_CODE` denormalized): holdings/cash/nav/hist/cashflow/fee/work. `T_SI_PORTFOLIO` = bảng sub-account (`C_SI_ACCOUNT` UNIQUE + filtered-unique ACTIVE per (cust,master) + `C_CLOSE_DATE`) |
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
09_FEE.sql         -- phí QL: 4 bảng phí + SP_FEE_RUN_DAILY (điểm vào — app gọi MỖI NGÀY LỊCH, NGOÀI EOD) = accrue + close; collect/BO-result + UDF nợ phí (DEBT/ACCRUING) + 3 báo cáo (SP_RPT_FEE_DAILY/_CHARGE/_COLLECTION). (Lịch GD T_TRADING_HOLIDAY + UDF_IS_BUSINESS_DATE đã lên CORE 01/02.)
03_SMOKE.sql       -- smoke test core (1 SI, 1 KH, 4 phiên) — verify số đúng
07_PM_SMOKE.sql    -- smoke PM (1 master × 3 KH × 3 phiên) — verify AUM-weighted/TE/deviation/dist/top-N
10_FEE_SMOKE.sql   -- smoke phí (case 30/4-1/5 tách 2 dòng + FIFO collect + BO result + hook) — cần 09
04_BENCH.sql       -- (benchmark) seed dataset lớn theo scale + chạy EOD — dùng qua bench.ps1
```
(`05_API`/`06_PM_API` chạy sau `02` — read-only, không cần cho EOD/bench; cần cho API. `07_PM_SMOKE` cần `06`.)
(`09_FEE` chạy sau `02` — phí tách riêng AUM; hook trong `SP_EOD_SET_SOURCE_READY` resolve runtime qua deferred-name nên KHÔNG cài `09` thì EOD vẫn chạy (guard skip fee). `10_FEE_SMOKE` cần `09`.)
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

## Lịch giao dịch (`T_TRADING_HOLIDAY`) — CORE, không phải chuyện riêng của phí
**`UDF_IS_BUSINESS_DATE(@d)`** = định nghĩa DUY NHẤT của "ngày GD": `KHÔNG T7/CN` **AND** `KHÔNG có trong T_TRADING_HOLIDAY`. *(Trước 2026-07-11, lịch chỉ nằm trong `09_FEE.sql`; engine thì suy ngày GD từ "`T_PRICE_DAILY` có dòng" → app backfill giá theo **lịch dương** (nguồn giá carry-forward phiên gần nhất cho ngày nghỉ) biến T7/CN/lễ thành **phiên giả**, và master index — chuỗi **nhân dồn** — nhân thêm 1 factor mỗi ngày nghỉ ⇒ **1 cuối tuần sai +21%** (1210 → 1464.10). Nay lịch nằm ở CORE, mọi đường TÍNH đều gate bằng nó.)*

**Cái gì bị gate, cái gì KHÔNG:**

| | Ngày T7/CN/lễ |
|---|---|
| `SP_INGEST_ASSET_NAV` (aum, tiền, **tiền khả dụng**, daily_return) | ✅ **VẪN NHẬN** — Asset gửi **mọi ngày lịch (365)**; nạp/rút cuối tuần vẫn đổi AUM. Ngày nghỉ `daily_return` phải = 0/NULL ([contract](../docs/SDI-daily-return-contract.md)) |
| **Phí QL** — **`SP_FEE_RUN_DAILY`** (09_FEE, **NGOÀI EOD**) | ✅ **VẪN CHẠY** — app gọi **mỗi ngày lịch**, ngay sau ingest Asset. Phí theo **ngày dương lịch**, base = **AUM của CHÍNH ngày đó** ⇒ nạp/rút cuối tuần vào base phí ngay hôm đó. Chốt kỳ ở **ngày CUỐI THÁNG dương lịch** (kể cả T7/CN/lễ); **chưa chốt tháng chưa thu**. ⚠️ Cố tình KHÔNG để trong `SP_EOD_RUN` — EOD bị gate ngày GD nên sẽ **mất phí ~115 ngày nghỉ/năm** |
| `SP_INGEST_PRICE_DAILY` (giá) | ❌ **err=23** — sở không có phiên thì không có giá |
| `SP_EOD_RUN` / `SP_EOD_RUN_INDEX` / `SP_EOD_SET_SOURCE_READY` | ❌ **err=13** |
| `SP_EOD_SI_INDEX` (gọi thẳng) | ❌ **THROW 51013** trước mọi DML |
| `SP_EOD_RECOMPUTE_INDEX_RANGE` | nhận **dải ngày dương lịch**, tự bỏ ngày nghỉ |
| `UDF_PREV_BUSINESS_DATE` / `UDF_LAST_BUSINESS_DATE` | chỉ trả **phiên GD** (không anchor vào T7/CN/lễ) |

⇒ **Dù dòng giá ngày nghỉ LỌT vào `T_PRICE_DAILY` (INSERT thẳng), index vẫn ĐÚNG** — smoke có test đúng case này.

```sql
-- ops/BO nạp lịch nghỉ (T7/CN KHÔNG cần khai — rule tự loại). is_delete=1 để gỡ.
DECLARE @ec INT, @em NVARCHAR(400);
EXEC SP_INGEST_TRADING_HOLIDAY N'[{"holiday_date":"2026-02-17","note":"Tết Bính Ngọ"}]',
     @p_user='ops', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
```
⚠️ Seed `01_TABLES.sql` **CHỈ có lễ dương lịch cố định** (1/1, 30/4, 1/5, 2/9 — 2025..2027). **Tết / Giỗ Tổ / nghỉ bù là âm lịch ⇒ ops PHẢI nạp hằng năm.** Thiếu 1 ngày lễ mà hôm đó có dòng giá ⇒ index cộng dồn sai đúng ngày đó. Nạp lễ cho ngày QUÁ KHỨ đã tính index ⇒ phải chạy lại `SP_EOD_RECOMPUTE_INDEX_RANGE` **từ inception**.

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
-- (BRD 2026-06-22: bỏ bước SP_EOD_SET_ASSET_SYNCED — SDI không push asset sang Asset; EOD_DONE = trạng thái cuối)
-- sửa nguồn rồi chạy lại: EXEC SP_EOD_RESET @p_business_date='2026-01-06', ... (đặt watermark C_EOD_RESET_AT
--   để gate cho job chạy LẠI — GIỮ NGUYÊN T_EOD_RUN làm log/audit, KHÔNG xóa; clear break; recompute)
```

### Sửa quá khứ — RE-INGEST Asset NAV (KHÔNG reconstruct-from-history)
[BRD asset-sync] **`SP_EOD_RECOMPUTE_RANGE` ĐÃ GỠ**: SDI không tự tính NAV nên không reconstruct từ history. NAV ngày quá khứ SAI → **Asset GỬI LẠI** dòng `T_SI_ASSET_DAILY` ngày đó (`SP_INGEST_ASSET_NAV` idempotent: DELETE+INSERT theo `(date, si)`), rồi **chạy lại** compute/agg/TE ngày đó để derive lại unit/UP/PnL/lũy kế:
```sql
DECLARE @ec INT, @em NVARCHAR(400);
-- Asset re-feed NAV ngày cũ (idempotent) rồi tính lại chuỗi từ ngày đó:
EXEC SP_INGEST_ASSET_NAV @p_json=N'[{"si_account":"...","nav":...,"stock_value":...,"cash":...,"cash_in":0,"cash_out":0}]',
     @p_business_date='2026-01-06', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
EXEC SP_EOD_COMPUTE   '2026-01-06';   -- derive lại unit/UP/PnL/return (NAV lấy từ Asset)
EXEC SP_EOD_SI_AGG    '2026-01-06';   -- master agg (AUM/unit price)
EXEC SP_EOD_TE_ACCUM  '2026-01-06';   -- lũy kế active return (TE prefix-sum)
```
- **Index master** sửa riêng: `SP_EOD_RECOMPUTE_INDEX_RANGE(@from,@to)` (loop từ inception) — GIỮ (index do SDI tính từ giá×weight, không phụ thuộc Asset NAV).
  - **Nạp rổ CHỈ qua `SP_INGEST_MASTER_PORTFOLIO_TICKER`** (thêm 2026-08-01). Một lần nạp = **TOÀN BỘ rổ** tại `@p_effective_date`, không phải phần thay đổi. **Gỡ mã = ghi `weight = 0`**, không được bỏ trống. Ghi thẳng vào bảng là bỏ qua cổng: rổ sót mã vẫn cho ra index "hợp lệ" mà **sai im lặng** (đo thật: rổ `RA 0.6 + RB 0.4`, sót `RB` ⇒ index **1200.00** thay vì 1040.00, `err=0`) — vì `FACTOR` chuẩn hoá bằng `/Σw` nên mất mã không làm vỡ thang, chỉ lặng lẽ đổi sang theo dõi rổ khác.
    - `err`: `20` tham số/JSON · `21` master không tồn tại/CLOSED · `22` dòng sai (rỗng/trùng/âm) · `23` Σweight sai thang · `24` **SÓT MÃ** so với version trước.
    - Vì sao "gỡ = weight 0" mới chặn được sót mã: với cơ chế **vắng mặt**, *"gỡ mã"* và *"sót mã"* trông **y hệt nhau**, không phân biệt nổi. Có dòng 0 thì hợp đồng thành *"version mới phải liệt kê mọi mã weight>0 của version liền trước"* ⇒ kiểm được.
    - Dòng `0` chỉ cần mang **một lần** ở version ngay sau khi gỡ ⇒ rổ không phình.
    - J12 lọc `weight <> 0` khi **đòi giá** và khi **tính** — nếu không, mã vừa gỡ (thường đã huỷ niêm yết) bị completeness đòi giá ⇒ `THROW 51011` chặn index **mọi master**, vĩnh viễn.
  - **`T_MASTER_PORTFOLIO_TICKER_HIST`** — vết audit mọi lần nạp (version đã tính index **vẫn được sửa tại chỗ**, nên bảng chính chỉ giữ trạng thái hiện tại). ⚠️ Xếp hạng "rổ tại thời điểm T" phải dùng **`C_BATCH_SEQ`**, không dùng `C_CONFIRM_TIME`: `SYSDATETIME()` mịn ~1ms nên hai lần nạp liên tiếp trùng mốc là bình thường (smoke bắt được ngay lần chạy đầu). Và phải **`RANK`/`DENSE_RANK`**, không `ROW_NUMBER` — một batch có nhiều dòng cùng `C_BATCH_SEQ`, `ROW_NUMBER` đánh số *dòng* nên chỉ giữ 1 mã.
  - **Scope as-of** (sửa 2026-07-31): định nghĩa DUY NHẤT ở iTVF **`UDF_INDEX_BASKET_ASOF(@d)`** — `C_STATUS='ACTIVE'` + `C_INCEPTION_DATE <= @d` + rổ hiệu lực (`MAX(eff_date) ≤ @d`), `LEFT JOIN` giá cùng ngày. `SP_EOD_SI_INDEX` đổ nó vào `@basket` **một lần** rồi validate *và* tính trên chính bộ đó; `SP_EOD_RUN_INDEX` gate gọi thẳng iTVF ⇒ **không thể lệch scope**. Trước đó vị từ bị chép 4 lần — đúng class bug đã dính. `C_INCEPTION_DATE` **NOT NULL** — nó là **vị từ tính toán**, không phải metadata hiển thị. Thiếu nó thì rổ **backdate** kéo master mới ngược về ngày chưa tồn tại: mã trong rổ niêm yết sau ⇒ completeness (**all-or-nothing xuyên master**) `THROW 51011` mỗi ngày ⇒ **chặn luôn việc tính lại của mọi master hợp lệ khác**.
  - Master **CLOSED**: không tính lại (đóng băng lịch sử).
  - `DELETE` trong `SP_EOD_SI_INDEX` **cố ý rộng hơn** `INSERT` (không lọc inception) ⇒ recompute **tự dọn** index ma mà bản cũ đã ghi ở ngày trước inception.

> ### ⚠️ Nâng cấp DB đã chạy — `C_INCEPTION_DATE` từ nay là **VỊ TỪ TÍNH TOÁN**, không còn là metadata
>
> **Trước khi chạy recompute lịch sử**, soi những master có index SỚM HƠN inception:
> ```sql
> SELECT mp.C_MASTER_CODE, mp.C_INCEPTION_DATE, MIN(idx.C_BUSINESS_DATE) AS first_index_date
> FROM T_MASTER_PORTFOLIO mp
> INNER JOIN T_MASTER_INDEX_DAILY idx ON idx.C_MASTER_CODE = mp.C_MASTER_CODE
> GROUP BY mp.C_MASTER_CODE, mp.C_INCEPTION_DATE
> HAVING MIN(idx.C_BUSINESS_DATE) < mp.C_INCEPTION_DATE;
> ```
> Mỗi dòng trả về là **một trong hai**, và ops phải phân biệt trước khi chạy:
> - **index MA** do bản cũ sinh ra ở ngày master chưa tồn tại → recompute dọn giúp, đúng ý.
> - **inception KHAI SAI** (điền muộn hơn thực tế) → recompute sẽ **XOÁ lịch sử index THẬT** và không ghi lại. Sửa `C_INCEPTION_DATE` cho đúng **trước**.
>
> Schema đã đổi sang `NOT NULL`. DB dựng từ script cũ phải chạy:
> ```sql
> UPDATE T_MASTER_PORTFOLIO SET C_INCEPTION_DATE = '<ngày thật>' WHERE C_INCEPTION_DATE IS NULL;
> ALTER TABLE T_MASTER_PORTFOLIO ALTER COLUMN C_INCEPTION_DATE DATE NOT NULL;
> ```
> Chưa chạy mà còn NULL → `SP_EOD_SI_INDEX` **THROW 51014** (recompute → `err=14`) thay vì loại master im lặng.
- **Composition** (`T_MASTER_HOLDING_BALANCE`) as-of: FO gửi lại holdings ngày đó (qua `SP_INGEST_CUSTOMER`) nếu cần sửa.
- **Idempotent**: re-ingest cùng ngày → DELETE+INSERT, chạy lại compute = ghi đè sạch (`T_SI_BALANCE` UQ theo (date,si)).

> **`SP_EOD_COMPUTE_CORE @p_d`** = lõi công thức per-ngày (CF → NAV=Asset gửi (KHÔNG tự tính/trừ) → J09 PnL → J10 Unit → ghi `T_SI_UNIT_LEDGER` + `T_SI_BALANCE` SCOPE theo các tiểu khoản trong `T_EOD_WORK`), chạy trên `T_EOD_WORK` ĐÃ seed. **Forward (`SP_EOD_COMPUTE`) + rerun DÙNG CHUNG** lõi này (DRY, 1 công thức). Forward = seed từ `T_SI_ASSET_DAILY` (Asset NAV) + EXEC core + roll-forward `NAV_CURRENT`. [BRD] SDI KHÔNG accrue phí (không J06/payable).
**Trạng thái `T_EOD_PIPELINE`**: MKT_DATA (PENDING|READY — pull API BO, không đếm) + FO_INGEST (PENDING|READY, kèm total/received cust_code) → INDEX (PENDING|DONE) →
EOD (PENDING|RUNNING|DONE|FAILED) → RECONCILE (PENDING|PASS|**BREAK**);
overall WAITING_DATA→READY→EOD_RUNNING→(RECONCILE_BREAK | **EOD_DONE**). **EOD_DONE = trạng thái CUỐI khi reconcile PASS** (BRD 2026-06-22: bỏ ASSET_SYNC/COMPLETED — SDI không push asset sang Asset).

**Master index TÁCH khỏi pipeline customer** (`SP_EOD_RUN_INDEX`, trigger khi BO ready): chỉ cần giá + target
weight, KHÔNG cần FO → tính+lưu+sync Asset sớm, độc lập. Pipeline customer `SP_EOD_RUN` (cần MKT/FO READY +
INDEX DONE) gọi tuần tự:
`J0 gate → J07 compute (MTM→NAV→PnL→Unit) → J11 SI agg → J12B TE accum (đọc index đã tính) → J13 reconcile (recorder break) → [CỔNG: có break ⇒ chặn, KHÔNG chạy J14] → J14 snapshot`.
(J13 GHI chi tiết lệch vào **`T_EOD_RECON_BREAK`** — KHÔNG throw để break được commit; `SP_EOD_RUN` đọc bảng break sau J13 → có break ⇒ RECONCILE=BREAK + chặn publish. Data J07-J12B đã commit từng step → nghiệp vụ soi break trên data đó; sửa nguồn → `SP_EOD_RESET` → chạy lại.)
(J12B `SP_EOD_TE_ACCUM` [PM tool]: lũy kế per-KH `C_ACCUM_ACTIVE_RET/_SQ` + `C_RET_DAY_COUNT` vào `T_SI_BALANCE` — active return = KH return − master index return; chạy sau J12 vì cần index daily return. Cho phép tính TE qua range BẤT KỲ bằng HIỆU 2 mốc base/end (prefix-sum) → serve-layer PM đọc 2 lát thay vì quét lịch sử. Idempotent: accum@d = accum@prev + a@d. Bench medium ~676ms/phiên.)
([BRD asset-sync] **SDI KHÔNG còn accrue phí QL.** Asset gửi NAV RÒNG (đã trừ phí QL sẵn) → SDI dùng trực tiếp; BO KHÔNG gửi số accrued (phí chỉ effect khi BO cắt thật, qua cash — model realized). **AUM = NAV** (gross = net, không tách payable). Đã GỠ: cột `C_PAYABLE_FEE` + `C_FEE_ACCUM` + J06 accrue + catalog `T_FEE_CONFIG` + `SP_INGEST_FEE_CHARGE` + ledger `T_SI_INCOME_FEE`. Thuế GD/phí lưu ký: FO/Asset net thẳng vào cash.)

## Ingest FO (Kafka per-KH) — `SP_INGEST_CUSTOMER`
FO đồng bộ EOD qua **Kafka, mỗi event = 1 KH** (gồm các sub-account: cash + holdings + cổ tức/phí). App đọc event → `EXEC SP_INGEST_CUSTOMER @json` (JSON). Xử lý **NGAY khi nhận** (forward):
- **Cash** → cập nhật `T_SI_CURRENT` (`C_CASH`+`C_PENDING_CASH`+`C_DIV_CASH`) + diff interval `T_SI_CASH_HIST` (**đủ 3 khoản** `C_CASH`+`C_PENDING_CASH`+`C_DIV_CASH` — trước chỉ cash; SCD-2: dòng mới khi BẤT KỲ khoản nào đổi → reconstruct receivables AS-OF cho rerun quá khứ). Maintain tại ingest + `SP_EOD_HISTORY`.
- **Holdings** → overwrite `T_SI_PORTFOLIO_HOLDING` (current) + diff interval `T_SI_HOLDING_HIST`.
- **Cổ tức/phí**: [BRD asset-sync] đã gộp trong tiền/NAV ròng Asset gửi (SDI KHÔNG quản chi tiết income/fee; `T_SI_INCOME_FEE` + accrue + `T_FEE_CONFIG` + `SP_INGEST_FEE_CHARGE` đã GỠ).
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
| FR-06 | `SP_GET_ASSET_REPORT` | si_account, asOf | RS1 summary (NAV + cash + stock từ Asset @asOf; **AUM = stock + cash = NAV** — phí QL đã trừ sẵn trong NAV, KHÔNG có cột phí accrued); RS2 holdings @asOf. [BRD asset-sync] **đã GỠ RS3/RS4/RS5** (chi tiết income/fee) + cột `C_FEE_ACCRUED_TOTAL`/`C_PAYABLE_FEE` + guard Option B — SDI không quản chi tiết phí |

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

Công thức (spec §2): AUM = `C_LAST_AUM` (per KH; phí QL đã trừ trong NAV Asset gửi ⇒ AUM = NAV); DM tổng KH = AUM-weighted end-weight (`ΣWᵢ·PnLᵢ`, PnL=TWR unit_price; 2 API đối chiếu RET_INDEX/COMPOUND cùng kết quả); deviation = (R_KH−R_master_index)×10000 BPS; TE per-KH = `STDEV(R_KH,t−R_master,t)×√X` (X=#ngày GD, cap 252), master = AUM-weighted. Ngưỡng per-master `T_MASTER_PM_CONFIG` (NULL→default `UDF_PM_CONFIG`). Verify: `07_PM_SMOKE.sql` (3 KH, số tính tay — KH_ret=.08/master=.071/dev=90BPS/TE≈.0297 MED/histogram/top-N đúng).

> Chưa implement (mở rộng): ingestion file FO → `T_SI_PORTFOLIO_HOLDING` (current) + `T_FO_CASH_SYNC` (feed cash) + `T_SI_INCOME_FEE` (cổ tức/phí, `BULK INSERT`), XIRR (qua SQL CLR), partition/columnstore prod (gồm `T_SI_BALANCE` CCI + interval hist partition theo `valid_from`). *(J15 publish tài sản KH + master NAV/perf → Asset đã bỏ — BRD 2026-06-22; Master Index VẪN đẩy Asset qua `SP_EOD_RUN_INDEX` (`SP_GET_ASSET_INDEX_SNAPSHOT`). Xem [docs/SDI-asset-gap.md](../docs/SDI-asset-gap.md).)*
