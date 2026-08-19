# Snapshot near-realtime từ FO + khung job chạy nền

> **Bài toán:** SDI định kỳ gọi sang FO lấy ảnh chụp tài sản của khách hàng indexing trong phiên, gộp lên cấp master để PM dashboard nhìn được số gần thời gian thực.
>
> **Cần trước hết là hạ tầng:** một khung chạy job nền (1) dùng được cho **mọi** loại job, (2) đẩy vào là chạy ngay, (3) chạy nhiều pod mà không xử lý trùng, (4) riêng job gọi FO thì **chỉ** 9h–15h ngày giao dịch.
>
> **Hiện thực:** [`db/11_JOB.sql`](../db/11_JOB.sql) · [`db/12_JOB_SMOKE.sql`](../db/12_JOB_SMOKE.sql) · [`reference/csharp/job/`](../reference/csharp/job/)

---

## 0. Ba nguyên tắc — kế thừa từ [thiết kế Kafka batch sync](SDI-kafka-batch-sync-design.md)

> ### ① Redis lo TỐC ĐỘ. DB lo TÍNH ĐÚNG.
> Redis Streams là **đường vận chuyển + chuông cửa**, không phải sổ cái. Mất sạch Redis chỉ được phép làm **chậm**, không được phép làm **mất job** hay **chạy hai lần**.
>
> ### ② Chống trùng phải là RÀNG BUỘC DỮ LIỆU, không phải một lời hứa của middleware.
> "Consumer group giao mỗi message cho một consumer" là đúng — nhưng nó không hứa *xử lý đúng một lần*.
>
> ### ③ Dòng giữa phiên KHÔNG BAO GIỜ được đóng vai số chốt.
> Đây là rủi ro lớn nhất của phương án "đổ thẳng vào bảng EOD", và nó sai **âm thầm**.

---

## 1. Hai tầng, đừng trộn

| | Tầng A — KHUNG JOB | Tầng B — NGHIỆP VỤ FO |
|---|---|---|
| Biết gì về FO | **Không gì cả** | Tất cả |
| Bảng | `T_JOB_DEFINITION`, `T_JOB_RUN` | `T_SI_BALANCE`, `T_MASTER_BALANCE` (cột `C_SRC`) |
| Proc | `SP_JOB_ENQUEUE(_DUE)` · `_CLAIM` · `_HEARTBEAT` · `_COMPLETE` · `_REAP` · `_PURGE` | `SP_GET_FO_SNAPSHOT_SCOPE` · `SP_INGEST_FO_SNAPSHOT_RT` · `SP_RT_MASTER_AGG` · `SP_GET_PM_RT_OVERVIEW` |
| Thêm job mới | INSERT 1 dòng + 1 class C# | — |

Thêm loại job thứ hai (dọn dẹp, tính lại index, đẩy báo cáo…) **không sửa một dòng nào** ở tầng A. Đó là thước đo duy nhất của chữ "generic".

---

## 2. Vòng đời một lượt chạy

```
                      ┌──────────────── SP_JOB_ENQUEUE_DUE (mọi pod, 10s)
                      │                 hoặc SP_JOB_ENQUEUE (ai cũng đẩy được)
                      ▼
   T_JOB_RUN  ──── READY ──┐
                      ▲    │  XADD {jobRunId} → Redis Stream
                      │    ▼
                      │  worker XREADGROUP  ──►  SP_JOB_CLAIM  ──► RUNNING ──► DONE
                      │                              │ err=5 (pod khác giữ) → XACK, bỏ qua
                      │                              │ err=3 (ngoài giờ)    → SKIPPED
                      │                              │ err=6 (hết lượt thử) → DEAD
                      │                              ▼
                      └──── SP_JOB_REAP ◄──── lease hết hạn (pod chết)
                            (30s, mọi pod)
```

**Chỉ có `jobRunId` đi trong stream.** Không payload, không trạng thái. Worker cầm id rồi hỏi DB mọi thứ khác — nếu chở bản chụp cấu hình trong message thì một job vừa bị sửa/tắt vẫn chạy theo bản cũ đang nằm trong stream.

---

## 3. Bốn yêu cầu BRD — đáp ứng bằng cái gì

### (1) Chạy được với MỌI loại job

`T_JOB_DEFINITION.C_HANDLER` là khoá tra trong `JobRegistry` phía C#. DB không biết handler làm gì.

```sql
INSERT INTO T_JOB_DEFINITION (C_JOB_CODE, C_JOB_NAME, C_HANDLER, C_INTERVAL_SEC, C_TIMEOUT_SEC)
VALUES ('CLEANUP_TMP', N'Dọn bảng tạm', 'CleanupJobHandler', 3600, 600);
```

Job **không** định kỳ thì để `C_INTERVAL_SEC = NULL` — nó chỉ chạy khi có người đẩy. Job **không** giới hạn giờ thì để `C_WINDOW_FROM/TO = NULL`.

Smoke `12_JOB_SMOKE.sql` khối (A) cố tình chạy toàn bộ vòng đời trên một job giả `SMK_ANY` **không liên quan gì tới FO** — để chứng minh khung không cưới lấy một nghiệp vụ.

### (2) Đẩy job vào là xử lý ngay

`SP_JOB_ENQUEUE` trả `@p_job_run_id` → app `XADD` → worker đang ở vòng đọc stream nhặt trong ~500ms. Không có vòng chờ, không polling DB.

`fire_key` làm cho việc đẩy trở nên **idempotent**: cùng `requestId` gọi 10 lần vẫn đúng một lượt chạy (`err=4`, trả id cũ).

### (3) Nhiều pod, không xử lý trùng — ba lớp

| Lớp | Bắt được gì | Không bắt được gì |
|---|---|---|
| Consumer group Redis | Đường chạy bình thường: 1 entry → 1 pod | `XAUTOCLAIM`, `XADD` trùng, pod restart |
| **`SP_JOB_CLAIM`** (`UPDATE ... WHERE C_STATUS='READY'`) | **Mọi thứ.** 20 pod cầm cùng id ⇒ đúng 1 thắng | Pod thắng rồi treo |
| Heartbeat có kiểm chủ sở hữu | Pod treo → lease hết → pod khác giành; pod cũ tỉnh dậy nhận `still_mine=false` → tự dừng | — |

Lớp 2 là chốt thật. Lớp 1 chỉ để **rẻ** (đỡ 19 lần gọi DB vô ích), lớp 3 để pod zombie không ghi song song.

Ngoài ra `UQ_JOB_RUN_NK (C_JOB_CODE, C_FIRE_KEY)` chặn trùng **ngay từ khâu sinh job**: 10 pod cùng quét thấy slot 9:15 tới hạn, cùng INSERT — CSDL cho đúng một pod thắng, 9 pod nhận lỗi trùng khoá và im lặng bỏ qua. **Không cần leader election.**

### (4) FO chỉ chạy 9h–15h ngày GD — bốn tầng

| Tầng | Ở đâu | Bắt ca gì |
|---|---|---|
| 1 | `SP_JOB_ENQUEUE(_DUE)` | Không sinh lượt chạy lúc 15h30 |
| 2 | `SP_JOB_CLAIM` | **Job sinh lúc 14h59, pod nhặt lúc 15h02** (stream tồn đọng, pod restart, reaper trả lại) → `SKIPPED` |
| 3 | `SP_INGEST_FO_SNAPSHOT_RT` | Gọi proc bằng tay; ghi ngày không phải hôm nay; ngày nghỉ |
| **4** | `TradingWindowGuard` (C#) | **Chu kỳ 1000 batch khởi động 14h50, tới batch 700 thì đã 15h02** |

Tầng 4 là tầng **duy nhất** bắt được ca cuối: tầng 1 và 2 chỉ kiểm **một lần, lúc bắt đầu**, còn một chu kỳ quét thì kéo dài nhiều phút. Guard đặt **ngay trước mỗi HTTP call**, không phải mỗi N batch — kiểm thưa ra là mở lại một khe hở đúng bằng N batch, và khe đó sẽ được lấp vào đúng ngày chu kỳ chạy chậm nhất.

**Luật giờ chỉ định nghĩa MỘT chỗ:** `UDF_JOB_IN_WINDOW` đọc `T_JOB_DEFINITION`. Cả tầng 1, 2, 3 đều gọi đúng hàm đó; tầng 4 nhận khung giờ qua DI từ chính bảng đó. Không hard-code 9h–15h trong code C#.

**Biên `[from, to)`**: 15:00:00 chẵn là **đã đóng**. "Đến 3h chiều" nghĩa là phiên hết lúc 3h, không phải "còn được gọi thêm một nhịp lúc 3h".

**Đồng hồ**: `UDF_JOB_NOW()` neo `SYSUTCDATETIME()` rồi đổi sang giờ VN. `GETDATE()` trả giờ hệ điều hành — một pod SQL chạy UTC là khung 9h–15h lệch 7 tiếng, tức job gọi FO lúc 16h–22h giờ VN. Phía C# dùng `TimeZoneInfo` cùng múi, thử cả tên Windows lẫn Linux.

---

## 4. Dữ liệu T0 đổ vào đâu — và cái giá của nó

**Quyết định:** ghi **thẳng** vào `T_SI_BALANCE` / `T_MASTER_BALANCE` của ngày hôm nay, upsert theo khoá tự nhiên sẵn có.

Điều đó có nghĩa là trong cùng một bảng, cùng một ngày, tồn tại hai loại dòng trông **y hệt nhau** với mọi câu truy vấn cũ. Nên bắt buộc phải có cột phân biệt:

```sql
C_SRC VARCHAR(3) NOT NULL DEFAULT 'EOD'   -- 'EOD' = số chốt · 'RT' = ảnh chụp giữa phiên
C_RT_AT DATETIME NULL                     -- thời điểm FO chụp (chỉ dòng RT)
```

`DEFAULT 'EOD'` là có chủ đích: mọi đường ghi cũ giữ nguyên hành vi, không phải sửa.

### Bốn chỗ sẽ hỏng nếu không lọc `C_SRC='EOD'`

| # | Nơi | Hỏng thế nào | Mức |
|---|---|---|---|
| 1 | `SP_EOD_RUN` **err=12** — "mọi SI ACTIVE phải có dòng `T_SI_BALANCE @d`" | Job RT đổ dòng cho **mọi** tiểu khoản lúc 9h ⇒ cổng khoá thật của cả pipeline **PASS GIẢ** dù Asset chưa gửi một dòng chốt nào ⇒ EOD chạy trên số giữa phiên | ☠️ |
| 2 | `SP_EOD_FEE_ACCRUE` — base phí = `C_AUM @d` | Thu phí trên AUM lúc 9h15. Phí đã chốt kỳ thì **bất biến** ⇒ sai là phải đi đòi/hoàn | ☠️ |
| 3 | `MAX(C_BUSINESS_DATE)` ở **~30 chỗ** trong `05_API` / `06_PM_API` | "Phiên gần nhất" nhảy sang hôm nay lúc 9h01 ⇒ báo cáo AUM, TWR, TE, deviation đọc số giữa phiên. Kéo theo `@cutoff` lệch 1 ngày ⇒ **mọi** kỳ 1M/3M/YTD lệch mốc đầu; 2 lát TE trượt khỏi dòng có prefix-sum ⇒ TE = NULL hàng loạt | ⚠️ |
| 4 | `SP_EOD_TE_ACCUM` — lát `@prev` | Dòng RT có `accum = 0` ⇒ prefix-sum TE **reset về 0 giữa chuỗi** ⇒ TE của mọi kỳ chứa ngày đó sai, và sai không NULL, không lỗi | ⚠️ |

Smoke `12_JOB_SMOKE.sql` ca **B7** đo trực tiếp cái (1):

```
có lọc = 2 (đúng: vẫn thấy thiếu 2 KH)
không lọc = 0 (sẽ mở cổng oan)
```

### Hai luật bất di bất dịch

**① RT không bao giờ đè EOD.** `MERGE ... WHEN MATCHED AND t.C_SRC='RT' THEN UPDATE`. Dòng chốt đã tồn tại ⇒ batch RT tới muộn (retry, pod zombie, chạy tay) bị bỏ qua và **đếm vào `@p_skipped_eod`** — có số để log, không im lặng.

Chiều ngược lại thì **được**: `SP_INGEST_ASSET_NAV` `DELETE+INSERT` theo `(date, si)` xoá bất kể nguồn ⇒ cuối ngày dòng RT **tan** vào dòng chốt. Đó là chỗ hai thế giới gặp nhau.

**② RT chỉ ghi cho hôm nay, chỉ ngày GD.** Ghi lùi quá khứ là vô nghĩa (quá khứ đã chốt) và là đường ngắn nhất để hỏng lịch sử.

### `C_DAILY_RETURN` của dòng RT = `NULL`, không phải `0`

Lợi suất ngày trong SDI là **TWR đã khử dòng tiền**. Giữa phiên không có dòng tiền cả ngày ⇒ **không thể** tính TWR. Điền `0` là khẳng định "hôm nay lãi đúng 0%" — một câu sai sẽ chui thẳng vào chuỗi `∏(1+r)` nếu sau này ai đó lỡ bỏ bộ lọc.

Biến động giữa phiên được tính **riêng, on-read**, ở `SP_GET_PM_RT_OVERVIEW`, nơi nó được gắn nhãn đúng bản chất.

---

### Cái giá của cột `C_SRC` — con số, không phải cảm tính

`T_SI_BALANCE` là bảng lớn nhất hệ (~2,5 tỷ dòng) và **hiện KHÔNG nén** — `CREATE TABLE` của nó không có `WITH (DATA_COMPRESSION = PAGE)` (khác `T_SI_CURRENT` / `T_SI_PORTFOLIO_HOLDING`), và cả 5 index đều `NONE` (kiểm bằng `sys.partitions`).

`VARCHAR(3)` tốn ~5 byte/dòng (3 byte dữ liệu + 2 byte trong mảng offset của cột biến độ dài), nhân với **3 nơi** — bảng + `IX_SI_NAV_BALANCE_MASTER` + `IX_SI_NAV_BALANCE_ACCT`:

```
2,5 tỷ dòng × 5 byte × 3 ≈ 37 GB
```

**Vì sao vẫn để `C_SRC` trong `INCLUDE` của cả hai index:** `C_SRC='EOD'` là **vị từ lọc**. Không nằm trong index thì mỗi dòng phải key-lookup về clustered index để đọc giá trị — với các truy vấn PM quét cả master (5.000 KH × N ngày) đó là hàng triệu lookup ngẫu nhiên. Đổi 37 GB đĩa lấy việc **không** làm chậm mọi màn hình PM là đổi đúng.

**Khuyến nghị DBA (chưa làm — nằm ngoài phạm vi thay đổi này):** bật `DATA_COMPRESSION = PAGE` cho `T_SI_BALANCE` và hai index của nó. `C_SRC` có giá trị gần như bất biến (`'EOD'` ở 99,99% dòng) nên từ điển nén trang gần như xoá sạch chi phí của nó — và nén cũng thu nhỏ toàn bộ phần còn lại của bảng. Đây là quyết định hạ tầng (đánh đổi CPU/IO) nên thuộc `00_INFRA.sql`, không nhét vào `01_TABLES.sql`.

### Dòng RT mồ côi — có thể xảy ra, và vô hại theo thiết kế

Kịch bản: FO ghi dòng RT cho tiểu khoản X ngày thứ Hai; Asset **không bao giờ** gửi số chốt cho X ngày đó (EOD bị chặn đúng bởi `err=12`). Dòng RT của X ngày thứ Hai **nằm lại trong lịch sử**.

Hậu quả: **không có** — mọi reader đều lọc `C_SRC='EOD'` nên nó vô hình với EOD, phí, báo cáo, hiệu suất. Và khi Asset gửi bù (re-ingest ngày cũ), `DELETE+INSERT` theo `(date, si)` thay nó bằng số chốt.

Nói cách khác: dòng mồ côi là **triệu chứng nhìn thấy được** của việc Asset gửi thiếu, không phải nguyên nhân gây sai. Muốn dọn thì thêm một job `PURGE_ORPHAN_RT` — đúng loại việc mà khung job sinh ra để làm.

---

## 5. Chu kỳ quét FO

```
SP_GET_FO_SNAPSHOT_SCOPE        → tiểu khoản indexing ACTIVE, ORDER BY cust_code
   ↓ cắt 50 KHÁCH HÀNG/batch (ranh giới KH, không xẻ đôi một khách hàng)
song song 4  ──► TradingWindowGuard.EnsureOpen()   ← tầng 4, TRƯỚC MỖI CALL
                 ↓
                 FO API
                 ↓
                 SP_INGEST_FO_SNAPSHOT_RT  (ghi NGAY, không gom hết rồi ghi một lần)
   ↓ hết batch
SP_RT_MASTER_AGG                → gộp master ĐÚNG MỘT LẦN, cuối chu kỳ
```

**50 KH ≠ 50 tiểu khoản.** `UQ_SI_PORTFOLIO_ACTIVE` cho mỗi KH **1 tiểu khoản ACTIVE trên mỗi master** ⇒ một KH đầu tư K master mang theo K tiểu khoản. Batch cắt theo **khách hàng** (đúng BRD) nên payload gửi FO có thể tới 50×K dòng. Scope sắp theo `cust_code` để ranh giới batch không bao giờ rơi vào giữa các tiểu khoản của cùng một người — nếu rơi thì cùng một KH bị hỏi ở hai batch, hai thời điểm, và số của họ khớp nhau chỉ do may mắn.

**Ghi ngay từng batch, không gom.** Gom hết rồi ghi nghĩa là hỏng ở batch 999 thì mất trắng 998 batch trước.

**Gộp master một lần, cuối chu kỳ.** Gộp sau mỗi batch cho ra con số master "nửa cũ nửa mới" mà dashboard không phân biệt được.

**`C_SINGLETON=1` + `timeout 840s < 900s`**: chu kỳ trước chưa xong thì không mở chu kỳ mới (1000 batch chồng 2 chu kỳ là tự bắn vào chân mình ở phía FO); mà pod chết thì lượt đó được thu hồi **trước** khi slot kế tiếp tới.

**Hết giờ giữa chừng là kết cục BÌNH THƯỜNG**, không phải lỗi: dừng vòng lặp, **vẫn** gộp master trên phần đã ghi, trả về số dòng đã xử lý, **không ném exception**. Ném thì lượt bị đánh FAILED rồi retry, và retry sau 15h chỉ đập vào tầng 2 rồi SKIPPED — nhật ký đầy tiếng ồn đỏ vô nghĩa.

---

## 6. Dashboard đọc gì

`SP_GET_PM_RT_OVERVIEW` là proc **duy nhất** được phép đọc dòng RT. `05_API` / `06_PM_API` vẫn chỉ đọc số chốt.

### Biến động phải tính TRÊN CÙNG TẬP tiểu khoản

`C_AUM_PREV_SAMESET` = Σ AUM phiên chốt gần nhất, **chỉ của những tiểu khoản có số RT hôm nay**.

Nếu lấy thẳng AUM master phiên trước: FO trả về 4.800/5.000 tiểu khoản thì hiệu số gồm cả 200 khách hàng chưa có số ⇒ dashboard **đỏ rực −4% trong khi thị trường không hề rơi**. Lỗi kiểu này rất khó cãi lại, vì con số nào cũng "có thật".

### Ba cột nói sự thật về độ đầy đủ

| Cột | Ý nghĩa |
|---|---|
| `C_SI_COUNT_RT` / `C_SI_COUNT_TOTAL` | n/N — FO đã trả về bao nhiêu trên tổng |
| `C_COVERAGE` | `FULL` \| `PARTIAL` |
| `C_STALE_MINUTES` | phút kể từ ảnh chụp cuối. Vượt ~2 chu kỳ ⇒ job đang hỏng |

Không có ba cột này thì một tổng AUM thiếu 200 khách hàng trông **y hệt** một tổng AUM đủ.

⚠️ `C_CHANGE_PCT` là **biến động tài sản, chưa khử dòng tiền** — khách nạp 10 tỷ lúc 10h hiện thành "tăng". Nhãn trên giao diện phải nói đúng điều đó. Số hiệu suất thật vẫn lấy từ `06_PM_API` (T-1, TWR).

---

## 7. Ma trận hỏng hóc — có ô nào làm SAI SỐ LIỆU không?

| Hỏng gì | Hậu quả | Sai số liệu? |
|---|---|---|
| Redis mất sạch key / `FLUSHALL` | Message biến mất, `T_JOB_RUN` vẫn READY → `SP_JOB_REAP` đẩy lại sau ≤30s | ❌ |
| `XADD` hụt (Redis chết lúc scheduler đẩy) | Y như trên | ❌ |
| Pod chết giữa chu kỳ | Lease hết hạn → REAP thu hồi → pod khác chạy lại. Ingest idempotent (MERGE) nên chạy lại vô hại | ❌ |
| Pod **treo** rồi tỉnh lại (zombie) | Heartbeat trả `still_mine=false` → tự huỷ token → dừng. `SP_JOB_COMPLETE` của nó cũng bị từ chối (`err=5`) | ❌ |
| Cùng `jobRunId` giao cho 20 pod | `SP_JOB_CLAIM`: đúng 1 thắng, 19 nhận `err=5` | ❌ |
| 10 pod cùng quét slot 9:15 | `UQ_JOB_RUN_NK`: đúng 1 dòng | ❌ |
| Job nằm trong stream vắt qua 15h00 | Tầng 2 → `SKIPPED`, **không** chạm FO | ❌ |
| Chu kỳ chạy vắt qua 15h00 | Tầng 4 → dừng giữa chừng, giữ phần đã ghi, coverage `PARTIAL` | ❌ |
| FO trả thiếu khách hàng | `C_RT_SI_COUNT < C_TOTAL_ACCOUNT` → `PARTIAL`; mốc so sánh dùng **cùng tập** nên % không méo | ❌ |
| FO trả tài khoản không thuộc SDI | `INNER JOIN` registry lọc | ❌ |
| FO trả số rác cho một KH | Dòng RT sai **1 nhịp 15 phút** rồi bị đè; EOD ghi đè hẳn cuối ngày; không vào phí/báo cáo/EOD | ⚠️ chỉ hiển thị |
| Batch RT tới muộn sau khi Asset đã chốt | `MERGE` từ chối, `@p_skipped_eod` tăng, có log | ❌ |
| Ai đó gọi `SP_INGEST_FO_SNAPSHOT_RT` cho ngày quá khứ | `err=22`, 0 dòng | ❌ |
| SQL Server đổi timezone hệ điều hành | `UDF_JOB_NOW()` neo UTC → không xê dịch | ❌ |
| **Ai đó bỏ bộ lọc `C_SRC='EOD'` ở một reader** | **EOD/phí/báo cáo đọc số giữa phiên** | ☠️ **CÓ** |

**Ô cuối là ô duy nhất có dấu ☠️** — và nó là cái giá của việc đổ RT vào chung bảng EOD. Phòng thủ hiện có:

1. Banner cảnh báo ở đầu `05_API.sql` và `06_PM_API.sql`;
2. Đánh dấu `-- ★RT` tại từng điểm lọc;
3. Ca smoke **B7/B8/B9** đo đúng cái hố đó và in ra con số "không lọc thì sẽ sai bao nhiêu".

Khi thêm proc mới đọc hai bảng này, câu hỏi bắt buộc: **proc này cần số chốt hay số giữa phiên?**

---

## 8. Khối lượng

| | Số |
|---|---|
| Chu kỳ | 15 phút → 24 lượt/ngày GD (9h–15h) |
| Tiểu khoản | ~50k ⇒ ~1.000 batch/chu kỳ (50 KH/batch) |
| Dòng `T_JOB_RUN` | ~24/ngày (một worker chạy cả chu kỳ ⇒ **không** sinh dòng cho từng batch) |
| Dòng `T_SI_BALANCE` phát sinh | **0** — RT upsert đè lên chính dòng của hôm nay, và cuối ngày bị dòng EOD thay thế |
| Dọn nhật ký | `SP_JOB_PURGE @p_keep_days=30` (không bao giờ xoá `DEAD`) |

Điểm đáng chú ý: **RT không làm bảng lịch sử phình thêm một dòng nào**. Mỗi tiểu khoản vẫn đúng 1 dòng/ngày — đó là hệ quả trực tiếp của việc upsert theo đúng khoá tự nhiên `UQ_SI_NAV_BALANCE_NK (date, si)` thay vì thêm dòng mới mỗi nhịp.

---

## 9. Triển khai

```powershell
sqlcmd -S .\SQLEXPRESS -E -d SDI -b -f 65001 -i 01_TABLES.sql   # thêm C_SRC / C_RT_AT (DB mới)
sqlcmd -S .\SQLEXPRESS -E -d SDI -b -f 65001 -i 02_SP_ENGINE.sql
sqlcmd -S .\SQLEXPRESS -E -d SDI -b -f 65001 -i 05_API.sql
sqlcmd -S .\SQLEXPRESS -E -d SDI -b -f 65001 -i 06_PM_API.sql
sqlcmd -S .\SQLEXPRESS -E -d SDI -b -f 65001 -i 09_FEE.sql
sqlcmd -S .\SQLEXPRESS -E -d SDI -b -f 65001 -i 11_JOB.sql      # bảng job + proc + seed FO_SNAPSHOT_RT
sqlcmd -S .\SQLEXPRESS -E -d SDI -b -f 65001 -i 12_JOB_SMOKE.sql
```

⚠️ **DB đã có dữ liệu** thì `01_TABLES.sql` không chạy lại được (nó là DDL tạo mới). Cần script `ALTER TABLE` bổ sung `C_SRC` / `C_RT_AT` / `C_RT_SI_COUNT` + dựng lại 2 index của `T_SI_BALANCE` — **chưa viết**, xem phần "còn thiếu" dưới.

Bật/tắt hoặc đổi lịch job **không cần deploy lại code**:

```sql
UPDATE T_JOB_DEFINITION SET C_INTERVAL_SEC = 300     WHERE C_JOB_CODE = 'FO_SNAPSHOT_RT';  -- 5 phút
UPDATE T_JOB_DEFINITION SET C_WINDOW_TO    = '14:45' WHERE C_JOB_CODE = 'FO_SNAPSHOT_RT';
UPDATE T_JOB_DEFINITION SET C_ENABLED      = 0       WHERE C_JOB_CODE = 'FO_SNAPSHOT_RT';  -- tắt khẩn cấp
```

Theo dõi: `EXEC SP_GET_JOB_STATUS @p_err_code=..., @p_err_msg=...` — RS1 sức khoẻ từng job (có cột `C_IN_WINDOW_NOW`), RS2 các lượt `DEAD`/`FAILED` cần người xử lý.

---

## 10. Còn thiếu — cần chốt trước khi lên prod

| # | Việc | Vì sao chưa làm |
|---|---|---|
| 1 | **Hợp đồng API FO**: đường dẫn, cách định danh (`si_account` hay `sub_account_no`), tên field, giới hạn kích thước batch, timeout, cơ chế xác thực | Chưa có tài liệu từ phía FO. `IFoSnapshotClient` là ranh giới đã chừa sẵn; JSON vào `SP_INGEST_FO_SNAPSHOT_RT` theo đúng quy ước của `SP_INGEST_ASSET_NAV`. **Sai tên field ⇒ `OPENJSON` trả NULL ⇒ `err=21`** (chặn, không ghi rác) |
| 2 | **Script migration `ALTER TABLE`** cho DB đang chạy | `01_TABLES.sql` chỉ dựng DB mới |
| 3 | **Đo tải thật**: 1.000 batch × 4 luồng có xong trong 15 phút không, FO chịu được bao nhiêu request/giây | Cần biết SLA của FO. Nếu không kịp thì nới `parallel` trong payload — hoặc chuyển sang mô hình fan-out (mỗi batch một job con, chia tải nhiều pod) |
| 4 | **Ai gọi `SP_JOB_PURGE`** | Nên chính nó là một `T_JOB_DEFINITION` — dùng khung để dọn cho khung |

---

## 11. Tóm tắt — 5 câu

1. **Chống trùng nằm ở `UPDATE ... WHERE C_STATUS='READY'`**, không nằm ở consumer group. Redis đi trước cho nhanh, DB chốt lại cho đúng.
2. **`UQ (job_code, fire_key)` thay thế leader election.** 10 pod cùng quét một slot vẫn ra đúng một lượt chạy.
3. **`SP_JOB_REAP` là cái giá phải trả cho việc để hàng đợi ở Redis** — và nó trả đủ: mất sạch Redis chỉ tốn 30 giây, không mất job nào.
4. **Guard khung giờ 4 tầng không thừa.** Tầng 4 là tầng duy nhất bắt được ca "chu kỳ dài vắt qua giờ đóng cửa" — thứ mà 3 tầng kia về bản chất không thể thấy.
5. **Cột `C_SRC` là toàn bộ tính đúng của phương án đổ RT vào bảng EOD.** Bỏ nó ở một chỗ thôi là cổng khoá EOD pass giả, phí tính trên AUM lúc 9h15, và báo cáo đọc số chưa chốt — cả ba đều **sai âm thầm**.
