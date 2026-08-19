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

## 2b. Vì sao dùng Redis Streams — và nó KHÔNG mua cho ta cái gì

### Streams giữ vai trò gì

**Đường vận chuyển + chuông cửa. Hết.** Nó **không** giữ một mảnh tính đúng nào:

| Câu hỏi | Ai trả lời |
|---|---|
| "Job này đã được xếp lịch chưa?" | `UQ_JOB_RUN_NK (job_code, fire_key)` |
| "Ai được chạy lượt này?" | `SP_JOB_CLAIM` — `UPDATE ... WHERE C_STATUS='READY'` |
| "Lượt này còn hiệu lực không?" | `C_MAX_DELAY_SEC` + `UDF_JOB_IN_WINDOW` |
| "Pod này còn là chủ không?" | lease + `C_OWNER` trong `SP_JOB_HEARTBEAT` |
| **"Có việc mới — dậy đi!"** | **Redis Stream** |

Xoá sạch Redis thì hệ **chậm đi**, không sai đi. Đó là điều kiện để nguyên tắc ① đứng vững.

### So với ba lựa chọn khác

| Phương án | Điểm chết |
|---|---|
| **Redis pub/sub** | Bắn-rồi-quên. Message phát ra lúc không pod nào đang nghe là **mất luôn**. Không có dấu vết để nhặt lại. |
| **Redis List (`BLPOP`)** | Không có consumer group, không có pending list. Pod `BLPOP` xong rồi chết ⇒ message **bốc hơi**, không ai biết nó từng tồn tại. |
| **DB polling 1–2 giây** | Đúng và đơn giản nhất, nhưng dồn tải nhàn rỗi lên SQL Server: 10 pod × 1 query/giây = **600 query/phút** chỉ để hỏi "có gì mới không", 24/7, kể cả 2 giờ sáng. |
| **Redis Streams** | Có consumer group (chia việc không cần điều phối) + **PEL**: pod nhận message rồi chết vẫn để lại dấu, `XAUTOCLAIM` nhặt được. Tải nhàn rỗi rơi vào Redis thay vì SQL Server. |

`XAUTOCLAIM` + PEL là thứ pub/sub và List **không thể** có. Đó là lý do kỹ thuật thật sự để chọn Streams giữa các phương án Redis.

### ⚠️ Sự thật cần biết: không có "blocking" thật

`StackExchange.Redis` **không hỗ trợ** lệnh chặn (`XREADGROUP ... BLOCK`) — thư viện này ghép nhiều lệnh trên một kết nối dùng chung, một lệnh chặn sẽ treo cả kết nối của mọi thứ khác. Nên `JobDispatcherService` **vẫn là vòng lặp hỏi thăm**, chỉ khác là hỏi Redis (mỗi 500ms) thay vì hỏi SQL Server.

⇒ Lợi ích thực tế, nói cho đúng:
- **độ trễ ≤ 500ms** thay vì ≤ 1–2 giây;
- **tải hỏi thăm rơi vào Redis** (20 lệnh/giây với 10 pod — không đáng kể) thay vì vào SQL Server;
- **PEL/XAUTOCLAIM** cứu được message đã giao cho pod đã chết.

Không phải "đẩy tức thì". Ai đọc code mà tưởng là push thật sẽ đặt kỳ vọng sai lúc đo độ trễ.

### Khi nào NÊN bỏ Streams

Nếu **không** bao giờ thêm job kiểu fan-out (mỗi chu kỳ sinh hàng nghìn job con) và độ trễ 1–2 giây là chấp nhận được, thì **DB polling thuần** là lựa chọn đúng: bỏ được ~200 dòng code, một dependency, và toàn bộ phần consumer group / XACK / PEL. Với 24 lượt job mỗi ngày, đó là một sự đơn giản hoá hoàn toàn chính đáng.

Lý do giữ Streams:
1. Job đẩy tay qua API (yêu cầu "cứ có job đẩy vào là xử lý") muốn dưới một giây, không phải 2 giây.
2. Nếu sau này chuyển FO sang mô hình fan-out (1000 job con/chu kỳ), DB polling sẽ tệ đi rất nhanh còn Streams thì không.

Đổi ý lúc nào cũng được mà **không đụng tới tính đúng**: bỏ Streams đi thì chỉ cần `SP_JOB_REAP` đổi từ "lưới an toàn" thành "đường chính", còn `T_JOB_RUN` và toàn bộ chốt chặn giữ nguyên. Đó chính là lợi ích của việc không giao một mảnh tính đúng nào cho Redis.

---

## 2c. Nhịp tim — và vì sao nó KHÔNG gọi DB liên tục

Nhịp tim làm hai việc: **gia hạn lease** (để không ai giật mất job đang chạy) và **phát hiện mất quyền** (lease bị thu hồi, hoặc job vừa bị TẮT).

### Bản đầu SAI: 1000 câu UPDATE vào một dòng

`FoSnapshotJobHandler` gọi `ctx.HeartbeatAsync(...)` **sau mỗi batch**. Với ~1000 batch/chu kỳ:

- **1000 câu `UPDATE T_JOB_RUN`** mỗi chu kỳ — tất cả vào **đúng một dòng**;
- phát ra từ **4 luồng song song** ⇒ chúng xếp hàng chờ khoá dòng của nhau;
- ×24 chu kỳ/ngày = **24.000 lượt ghi/ngày**, chỉ để cập nhật một con số mà **không ai đọc** trong lúc job đang chạy.

Không đủ để sập, nhưng là một điểm nghẽn khoá tự chế và hoàn toàn vô ích.

### Bản sửa: tiến độ ở RAM, chạm DB theo nhịp

`JobContext` tách làm ba, ranh giới rõ ràng:

| Hàm | Chạm DB? | Dùng khi nào |
|---|---|---|
| `ReportProgress(rows)` | **Không** — `Interlocked` vào RAM | Gọi thoải mái trong vòng lặp nóng |
| `IsStillMine()` | **Không** — đọc cờ trong RAM | Kiểm mỗi vòng lặp để dừng sớm |
| `HeartbeatAsync()` | Có, **nhưng chặn tần suất 5s** | Chỉ khi cần câu trả lời tươi ngay trước một việc không hoàn tác được |

Chỉ **một** nhịp tim nền chạm DB, và mọi lời gọi đều đi qua cùng một cửa có `SemaphoreSlim(1)` — bốn luồng không thể xếp hàng ghi cùng một dòng nữa.

**Nhịp = `C_TIMEOUT_SEC / 4`, kẹp trong [5s, 30s].** Không để cứng 20 giây:
- nhỏ hơn hẳn lease (biên 4 lần) ⇒ DB chậm một nhịp cũng không mất job vào tay pod khác;
- không thưa quá ⇒ "job vừa bị TẮT" được phát hiện trong tối đa 30 giây.

Job FO (`timeout 840s`) ⇒ nhịp 30s. Job khai `timeout 60s` ⇒ nhịp 15s.

### Tải DB thực tế của cả khung job

Với **10 pod**, trạng thái ổn định:

| Nguồn | Tần suất | Tổng (10 pod) |
|---|---|---|
| `SP_JOB_ENQUEUE_DUE` | 10s/lần, mọi pod | 60 lượt/phút |
| `SP_JOB_REAP` | 30s/lần, mọi pod | 20 lượt/phút |
| Nhịp tim | 30s/lần, **chỉ pod đang chạy job** | 2 lượt/phút |
| `SP_JOB_CLAIM` + `SP_JOB_COMPLETE` | 2 lượt mỗi lần job chạy | 48 lượt/**ngày** |
| **Tổng** | | **≈ 1,4 truy vấn/giây** |

So với bản đầu: riêng nhịp tim đã là **24.000 lượt ghi/ngày** dồn vào một dòng. Nay còn **~2.900 lượt/ngày** cho **toàn bộ** khung job, và không còn tranh khoá.

Đổi lại, tải hỏi thăm Redis là ~20 lệnh/giây với 10 pod (mỗi pod đọc stream mỗi 500ms) — Redis xử lý cỡ đó bằng vài phần trăm một nhân CPU.

> Muốn giảm nữa thì nới `ScanEvery` (10s) — nó đang lấy mẫu dày gấp 3 lần chu kỳ nhỏ nhất mà cấu hình cho phép (30s). Nhưng ở mức 1,4 truy vấn/giây thì không có gì để tối ưu.

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

**Luật giờ chỉ định nghĩa MỘT chỗ:** `T_JOB_DEFINITION`. Tầng 1, 2, 3 gọi `UDF_JOB_IN_WINDOW` (đọc bảng đó); tầng 4 **đọc thẳng bảng đó lúc chạy**, nhớ tạm 60 giây. Không hard-code 9h–15h trong C#, và không nhận khung giờ qua hằng số lúc khởi động — xem §3c câu 6.

**Biên `[from, to)`**: 15:00:00 chẵn là **đã đóng**. "Đến 3h chiều" nghĩa là phiên hết lúc 3h, không phải "còn được gọi thêm một nhịp lúc 3h".

**Đồng hồ**: `UDF_JOB_NOW()` neo `SYSUTCDATETIME()` rồi đổi sang giờ VN. `GETDATE()` trả giờ hệ điều hành — một pod SQL chạy UTC là khung 9h–15h lệch 7 tiếng, tức job gọi FO lúc 16h–22h giờ VN. Phía C# dùng `TimeZoneInfo` cùng múi, thử cả tên Windows lẫn Linux.

---

## 3b. Đổi cấu hình chu kỳ — và luật "cấu hình cũ chết theo cấu hình cũ"

Chu kỳ (15 phút / 30 phút / 1 tiếng), khung giờ, bật/tắt, payload đều nằm ở `T_JOB_DEFINITION`. Đổi bằng **cổng** `SP_SET_JOB_SCHEDULE`, không deploy lại code:

```sql
DECLARE @ec INT, @em NVARCHAR(400), @purged INT;
EXEC SP_SET_JOB_SCHEDULE @p_job_code='FO_SNAPSHOT_RT', @p_interval_sec=3600,   -- 1 tiếng/lần
     @p_user='ops', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_purged_runs=@purged OUTPUT;
```

### Vì sao đổi cấu hình BẮT BUỘC phải dọn hàng đợi

`T_JOB_RUN` giữ **bản chụp** của cấu hình tại thời điểm sinh: mốc slot nằm trong `C_FIRE_KEY`, tham số nằm trong `C_PAYLOAD`. Đổi 15 phút thành 60 phút lúc 09:16 mà không dọn thì lượt 09:15 của lưới **cũ** vẫn nằm trong hàng đợi và vẫn chạy. Người vận hành vừa bấm "1 tiếng một lần" xong lại thấy job chạy đúng nhịp 15 phút — và sẽ kết luận là cấu hình không ăn.

Cổng làm **dọn trước, ghi sau**, trong một giao dịch:

| Trạng thái lượt chạy | Xử lý | Vì sao |
|---|---|---|
| `READY` (chưa ai chạy) | **XOÁ** | Nội dung duy nhất của nó là "đã từng được xếp lịch" — thông tin đó đã nằm trong lịch sử cấu hình |
| `RUNNING` | **GIỮ** | Không thể dừng một pod đang gọi FO dở bằng một câu DELETE. Nó chạy nốt rồi tự đóng sổ |
| `DONE`/`FAILED`/`DEAD` | **GIỮ** | Là bằng chứng việc đã chạy — xoá là phá vết |

> Khác `SP_EOD_RESET` (proc đó **cố ý giữ** `T_EOD_RUN`): ở đó xoá là phá nhật ký việc **đã chạy**; ở đây dọn là bỏ một dòng **chưa bao giờ chạy**.

Muốn chặn cả lượt đang chạy thì **tắt job** (`@p_enabled=0`): nhịp heartbeat kế tiếp trả `still_mine=0` và worker tự dừng trong ~20 giây. Không có vế này thì "tắt khẩn cấp" chỉ chặn được lượt sau, còn chu kỳ đang bắn 1000 request sang FO vẫn bắn nốt — tức là đúng lúc cần tắt nhất thì nút tắt không có tác dụng.

### Trigger — ngoại lệ duy nhất trong repo, và vì sao

`TR_JOB_DEFINITION_PURGE_PENDING` là **trigger duy nhất** trong toàn bộ `db/`. Repo vốn theo lối "cổng proc". Ngoại lệ ở đây có lý do: cổng proc bảo vệ **tính đúng của dữ liệu ghi vào**, còn thứ cần giữ ở đây là một **bất biến giữa hai bảng** — *"không lượt chạy nào được sống lâu hơn cấu hình sinh ra nó"*. Bất biến giữa hai bảng phải gác ở tầng dữ liệu, y như `UNIQUE`/`CHECK`; nếu không thì chỉ cần một câu `UPDATE T_JOB_DEFINITION SET C_INTERVAL_SEC=3600` gõ tay lúc 2 giờ sáng là luật vỡ — im lặng, và đúng vào lúc không ai ngồi xem.

Trigger chỉ bắn khi giá trị **thật sự đổi** (so `inserted` vs `deleted`), không phải khi cột chỉ xuất hiện trong câu `SET`. Một ORM ghi lại cả hàng với đúng giá trị cũ sẽ **không** làm bay hàng đợi đang hợp lệ.

---

## 3c. Bảy câu hỏi rủi ro cao — trả lời bằng cơ chế, không bằng niềm tin

### 1. Pod mới init — có mất job không?

**Không.** Bốn đường cứu, độc lập nhau:

| Job kẹt ở đâu | Ai cứu | Sau bao lâu |
|---|---|---|
| Dòng `READY`, message chưa bao giờ vào stream (`XADD` hụt, pod chết ngay sau `INSERT`) | `SP_JOB_REAP` bước (3) | ≤ 30s |
| Message đã vào stream nhưng Redis mất sạch (`FLUSHALL`, cụm không bền) | `SP_JOB_REAP` bước (3) | ≤ 30s |
| Message đã giao cho pod rồi pod chết (nằm trong PEL của consumer đã chết) | `XAUTOCLAIM` | ≤ 60s + idle 2 phút |
| Lượt đã `RUNNING` rồi pod chết | lease hết hạn → `SP_JOB_REAP` bước (1) | ≤ timeout + 30s |

Điểm cốt lõi: **`T_JOB_RUN` là sổ cái, Redis chỉ là đường vận chuyển.** Không có trạng thái nào chỉ tồn tại trong Redis, nên không có trạng thái nào mất theo Redis.

Consumer group tạo ở `$` (chỉ nhận message mới) — cố ý. Tạo ở `0` thì pod đầu tiên khởi động sẽ hút lại toàn bộ lịch sử stream và bắn hàng nghìn lượt claim vô nghĩa; tất cả đều bị `SP_JOB_CLAIM` chặn (trạng thái đã `DONE`), nhưng vẫn là một trận bão query mỗi lần deploy.

### 2. Có sinh lại job cũ không?

**Không**, ba lớp chặn:

1. `UQ_JOB_RUN_NK (C_JOB_CODE, C_FIRE_KEY)` — cùng một slot không thể có hai dòng.
2. `C_FIRE_KEY` chứa **ngày + giờ + phút + giây** ⇒ slot của hôm qua không thể trùng slot hôm nay.
3. `SP_JOB_PURGE` có **sàn 1 ngày**. Đây không phải thận trọng thừa: dọn một lượt `DONE` mà mốc slot của nó **vẫn là slot hiện tại** thì `NOT EXISTS` trong `SP_JOB_ENQUEUE_DUE` lại thấy trống ⇒ sinh lại đúng lượt vừa xong ⇒ **job chạy hai lần trong một slot**. Chính hàng rào chống trùng bị chặt mất chân bởi thao tác dọn dẹp.

### 3. Có quét và chạy lại job quá giờ không?

**Không.** Và đây là chỗ bản đầu **sai thật**:

> Lượt sinh 14:59 không kịp chạy → nằm `READY` suốt đêm (reaper không đẩy vì ngoài khung giờ, và không ai đóng dấu nó cả). Sáng hôm sau 09:00 khung giờ **mở lại** ⇒ nó được đẩy ⇒ **chạy lại một lượt của hôm qua**. Guard khung giờ hoàn toàn không cứu được, vì 09:00 hôm sau là "trong giờ" một cách chính đáng.

Đã thêm `C_MAX_DELAY_SEC` (mặc định = `C_INTERVAL_SEC`), đo từ `C_RUN_AFTER`, chặn ở **hai** chỗ: `SP_JOB_REAP` đóng dấu `SKIPPED` chủ động, và `SP_JOB_CLAIM` từ chối nếu nó lọt tới tay worker bằng đường khác.

Đo từ `C_RUN_AFTER` chứ không phải `C_ENQUEUED_AT` là có chủ đích: lượt **retry** có `C_RUN_AFTER` mới, nên nó không bị tính là "cũ" chỉ vì lượt gốc sinh từ 15 phút trước. Đo nhầm mốc là giết sạch retry của mọi job chu kỳ ngắn.

**Cũng không back-fill:** scheduler chỉ tính **slot hiện tại**. Dừng dịch vụ nửa ngày rồi bật lại ⇒ sinh **1** lượt, không phải 20 lượt dồn toa. Với một job chụp ảnh "bây giờ" thì 20 ảnh của quá khứ là 20 lần vô nghĩa.

### 4. Chưa có cấu hình thời gian thì job chạy thế nào?

`C_INTERVAL_SEC IS NULL` ⇒ chế độ **`ON_DEMAND`**: scheduler không sinh lượt nào, job chỉ chạy khi có người đẩy qua `SP_JOB_ENQUEUE`. Đẩy tay vẫn chạy được ngay, đầy đủ claim/lease/retry như thường.

Lượt của job `ON_DEMAND` **không tự hết hạn** (không có `C_INTERVAL_SEC` để suy ra mốc) — job người ta đẩy tay phải nằm đó chờ tới lượt, không được tự bốc hơi. Cần hết hạn thì khai `C_MAX_DELAY_SEC` tường minh.

### 5. Có cấu hình rồi xong xoá đi thì sao?

`@p_clear_interval=1` ⇒ dọn lượt chờ + ngừng sinh + job về `ON_DEMAND`. Lượt đang `RUNNING` vẫn chạy nốt.

⚠️ **Job im lặng ngừng chạy là trạng thái nguy hiểm** — nhìn vào dữ liệu thì "đã xoá chu kỳ" và "chưa bao giờ cấu hình" giống hệt nhau. Vì thế `SP_GET_JOB_STATUS` trả thẳng cột `C_SCHEDULE_MODE` = `INTERVAL` | `ON_DEMAND` | `DISABLED`, thay vì bắt người xem tự suy từ chỗ `C_INTERVAL_SEC` bị `NULL`.

### 6. Đổi cấu hình xong, tầng nào còn dùng cấu hình cũ?

Không tầng nào — nhưng chỗ này **đã từng sai**: `TradingWindowGuard` (tầng 4, C#) bản đầu nhận `from`/`to` qua DI và giữ suốt đời tiến trình. Đổi khung giờ trong DB thì tầng 1/2/3 (nằm trong SQL) đổi tức thì, còn tầng 4 vẫn gác theo khung **cũ** cho tới lần deploy sau — mà tầng 4 lại đúng là tầng duy nhất trực tiếp gọi FO. Nay guard **đọc `T_JOB_DEFINITION` lúc chạy**, nhớ tạm 60 giây.

Đọc DB hỏng ⇒ giữ giá trị đọc được lần cuối và đi tiếp. Chưa từng đọc được lần nào ⇒ **từ chối chạy**: chưa biết luật thì không gọi hệ ngoài.

### 7. Đổi cấu hình có giết nhầm hàng đợi đang hợp lệ không?

Không. Trigger so `inserted` vs `deleted`, chỉ dọn khi giá trị **thật sự đổi**. Đổi `C_TIMEOUT_SEC`/`C_MAX_ATTEMPT`, hoặc ORM ghi lại cả hàng với đúng giá trị cũ — hàng đợi nguyên vẹn.

> Toàn bộ 7 câu trên có ca kiểm chứng trong `db/12_JOB_SMOKE.sql` khối **(C)** (20 ca), trừ ba dòng có chữ *Redis* ở câu 1 — chúng cần một cụm Redis thật để chạy; phần DB của chúng đã được kiểm.

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
