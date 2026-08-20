# Snapshot near-realtime từ FO + khung job chạy nền

> **Bài toán:** SDI định kỳ gọi sang FO lấy ảnh chụp tài sản của khách hàng indexing trong phiên, gộp lên cấp master để PM dashboard nhìn được số gần thời gian thực.
>
> **Cần trước hết là hạ tầng:** một khung chạy job nền (1) dùng được cho **mọi** loại job, (2) đẩy vào là chạy ngay, (3) chạy nhiều pod mà không xử lý trùng, (4) riêng job gọi FO thì **chỉ** 9h–15h ngày giao dịch.
>
> **Hiện thực:** [`db/11_JOB.sql`](../db/11_JOB.sql) · [`db/12_JOB_SMOKE.sql`](../db/12_JOB_SMOKE.sql) · [`reference/csharp/job/`](../reference/csharp/job/)

---

## 0. Ba nguyên tắc — kế thừa từ [thiết kế Kafka batch sync](SDI-kafka-batch-sync-design.md)

> ### ① TẦNG NHẮN TIN lo TỐC ĐỘ. DB lo TÍNH ĐÚNG.
> Kafka chỉ là **chuông cửa**, không phải sổ cái. Broker chết chỉ được phép làm **chậm** (≤30 giây, nhờ `SP_JOB_RECOVER`), không được phép làm **mất job** hay **chạy hai lần**.
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
| Proc | `SP_JOB_CLAIM_SLOT` · `_HEARTBEAT` · `_COMPLETE` · `_RECOVER` · `_MARK_SKIPPED` | `SP_GET_FO_SNAPSHOT_SCOPE` · `SP_INGEST_FO_SNAPSHOT_RT` · `SP_RT_MASTER_AGG` |
| Thêm job mới | INSERT 1 dòng + 1 class C# | — |

Thêm loại job thứ hai (dọn dẹp, tính lại index, đẩy báo cáo…) **không sửa một dòng nào** ở tầng A. Đó là thước đo duy nhất của chữ "generic".

### Cái gì nằm trên luồng chạy job, cái gì không

Sau vài vòng refactor (bộ quét chuyển lên pod, Kafka làm chuông cửa, Redis lọc trước), một số proc **không còn nằm trên đường chạy của một lượt job**. Chúng vẫn đúng và vẫn deploy, nhưng đọc `11_JOB.sql` mà tưởng mọi thứ đều chạy mỗi 15 phút là hiểu sai kiến trúc.

| Object | Vai trò | Chạy khi nào |
|---|---|---|
| `UDF_JOB_NOW` · `UDF_JOB_IN_WINDOW` | đồng hồ + khung giờ | mỗi lượt |
| `SP_JOB_CLAIM_SLOT` → `_HEARTBEAT` → `_COMPLETE` | vòng đời một lượt | mỗi lượt |
| `SP_JOB_MARK_SKIPPED` | đóng dấu xác lượt bị guard chặn | chỉ khi có dòng retry/hồi phục nằm chờ |
| `SP_JOB_RECOVER` | lưới an toàn | 30s/lần, **chỉ khi có khung giờ đang mở** |
| `SP_GET_FO_SNAPSHOT_SCOPE` → `SP_INGEST_FO_SNAPSHOT_RT` → `SP_RT_MASTER_AGG` | tầng B | mỗi lượt FO |
| `UDF_JOB_SLOT_AT` · `UDF_JOB_FIRE_KEY` | **THAM CHIẾU** — pod tính, SQL đối chiếu | mỗi lượt, nhưng **không phải nơi sinh mốc** |
| `SP_GET_SCHEDULABLE_JOBS` | dự phòng | chỉ khi cache Redis trống |
| `SP_SET_JOB_SCHEDULE` + `TR_..._PURGE_PENDING` | đường **cấu hình** | khi người vận hành đổi lịch |
| ⚠️ `SP_JOB_PURGE` | dọn nhật ký | **chưa có ai gọi** |
| ⚠️ `SP_GET_JOB_STATUS` | API theo dõi cho ops/UI | **chưa có consumer** |
| `SP_GET_PM_RT_OVERVIEW` | API đọc cho dashboard PM | khi người dùng mở màn hình |

Hai dòng có ⚠️ là thứ đáng chú ý:

- **`SP_JOB_PURGE` chưa được đấu.** Không gọi thì `T_JOB_RUN` tích ~25 dòng/ngày (~9.000/năm) — không sập gì, nhưng cũng không ai dọn. Cách đấu gọn nhất là khai chính nó thành một job (`INSERT T_JOB_DEFINITION ... 'JobPurgeHandler', 86400`) — dùng khung này để dọn cho khung.
- **`SP_GET_JOB_STATUS` chưa có consumer**, nhưng **đừng bỏ**: nó là chỗ *duy nhất* phơi ra `C_RECOVER_WAKE_7D`, tức là chỗ duy nhất phát hiện được *"chuông Kafka đã tắt từ lâu mà hệ vẫn chạy đúng nhờ bộ hồi phục"*. Bỏ đi là mất luôn tín hiệu đó.

**Đã xoá** (đừng đi tìm): `SP_JOB_ENQUEUE_DUE` (bộ quét trên pod thay thế) · `UDF_JOB_CAN_RUN` (hạn tươi neo vào mốc thay thế) · `SP_JOB_ENQUEUE` + `SP_JOB_CLAIM` (**gộp thành `SP_JOB_CLAIM_SLOT`** — xem §2).

---

## 2. Vòng đời một lượt chạy

```
   Bộ quét trong pod (10s)  ── KHÔNG CHẠM DB ────────────────────┐
        │ tự tính mốc từ cấu hình đã cache                        │
        │ SET NX  SDI:JOB:SLOT:{code}:{fireKey}                    │
        │   ├─ lấy được khoá ──► produce {code, slot, key}  ───────┤
        │   ├─ khoá đã có     ──► pod khác lo rồi, bỏ qua mốc      │
        │   └─ REDIS LỖI      ──► DỪNG CẢ NHỊP QUÉT (xem dưới)     │
        │                                                          ▼
        │                                        topic sdi.job.notify
        │                                                          │
        │                                consumer (commit offset NGAY)
        │                                                          ▼
        │                                            SP_JOB_CLAIM_SLOT
        │                                                          │
        │   ┌──────────────────────────────────────────────────────┤
        │   ▼ err=0                                                │ err≠0 → không chạy
        │  T_JOB_RUN: dòng SINH RA ĐÃ Ở 'RUNNING' ──► DONE          │  3 ngoài khung/quá hạn → SKIPPED
        │        ▲                                                  │  5 pod khác giữ  · 6 DEAD
        │        │                                                  │  7 singleton     · 20 sai lưới
        │        └──── SP_JOB_RECOVER ◄──── lease hết hạn (pod chết)
        │              (30s, mọi pod — chỉ khi có khung đang mở)
        └───────────────────────────────────────────────────────────
```

### ★ Bộ quét KHÔNG ghi DB

Trước đây một lượt job phải ghi DB **hai lần**: bộ quét `INSERT` một dòng `READY`, rồi worker `UPDATE` nó thành `RUNNING`. Trạng thái `READY` đó sống vài chục mili-giây, **không ai đọc**, nhưng kéo theo hai lần validate y hệt nhau (khung giờ, hạn tươi, singleton) nằm ở hai proc khác nhau — tức hai chỗ để lệch nhau.

Nay bộ quét **không chạm DB một lần nào**. Nó đặt khoá Redis rồi produce thẳng. Pod nào nhận được message thì gọi `SP_JOB_CLAIM_SLOT`; dòng `T_JOB_RUN` sinh ra **đã ở `RUNNING`**, do đúng pod sẽ chạy nó tạo ra. Một lượt job = **một** lệnh ghi DB lúc bắt đầu.

Hệ quả: **hàng đợi `READY` gần như luôn rỗng.** Còn dòng `READY` chỉ trong hai trường hợp — lượt retry đang chờ backoff, và lượt vừa bị `SP_JOB_RECOVER` thu về.

### ★ Redis lỗi ⇒ DỪNG, không có đường vòng qua DB

Nhịp quét bắt gặp lỗi Redis thì **`return` luôn cả vòng quét**, không produce gì hết, không rơi về quét DB.

Nghe thì ngược với "Redis không giữ mảnh tính đúng nào" (§2b), nhưng hai câu này nói về **hai chuyện khác nhau**:

- *Mất khoá* (Redis restart / evict / FLUSHALL) ⇒ nhiều pod cùng thấy "chưa ai làm" ⇒ cùng gọi `SP_JOB_CLAIM_SLOT` ⇒ `UQ (job_code, fire_key)` cho **đúng một** pod thắng. Vô hại — chỉ tốn vài lượt gọi DB thừa. Tính đúng **không** phụ thuộc Redis.
- *Redis chết hẳn* ⇒ **không còn ai lọc**. 10 pod × 6 nhịp/phút, mỗi nhịp bắn message cho mọi mốc tới hạn — cơn bão message và query mà `UQ` vẫn chặn đúng nhưng chẳng để làm gì.

Và cái giá của việc **dừng** là chấp nhận được, vì đây là số **near-realtime**: bỏ một mốc 15 phút thì mốc sau lấp lại. Đổi lại, ta không phải nuôi một nhánh dự phòng "quét DB" mà **không bao giờ chạy trong lúc bình thường** — tức là nhánh sẽ chạy lần đầu vào đúng lúc đang có sự cố.

**Trên đường truyền chỉ có `{code, slot, key}`** — mốc, chứ không phải trạng thái. Worker cầm mốc rồi hỏi DB mọi thứ khác. Chở bản chụp cấu hình trong tin nhắn là mở đường cho một job vừa bị sửa/tắt vẫn chạy theo bản cũ đang bay.

---

## 2b. Vì sao Kafka — và cái bẫy phải né khi dùng nó

> **Lịch sử:** Redis Streams (bản đầu) → Redis Pub/Sub → **Kafka** (chốt 2026-08-19). Mục này giữ cả ba lập luận để người sau không phải lần lại từ đầu.

### Vì sao đổi

Hệ **đã có sẵn Kafka** để xử lý event: consumer pattern, monitoring, alerting, người trực đã quen. Dựng thêm một tầng nhắn tin thứ hai bằng Redis là bắt cả tổ chức nuôi hai thứ làm cùng một việc. Đổi sang Kafka là **bớt đi** một công nghệ, không phải đổi ngang.

Và Kafka còn tốt hơn Pub/Sub ở đúng chỗ quan trọng: **nó bền**.

| | Redis Pub/Sub | **Kafka** |
|---|---|---|
| Message sống qua restart pod | ❌ mất | ✅ đọc tiếp từ offset |
| Message sống qua sự cố broker | ❌ mất | ✅ có lưu |
| `SP_JOB_RECOVER` đóng vai gì | **đường hồi phục chính** | **lưới an toàn** (đúng vai) |
| Chia việc nhiều pod | phát cho tất cả, N−1 claim hụt | consumer group, mỗi partition một consumer |
| Đã có trong hệ | ❌ phải dựng thêm | ✅ |

### Redis còn vai trò gì? — Lọc trước, và chỉ thế

> Bản trước của mục này viết *"Redis không còn vai trò gì"*. Câu đó **đúng về tính đúng, sai về tải** — và đã được sửa sau khi đo.

Chống trùng khi 10 pod cùng quét là `UQ (job_code, fire_key)` ở tầng dữ liệu, không phải lock — điều đó không đổi. Nhưng bỏ Redis đi thì **mỗi nhịp quét của mỗi pod là một lượt gọi DB**:

| | Không có Redis | Có Redis lọc trước |
|---|---|---|
| Nhịp quét chạm DB | 10 pod × 6/phút × 24h = **~86.400 lượt/ngày** | **~25 lượt/ngày** (đúng 1 lượt mỗi mốc) |
| Để sinh ra | 25 lượt job | 25 lượt job |
| Tỷ lệ | ~3.400 lượt hỏi cho **mỗi** job | 1:1 |

Với chu kỳ 15 phút thì **89/90 nhịp quét không có gì tới hạn** — đó là 3.400 lần hỏi một câu đã biết trước câu trả lời. Tải tuyệt đối nhỏ, nhưng không có lý do gì để trả.

```
Timer 10s trong pod
   ↓ tự tính mốc (cấu hình period đã cache) — KHÔNG chạm DB
   ↓ SET NX SDI:JOB:SLOT:{code}:{fireKey}    ← lọc: đúng 1 pod đi tiếp
produce Kafka {code, slot, key}
   ↓
consumer → SP_JOB_CLAIM_SLOT → sinh dòng RUNNING + chạy   ← lần chạm DB DUY NHẤT
```

**Redis chỉ trả lời *"có đáng gọi DB không"*, không trả lời *"ai được chạy"*.** Đó là lý do nó được phép sai. Cấu hình `period` nằm ở DB (nguồn sự thật) và được cache sang Redis khi update.

### ⚠️⚠️ Cái bẫy: KHÔNG BAO GIỜ chạy job trong vòng poll

`docs/SDI-kafka-batch-sync-design.md` §6 đã ghi lại bằng máu:

> *"chain chạy trong handler → block consumer → vượt `max.poll.interval` → Kafka đá pod (nhưng **KHÔNG giết thread** — nó vẫn ghi DB!) → zombie ghi song song → rebalance → giao lại → duplicate → vòng xoáy tự khuếch đại."*

Một chu kỳ quét FO chạy **vài phút** (1000 batch); `max.poll.interval.ms` mặc định **5 phút**. Chạy job trong vòng poll là nhảy thẳng vào cái bẫy đó.

**Cách né:** `Consume` → **commit offset ngay** → `SP_JOB_CLAIM_SLOT` → ném job sang Task nền → quay lại `Consume`. Vòng poll luôn rảnh.

**Commit trước khi chạy** nghe ngược tai nhưng là lựa chọn đúng: message không phải sổ cái, `T_JOB_RUN` mới là. Pod chết sau commit ⇒ lease hết hạn ⇒ `SP_JOB_RECOVER` thu hồi ⇒ chạy lại. Còn commit *sau* khi job xong thì vòng poll phải chờ vài phút — đổi một lưới cứu đã có lấy một cái bẫy đã biết.

> Khung job hiện tại có thứ luồng Asset cũ không có: **lease + heartbeat**. Pod bị đá vẫn heartbeat nên vẫn giữ lease, pod mới claim nhận `err=5` ⇒ vòng xoáy **không hình thành**. Nhưng đó là lưới cứu, không phải lý do để cố tình nhảy xuống vực.

### Cấu hình bắt buộc

```properties
enable.auto.commit   = false      # commit TAY, ngay sau khi nhận
auto.offset.reset    = latest     # group mới KHÔNG dội lại cả topic
max.poll.interval.ms = mặc định   # không cần nới, vì không chạy job trong poll
```

`auto.offset.reset=earliest` + group mới = dội về hàng nghìn mốc cũ. Ở hệ này thì **vô hại** — `SP_JOB_CLAIM_SLOT` chặn ở hạn tươi (`err=3`) hoặc ở `UQ` (`err=5`, mốc đã `DONE`) — nhưng vẫn là một trận bão query mỗi lần ai đó đổi tên group.

### Topic & partition

- **Topic riêng** `sdi.job.notify`, tách khỏi luồng ingest Asset. Cách ly hai chiều: chu kỳ FO chậm không đẩy lag sang ingest, và ngược lại. ~25 message/ngày nên chi phí gần bằng 0.
- **Key = `fireKey`** (mốc, dạng `yyyyMMddHHmmss` — rải đều partition). KHÔNG dùng `jobCode` làm key: như thế mọi lượt FO dồn vào một partition ⇒ một consumer ⇒ một pod gánh hết. Thứ tự message không quan trọng vì mọi quyết định nằm ở `SP_JOB_CLAIM_SLOT`.
- **Value = `{code, slot, key}`** (JSON, lớp `JobMessage`). Không chở payload cấu hình — worker hỏi DB.
- **Số partition ≥ số pod** muốn chạy job song song.

### Đường lùi

Muốn bỏ luôn Kafka khỏi khung job thì cho bộ quét gọi thẳng `SP_JOB_CLAIM_SLOT` tại chỗ (bỏ bước produce) — proc đó vốn đã là cổng duy nhất, không cần biết message tới từ đâu. `T_JOB_RUN` và toàn bộ chốt chặn giữ nguyên. Đó là lợi ích của việc **không giao một mảnh tính đúng nào cho tầng nhắn tin** — đổi tầng vận chuyển là việc của một buổi chiều, không phải một đợt refactor.

---

## 2c. Mốc sinh job — và vì sao "được chạy" khác "được sinh"

### Mốc cố định theo phiên, không trôi

Mốc slot neo vào **00:00 giờ VN**: `slot = 00:00 + floor(giây_từ_nửa_đêm / period) × period`. Với khung `[09:00, 15:00]`:

| Chu kỳ | Số mốc | Các mốc |
|---|---|---|
| 1 tiếng | **7** | 9, 10, 11, 12, 13, 14, **15** |
| 30 phút | 13 | 9:00, 9:30, … , **15:00** |
| 15 phút | 25 | 9:00, 9:15, … , **15:00** |

Nếu tính mốc theo "lần chạy trước + period" thì mỗi lần pod restart / job chậm là mốc trôi đi, và sau một ngày không ai đoán được job chạy vào phút nào — nhật ký thành thứ không đối chiếu được với dữ liệu FO.

### ★ Biên `[from, to]` đóng hai đầu, và MỌI phép kiểm giờ đều neo vào MỐC SLOT

Luật nghiệp vụ: **15:00 là mốc CUỐI CÙNG được gọi sang FO** (ảnh chụp đóng cửa). Cái bị cấm là **sinh thêm mốc mới sau khi hết phiên** — phải đợi phiên GD kế tiếp.

Và một luật thứ hai, quan trọng không kém: **số near-realtime chỉ có nghĩa TRONG phiên**, để PM ra quyết định. Trễ 40 phút thì nó không còn là near-realtime, nó là số rác — chạy cho có chỉ tổ gọi FO ngoài giờ. Vì thế **không xây cơ chế "gửi bằng được sau khi FO trễ"**.

Hai luật đó gộp lại thành **hai phép kiểm, cả hai đều neo vào `C_SLOT_AT`** (mốc mà lượt chạy đáng lẽ chạy):

| Phép kiểm | Hỏi gì | Ở đâu |
|---|---|---|
| `UDF_JOB_IN_WINDOW(job, slot)` | mốc có nằm trong `[09:00, 15:00]` + đúng ngày GD không? | tầng 1 (sinh), tầng 2 (claim), tầng 4 (C#) |
| `C_MAX_DELAY_SEC` | `now − slot` còn trong hạn tươi không? | tầng 2 (claim), tầng 4 (C#) |

**Không còn khái niệm "khung giờ + ân hạn".** Bản trước có `UDF_JOB_CAN_RUN` = `[from, to + C_TIMEOUT_SEC]` — đã **xoá**. Nó thừa: hạn tươi neo vào mốc đã làm đúng việc đó, bằng một con số dễ hiểu hơn ("lượt 15:00 hết hiệu lực lúc 15:10") thay vì một phép cộng hai cấu hình.

### Hai chi tiết nhỏ mà sai là hỏng cả tính năng

**① Kiểm biên trên MỐC SLOT, KHÔNG phải trên thời điểm quét/claim.** Bộ quét chạy mỗi 10 giây nên nó gần như không bao giờ chạy đúng `15:00:00.000` — nó chạy lúc `15:00:04`. Áp khung lên `now` thì `15:00:04 > 15:00:00` ⇒ **mốc 15:00 không bao giờ sinh ra**, và người ta chỉ phát hiện khi thắc mắc vì sao ảnh chụp đóng cửa không có. Cùng lỗi đó ở tầng claim: lượt 15:00 nhận lúc `15:00:03` sẽ bị từ chối.

Neo vào mốc thì cả hai đều đúng: `15:00:04 → slot 15:00:00 → trong khung ⇒ sinh`; `15:15:04 → slot 15:15:00 > 15:00 ⇒ không sinh`, đợi phiên kế tiếp.

**② Hạn tươi đo từ `C_SLOT_AT`, KHÔNG phải `C_RUN_AFTER`.** `C_RUN_AFTER` bị đẩy lên sau mỗi lần retry ⇒ một lượt thử lại mãi sẽ **tự làm mới hạn tươi của chính nó** và bò qua giờ đóng cửa. Neo vào mốc thì hạn là tuyệt đối: lượt 15:00 hết hiệu lực lúc 15:10, bất kể đã nằm chờ hay thử lại mấy lần.

> Đánh đổi đã biết và chấp nhận: retry chỉ có ý nghĩa khi còn trong hạn tươi. Job cấu hình `retry_delay` dài hơn `max_delay` thì lượt thử lại sẽ bị `SKIPPED` — đúng ý đồ, không phải lỗi.

### Không còn cửa hậu

Bản trước có `@p_ignore_window` để vận hành "chạy tay ngoài khung". **Đã bỏ.** Một cửa hậu mà job gọi hệ ngoài cũng đi qua được thì nó không phải cửa hậu, nó là cái lỗ. Cần chạy ngoài khung thì sửa khung bằng `SP_SET_JOB_SCHEDULE` — có dấu vết, có người chịu trách nhiệm, và tự dọn lượt chờ của cấu hình cũ.

### Và chặn SỚM

`FoSnapshotJobHandler` gọi guard **trước khi đọc scope** (~50k dòng), trước khi chia batch, trước khi chạm FO. Ngoài giờ thì không có lý do gì để tốn một câu query 50k dòng rồi mới phát hiện ra điều đó. Trong vòng lặp vẫn kiểm lại trước **mỗi** call — vì hạn tươi có thể hết giữa chừng khi FO chậm.

---

## 2d. Nhịp tim — và vì sao nó KHÔNG gọi DB liên tục

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
| Bộ quét (`SET NX` Redis) | 10s/lần, mọi pod | 60 lượt/phút — **vào Redis, KHÔNG vào DB** |
| `SP_JOB_RECOVER` | 30s/lần, có vé Redis + chỉ khi khung mở | ~2 lượt/phút |
| Nhịp tim | 30s/lần, **chỉ pod đang chạy job** | 2 lượt/phút |
| `SP_JOB_CLAIM_SLOT` + `SP_JOB_COMPLETE` | 2 lượt mỗi lần job chạy | 48 lượt/**ngày** |
| **Tổng vào DB** | | **≈ 0,1 truy vấn/giây** |

So với bản đầu: riêng nhịp tim đã là **24.000 lượt ghi/ngày** dồn vào một dòng, và riêng bộ quét là **~86.400 lượt/ngày**. Nay **toàn bộ** khung job chạm DB khoảng **~150 lượt/ngày**, và không còn tranh khoá. Phần việc quét dời hẳn sang Redis.

Đổi lại, tải trên Kafka là ~25 message/ngày cộng với vòng poll của mỗi consumer — không đáng kể so với luồng ingest Asset đang chạy trên cùng cụm.

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

App produce thẳng `{code, slot, key}` vào `sdi.job.notify` → consumer nhận trong vài chục mili-giây → `SP_JOB_CLAIM_SLOT` sinh dòng `RUNNING` và chạy. Không vòng chờ, không polling DB, không bước enqueue trung gian.

`fire_key` làm cho việc đẩy trở nên **idempotent**: cùng `requestId` gọi 10 lần vẫn đúng một lượt chạy — lần thứ hai vỡ `UQ` và nhận `err=5`.

Job **không** định kỳ (`C_INTERVAL_SEC IS NULL`) thì không có lưới mốc: người đẩy truyền `@p_slot_at = <lúc đẩy>` và tự đặt `@p_fire_key = requestId`. Vẫn đi qua **đúng một cổng**, không có đường tắt riêng cho việc đẩy tay.

### (3) Nhiều pod, không xử lý trùng — ba lớp

| Lớp | Bắt được gì | Không bắt được gì |
|---|---|---|
| Kafka consumer group | Mỗi partition một consumer — đủ cho đường chạy bình thường | rebalance giao lại, pod zombie, bộ hồi phục phát trùng |
| **`SP_JOB_CLAIM_SLOT`** (`INSERT` vỡ `UQ` → `UPDATE` có điều kiện) | **Mọi thứ.** 20 pod cầm cùng mốc ⇒ đúng 1 thắng | Pod thắng rồi treo |
| Heartbeat có kiểm chủ sở hữu | Pod treo → lease hết → pod khác giành; pod cũ tỉnh dậy nhận `still_mine=false` → tự dừng | — |

Lớp 2 là chốt thật. Lớp 1 chỉ để **rẻ** (đỡ những lần claim vô ích), lớp 3 để pod zombie không ghi song song — mà zombie là chuyện có thật với Kafka: pod vượt `max.poll.interval` bị đá khỏi group nhưng thread vẫn chạy. Xem §2b.

Chốt chặn nằm ở `UQ_JOB_RUN_NK (C_JOB_CODE, C_FIRE_KEY)`: 10 pod cùng nhận message mốc 9:15, cùng `INSERT` — CSDL cho đúng một pod thắng, 9 pod vỡ khoá và nhận `err=5`. **Không cần leader election.** Vì việc sinh dòng và việc giành quyền nay là **cùng một lệnh**, không còn khe hở giữa "đã sinh" và "đã có chủ".

### (4) FO chỉ chạy 9h–15h ngày GD — bốn tầng

| Tầng | Ở đâu | Bắt ca gì |
|---|---|---|
| 1 | Bộ quét trong pod | Không produce message cho mốc 15h30 — không có mốc nào ngoài khung được sinh ra |
| 2 | `SP_JOB_CLAIM_SLOT` | Mốc ngoài khung / sai ngày GD / **quá hạn tươi** → `err=3`. Mốc 15:00 vẫn giành được ở 15:00:03, nhưng hết hiệu lực lúc 15:10. Có dòng nằm chờ (retry/hồi phục) thì đóng dấu `SKIPPED` qua `SP_JOB_MARK_SKIPPED` |
| 3 | `SP_INGEST_FO_SNAPSHOT_RT` | Gọi proc bằng tay; ghi ngày không phải hôm nay; ngày nghỉ |
| **4** | `TradingWindowGuard` (C#) | **Chu kỳ chạy quá hạn tươi** (FO chậm). Đọc `MaxDelaySec` từ DB — CÙNG con số tầng 2 dùng. Chặn TRƯỚC khi đọc scope, và trước MỖI call FO |

Tầng 4 là tầng **duy nhất** bắt được ca cuối: tầng 1 và 2 chỉ kiểm **một lần, lúc bắt đầu**, còn một chu kỳ quét thì kéo dài nhiều phút. Guard đặt **ngay trước mỗi HTTP call**, không phải mỗi N batch — kiểm thưa ra là mở lại một khe hở đúng bằng N batch, và khe đó sẽ được lấp vào đúng ngày chu kỳ chạy chậm nhất.

**Luật giờ chỉ định nghĩa MỘT chỗ:** `T_JOB_DEFINITION`. Tầng 1, 2, 3 gọi `UDF_JOB_IN_WINDOW` (đọc bảng đó); tầng 4 **đọc thẳng bảng đó lúc chạy**, nhớ tạm 60 giây. Không hard-code 9h–15h trong C#, và không nhận khung giờ qua hằng số lúc khởi động — xem §3c câu 6.

**Biên `[from, to]` ĐÓNG hai đầu** — 15:00 là mốc cuối cùng được sinh, và lượt đó còn hiệu lực trong `C_MAX_DELAY_SEC` kể từ mốc (FO: tới 15:10). Chi tiết + lý do ở §2c.

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
| Dòng `READY`, tin chưa bao giờ phát được (`PUBLISH` hụt, pod chết ngay sau `INSERT`) | `SP_JOB_RECOVER` bước (3) | ≤ 30s |
| Broker chết lúc produce | `SP_JOB_RECOVER` bước (3) | ≤ 30s |
| Message đã commit nhưng pod chết trước khi claim | `SP_JOB_RECOVER` bước (3) | ≤ 30s |
| Pod vượt `max.poll.interval` bị đá, rebalance giao lại | `SP_JOB_CLAIM_SLOT` err=5 (zombie còn giữ lease) | ngay |
| **Redis chết** | Bộ quét **dừng nhịp**, không produce, không quét DB thay thế ⇒ **bỏ mốc**. Mốc sau lấp lại khi Redis trở lại | tối đa 1 chu kỳ mất số |
| **Message thất lạc trước khi có pod nào giành** | ⚠️ **KHÔNG bắt được** — chưa có dòng `T_JOB_RUN` nào để `SP_JOB_RECOVER` tìm. Đây là cái giá của việc bỏ ghi DB lúc sinh job | mốc sau lấp lại |
| Mọi pod đều bận (hết hạn mức job đồng thời) nên không ai claim | `SP_JOB_RECOVER` bước (3) | ≤ 30s |
| Lượt đã `RUNNING` rồi pod chết | lease hết hạn → `SP_JOB_RECOVER` bước (1) | ≤ timeout + 30s |

Điểm cốt lõi: **`T_JOB_RUN` là sổ cái, Kafka chỉ là đường vận chuyển.** Không có trạng thái nào chỉ tồn tại trong Kafka, nên không có trạng thái nào mất theo broker.

Pod mới khởi động đọc tiếp từ offset đã commit của group — không hút lại lịch sử, miễn là `auto.offset.reset=latest` (xem §2b). Đặt `earliest` thì một group mới sẽ dội về hàng nghìn mốc cũ; tất cả đều bị `SP_JOB_CLAIM_SLOT` chặn ở hạn tươi hoặc ở `UQ` — vô hại, nhưng là một trận bão query mỗi lần ai đó đổi tên group.

### 2. Có sinh lại job cũ không?

**Không**, ba lớp chặn:

1. `UQ_JOB_RUN_NK (C_JOB_CODE, C_FIRE_KEY)` — cùng một slot không thể có hai dòng.
2. `C_FIRE_KEY` chứa **ngày + giờ + phút + giây** ⇒ slot của hôm qua không thể trùng slot hôm nay.
3. `SP_JOB_PURGE` có **sàn 1 ngày**. Đây không phải thận trọng thừa: dọn một lượt `DONE` mà mốc slot của nó **vẫn là slot hiện tại** thì `NOT EXISTS` trong `SP_JOB_ENQUEUE_DUE` lại thấy trống ⇒ sinh lại đúng lượt vừa xong ⇒ **job chạy hai lần trong một slot**. Chính hàng rào chống trùng bị chặt mất chân bởi thao tác dọn dẹp.

### 3. Có quét và chạy lại job quá giờ không?

**Không.** Và đây là chỗ bản đầu **sai thật**:

> Lượt sinh 14:59 không kịp chạy → nằm `READY` suốt đêm (bộ hồi phục không đẩy vì ngoài khung giờ, và không ai đóng dấu nó cả). Sáng hôm sau 09:00 khung giờ **mở lại** ⇒ nó được đẩy ⇒ **chạy lại một lượt của hôm qua**. Guard khung giờ hoàn toàn không cứu được, vì 09:00 hôm sau là "trong giờ" một cách chính đáng.

Đã thêm `C_MAX_DELAY_SEC` (mặc định = `C_INTERVAL_SEC`), đo từ **`C_SLOT_AT`** (mốc), chặn ở **hai** chỗ: `SP_JOB_RECOVER` đóng dấu `SKIPPED` chủ động, và `SP_JOB_CLAIM_SLOT` từ chối nếu nó lọt tới tay worker bằng đường khác. Neo vào mốc chứ không phải `C_RUN_AFTER` là điểm mấu chốt: neo vào `C_RUN_AFTER` thì một lượt retry mãi sẽ **tự làm mới hạn của chính nó** rồi bò qua giờ đóng cửa.

Đo từ `C_RUN_AFTER` chứ không phải `C_ENQUEUED_AT` là có chủ đích: lượt **retry** có `C_RUN_AFTER` mới, nên nó không bị tính là "cũ" chỉ vì lượt gốc sinh từ 15 phút trước. Đo nhầm mốc là giết sạch retry của mọi job chu kỳ ngắn.

**Cũng không back-fill:** scheduler chỉ tính **slot hiện tại**. Dừng dịch vụ nửa ngày rồi bật lại ⇒ sinh **1** lượt, không phải 20 lượt dồn toa. Với một job chụp ảnh "bây giờ" thì 20 ảnh của quá khứ là 20 lần vô nghĩa.

### 4. Chưa có cấu hình thời gian thì job chạy thế nào?

`C_INTERVAL_SEC IS NULL` ⇒ chế độ **`ON_DEMAND`**: scheduler không sinh mốc nào, job chỉ chạy khi có người đẩy qua `SP_JOB_CLAIM_SLOT` (truyền mốc = lúc đẩy + `fire_key` = requestId). Đẩy tay vẫn chạy được ngay, đầy đủ claim/lease/retry như thường.

Lượt của job `ON_DEMAND` **không tự hết hạn** (không có `C_INTERVAL_SEC` để suy ra mốc) — job người ta đẩy tay phải nằm đó chờ tới lượt, không được tự bốc hơi. Cần hết hạn thì khai `C_MAX_DELAY_SEC` tường minh.

### 5. Có cấu hình rồi xong xoá đi thì sao?

`@p_clear_interval=1` ⇒ dọn lượt chờ + ngừng sinh + job về `ON_DEMAND`. Lượt đang `RUNNING` vẫn chạy nốt.

⚠️ **Job im lặng ngừng chạy là trạng thái nguy hiểm** — nhìn vào dữ liệu thì "đã xoá chu kỳ" và "chưa bao giờ cấu hình" giống hệt nhau. Vì thế `SP_GET_JOB_STATUS` trả thẳng cột `C_SCHEDULE_MODE` = `INTERVAL` | `ON_DEMAND` | `DISABLED`, thay vì bắt người xem tự suy từ chỗ `C_INTERVAL_SEC` bị `NULL`.

### 6. Đổi cấu hình xong, tầng nào còn dùng cấu hình cũ?

Không tầng nào — nhưng chỗ này **đã từng sai**: `TradingWindowGuard` (tầng 4, C#) bản đầu nhận `from`/`to` qua DI và giữ suốt đời tiến trình. Đổi khung giờ trong DB thì tầng 1/2/3 (nằm trong SQL) đổi tức thì, còn tầng 4 vẫn gác theo khung **cũ** cho tới lần deploy sau — mà tầng 4 lại đúng là tầng duy nhất trực tiếp gọi FO. Nay guard **đọc `T_JOB_DEFINITION` lúc chạy**, nhớ tạm 60 giây.

Đọc DB hỏng ⇒ giữ giá trị đọc được lần cuối và đi tiếp. Chưa từng đọc được lần nào ⇒ **từ chối chạy**: chưa biết luật thì không gọi hệ ngoài.

### 7. Đổi cấu hình có giết nhầm hàng đợi đang hợp lệ không?

Không. Trigger so `inserted` vs `deleted`, chỉ dọn khi giá trị **thật sự đổi**. Đổi `C_TIMEOUT_SEC`/`C_MAX_ATTEMPT`, hoặc ORM ghi lại cả hàng với đúng giá trị cũ — hàng đợi nguyên vẹn.

> Toàn bộ 7 câu trên có ca kiểm chứng trong `db/12_JOB_SMOKE.sql` khối **(C)** (20 ca), trừ mấy dòng liên quan tới *broker* ở câu 1 — chúng cần một cụm Kafka thật để chạy; phần DB của chúng đã được kiểm.

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
   ↓ cắt đều 50 TIỂU KHOẢN/batch (không quan tâm khách hàng)
song song 4  ──► TradingWindowGuard.EnsureOpen()   ← tầng 4, TRƯỚC MỖI CALL
                 ↓
                 FO API
                 ↓
                 SP_INGEST_FO_SNAPSHOT_RT  (ghi NGAY, không gom hết rồi ghi một lần)
   ↓ hết batch
SP_RT_MASTER_AGG                → gộp master ĐÚNG MỘT LẦN, cuối chu kỳ
```

**Chia đều 50 tiểu khoản/batch, không quan tâm khách hàng.**

Bản đầu cắt ở ranh giới **khách hàng** để "không xẻ đôi một khách". Lý do đó **không đứng vững**: `UQ_SI_PORTFOLIO_ACTIVE` chỉ cho mỗi KH **một tiểu khoản ACTIVE trên mỗi master**, nên hai tiểu khoản của cùng một khách hàng **luôn thuộc hai master khác nhau** — tách chúng ra không thể gây lệch số trong cùng một master, mà master mới là đơn vị `SP_RT_MASTER_AGG` gộp.

Đổi lại, cắt theo khách hàng làm **kích thước batch dao động**: 50 KH có thể ra 50 hay 150 dòng tuỳ mỗi người đầu tư mấy master. FO nhận payload lúc to lúc nhỏ, và `batchSize` không còn nói lên điều gì về tải thật sự gửi đi.

⇒ Chia đều: mọi batch đúng 50 dòng (trừ batch cuối), payload đoán trước được, các luồng song song gánh đều nhau.

**Scope sắp theo `C_MASTER_CODE` trước** (không phải `cust_code`): thứ tự này quyết định tiểu khoản nào bị chụp gần nhau về thời gian. Xếp theo master ⇒ tiểu khoản cùng một master nằm liền nhau ⇒ vào cùng batch hoặc các batch kề nhau ⇒ `SP_RT_MASTER_AGG` gộp trên một tập nhất quán hơn về thời điểm.

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
| Broker Kafka chết / topic bị xoá | Message biến mất, `T_JOB_RUN` vẫn READY → `SP_JOB_RECOVER` produce lại sau ≤30s | ❌ |
| Produce hụt (broker chết lúc scheduler rung chuông) | Y như trên | ❌ |
| Pod chết giữa chu kỳ | Lease hết hạn → RECOVER thu hồi → pod khác chạy lại. Ingest idempotent (MERGE) nên chạy lại vô hại | ❌ |
| Pod **treo** rồi tỉnh lại (zombie) | Heartbeat trả `still_mine=false` → tự huỷ token → dừng. `SP_JOB_COMPLETE` của nó cũng bị từ chối (`err=5`) | ❌ |
| Cùng một mốc giao cho 20 pod | `SP_JOB_CLAIM_SLOT`: đúng 1 thắng, 19 nhận `err=5` | ❌ |
| 10 pod cùng quét slot 9:15 | `UQ_JOB_RUN_NK`: đúng 1 dòng | ❌ |
| Job nằm chờ trong hàng đợi vắt qua 15h00 | Tầng 2 → `SKIPPED`, **không** chạm FO | ❌ |
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

1. **Chống trùng nằm ở `UPDATE ... WHERE C_STATUS='READY'`**, không nằm ở tầng giao tin. Kafka rung chuông cho nhanh, DB chốt lại cho đúng — nên rebalance giao lại hay pod zombie đều chỉ dẫn tới `err=5`.
2. **`UQ (job_code, fire_key)` thay thế leader election.** 10 pod cùng quét một slot vẫn ra đúng một lượt chạy.
3. **`SP_JOB_RECOVER` là lưới an toàn** — Kafka có lưu nên message sống qua restart; bộ hồi phục chỉ còn lo produce hụt, mọi pod đều bận, và pod chết giữa chừng. Khung job **không đụng Redis một dòng nào**.
4. **Guard khung giờ 4 tầng không thừa.** Tầng 4 là tầng duy nhất bắt được ca "chu kỳ dài vắt qua giờ đóng cửa" — thứ mà 3 tầng kia về bản chất không thể thấy.
5. **Cột `C_SRC` là toàn bộ tính đúng của phương án đổ RT vào bảng EOD.** Bỏ nó ở một chỗ thôi là cổng khoá EOD pass giả, phí tính trên AUM lúc 9h15, và báo cáo đọc số chưa chốt — cả ba đều **sai âm thầm**.
