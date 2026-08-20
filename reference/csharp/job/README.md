# Khung job chạy nền + snapshot near-realtime FO — bản tham chiếu C#

> Code C# **không nằm trong repo này** (nó ở `sdi-core-messaging-process`). Đây là bản tham chiếu viết theo thiết kế ở [`docs/SDI-nearrt-fo-snapshot-design.md`](../../../docs/SDI-nearrt-fo-snapshot-design.md), đi kèm [`db/11_JOB.sql`](../../../db/11_JOB.sql).

## File

| File | Vai trò |
|---|---|
| `JobContracts.cs` | `IJobHandler` · `JobContext` · `JobRegistry` · `ISdiJobGateway` (map 1-1 sang SP) |
| `JobTopicKeys.cs` | Topic/consumer group Kafka + hằng số nguồn đánh thức |
| `JobRedisKeys.cs` | Khoá Redis: lọc mốc (`SET NX`) · vé RECOVER · cache cấu hình |
| `JobSchedulerService.cs` | Quét lịch (10s, **0 lượt gọi DB/nhịp**) · RECOVER (30s, có vé + cửa chặn khung giờ) |
| `JobDispatcherService.cs` | Consumer: `Consume` → **commit ngay** → `SP_JOB_CLAIM_SLOT` → chạy handler **ngoài vòng poll** |
| `TradingWindowGuard.cs` | **Guard tầng 4** — chặn ngay trước từng HTTP call sang FO |
| `FoSnapshotJobHandler.cs` | Nghiệp vụ: scope → cắt 50 KH/batch → gọi FO → ingest RT → gộp master |

## Đăng ký DI

```csharp
services.AddSingleton<IDatabase>(sp =>                            // Redis: LỌC TRƯỚC, không giữ tính đúng
    sp.GetRequiredService<IConnectionMultiplexer>().GetDatabase());
services.AddSingleton(new ConsumerConfig { BootstrapServers = cfg["Kafka:Brokers"] });
services.AddSingleton<IProducer<string, string>>(_ =>
    new ProducerBuilder<string, string>(
        new ProducerConfig { BootstrapServers = cfg["Kafka:Brokers"], Acks = Acks.All }).Build());
services.AddSingleton<ISdiJobGateway, SdiJobGateway>();          // tự cài bằng Dapper/ADO
services.AddSingleton<IJobRegistry, JobRegistry>();
services.AddSingleton<IJobHandler, FoSnapshotJobHandler>();      // thêm job mới = thêm 1 dòng ở đây
services.AddSingleton<IFoSnapshotClient, FoSnapshotHttpClient>(); // HttpClient thật của bạn
// Guard ĐỌC khung giờ từ T_JOB_DEFINITION (nhớ tạm 60s) — KHÔNG truyền 9h-15h vào đây.
// Truyền hằng số vào thì đổi cấu hình trong DB sẽ không tới được tầng 4, và tầng 4 là tầng
// duy nhất trực tiếp gọi FO.
services.AddSingleton(sp => new TradingWindowGuard(
    sp.GetRequiredService<ISdiJobGateway>(), jobCode: "FO_SNAPSHOT_RT"));

services.AddHostedService<JobSchedulerService>();                 // chạy trên MỌI pod — an toàn
services.AddHostedService<JobDispatcherService>();                // chạy trên MỌI pod
```

Không cần leader election, không cần lock phân tán, không cần cấu hình "pod nào là scheduler". Chạy 1 pod hay 20 pod đều đúng.

## Thêm một loại job mới — 2 bước, không sửa khung

```sql
INSERT INTO T_JOB_DEFINITION (C_JOB_CODE, C_JOB_NAME, C_HANDLER, C_INTERVAL_SEC, C_TIMEOUT_SEC)
VALUES ('CLEANUP_TMP', N'Dọn bảng tạm', 'CleanupJobHandler', 3600, 600);
```

```csharp
public class CleanupJobHandler : IJobHandler
{
    public string HandlerKey => "CleanupJobHandler";
    public async Task<long> RunAsync(JobContext ctx, CancellationToken ct) { /* ... */ return n; }
}
```

Không đụng `JobSchedulerService`, `JobDispatcherService`, hay bất kỳ proc nào ở tầng A của `11_JOB.sql`.

## Đổi chu kỳ (15' / 30' / 1 tiếng) — dùng CỔNG, đừng UPDATE thẳng

```csharp
var (err, msg, purged) = await _db.SetJobScheduleAsync(
    "FO_SNAPSHOT_RT", intervalSec: 1800, clearInterval: false,
    windowFrom: null, windowTo: null, clearWindow: false,
    businessDayOnly: null, enabled: null, maxDelaySec: null, payload: null,
    user: currentUser, ct);
Log.Information("[JOB] Đổi lịch: {Msg} (dọn {N} lượt chờ của cấu hình cũ)", msg, purged);
```

`SP_SET_JOB_SCHEDULE` làm hai việc trong **một giao dịch**: dọn sạch lượt `READY` sinh bởi cấu hình cũ, rồi mới ghi cấu hình mới. Nếu ai đó `UPDATE T_JOB_DEFINITION` thẳng (script vận hành, tool DB), **trigger `TR_JOB_DEFINITION_PURGE_PENDING` vẫn dọn** — luật không phụ thuộc vào việc người ta có đi qua cổng hay không.

Đổi cấu hình **không** giết lượt đang `RUNNING` (không thể DELETE một pod đang gọi FO dở). Muốn dừng hẳn thì tắt job: `enabled: false` ⇒ nhịp heartbeat kế tiếp trả `still_mine=false` ⇒ worker tự dừng trong ~20 giây.

## Đẩy job tuỳ ý (ngoài lịch) — "cứ có job đẩy vào là chạy"

Đẩy tay = **produce một message**, y hệt bộ quét. Không có API riêng, không có đường tắt: dòng `T_JOB_RUN` vẫn do pod nhận message tạo ra qua `SP_JOB_CLAIM_SLOT`.

```csharp
var slot = TradingWindowGuard.NowVn();     // job on-demand không có lưới mốc ⇒ mốc = lúc đẩy
await _producer.ProduceAsync(JobTopicKeys.Topic, new Message<string, string> {
    Key   = JobTopicKeys.KeyOf(requestId),                      // idempotent theo requestId
    Value = JobMessage.Serialize("CLEANUP_TMP", slot, requestId)
});
```

Đẩy hai lần cùng `requestId` ⇒ pod thứ hai vỡ `UQ (job_code, fire_key)` và nhận `err=5`: **không tạo lượt trùng**. Đó không phải lỗi và không cần retry.

Muốn biết ngay kết quả (API đồng bộ) thì gọi thẳng `SP_JOB_CLAIM_SLOT` rồi chạy handler tại chỗ — cùng một cổng, chỉ bỏ bước đi vòng qua Kafka:

```csharp
var claim = await _db.ClaimSlotAsync("CLEANUP_TMP", slot, owner: podId,
                                     fireKey: requestId, source: "manual", ct);
if (claim.Err != 0) return claim;          // 2 job tắt · 3 ngoài khung · 5 đã đẩy rồi · 7 singleton
```

Consumer nhận trong vài chục mili-giây. Produce hụt cũng không mất job: dòng `T_JOB_RUN` vẫn
`READY` và `SP_JOB_RECOVER` produce lại sau ≤30 giây.

---

## Tải DB của khung job — đo thật

| | Trước | Sau |
|---|---|---|
| Nhịp quét lịch (10s × 10 pod) | `SP_JOB_ENQUEUE_DUE` ~50 logical reads **mỗi nhịp** | **0 lượt gọi DB** (Redis `SET NX`) |
| Lượt quét/ngày để sinh 25 job | **~86.400** (≈3.400 lượt hỏi/job) | **~25** |
| `SP_JOB_RECOVER` | 30s × 10 pod = 20 lượt/phút, 24/7 | có **vé Redis** (1 pod) + **cửa chặn khung giờ** ⇒ ~2 lượt/phút, và **0 ngoài 09:00–15:10** |
| Đọc cấu hình | mỗi nhịp | cache Redis, app xoá khi đổi lịch ⇒ ~0 |

**Redis không giữ mảnh tính đúng nào.** Mất khoá mốc ⇒ nhiều pod cùng gọi `SP_JOB_CLAIM_SLOT` ⇒ `UQ (job_code, fire_key)` cho đúng một pod thắng, còn lại `err=5`. Tệ nhất của việc mất *khoá*: vài lượt gọi DB thừa — **không job nào chạy hai lần, không job nào mất**.

**Nhưng Redis *lỗi* thì bộ quét DỪNG nhịp** (`return`), không produce và **không** rơi về quét DB. Mất khoá ≠ mất Redis: mất khoá là vài query thừa, còn mất Redis là *không còn ai lọc* — 10 pod × 6 nhịp/phút bắn message cho mọi mốc. Đây là số near-realtime nên bỏ một mốc là chấp nhận được; nuôi một nhánh dự phòng không bao giờ chạy lúc bình thường thì không.

**Pod tính mốc, DB đối chiếu.** Bộ quét tự tính mốc để khỏi hỏi DB; `SP_JOB_CLAIM_SLOT` so lại với `UDF_JOB_SLOT_AT`. Pod lệch múi giờ / chạy bản cũ / sai chu kỳ ⇒ **bị từ chối `err=20`**, không trôi lệch âm thầm. Phép tính mốc vẫn có ca kiểm chứng trong `12_JOB_SMOKE.sql` vì bản tham chiếu nằm ở SQL.

## Ba câu hỏi sẽ bị hỏi khi review

### 1. "Kafka consumer group đã chống trùng rồi, sao còn `SP_JOB_CLAIM_SLOT`?"

Consumer group hứa *mỗi partition giao cho một consumer*, **không** hứa *mỗi message xử lý đúng một lần*. Ba đường làm nó giao lại cùng một `jobRunId`: rebalance khi pod vào/ra group; pod vượt `max.poll.interval` bị đá nhưng **thread vẫn chạy** (zombie); và `SP_JOB_RECOVER` produce lại.

`SP_JOB_CLAIM_SLOT` là một `INSERT` vào `UQ (job_code, fire_key)`: ai chèn được dòng người đó chạy — thứ **không thể có hai người thắng**, và nó nằm ở nơi mọi pod nhìn thấy cùng một sự thật. Dòng đã tồn tại (retry / lượt bị hồi phục / pod chết) thì rơi sang `UPDATE` có điều kiện, cũng nguyên tử.

### 2. "Chạy job trong consumer có sao không?"

**Có, và đây là cái bẫy nguy hiểm nhất của cả thiết kế.** Chu kỳ FO chạy vài phút, `max.poll.interval.ms` mặc định 5 phút. Chạy job trong vòng poll ⇒ Kafka đá pod khỏi group ⇒ nhưng **không giết thread** ⇒ pod cũ thành zombie vẫn ghi DB trong khi pod mới xử lý lại cùng message. `docs/SDI-kafka-batch-sync-design.md` §6 đã ghi lại đúng vòng xoáy này.

⇒ `Consume` → **commit offset ngay** → claim → ném job sang Task nền → quay lại `Consume`. Vòng poll luôn rảnh.

Commit *trước* khi chạy nghe ngược tai, nhưng message không phải sổ cái — `T_JOB_RUN` mới là. Pod chết sau commit ⇒ lease hết hạn ⇒ bộ hồi phục thu hồi ⇒ chạy lại.

### 3. "Broker chết thì mất job?"

Không. Dòng `T_JOB_RUN` vẫn `READY`; `SP_JOB_RECOVER` produce lại sau ≤30 giây. Kafka có lưu nên hầu hết trục trặc kết nối tự khỏi mà không cần tới bộ hồi phục — nó là **lưới an toàn**, không phải đường chính.

⚠️ **Cái bẫy vận hành:** chuông tắt hẳn thì hệ **vẫn chạy đúng**, chỉ chậm ~30 giây — và không ai nhận ra. Vì thế mỗi lượt chạy ghi `C_CLAIM_SOURCE` ('kafka' | 'recover'), và `SP_GET_JOB_STATUS` trả `C_RECOVER_WAKE_7D`. Con số đó xấp xỉ tổng số lượt ⇒ Kafka đã chết từ lâu.

### 3. "Guard khung giờ 4 tầng có thừa không?"

Không. Mỗi tầng bắt một ca mà tầng khác **không thể** bắt:

| Tầng | Ở đâu | Bắt ca gì |
|---|---|---|
| 1 | Bộ quét trong pod | Không produce message cho mốc 15h30 |
| 2 | `SP_JOB_CLAIM_SLOT` | Mốc 14h59, pod nhặt lúc 15h02 (message tồn đọng / pod restart / bộ hồi phục trả lại) |
| 3 | `SP_INGEST_FO_SNAPSHOT_RT` | Ai đó gọi proc bằng tay; job ghi ngày không phải hôm nay |
| **4** | `TradingWindowGuard` (C#) | **Chu kỳ 1000 batch khởi động lúc 14h50, tới batch 700 thì đã 15h02** |

Tầng 4 là tầng duy nhất bắt được ca cuối — vì tầng 1 và 2 chỉ kiểm **một lần, lúc bắt đầu**, còn chu kỳ thì kéo dài nhiều phút. Bỏ tầng 4 nghĩa là vẫn có hàng trăm request bay sang FO sau giờ đóng cửa, và cả 3 tầng kia đều đã "duyệt" nó từ trước.

Luật giờ chỉ được định nghĩa **một chỗ**: `UDF_JOB_IN_WINDOW` + `T_JOB_DEFINITION`. `TradingWindowGuard` **đọc khung giờ từ chính bảng đó lúc chạy** (nhớ tạm 60 giây), không nhận hằng số lúc khởi động và không hard-code 9h–15h trong C#. Nhận hằng số thì đổi cấu hình trong DB sẽ tới được tầng 1/2/3 mà **không** tới được tầng 4 — đúng cái tầng trực tiếp gọi FO lại là tầng hiểu sai luật.

### 4. "Đổi cấu hình / pod restart / job quá giờ — có mất hay chạy nhầm không?"

| Tình huống | Điều gì xảy ra | Mất job? | Chạy nhầm? |
|---|---|---|---|
| Đổi chu kỳ 15' → 1 tiếng | `SP_SET_JOB_SCHEDULE` dọn lượt `READY` cũ **rồi** ghi cấu hình mới (một giao dịch) | ❌ | ❌ |
| Có người `UPDATE` thẳng bảng cấu hình | Trigger `TR_JOB_DEFINITION_PURGE_PENDING` dọn thay | ❌ | ❌ |
| Đổi cột không liên quan lịch (timeout/retry) | Trigger **không** động vào hàng đợi | ❌ | ❌ |
| Pod mới init / pod restart | Đọc tiếp từ offset đã commit (`auto.offset.reset=latest`); dòng `READY` chưa ai chạy → `SP_JOB_RECOVER` | ❌ | ❌ |
| Broker Kafka chết / topic bị xoá | `SP_JOB_RECOVER` produce lại trong ≤30s | ❌ | ❌ |
| Dừng dịch vụ nửa ngày rồi bật lại | Scheduler **chỉ** sinh slot HIỆN TẠI — không dồn slot đã lỡ | ❌ | ❌ |
| Lượt 14:59 không kịp chạy, sang hôm sau | `C_MAX_DELAY_SEC` → `SKIPPED`. **Không** chạy lại việc của hôm qua | ❌ | ❌ |
| Xoá chu kỳ (`clearInterval`) | Ngừng sinh + dọn lượt chờ; job về chế độ `ON_DEMAND` | ❌ | ❌ |
| Chưa từng cấu hình chu kỳ | `ON_DEMAND` — chỉ chạy khi có người đẩy. `SP_GET_JOB_STATUS` hiện rõ chế độ | ❌ | ❌ |
| Tắt job giữa lúc đang chạy | Heartbeat trả `still_mine=false` → worker dừng trong ~20s | ❌ | ❌ |

Toàn bộ bảng này có ca kiểm chứng trong `db/12_JOB_SMOKE.sql` khối **(C)** — trừ mấy dòng liên quan tới *broker/rebalance*, vốn cần một cụm Kafka thật để chạy (phần DB của chúng đã được kiểm).
