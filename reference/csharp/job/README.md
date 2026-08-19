# Khung job chạy nền + snapshot near-realtime FO — bản tham chiếu C#

> Code C# **không nằm trong repo này** (nó ở `sdi-core-messaging-process`). Đây là bản tham chiếu viết theo thiết kế ở [`docs/SDI-nearrt-fo-snapshot-design.md`](../../../docs/SDI-nearrt-fo-snapshot-design.md), đi kèm [`db/11_JOB.sql`](../../../db/11_JOB.sql).

## File

| File | Vai trò |
|---|---|
| `JobContracts.cs` | `IJobHandler` · `JobContext` · `JobRegistry` · `ISdiJobGateway` (map 1-1 sang SP) |
| `JobChannelKeys.cs` | Kênh Redis Pub/Sub (chuông cửa) + hằng số nguồn đánh thức |
| `JobSchedulerService.cs` | Quét lịch (10s) · REAP (30s) — chạy trên **mọi** pod |
| `JobDispatcherService.cs` | Worker: nghe kênh → `SP_JOB_CLAIM` → chạy handler → `SP_JOB_COMPLETE` |
| `TradingWindowGuard.cs` | **Guard tầng 4** — chặn ngay trước từng HTTP call sang FO |
| `FoSnapshotJobHandler.cs` | Nghiệp vụ: scope → cắt 50 KH/batch → gọi FO → ingest RT → gộp master |

## Đăng ký DI

```csharp
services.AddSingleton<ISubscriber>(sp =>                          // Pub/Sub: kết nối subscriber riêng
    sp.GetRequiredService<IConnectionMultiplexer>().GetSubscriber());
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

```csharp
var (id, err) = await _db.EnqueueAsync("CLEANUP_TMP", fireKey: requestId, payload: null,
                                       businessDate: null, user: "api", ct);
if (err == 0)
    await _sub.PublishAsync(JobChannelKeys.NotifyChannel, id);
// err == 4 ⇒ requestId này đã đẩy rồi, KHÔNG tạo trùng. Không phải lỗi, không cần retry.
```

Worker đang nằm nghe kênh → nhận trong khoảng **một mili-giây** (đẩy thật, không hỏi thăm).
`PublishAsync` trả về **số pod đã nhận**; bằng 0 nghĩa là không ai đang nghe — hãy log lại, đó là
cách duy nhất phân biệt "Redis ổn nhưng subscriber chết" với "Redis chết".

---

## Ba câu hỏi sẽ bị hỏi khi review

### 1. "Pub/Sub phát cho MỌI pod thì chẳng phải job chạy 10 lần à?"

Không. Pub/Sub chỉ quyết **ai nghe được tin**, không quyết **ai được chạy**. Cả 10 pod cùng lao vào `SP_JOB_CLAIM` — một `UPDATE ... WHERE C_STATUS='READY'` — đúng một pod đổi được trạng thái, 9 pod nhận `err=5` rồi đi tiếp. Chốt chặn nằm ở nơi mọi pod nhìn thấy cùng một sự thật, không nằm ở tầng giao tin.

Giá phải trả: ~9 lượt claim hụt cho mỗi job. Ở 24 lượt/ngày là ~240 truy vấn/ngày — không đáng kể. `JobDispatcherService` còn chặn bớt bằng hạn mức job đồng thời: pod đang bận thì **không thèm claim**, nhường pod rảnh (đây cũng chính là cách chia tải thay cho consumer group của Streams).

### 2. "Pub/Sub bắn-rồi-quên thì mất tin là mất job?"

Mất *tin*, không mất *job*. Dòng `T_JOB_RUN` vẫn ở đó với trạng thái `READY`. `SP_JOB_REAP` quét đúng những dòng đó (READY, tới hạn, nằm quá `stale_sec` giây) và **phát lại**.

⇒ Hậu quả tối đa của việc Redis chết hẳn: **chậm một nhịp reaper (30 giây)**. Không dòng nào mất, không dòng nào chạy hai lần.

Đây là lý do `NotifyAsync` chỉ log WARNING khi publish hụt thay vì ném — ném ở đó là biến một sự cố tự hồi phục thành một lượt chạy FAILED.

⚠️ **Cái bẫy vận hành:** chuông tắt hẳn thì hệ **vẫn chạy đúng**, chỉ chậm ~30 giây — và không ai nhận ra. Vì thế mỗi lượt chạy ghi `C_CLAIM_SOURCE` ('notify' | 'reap'), và `SP_GET_JOB_STATUS` trả `C_REAP_WAKE_7D`. Con số đó xấp xỉ tổng số lượt ⇒ Pub/Sub đã chết từ lâu.

### 3. "Guard khung giờ 4 tầng có thừa không?"

Không. Mỗi tầng bắt một ca mà tầng khác **không thể** bắt:

| Tầng | Ở đâu | Bắt ca gì |
|---|---|---|
| 1 | `SP_JOB_ENQUEUE(_DUE)` | Không sinh lượt chạy lúc 15h30 |
| 2 | `SP_JOB_CLAIM` | Job sinh lúc 14h59, pod nhặt lúc 15h02 (stream tồn đọng / pod restart / reaper trả lại) |
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
| Pod mới init / pod restart | Consumer group tạo ở `$`; entry treo của pod chết → `XAUTOCLAIM`; dòng `READY` mất message → `SP_JOB_REAP` | ❌ | ❌ |
| Redis `FLUSHALL` | `SP_JOB_REAP` đẩy lại trong ≤30s | ❌ | ❌ |
| Dừng dịch vụ nửa ngày rồi bật lại | Scheduler **chỉ** sinh slot HIỆN TẠI — không dồn slot đã lỡ | ❌ | ❌ |
| Lượt 14:59 không kịp chạy, sang hôm sau | `C_MAX_DELAY_SEC` → `SKIPPED`. **Không** chạy lại việc của hôm qua | ❌ | ❌ |
| Xoá chu kỳ (`clearInterval`) | Ngừng sinh + dọn lượt chờ; job về chế độ `ON_DEMAND` | ❌ | ❌ |
| Chưa từng cấu hình chu kỳ | `ON_DEMAND` — chỉ chạy khi có người đẩy. `SP_GET_JOB_STATUS` hiện rõ chế độ | ❌ | ❌ |
| Tắt job giữa lúc đang chạy | Heartbeat trả `still_mine=false` → worker dừng trong ~20s | ❌ | ❌ |

Toàn bộ bảng này có ca kiểm chứng trong `db/12_JOB_SMOKE.sql` khối **(C)** — trừ ba dòng có chữ *Redis*, vốn cần một cụm Redis thật để chạy (phần DB của chúng đã được kiểm).
