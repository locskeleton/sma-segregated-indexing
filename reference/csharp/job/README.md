# Khung job chạy nền + snapshot near-realtime FO — bản tham chiếu C#

> Code C# **không nằm trong repo này** (nó ở `sdi-core-messaging-process`). Đây là bản tham chiếu viết theo thiết kế ở [`docs/SDI-nearrt-fo-snapshot-design.md`](../../../docs/SDI-nearrt-fo-snapshot-design.md), đi kèm [`db/11_JOB.sql`](../../../db/11_JOB.sql).

## File

| File | Vai trò |
|---|---|
| `JobContracts.cs` | `IJobHandler` · `JobContext` · `JobRegistry` · `ISdiJobGateway` (map 1-1 sang SP) |
| `JobStreamKeys.cs` | Khoá Redis Streams + tạo consumer group |
| `JobSchedulerService.cs` | Quét lịch (10s) · REAP (30s) · XAUTOCLAIM (60s) — chạy trên **mọi** pod |
| `JobDispatcherService.cs` | Worker: đọc stream → `SP_JOB_CLAIM` → chạy handler → `SP_JOB_COMPLETE` → XACK |
| `TradingWindowGuard.cs` | **Guard tầng 4** — chặn ngay trước từng HTTP call sang FO |
| `FoSnapshotJobHandler.cs` | Nghiệp vụ: scope → cắt 50 KH/batch → gọi FO → ingest RT → gộp master |

## Đăng ký DI

```csharp
services.AddSingleton<ISdiJobGateway, SdiJobGateway>();          // tự cài bằng Dapper/ADO
services.AddSingleton<IJobRegistry, JobRegistry>();
services.AddSingleton<IJobHandler, FoSnapshotJobHandler>();      // thêm job mới = thêm 1 dòng ở đây
services.AddSingleton<IFoSnapshotClient, FoSnapshotHttpClient>(); // HttpClient thật của bạn
services.AddSingleton(sp => new TradingWindowGuard(
    from: new TimeSpan(9, 0, 0), to: new TimeSpan(15, 0, 0),
    isBusinessDate: d => sp.GetRequiredService<ISdiJobGateway>().IsBusinessDateAsync(d)));

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

## Đẩy job tuỳ ý (ngoài lịch) — "cứ có job đẩy vào là chạy"

```csharp
var (id, err) = await _db.EnqueueAsync("CLEANUP_TMP", fireKey: requestId, payload: null,
                                       businessDate: null, user: "api", ct);
if (err == 0)
    await _redis.StreamAddAsync(JobStreamKeys.Stream, JobStreamKeys.FieldJobRunId, id);
// err == 4 ⇒ requestId này đã đẩy rồi, KHÔNG tạo trùng. Không phải lỗi, không cần retry.
```

Worker đang chờ ở vòng đọc stream → nhặt trong vòng ~500ms.

---

## Ba câu hỏi sẽ bị hỏi khi review

### 1. "Redis Streams đã có consumer group, sao còn cần `SP_JOB_CLAIM`?"

Consumer group hứa *mỗi message giao cho một consumer*, **không** hứa *mỗi message được xử lý đúng một lần*. Ba đường làm nó giao lại cùng một `jobRunId`:

- `XAUTOCLAIM` sau khi pod nhận rồi treo (pod treo ≠ pod chết — nó vẫn có thể tỉnh dậy và chạy tiếp);
- `XADD` hai lần (scheduler + reaper cùng thấy một dòng READY);
- pod restart giữa lúc đang xử lý, entry còn trong pending list.

`SP_JOB_CLAIM` là một `UPDATE ... WHERE C_STATUS='READY'`. Ai đổi được trạng thái người đó chạy. Đó là thứ **không thể có hai người thắng**, và nó nằm ở nơi mọi pod nhìn thấy cùng một sự thật.

### 2. "Đặt hàng đợi ở Redis thì mất Redis là mất job?"

Mất *message*, không mất *job*. Dòng `T_JOB_RUN` vẫn ở đó với trạng thái `READY`. `SP_JOB_REAP` quét đúng những dòng đó (READY, tới hạn, nằm quá `stale_sec` giây) và trả về để `XADD` lại.

⇒ Hậu quả tối đa của việc `FLUSHALL` nguyên cụm Redis: **chậm một nhịp reaper (30 giây)**. Không có dòng nào mất, không có dòng nào chạy hai lần.

Đây là lý do `PushAsync` chỉ log WARNING khi `XADD` hụt thay vì ném — ném ở đó là biến một sự cố tự hồi phục thành một lượt chạy FAILED.

### 3. "Guard khung giờ 4 tầng có thừa không?"

Không. Mỗi tầng bắt một ca mà tầng khác **không thể** bắt:

| Tầng | Ở đâu | Bắt ca gì |
|---|---|---|
| 1 | `SP_JOB_ENQUEUE(_DUE)` | Không sinh lượt chạy lúc 15h30 |
| 2 | `SP_JOB_CLAIM` | Job sinh lúc 14h59, pod nhặt lúc 15h02 (stream tồn đọng / pod restart / reaper trả lại) |
| 3 | `SP_INGEST_FO_SNAPSHOT_RT` | Ai đó gọi proc bằng tay; job ghi ngày không phải hôm nay |
| **4** | `TradingWindowGuard` (C#) | **Chu kỳ 1000 batch khởi động lúc 14h50, tới batch 700 thì đã 15h02** |

Tầng 4 là tầng duy nhất bắt được ca cuối — vì tầng 1 và 2 chỉ kiểm **một lần, lúc bắt đầu**, còn chu kỳ thì kéo dài nhiều phút. Bỏ tầng 4 nghĩa là vẫn có hàng trăm request bay sang FO sau giờ đóng cửa, và cả 3 tầng kia đều đã "duyệt" nó từ trước.

Luật giờ chỉ được định nghĩa **một chỗ**: `UDF_JOB_IN_WINDOW` + `T_JOB_DEFINITION`. `TradingWindowGuard` nhận khung giờ qua tham số DI (đọc từ chính bảng đó lúc khởi động), **không** hard-code lại 9h–15h trong code C#.
