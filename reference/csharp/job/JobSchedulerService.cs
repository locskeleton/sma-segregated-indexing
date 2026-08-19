using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Serilog;
using StackExchange.Redis;

namespace SdiCoreMessagingProcess.Jobs;

/// <summary>
/// BỘ QUÉT LỊCH + ĐƯỜNG HỒI PHỤC — chạy trên MỌI pod (không cần bầu leader, không cần lock phân tán).
///
/// Hai việc, hai nhịp riêng:
///   ① 10s/lần — SP_JOB_ENQUEUE_DUE: job định kỳ nào tới hạn thì tạo lượt chạy rồi PUBLISH.
///      Chạy trên mọi pod mà không sinh trùng, vì UQ (job_code, fire_key) trong DB quyết ai thắng.
///      Không cần leader election — thứ mà mỗi lần hỏng lại hỏng vào lúc không ai để ý.
///   ② 30s/lần — SP_JOB_REAP: thu hồi lượt của pod đã chết, đóng dấu lượt quá hạn, và **phát lại**
///      những lượt READY chưa ai chạy.
///
/// ★ ② LÀ ĐƯỜNG HỒI PHỤC CHÍNH, KHÔNG PHẢI DỰ PHÒNG. Pub/Sub bắn-rồi-quên: tin phát ra lúc pod
///   đang restart / mất kết nối / Redis chết là mất luôn, trong khi dòng T_JOB_RUN vẫn nằm đó ở
///   trạng thái READY. Không có ② thì job đó nằm im vĩnh viễn và KHÔNG AI BIẾT.
///   Có ② thì mất tin ⇒ chậm tối đa 30 giây. Phát trùng cũng vô hại vì SP_JOB_CLAIM chỉ cho một
///   pod thắng.
///
/// ★ KHUNG GIỜ: proc SP_JOB_ENQUEUE_DUE tự lọc (guard tầng 1). Scheduler KHÔNG tự kiểm giờ —
///   cố ý. Luật giờ nằm ĐÚNG MỘT CHỖ (UDF_JOB_IN_WINDOW). Cài luật ở cả C# lẫn SQL là cách chắc
///   chắn để một ngày nào đó hai bên hiểu khác nhau, và bên sai sẽ là bên gọi FO lúc 15h05.
///
/// (Bản Redis Streams trước đây còn hai nhịp nữa — XAUTOCLAIM và dọn consumer chết. Đổi sang
///  Pub/Sub thì cả hai biến mất: không có pending list để cứu, không có consumer để rò rỉ.)
/// </summary>
public class JobSchedulerService : BackgroundService
{
    private static readonly TimeSpan ScanEvery = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan ReapEvery = TimeSpan.FromSeconds(30);

    private readonly ISubscriber    _sub;
    private readonly ISdiJobGateway _db;
    private readonly string         _owner;

    private DateTime _lastReap = DateTime.MinValue;

    public JobSchedulerService(ISubscriber sub, ISdiJobGateway db)
    {
        _sub   = sub;
        _db    = db;
        _owner = $"{Environment.MachineName}#{Environment.ProcessId}";
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        Log.Information("[JOB] Scheduler khởi động trên pod {Owner}", _owner);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                // ① tới hạn → tạo lượt → rung chuông
                var due = await _db.EnqueueDueAsync(ct);
                await NotifyAsync(due, "scheduler");

                // ② đường hồi phục
                if (DateTime.UtcNow - _lastReap > ReapEvery)
                {
                    _lastReap = DateTime.UtcNow;
                    var revived = await _db.ReapAsync(staleSec: 30, ct);
                    if (revived.Count > 0)
                        Log.Warning("[JOB] REAP: {N} lượt READY không ai chạy (tin bị mất?) — phát lại", revived.Count);
                    await NotifyAsync(revived, "reap");
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, "[JOB] Scheduler lỗi một nhịp — bỏ qua, nhịp sau chạy tiếp");
            }

            try { await Task.Delay(ScanEvery, ct); } catch { break; }
        }
    }

    /// <summary>
    /// PUBLISH chỉ chở jobRunId — không payload, không trạng thái. Worker cầm id rồi hỏi DB mọi thứ
    /// khác; chở bản chụp cấu hình trong tin nhắn là mở đường cho một job vừa bị sửa/tắt vẫn chạy
    /// theo bản cũ đang bay trên đường truyền.
    ///
    /// Publish hụt (Redis chết) KHÔNG phải thảm hoạ: dòng T_JOB_RUN đã nằm đó ở trạng thái READY và
    /// nhịp REAP sau sẽ phát lại. Vì thế ở đây chỉ log WARNING, không ném — ném là biến một sự cố
    /// tự hồi phục thành một lượt chạy FAILED.
    /// </summary>
    private async Task NotifyAsync(IReadOnlyList<DueJob> jobs, string src)
    {
        foreach (var j in jobs)
        {
            try
            {
                // PublishAsync trả về SỐ CLIENT đã nhận. 0 = không pod nào đang nghe.
                //   Đây là tín hiệu quý: nó phân biệt "Redis ổn nhưng không ai nghe" (đang deploy,
                //   subscriber chết, sai tên kênh giữa hai phiên bản) với "Redis chết" (ném lỗi).
                //   Không có nó thì cả hai đều im lặng như nhau và ta chỉ thấy job chậm 30 giây.
                var received = await _sub.PublishAsync(JobChannelKeys.NotifyChannel, j.JobRunId);
                if (received == 0)
                    Log.Warning("[JOB] {Src} → runId={Id}: KHÔNG pod nào đang nghe kênh {Ch} — " +
                                "reaper sẽ nhặt lại sau ≤30s", src, j.JobRunId, JobChannelKeys.Notify);
                else
                    Log.Information("[JOB] {Src} → rung chuông {Code} runId={Id} ({N} pod nhận)",
                        src, j.JobCode, j.JobRunId, received);
            }
            catch (Exception ex)
            {
                Log.Warning(ex, "[JOB] PUBLISH hụt cho runId={Id} — REAP sẽ phát lại", j.JobRunId);
            }
        }
    }
}
