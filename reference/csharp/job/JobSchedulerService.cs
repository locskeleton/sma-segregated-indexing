using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Serilog;
using StackExchange.Redis;

namespace SdiCoreMessagingProcess.Jobs;

/// <summary>
/// BỘ QUÉT LỊCH + LƯỚI AN TOÀN — chạy trên MỌI pod (không cần bầu leader, không cần lock phân tán).
///
/// Ba việc, mỗi việc một nhịp riêng:
///   ① 10s/lần — SP_JOB_ENQUEUE_DUE: job định kỳ nào tới hạn thì tạo lượt chạy rồi XADD.
///      Chạy trên mọi pod mà không sinh trùng, vì UQ (job_code, fire_key) trong DB quyết ai thắng.
///      Không cần leader election — thứ mà mỗi lần hỏng lại hỏng vào lúc không ai để ý.
///   ② 30s/lần — SP_JOB_REAP: thu hồi lượt của pod đã chết + ĐẨY LẠI những lượt READY bị mất
///      message. Đây là thứ bù đắp cho việc hàng đợi nằm ở Redis (xem JobStreamKeys).
///   ③ 60s/lần — XAUTOCLAIM: entry nằm trong pending list quá lâu (pod nhận rồi chết trước khi
///      XACK) được giao cho pod còn sống. Không có bước này thì message đó nằm trong pending
///      MÃI MÃI: DB đã thu hồi lượt chạy nhưng không còn ai đánh thức nó.
///
/// ★ KHUNG GIỜ: proc SP_JOB_ENQUEUE_DUE tự lọc (guard tầng 1). Scheduler KHÔNG tự kiểm giờ —
///   cố ý. Luật giờ nằm ĐÚNG MỘT CHỖ (UDF_JOB_IN_WINDOW). Cài luật ở cả C# lẫn SQL là cách chắc
///   chắn để một ngày nào đó hai bên hiểu khác nhau, và bên sai sẽ là bên gọi FO lúc 15h05.
/// </summary>
public class JobSchedulerService : BackgroundService
{
    private static readonly TimeSpan ScanEvery    = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan ReapEvery    = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan ClaimEvery   = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan ConsumerSweep = TimeSpan.FromHours(1);
    /// <summary>Consumer im lặng lâu hơn ngần này VÀ không còn message treo ⇒ xoá khỏi group.</summary>
    private static readonly TimeSpan ConsumerIdleMax = TimeSpan.FromHours(6);

    private readonly IDatabase      _redis;
    private readonly ISdiJobGateway _db;
    private readonly string         _owner;

    private DateTime _lastReap  = DateTime.MinValue;
    private DateTime _lastClaim = DateTime.MinValue;
    private DateTime _lastSweep = DateTime.MinValue;

    public JobSchedulerService(IDatabase redis, ISdiJobGateway db)
    {
        _redis = redis;
        _db    = db;
        _owner = $"{Environment.MachineName}#{Environment.ProcessId}";
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await JobStreamKeys.EnsureGroupAsync(_redis);
        Log.Information("[JOB] Scheduler khởi động trên pod {Owner}", _owner);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                // ① tới hạn → tạo lượt → đánh thức worker
                var due = await _db.EnqueueDueAsync(ct);
                await PushAsync(due, "scheduler");

                // ② lưới an toàn cho hàng đợi nằm ở Redis
                if (DateTime.UtcNow - _lastReap > ReapEvery)
                {
                    _lastReap = DateTime.UtcNow;
                    var revived = await _db.ReapAsync(staleSec: 30, ct);
                    if (revived.Count > 0)
                        Log.Warning("[JOB] REAP: {N} lượt READY không có ai chạy (message mất?) — đẩy lại stream", revived.Count);
                    await PushAsync(revived, "reap");
                }

                // ③ entry treo trong pending list của pod đã chết
                if (DateTime.UtcNow - _lastClaim > ClaimEvery)
                {
                    _lastClaim = DateTime.UtcNow;
                    await AutoClaimStuckAsync();
                }

                // ④ dọn consumer chết — thứ DUY NHẤT trong thiết kế này phình không giới hạn
                if (DateTime.UtcNow - _lastSweep > ConsumerSweep)
                {
                    _lastSweep = DateTime.UtcNow;
                    await SweepDeadConsumersAsync();
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
    /// XADD chỉ chở jobRunId. XADD hụt (Redis chết) KHÔNG phải thảm hoạ: dòng T_JOB_RUN đã nằm đó
    /// ở trạng thái READY, và nhịp REAP sau sẽ đẩy lại. Vì thế ở đây chỉ log WARNING, không ném.
    /// </summary>
    private async Task PushAsync(IReadOnlyList<DueJob> jobs, string src)
    {
        foreach (var j in jobs)
        {
            try
            {
                await _redis.StreamAddAsync(JobStreamKeys.Stream,
                    JobStreamKeys.FieldJobRunId, j.JobRunId,
                    maxLength: JobStreamKeys.MaxLen, useApproximateMaxLength: true);
                Log.Information("[JOB] {Src} → đẩy {Code} runId={Id}", src, j.JobCode, j.JobRunId);
            }
            catch (Exception ex)
            {
                Log.Warning(ex, "[JOB] XADD hụt cho runId={Id} — REAP sẽ đẩy lại", j.JobRunId);
            }
        }
    }

    private async Task AutoClaimStuckAsync()
    {
        try
        {
            var res = await _redis.StreamAutoClaimAsync(
                JobStreamKeys.Stream, JobStreamKeys.Group, _owner,
                minIdleTimeInMs: (long)JobStreamKeys.PendingIdle.TotalMilliseconds,
                startAtId: "0-0", count: 50);

            if (res.ClaimedEntries.Length > 0)
                Log.Warning("[JOB] XAUTOCLAIM: nhận {N} entry treo từ pod đã chết", res.ClaimedEntries.Length);
            // KHÔNG xử lý ở đây: entry vừa claim đã thuộc pending list của pod này, vòng đọc của
            //   JobDispatcherService sẽ nhặt. Xử lý ở hai nơi = hai đường code làm cùng một việc,
            //   và đường ít chạy hơn sẽ là đường mục ruỗng không ai biết.
        }
        catch (Exception ex)
        {
            Log.Warning(ex, "[JOB] XAUTOCLAIM lỗi — bỏ qua nhịp này");
        }
    }

    /// <summary>
    /// ★ DỌN CONSUMER CHẾT — chỗ DUY NHẤT trong thiết kế này có thể phình KHÔNG GIỚI HẠN.
    ///
    /// Tên consumer là `hostname#pid`, và `XREADGROUP` TỰ TẠO consumer khi thấy tên lạ. Mỗi lần
    /// pod khởi động lại (deploy, OOM, evict, scale) là một tên mới ⇒ một bản ghi consumer mới
    /// nằm lại trong group VĨNH VIỄN. Redis không tự dọn.
    ///
    /// Không chết ai ngay: mỗi consumer rỗng chỉ tốn khoảng trăm byte. Nhưng nó là thứ **không có
    /// trần** — 10 pod × vài lần deploy/ngày × 365 ngày = hàng chục nghìn bản ghi, và `XPENDING` /
    /// `XINFO CONSUMERS` / `XAUTOCLAIM` đều phải đi qua danh sách đó. Bug loại này không gây sự cố,
    /// nó chỉ làm hệ chậm dần trong nhiều tháng cho tới lúc không ai còn nhớ vì sao.
    ///
    /// AN TOÀN: chỉ xoá consumer **không còn message treo** (`PendingMessageCount == 0`) và đã im
    /// lặng quá lâu. Xoá consumer CÒN pending là **vứt luôn** những entry đó khỏi PEL — mất hẳn
    /// đường để `XAUTOCLAIM` cứu. (Ở hệ này thì `SP_JOB_REAP` vẫn dựng lại được từ DB, nhưng
    /// không có lý do gì để tự đẩy mình vào chỗ phải nhờ lưới cứu.)
    /// </summary>
    private async Task SweepDeadConsumersAsync()
    {
        try
        {
            var consumers = await _redis.StreamConsumersAsync(JobStreamKeys.Stream, JobStreamKeys.Group);
            var removed = 0;
            foreach (var c in consumers)
            {
                if (c.Name == _owner) continue;                                    // không tự xoá mình
                if (c.PendingMessageCount > 0) continue;                            // còn việc treo ⇒ để XAUTOCLAIM lo
                if (c.IdleTimeInMilliseconds < ConsumerIdleMax.TotalMilliseconds) continue;

                await _redis.StreamDeleteConsumerAsync(JobStreamKeys.Stream, JobStreamKeys.Group, c.Name);
                removed++;
            }
            if (removed > 0)
                Log.Information("[JOB] Dọn {N} consumer chết khỏi group (pod đã restart/scale down)", removed);
        }
        catch (Exception ex)
        {
            Log.Warning(ex, "[JOB] Dọn consumer lỗi — bỏ qua nhịp này (không ảnh hưởng chạy job)");
        }
    }
}
