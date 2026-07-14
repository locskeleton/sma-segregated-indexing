using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Serilog;
using StackExchange.Redis;

namespace SdiCoreMessagingProcess.Services;

public enum JobRunStatus { Done, Failed, SkippedRunningElsewhere }

public sealed record JobRunResult(JobRunStatus Status, string JobId, long TotalMs, string? Msg = null)
{
    public bool Ok => Status == JobRunStatus.Done;
}

/// <summary>
/// GENERIC #2 — chạy MỘT JOB ĐƠN LẺ (không batch) đúng một lần, có log job + đo thời gian.
///
/// Dùng cho: fee accrue · fee charge · index · EOD · bất kỳ job đơn nào sau này.
/// (Trước đây mấy job này bị nhét vào SyncEodDataGeneric với msg="{}", totalRow=1, numProcessed=1
///  — tức là LỪA cái máy đếm-batch chạy qua. Chúng KHÔNG có batch nào cả.)
///
/// ⚠️ LOCK Ở ĐÂY CHỈ ĐỂ ĐỠ TỐN CPU, KHÔNG ĐỂ BẢO VỆ TÍNH ĐÚNG.
///    TTL sẽ hết vào lúc tệ nhất (pod GC pause, network blip) ⇒ 2 pod cùng chạy.
///    Điều đó VÔ HẠI vì mọi SP bên dưới đều idempotent:
///      • SP_FEE_RUN_DAILY  → MERGE upsert; kỳ ĐÃ/ĐANG THU thì khoá, không đè
///      • SP_EOD_RUN        → SP_EOD_STEP có resume-gate: job đã DONE thì BỎ QUA
///      • SP_EOD_RUN_INDEX  → DELETE+INSERT index ngày đó
///    ⇒ Chạy 2 lần = lãng phí, KHÔNG SAI SỐ. Đừng bao giờ để tiền phụ thuộc vào một cái TTL.
/// </summary>
public class JobRunner
{
    private readonly IDatabase     _redis;
    private readonly IJobLogWriter _jobLog;   // LogJobKafkaStarting / updateJob — repo sẵn có

    private static readonly TimeSpan LockTtl   = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan RenewEach = TimeSpan.FromSeconds(20);

    // Giải phóng lock CHỈ KHI nó vẫn là của mình (tránh xoá nhầm lock của pod khác sau khi TTL hết)
    private const string LuaReleaseIfMine = @"
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
else
    return 0
end";

    public JobRunner(IDatabase redis, IJobLogWriter jobLog)
    {
        _redis  = redis;
        _jobLog = jobLog;
    }

    /// <param name="bizType">Loại job (BizTypeDefine.*)</param>
    /// <param name="tranDate">Ngày dữ liệu — cùng bizType+tranDate = CÙNG MỘT job (định danh xác định)</param>
    /// <param name="func">Việc cần làm. Trả (Ok, Total, Success, Fail) để ghi log job.</param>
    public async Task<JobRunResult> RunOnceAsync(
        string bizType,
        string tranDate,
        Func<Task<(bool Ok, long Total, long Success, long Fail, string? Msg)>> func)
    {
        // ★ JOB_ID XÁC ĐỊNH TRƯỚC: bizType + tranDate. Không cần "message đầu tiên" tạo ra nó,
        //   không cần INCR==1, không cần lock để giành quyền tạo, không cần ai chờ ai.
        var lockKey  = $"JOB:{bizType}:{tranDate}:LOCK";
        var podToken = $"{Environment.MachineName}:{Guid.NewGuid():N}";

        // Best-effort: thua thì đứng ngoài. KHÔNG chờ (chờ trong handler = tự sát).
        if (!await _redis.StringSetAsync(lockKey, podToken, LockTtl, When.NotExists))
        {
            Log.Debug("[Job][{Biz}] {Date} — pod khác đang chạy, bỏ qua", bizType, tranDate);
            return new JobRunResult(JobRunStatus.SkippedRunningElsewhere, "", 0);
        }

        using var cts   = new CancellationTokenSource();
        var       renew = RenewLockAsync(lockKey, podToken, cts.Token);   // gia hạn TTL trong lúc chạy
        var       sw    = Stopwatch.StartNew();

        // MERGE theo (bizType, tranDate) ⇒ idempotent. Pod nào đến trước tạo, pod sau không làm gì.
        var jobId = await _jobLog.StartAsync(bizType, tranDate);

        try
        {
            var (ok, total, success, fail, msg) = await func();
            sw.Stop();

            await _jobLog.FinishAsync(jobId, bizType, total, success, fail, sw.ElapsedMilliseconds, ok, msg);

            Log.Information("[Job][{Biz}] {Date} {Status} — tổng {Total}, thành công {Ok}, lỗi {Fail}, {Ms}ms",
                            bizType, tranDate, ok ? "DONE" : "FAILED", total, success, fail, sw.ElapsedMilliseconds);

            return new JobRunResult(ok ? JobRunStatus.Done : JobRunStatus.Failed,
                                    jobId, sw.ElapsedMilliseconds, msg);
        }
        catch (Exception ex)
        {
            sw.Stop();
            await _jobLog.FinishAsync(jobId, bizType, 0, 0, 0, sw.ElapsedMilliseconds, ok: false, msg: ex.Message);
            Log.Error(ex, "[Job][{Biz}] {Date} EXCEPTION sau {Ms}ms", bizType, tranDate, sw.ElapsedMilliseconds);
            return new JobRunResult(JobRunStatus.Failed, jobId, sw.ElapsedMilliseconds, ex.Message);
        }
        finally
        {
            cts.Cancel();
            try { await renew; } catch { /* ignore */ }

            await _redis.ScriptEvaluateAsync(LuaReleaseIfMine,
                new RedisKey[] { lockKey }, new RedisValue[] { podToken });
        }
    }

    /// <summary>Gia hạn TTL lock trong lúc job đang chạy (job dài hơn 60s cũng không mất lock).</summary>
    private async Task RenewLockAsync(string key, string token, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try { await Task.Delay(RenewEach, ct); } catch (TaskCanceledException) { return; }

            // chỉ gia hạn nếu lock VẪN LÀ CỦA MÌNH
            var cur = await _redis.StringGetAsync(key);
            if (cur == token) await _redis.KeyExpireAsync(key, LockTtl);
            else return;   // mất lock rồi (TTL hết) → thôi. Job vẫn chạy tiếp — idempotent nên VÔ HẠI.
        }
    }
}

/// <summary>Ghi log job — map sang LogJobKafkaStarting / updateJob sẵn có.</summary>
public interface IJobLogWriter
{
    /// <summary>MERGE theo (bizType, tranDate) ⇒ gọi lại không sinh job trùng. Trả jobId.</summary>
    Task<string> StartAsync(string bizType, string tranDate);

    Task FinishAsync(string jobId, string bizType, long total, long success, long fail,
                     long totalMs, bool ok, string? msg);
}
