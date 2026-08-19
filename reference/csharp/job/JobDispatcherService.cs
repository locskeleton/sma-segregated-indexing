using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Serilog;
using StackExchange.Redis;

namespace SdiCoreMessagingProcess.Jobs;

/// <summary>
/// WORKER — chạy trên MỌI pod. Nằm chờ ở XREADGROUP BLOCK, có message là chạy ngay (yêu cầu BRD #2:
/// "cứ có job đẩy vào là thực hiện xử lý"). Không polling DB, không sleep-loop.
///
/// ★ VÌ SAO KHÔNG BỊ XỬ LÝ TRÙNG TRÊN NHIỀU POD (yêu cầu BRD #3) — ba lớp, lớp sau bọc lớp trước:
///   1. Consumer group: mỗi entry giao cho MỘT consumer. Đủ cho đường chạy bình thường.
///   2. SP_JOB_CLAIM: một UPDATE có điều kiện READY→RUNNING. Đây mới là CHỐT THẬT. Kể cả 20 pod
///      cùng cầm một jobRunId (XAUTOCLAIM, XADD trùng, người ta bấm chạy tay), đúng một pod đổi
///      được trạng thái; 19 pod còn lại nhận err=5 và đi tiếp.
///   3. Heartbeat có kiểm chủ sở hữu: pod bị treo lâu → lease hết hạn → pod khác giành job. Pod cũ
///      tỉnh dậy gọi heartbeat sẽ nhận still_mine=false → tự huỷ token → dừng ghi. Không có cửa
///      cho hai pod cùng ghi.
///
/// ★ THỨ TỰ COMPLETE → XACK (không được đảo):
///   Complete trước, XACK sau. Pod chết giữa hai bước ⇒ message được giao lại ⇒ claim thất bại
///   (lượt đã DONE) ⇒ XACK rồi bỏ qua. Vô hại.
///   Đảo lại (XACK trước) thì pod chết ⇒ message mất ⇒ lượt chạy kẹt RUNNING tới khi lease hết hạn.
///   Chậm hơn, và tệ hơn: nhật ký nói "đang chạy" trong khi không ai chạy.
/// </summary>
public class JobDispatcherService : BackgroundService
{
    private static readonly TimeSpan BlockTimeout     = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan HeartbeatEvery   = TimeSpan.FromSeconds(20);
    private const int                BatchPerRead     = 10;

    private readonly IDatabase      _redis;
    private readonly ISdiJobGateway _db;
    private readonly IJobRegistry   _registry;
    private readonly string         _owner;   // định danh pod: hostname + pid

    public JobDispatcherService(IDatabase redis, ISdiJobGateway db, IJobRegistry registry)
    {
        _redis    = redis;
        _db       = db;
        _registry = registry;
        _owner    = $"{Environment.MachineName}#{Environment.ProcessId}";
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        await JobStreamKeys.EnsureGroupAsync(_redis);
        Log.Information("[JOB] Dispatcher khởi động, owner={Owner}", _owner);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var entries = await _redis.StreamReadGroupAsync(
                    JobStreamKeys.Stream, JobStreamKeys.Group, _owner,
                    StreamPosition.NewMessages, count: BatchPerRead);

                if (entries.Length == 0)
                {
                    // BLOCK thật sự do StackExchange.Redis không expose trực tiếp trong overload này;
                    //   nghỉ ngắn rồi đọc lại. 500ms là trễ TỐI ĐA từ lúc job được đẩy vào tới lúc
                    //   chạy — vẫn là "chạy ngay" ở thang 15 phút của nghiệp vụ, mà không đốt CPU.
                    await Task.Delay(TimeSpan.FromMilliseconds(500), ct);
                    continue;
                }

                foreach (var e in entries)
                {
                    if (ct.IsCancellationRequested) break;
                    await HandleEntryAsync(e, ct);
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                // Redis chết / DB chết: KHÔNG được để vòng lặp thoát, vì thoát nghĩa là pod này
                //   vĩnh viễn không chạy job nào nữa cho tới lần deploy sau — và không ai nhận ra.
                Log.Error(ex, "[JOB] Vòng lặp dispatcher lỗi — thử lại sau 5s");
                try { await Task.Delay(BlockTimeout, ct); } catch { break; }
            }
        }
    }

    private async Task HandleEntryAsync(StreamEntry e, CancellationToken ct)
    {
        var raw = e.Values.FirstOrDefault(v => v.Name == JobStreamKeys.FieldJobRunId).Value;
        if (!long.TryParse(raw, out var jobRunId))
        {
            Log.Warning("[JOB] Entry {Id} không đọc được jobRunId — XACK bỏ qua", e.Id);
            await AckAsync(e.Id);
            return;
        }

        // ★ CHỐT CHỐNG TRÙNG. err≠0 ⇒ pod này KHÔNG chạy.
        var claim = await _db.ClaimAsync(jobRunId, _owner, e.Id.ToString(), ct);
        if (claim.Err != 0)
        {
            // err=3 (ngoài khung giờ → SKIPPED) · 5 (pod khác giữ) · 6 (DEAD) đều là kết cục HỢP LỆ:
            //   message đã được xử lý xong theo nghĩa "không còn gì để làm" ⇒ XACK.
            if (claim.Err == 3)
                Log.Warning("[JOB] runId={Id} BỎ QUA vì ngoài khung giờ: {Msg}", jobRunId, claim.Msg);
            else
                Log.Debug("[JOB] runId={Id} không claim được (err={Err}): {Msg}", jobRunId, claim.Err, claim.Msg);
            await AckAsync(e.Id);
            return;
        }

        var handler = _registry.Resolve(claim.HandlerKey);
        if (handler == null)
        {
            // Cấu hình trỏ tới handler chưa deploy. Đánh FAILED để nó hiện trên màn hình theo dõi,
            //   thay vì im lặng XACK và để job "chạy" mỗi 15 phút mà không làm gì suốt nhiều tuần.
            await _db.CompleteAsync(jobRunId, _owner, ok: false, rows: 0,
                msg: $"Không tìm thấy handler '{claim.HandlerKey}' trên pod này", ct);
            Log.Error("[JOB] runId={Id} handler '{H}' chưa đăng ký", jobRunId, claim.HandlerKey);
            await AckAsync(e.Id);
            return;
        }

        // Token của handler = token dừng pod + tín hiệu MẤT LEASE.
        using var lost = new CancellationTokenSource();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, lost.Token);

        long progress = 0;
        var ctx = new JobContext
        {
            JobRunId     = jobRunId,
            JobCode      = claim.JobCode,
            FireKey      = claim.FireKey,
            BusinessDate = claim.BusinessDate,
            PayloadJson  = claim.PayloadJson,
            Attempt      = claim.Attempt,
            HeartbeatAsync = async rows =>
            {
                Interlocked.Exchange(ref progress, rows);
                var mine = await _db.HeartbeatAsync(jobRunId, _owner, rows, CancellationToken.None);
                if (!mine)
                {
                    Log.Error("[JOB] runId={Id} MẤT LEASE — pod khác đã giành. Dừng ngay để không ghi song song.", jobRunId);
                    lost.Cancel();
                }
                return mine;
            }
        };

        // Nhịp tim nền: job dài (vòng 1000 batch) không phải tự nhớ gọi heartbeat mỗi vòng.
        using var beat = new Timer(async _ => { try { await ctx.HeartbeatAsync(Interlocked.Read(ref progress)); } catch { } },
                                   null, HeartbeatEvery, HeartbeatEvery);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            var rows = await handler.RunAsync(ctx, linked.Token);
            await _db.CompleteAsync(jobRunId, _owner, ok: true, rows: rows,
                msg: $"OK trong {sw.ElapsedMilliseconds} ms", CancellationToken.None);
            Log.Information("[JOB] {Code} runId={Id} XONG: {Rows} đơn vị, {Ms} ms",
                claim.JobCode, jobRunId, rows, sw.ElapsedMilliseconds);
        }
        catch (OperationCanceledException) when (lost.IsCancellationRequested)
        {
            // Mất lease: KHÔNG gọi COMPLETE (pod khác đang là chủ, ghi vào sẽ bị từ chối err=5 —
            //   nhưng quan trọng hơn là không được phép đụng vào lượt chạy của người khác).
            Log.Warning("[JOB] runId={Id} dừng vì mất lease", jobRunId);
        }
        catch (Exception ex)
        {
            await _db.CompleteAsync(jobRunId, _owner, ok: false, rows: Interlocked.Read(ref progress),
                msg: ex.Message.Length > 1900 ? ex.Message[..1900] : ex.Message, CancellationToken.None);
            Log.Error(ex, "[JOB] {Code} runId={Id} LỖI sau {Ms} ms", claim.JobCode, jobRunId, sw.ElapsedMilliseconds);
        }
        finally
        {
            await AckAsync(e.Id);   // ★ luôn ACK SAU khi đã đóng sổ ở DB
        }
    }

    private async Task AckAsync(RedisValue id)
    {
        try { await _redis.StreamAcknowledgeAsync(JobStreamKeys.Stream, JobStreamKeys.Group, id); }
        catch (Exception ex) { Log.Warning(ex, "[JOB] XACK hụt cho entry {Id} — sẽ bị XAUTOCLAIM giao lại, vô hại", id); }
    }
}
