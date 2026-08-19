using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Serilog;

namespace SdiCoreMessagingProcess.Jobs;

/// <summary>
/// WORKER — chạy trên MỌI pod. Consumer Kafka của topic `sdi.job.notify`.
///
/// ★★ LUẬT SỐ MỘT: VÒNG POLL KHÔNG BAO GIỜ ĐƯỢC CHẶN.
///   Trình tự: `Consume` → commit offset → `SP_JOB_CLAIM` → ném job sang Task nền → quay lại
///   `Consume`. Chu kỳ FO chạy vài phút nằm HOÀN TOÀN ngoài vòng poll.
///   Chạy job trong vòng poll = vượt `max.poll.interval.ms` = Kafka đá pod khỏi group, NHƯNG
///   không giết thread ⇒ pod cũ thành zombie vẫn ghi DB trong khi pod mới xử lý lại cùng message.
///   `docs/SDI-kafka-batch-sync-design.md` §6 đã ghi lại đúng vòng xoáy đó.
///
/// ★ COMMIT TRƯỚC KHI CHẠY — CỐ Ý, dù nghe ngược tai.
///   Message KHÔNG phải sổ cái; `T_JOB_RUN` mới là. Pod chết sau khi commit mà chưa chạy xong ⇒
///   lease hết hạn ⇒ `SP_JOB_REAP` thu hồi ⇒ chạy lại. Còn commit SAU khi job xong thì vòng poll
///   phải chờ vài phút — đúng cái bẫy ở trên. Đổi một lưới cứu đã có lấy một cái bẫy đã biết là
///   một vụ đổi tồi.
///
/// ★ VÌ SAO KHÔNG XỬ LÝ TRÙNG TRÊN NHIỀU POD (yêu cầu BRD #3) — hai lớp:
///   1. Kafka consumer group: mỗi partition giao cho một consumer. Đủ cho đường chạy bình thường.
///   2. `SP_JOB_CLAIM` (`UPDATE ... WHERE C_STATUS='READY'`) — **chốt thật**. Rebalance giao lại,
///      pod zombie, reaper phát trùng: đúng một pod đổi được trạng thái, còn lại nhận `err=5`.
///   Lớp phụ: heartbeat kiểm chủ sở hữu ⇒ pod mất lease tự dừng, không ghi song song.
///
/// ★ HẠN MỨC JOB ĐỒNG THỜI: pod đang chạy đủ job thì **không claim**, nhường pod rảnh. Mọi pod
///   đều bận ⇒ không ai claim ⇒ dòng vẫn `READY` ⇒ `SP_JOB_REAP` phát lại sau ≤30 giây.
/// </summary>
public class JobDispatcherService : BackgroundService
{
    /// <summary>Số job một pod chạy đồng thời. Job FO là singleton nên thực tế hiếm khi chạm trần.</summary>
    private const int MaxConcurrentJobs = 4;

    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(5);

    // Nhịp tim: KHÔNG để cứng. Phải nhỏ hơn hẳn lease (DB chậm một nhịp không được làm mất job vào
    //   tay pod khác), nhưng cũng chính là độ trễ phát hiện "job vừa bị TẮT" / "mình vừa mất quyền".
    //   ⇒ timeout/4, kẹp [5s, 30s]: luôn có biên 4 lần lease, tệ nhất cũng phản ứng trong 30 giây.
    private static readonly TimeSpan HeartbeatMin   = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan HeartbeatMax   = TimeSpan.FromSeconds(30);
    // Chặn tần suất cho lời gọi ÉP nhịp tim từ handler (ctx.HeartbeatAsync).
    private static readonly TimeSpan HeartbeatFloor = TimeSpan.FromSeconds(5);

    private readonly ConsumerConfig _cfg;
    private readonly ISdiJobGateway _db;
    private readonly IJobRegistry   _registry;
    private readonly string         _owner;   // định danh pod: hostname + pid
    private readonly SemaphoreSlim  _slots = new(MaxConcurrentJobs, MaxConcurrentJobs);

    public JobDispatcherService(ConsumerConfig cfg, ISdiJobGateway db, IJobRegistry registry)
    {
        _cfg = cfg;
        // Ba dòng dưới KHÔNG được đổi — xem JobTopicKeys.
        _cfg.GroupId          = JobTopicKeys.Group;
        _cfg.EnableAutoCommit = false;                    // commit TAY, ngay sau khi nhận
        _cfg.AutoOffsetReset  = AutoOffsetReset.Latest;   // group mới KHÔNG dội lại cả topic

        _db       = db;
        _registry = registry;
        _owner    = $"{Environment.MachineName}#{Environment.ProcessId}";
    }

    protected override Task ExecuteAsync(CancellationToken ct)
        // Vòng consume là công việc CHẶN của thư viện Kafka ⇒ chạy trên thread riêng, đừng chiếm
        //   thread pool của host.
        => Task.Factory.StartNew(() => ConsumeLoop(ct), ct,
               TaskCreationOptions.LongRunning, TaskScheduler.Default);

    private void ConsumeLoop(CancellationToken ct)
    {
        using var consumer = new ConsumerBuilder<string, string>(_cfg)
            .SetErrorHandler((_, e) => Log.Error("[JOB] Kafka lỗi: {Reason} (fatal={F})", e.Reason, e.IsFatal))
            .SetPartitionsAssignedHandler((_, parts) =>
                Log.Information("[JOB] Nhận {N} partition sau rebalance", parts.Count))
            .Build();

        consumer.Subscribe(JobTopicKeys.Topic);
        Log.Information("[JOB] Dispatcher nghe topic {T}, group {G}, owner={Owner}, trần {N} job đồng thời",
            JobTopicKeys.Topic, JobTopicKeys.Group, _owner, MaxConcurrentJobs);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var cr = consumer.Consume(ct);
                if (cr?.Message == null) continue;

                // ★ COMMIT NGAY. Vòng poll phải luôn rảnh (xem chú thích đầu lớp).
                consumer.Commit(cr);

                if (!long.TryParse(cr.Message.Value, out var jobRunId))
                {
                    Log.Warning("[JOB] Message không đọc được jobRunId: {V}", cr.Message.Value);
                    continue;
                }

                // Hết suất thì bỏ qua — pod khác nhận, hoặc reaper phát lại sau ≤30s. Bỏ qua ở đây
                //   RẺ hơn claim rồi mới phát hiện mình không có chỗ chạy.
                if (!_slots.Wait(0))
                {
                    Log.Debug("[JOB] Pod đang chạy đủ {N} job — bỏ qua runId={Id}", MaxConcurrentJobs, jobRunId);
                    continue;
                }

                _ = Task.Run(async () =>
                {
                    try { await RunOneAsync(jobRunId, JobTopicKeys.SourceKafka, ct); }
                    catch (Exception ex) { Log.Error(ex, "[JOB] runId={Id} lỗi ngoài dự kiến", jobRunId); }
                    finally { _slots.Release(); }
                }, ct);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                // Gồm cả CommitFailed khi pod vừa bị đá khỏi group. KHÔNG được thoát vòng lặp:
                //   thoát nghĩa là pod này vĩnh viễn không chạy job nào nữa tới lần deploy sau, và
                //   không ai nhận ra vì các pod khác vẫn chạy bình thường.
                Log.Error(ex, "[JOB] Vòng consume lỗi — thử lại sau {S}s", RetryDelay.TotalSeconds);
                try { Task.Delay(RetryDelay, ct).Wait(ct); } catch { break; }
            }
        }

        try { consumer.Close(); } catch { }
    }

    private async Task RunOneAsync(long jobRunId, string source, CancellationToken ct)
    {
        // ★ CHỐT CHỐNG TRÙNG. err≠0 ⇒ pod này KHÔNG chạy.
        var claim = await _db.ClaimAsync(jobRunId, _owner, source, ct);
        if (claim.Err != 0)
        {
            // err=3 (quá hạn chót / sai ngày GD → SKIPPED) · 5 (pod khác giữ) · 6 (DEAD) đều là
            //   kết cục HỢP LỆ, không phải sự cố.
            if (claim.Err == 3)
                Log.Warning("[JOB] runId={Id} BỎ QUA: {Msg}", jobRunId, claim.Msg);
            else
                Log.Debug("[JOB] runId={Id} không claim được (err={Err}): {Msg}", jobRunId, claim.Err, claim.Msg);
            return;
        }

        var handler = _registry.Resolve(claim.HandlerKey);
        if (handler == null)
        {
            // Cấu hình trỏ tới handler chưa deploy. Đánh FAILED để nó hiện trên màn hình theo dõi,
            //   thay vì im lặng bỏ qua và để job "chạy" mỗi 15 phút mà không làm gì suốt nhiều tuần.
            await _db.CompleteAsync(jobRunId, _owner, ok: false, rows: 0,
                msg: $"Không tìm thấy handler '{claim.HandlerKey}' trên pod này", ct);
            Log.Error("[JOB] runId={Id} handler '{H}' chưa đăng ký", jobRunId, claim.HandlerKey);
            return;
        }

        // Token của handler = token dừng pod + tín hiệu MẤT QUYỀN.
        using var lost   = new CancellationTokenSource();
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct, lost.Token);

        long progress  = 0;
        var  stillMine = 1;                       // cờ đọc từ RAM — handler kiểm mỗi vòng, miễn phí
        var  beatGate  = new SemaphoreSlim(1, 1); // ★ tuần tự hoá: nhiều luồng không xếp hàng UPDATE cùng một dòng
        var  lastBeat  = DateTime.MinValue;

        // MỘT đường duy nhất chạm DB cho nhịp tim. Cả Timer lẫn lời gọi ép từ handler đều qua đây.
        async Task<bool> BeatAsync(bool forced)
        {
            await beatGate.WaitAsync();
            try
            {
                // Chặn tần suất: handler gọi dày cỡ nào cũng không tạo thêm được một câu query.
                if (forced && DateTime.UtcNow - lastBeat < HeartbeatFloor)
                    return Volatile.Read(ref stillMine) == 1;

                var rows = Interlocked.Read(ref progress);
                var mine = await _db.HeartbeatAsync(jobRunId, _owner, rows, CancellationToken.None);
                lastBeat = DateTime.UtcNow;

                if (!mine)
                {
                    Volatile.Write(ref stillMine, 0);
                    Log.Error("[JOB] runId={Id} MẤT QUYỀN (lease bị thu hồi hoặc job vừa bị TẮT). " +
                              "Dừng ngay để không ghi song song.", jobRunId);
                    lost.Cancel();
                }
                return mine;
            }
            finally { beatGate.Release(); }
        }

        var ctx = new JobContext
        {
            JobRunId       = jobRunId,
            JobCode        = claim.JobCode,
            FireKey        = claim.FireKey,
            BusinessDate   = claim.BusinessDate,
            PayloadJson    = claim.PayloadJson,
            Attempt        = claim.Attempt,
            ReportProgress = rows => Interlocked.Exchange(ref progress, rows),  // RAM, 0 query
            IsStillMine    = () => Volatile.Read(ref stillMine) == 1,           // RAM, 0 query
            HeartbeatAsync = () => BeatAsync(forced: true)                      // có chặn tần suất
        };

        // Nhịp tim nền — NGUỒN DUY NHẤT của tải DB do heartbeat sinh ra.
        var beatEvery = TimeSpan.FromSeconds(Math.Clamp(claim.TimeoutSec / 4.0,
                            HeartbeatMin.TotalSeconds, HeartbeatMax.TotalSeconds));
        // await using: chờ callback đang chạy dứt hẳn rồi mới huỷ Timer.
        await using var beat = new Timer(async _ => { try { await BeatAsync(forced: false); } catch { } },
                                         null, beatEvery, beatEvery);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            var rows = await handler.RunAsync(ctx, linked.Token);
            await _db.CompleteAsync(jobRunId, _owner, ok: true, rows: rows,
                msg: $"OK trong {sw.ElapsedMilliseconds} ms", CancellationToken.None);
            Log.Information("[JOB] {Code} runId={Id} XONG: {Rows} đơn vị, {Ms} ms (đánh thức bởi {Src})",
                claim.JobCode, jobRunId, rows, sw.ElapsedMilliseconds, source);
        }
        catch (OperationCanceledException) when (lost.IsCancellationRequested)
        {
            // Mất quyền: KHÔNG gọi COMPLETE — pod khác đang là chủ, không được đụng vào lượt của họ.
            Log.Warning("[JOB] runId={Id} dừng vì mất quyền", jobRunId);
        }
        catch (Exception ex)
        {
            await _db.CompleteAsync(jobRunId, _owner, ok: false, rows: Interlocked.Read(ref progress),
                msg: ex.Message.Length > 1900 ? ex.Message[..1900] : ex.Message, CancellationToken.None);
            Log.Error(ex, "[JOB] {Code} runId={Id} LỖI sau {Ms} ms", claim.JobCode, jobRunId, sw.ElapsedMilliseconds);
        }
    }
}
