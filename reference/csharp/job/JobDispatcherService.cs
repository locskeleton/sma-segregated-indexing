using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Serilog;
using StackExchange.Redis;

namespace SdiCoreMessagingProcess.Jobs;

/// <summary>
/// WORKER — chạy trên MỌI pod. Nằm nghe kênh Pub/Sub, có thông báo là chạy ngay (yêu cầu BRD #2:
/// "cứ có job đẩy vào là thực hiện xử lý"). ĐẨY THẬT: không hỏi thăm Redis, không polling DB.
///
/// ★ VÌ SAO KHÔNG BỊ XỬ LÝ TRÙNG TRÊN NHIỀU POD (yêu cầu BRD #3):
///   Pub/Sub phát cho MỌI pod ⇒ cả 10 pod cùng nhận một `jobRunId`. Điều đó KHÔNG sao, vì chốt
///   chặn không nằm ở khâu giao tin: `SP_JOB_CLAIM` là một `UPDATE ... WHERE C_STATUS='READY'`,
///   đúng MỘT pod đổi được trạng thái, 9 pod còn lại nhận `err=5` và đi tiếp.
///   ⇒ Ở đây "ai nhận được tin" là chuyện vô thưởng vô phạt; "ai được chạy" mới là chuyện của DB.
///   Lớp thứ hai: heartbeat có kiểm chủ sở hữu — pod treo lâu, lease bị thu hồi, pod cũ tỉnh dậy
///   nhận `still_mine=false` rồi tự dừng. Không có cửa cho hai pod cùng ghi.
///
/// ★ HẠN MỨC JOB ĐỒNG THỜI (`MaxConcurrentJobs`): pod đang chạy đủ job rồi thì **không claim nữa**,
///   nhường pod rảnh. Đây vừa là cách chia tải tự nhiên (thay cho consumer group của Streams),
///   vừa là cách chặn một pod ôm hết việc rồi nghẽn. Nếu MỌI pod đều bận thì không ai claim —
///   dòng vẫn nằm `READY` và `SP_JOB_REAP` phát lại sau ≤30 giây. Không mất việc.
///
/// ★ HÀM CALLBACK CỦA PUB/SUB KHÔNG ĐƯỢC CHẶN. `OnMessage` xử lý tuần tự theo kênh: nếu chạy job
///   ngay trong callback thì một job dài 5 phút sẽ khoá luôn việc nhận thông báo tiếp theo của
///   pod đó. Nên callback chỉ đọc id rồi ném sang một Task nền có hạn mức, và trả về ngay.
///
/// ★ MẤT THÔNG BÁO: Pub/Sub bắn-rồi-quên — mất kết nối, pod đang restart, Redis chết ⇒ tin bay mất.
///   `SP_JOB_REAP` là ĐƯỜNG HỒI PHỤC CHÍNH (không phải dự phòng). Xem JobChannelKeys.
/// </summary>
public class JobDispatcherService : BackgroundService
{
    /// <summary>Số job một pod chạy đồng thời. Job FO là singleton nên thực tế hiếm khi chạm trần.</summary>
    private const int MaxConcurrentJobs = 4;

    // Nhịp tim: KHÔNG để cứng. Phải nhỏ hơn hẳn lease (DB chậm một nhịp không được làm mất job vào
    //   tay pod khác), nhưng cũng chính là độ trễ phát hiện "job vừa bị TẮT" / "mình vừa mất quyền".
    //   ⇒ timeout/4, kẹp [5s, 30s]: luôn có biên 4 lần lease, tệ nhất cũng phản ứng trong 30 giây.
    private static readonly TimeSpan HeartbeatMin   = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan HeartbeatMax   = TimeSpan.FromSeconds(30);
    // Chặn tần suất cho lời gọi ÉP nhịp tim từ handler (ctx.HeartbeatAsync).
    private static readonly TimeSpan HeartbeatFloor = TimeSpan.FromSeconds(5);

    private readonly ISubscriber    _sub;
    private readonly ISdiJobGateway _db;
    private readonly IJobRegistry   _registry;
    private readonly string         _owner;   // định danh pod: hostname + pid
    private readonly SemaphoreSlim  _slots = new(MaxConcurrentJobs, MaxConcurrentJobs);

    public JobDispatcherService(ISubscriber sub, ISdiJobGateway db, IJobRegistry registry)
    {
        _sub      = sub;
        _db       = db;
        _registry = registry;
        _owner    = $"{Environment.MachineName}#{Environment.ProcessId}";
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        // SubscribeAsync trả về hàng đợi tin của kênh. SE.Redis tự đăng ký lại sau khi mất kết nối —
        //   nhưng tin phát ra TRONG lúc mất kết nối thì mất luôn; SP_JOB_REAP lo phần đó.
        var queue = await _sub.SubscribeAsync(JobChannelKeys.NotifyChannel);

        queue.OnMessage(msg =>
        {
            if (!long.TryParse(msg.Message, out var jobRunId)) return;

            // KHÔNG chạy job trong callback (xem chú thích đầu lớp). Thử lấy một suất; hết suất thì
            //   bỏ qua — pod khác nhận, hoặc reaper phát lại. Bỏ qua ở đây RẺ hơn nhiều so với claim
            //   rồi mới phát hiện mình không có chỗ chạy.
            if (!_slots.Wait(0))
            {
                Log.Debug("[JOB] Pod đang chạy đủ {N} job — bỏ qua thông báo runId={Id}", MaxConcurrentJobs, jobRunId);
                return;
            }

            _ = Task.Run(async () =>
            {
                try { await RunOneAsync(jobRunId, JobChannelKeys.SourceNotify, ct); }
                catch (Exception ex) { Log.Error(ex, "[JOB] runId={Id} lỗi ngoài dự kiến", jobRunId); }
                finally { _slots.Release(); }
            }, ct);
        });

        Log.Information("[JOB] Dispatcher đang nghe kênh {Ch}, owner={Owner}, trần {N} job đồng thời",
            JobChannelKeys.Notify, _owner, MaxConcurrentJobs);

        // Không có vòng lặp hỏi thăm nào. Chỉ nằm chờ tới khi pod dừng.
        try { await Task.Delay(Timeout.Infinite, ct); }
        catch (OperationCanceledException) { }
        finally { await _sub.UnsubscribeAsync(JobChannelKeys.NotifyChannel); }
    }

    private async Task RunOneAsync(long jobRunId, string source, CancellationToken ct)
    {
        // ★ CHỐT CHỐNG TRÙNG. err≠0 ⇒ pod này KHÔNG chạy.
        var claim = await _db.ClaimAsync(jobRunId, _owner, source, ct);
        if (claim.Err != 0)
        {
            // err=3 (ngoài khung giờ / quá hạn → SKIPPED) · 5 (pod khác giữ) · 6 (DEAD) đều là kết
            //   cục HỢP LỆ. err=5 là chuyện THƯỜNG NGÀY với Pub/Sub: 10 pod nhận tin, 9 pod trượt.
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
