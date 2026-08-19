using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json;
using Serilog;
using StackExchange.Redis;

namespace SdiCoreMessagingProcess.Jobs;

/// <summary>Cấu hình lịch của một job — thứ DUY NHẤT bộ quét cần để tính mốc.</summary>
public sealed class SchedulableJob
{
    public string    JobCode         { get; init; } = "";
    public int       IntervalSec     { get; init; }
    public TimeSpan? WindowFrom      { get; init; }
    public TimeSpan? WindowTo        { get; init; }
    public bool      BusinessDayOnly { get; init; }
    /// <summary>COALESCE(C_MAX_DELAY_SEC, C_INTERVAL_SEC) — hạn tươi hiệu lực.</summary>
    public int?      MaxDelaySec     { get; init; }
}

/// <summary>
/// BỘ QUÉT LỊCH — chạy trên MỌI pod (không bầu leader, không lock phân tán cho tính đúng).
///
/// ★★ MỘT NHỊP QUÉT KHÔNG CHẠM DB.
///   Trước đây mỗi nhịp là một lượt `SP_JOB_ENQUEUE_DUE` (~50 logical reads). 10 pod × 6 lần/phút
///   × 24 giờ = **~86.400 lượt quét/ngày để sinh 25 job** — 3.400 lượt hỏi cho mỗi câu trả lời.
///   Nay một nhịp là: đọc cấu hình từ bộ nhớ → tính mốc → `SET NX` một khoá Redis. Chỉ khi đặt
///   được khoá (≈25 lần/ngày) mới đi tiếp xuống DB.
///
/// ★ AI GIỮ TÍNH ĐÚNG: vẫn là DB.
///   Redis mất khoá ⇒ nhiều pod cùng gọi `SP_JOB_ENQUEUE` ⇒ `UQ (job_code, fire_key)` cho đúng
///   một pod thắng, còn lại `err=4`. Tệ nhất: vài lượt gọi DB thừa. Không job nào chạy hai lần.
///   Và `SP_JOB_ENQUEUE` còn **đối chiếu mốc** pod gửi lên với `UDF_JOB_SLOT_AT` — pod tính sai
///   (lệch múi giờ, chạy bản cũ, sai chu kỳ) thì bị từ chối có mã lỗi, không trôi lệch âm thầm.
///
/// ★ CẤU HÌNH: đọc Redis cache (`SDI:JOB:CFG`); miss → DB → nạp lại cache. App xoá khoá đó ngay
///   sau khi đổi lịch qua `SP_SET_JOB_SCHEDULE`, nên nhịp kế tiếp đã thấy cấu hình mới.
///   Khung giờ 09:00–15:00 gần như không đổi, nên ở trạng thái ổn định lớp này cũng ~0 lượt gọi DB.
///
/// ★ RECOVER có VÉ: `SP_JOB_RECOVER` idempotent nên chạy trùng không sai — vé Redis chỉ để 10 pod không
///   cùng làm một việc mỗi 30 giây (20 lượt/phút → 2). Lấy hụt vé thì bỏ qua nhịp, không chờ.
/// </summary>
public class JobSchedulerService : BackgroundService
{
    private static readonly TimeSpan ScanEvery   = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan RecoverEvery   = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan ConfigEvery = TimeSpan.FromSeconds(60);

    private readonly IDatabase                 _redis;
    private readonly IProducer<string, string> _producer;
    private readonly ISdiJobGateway            _db;
    private readonly string                    _owner;

    private IReadOnlyList<SchedulableJob> _jobs = Array.Empty<SchedulableJob>();
    private DateTime _lastConfig = DateTime.MinValue;
    private DateTime _lastRecover   = DateTime.MinValue;
    private DateTime _bizDay     = DateTime.MinValue;
    private bool     _bizOk;

    public JobSchedulerService(IDatabase redis, IProducer<string, string> producer, ISdiJobGateway db)
    {
        _redis    = redis;
        _producer = producer;
        _db       = db;
        _owner    = $"{Environment.MachineName}#{Environment.ProcessId}";
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        Log.Information("[JOB] Scheduler khởi động trên pod {Owner}", _owner);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (DateTime.UtcNow - _lastConfig > ConfigEvery) await RefreshConfigAsync(ct);
                await ScanAsync(ct);

                if (DateTime.UtcNow - _lastRecover > RecoverEvery)
                {
                    _lastRecover = DateTime.UtcNow;
                    await RecoverAsync(ct);
                }

            }
            catch (Exception ex)
            {
                Log.Error(ex, "[JOB] Scheduler lỗi một nhịp — bỏ qua, nhịp sau chạy tiếp");
            }

            try { await Task.Delay(ScanEvery, ct); } catch { break; }
        }
    }

    /// <summary>Một nhịp quét. Đường chạy thường: 0 lượt gọi DB, chỉ vài lệnh Redis.</summary>
    private async Task ScanAsync(CancellationToken ct)
    {
        var now = TradingWindowGuard.NowVn();

        foreach (var j in _jobs)
        {
            // ① Tính mốc TẠI CHỖ — cùng công thức với UDF_JOB_SLOT_AT (DB sẽ đối chiếu lại).
            var slot = SlotAt(now, j.IntervalSec);

            // ② Khung giờ áp lên MỐC, không phải lên `now`: nhịp quét lúc 15:00:04 phải sinh được
            //    mốc 15:00:00, còn 15:15:04 thì không (hết phiên, đợi phiên GD kế tiếp).
            if (j.WindowFrom is { } from && j.WindowTo is { } to)
            {
                var tod = slot.TimeOfDay;
                if (tod < from || tod > to) continue;
            }
            if (j.BusinessDayOnly && !await IsBusinessDayAsync(slot.Date)) continue;

            // ③ ★ LỌC TRƯỚC BẰNG REDIS — chỗ tiết kiệm ~99% lượt gọi DB.
            var fireKey = FireKey(slot);
            bool mine;
            try
            {
                mine = await _redis.StringSetAsync(JobRedisKeys.SlotLock(j.JobCode, fireKey),
                    _owner, JobRedisKeys.SlotTtl(j.IntervalSec), When.NotExists);
            }
            catch (Exception ex)
            {
                // Redis hỏng ⇒ ĐI TIẾP xuống DB. Mất lớp lọc thì tốn thêm query, KHÔNG được phép
                //   làm job ngừng chạy — UQ(job_code, fire_key) vẫn chặn trùng.
                Log.Warning(ex, "[JOB] Redis lỗi khi lọc mốc {Code}/{Key} — đi thẳng xuống DB", j.JobCode, fireKey);
                mine = true;
            }
            if (!mine) continue;   // pod khác đã lo mốc này

            // ④ Chỉ tới đây mới chạm DB (~25 lượt/ngày).
            var (id, err) = await _db.EnqueueAsync(j.JobCode, fireKey: null, payload: null,
                                                   businessDate: slot.Date, slotAt: slot, user: "scheduler", ct);
            if (err == 0 && id > 0)
            {
                await NotifyAsync(id, j.JobCode, JobTopicKeys.SourceKafka, ct);
            }
            else if (err == 4)
            {
                // Khoá Redis mất nhưng DB vẫn nhớ — đúng ý đồ, không phải sự cố.
                Log.Debug("[JOB] {Code} mốc {Key} đã có (err=4) — lớp lọc Redis vừa hụt", j.JobCode, fireKey);
            }
            else if (err == 7)
            {
                Log.Information("[JOB] {Code} đang chạy (singleton) — bỏ mốc {Key}", j.JobCode, fireKey);
            }
            else if (err != 3)   // err=3 = mốc ngoài khung; DB vừa từ chối, bình thường
            {
                Log.Warning("[JOB] {Code} mốc {Key} enqueue lỗi err={Err}", j.JobCode, fireKey, err);
            }
        }
    }

    /// <summary>Cùng công thức với UDF_JOB_SLOT_AT: neo vào 00:00 giờ VN.</summary>
    internal static DateTime SlotAt(DateTime now, int intervalSec)
    {
        var midnight = now.Date;
        var secs = (long)(now - midnight).TotalSeconds;
        return midnight.AddSeconds(secs / intervalSec * intervalSec);
    }

    /// <summary>Cùng định dạng với UDF_JOB_FIRE_KEY. PHẢI có giây — xem chú thích hàm SQL đó.</summary>
    internal static string FireKey(DateTime slot) => slot.ToString("yyyyMMddHHmmss");

    /// <summary>
    /// ★ CỬA CHẶN TRƯỚC RECOVER — thứ giết nốt phần "quét DB liên tục" còn lại.
    ///
    /// RECOVER chỉ có việc khi CÓ THỂ đang tồn tại một lượt chạy sống: lượt READY chưa ai nhặt, hoặc
    /// lượt RUNNING của một pod đã chết. Cả hai chỉ sinh ra trong khung giờ của job, và chết hẳn
    /// sau `windowTo + maxDelay`. Ngoài khoảng đó **không có gì để thu hồi** — quét là quét không.
    ///
    /// Job FO chạy 09:00–15:00 ⇒ RECOVER chỉ cần chạy ~09:00–15:10. 18 tiếng còn lại mỗi ngày, cộng
    /// toàn bộ T7/CN/lễ, bộ quét **không gửi một câu truy vấn nào**.
    /// Job KHÔNG khai khung giờ ⇒ luôn coi là mở (đúng: nó có thể chạy bất cứ lúc nào).
    /// </summary>
    private bool AnyWindowOpen(DateTime now)
    {
        foreach (var j in _jobs)
        {
            if (j.WindowFrom is not { } from || j.WindowTo is not { } to) return true;  // không khai khung ⇒ luôn mở
            var until = to + TimeSpan.FromSeconds(j.MaxDelaySec ?? j.IntervalSec);
            if (now.TimeOfDay >= from && now.TimeOfDay <= until) return true;
        }
        return false;
    }

    private async Task RecoverAsync(CancellationToken ct)
    {
        // Không có khung nào đang mở ⇒ không thể có lượt chạy nào sống ⇒ khỏi hỏi DB.
        if (!AnyWindowOpen(TradingWindowGuard.NowVn())) return;

        try
        {
            // Vé: lấy hụt thì pod khác đang lo, bỏ qua nhịp này.
            var got = await _redis.StringSetAsync(JobRedisKeys.RecoverTicket, _owner,
                JobRedisKeys.RecoverTicketTtl, When.NotExists);
            if (!got) return;
        }
        catch (Exception ex)
        {
            Log.Warning(ex, "[JOB] Redis lỗi khi lấy vé RECOVER — chạy RECOVER luôn (idempotent)");
        }

        var revived = await _db.RecoverAsync(staleSec: 30, ct);
        if (revived.Count > 0)
            Log.Warning("[JOB] RECOVER: {N} lượt READY không ai chạy — produce lại", revived.Count);
        foreach (var r in revived)
            await NotifyAsync(r.JobRunId, r.JobCode, JobTopicKeys.SourceRecover, ct);
    }

    private async Task RefreshConfigAsync(CancellationToken ct)
    {
        _lastConfig = DateTime.UtcNow;
        try
        {
            var cached = await _redis.HashGetAllAsync(JobRedisKeys.ConfigHash);
            if (cached.Length > 0)
            {
                var list = new List<SchedulableJob>(cached.Length);
                foreach (var e in cached)
                {
                    var j = JsonConvert.DeserializeObject<SchedulableJob>(e.Value!);
                    if (j != null) list.Add(j);
                }
                _jobs = list;
                return;
            }
        }
        catch (Exception ex)
        {
            Log.Warning(ex, "[JOB] Đọc cache cấu hình lỗi — nạp thẳng từ DB");
        }

        // Cache trống/hỏng ⇒ nạp từ DB (nguồn sự thật) rồi nạp lại cache cho các pod khác.
        var fromDb = await _db.GetSchedulableJobsAsync(ct);
        _jobs = fromDb;
        try
        {
            var entries = new HashEntry[fromDb.Count];
            for (var i = 0; i < fromDb.Count; i++)
                entries[i] = new HashEntry(fromDb[i].JobCode, JsonConvert.SerializeObject(fromDb[i]));
            if (entries.Length > 0)
            {
                await _redis.HashSetAsync(JobRedisKeys.ConfigHash, entries);
                await _redis.KeyExpireAsync(JobRedisKeys.ConfigHash, JobRedisKeys.ConfigTtl);
            }
        }
        catch (Exception ex) { Log.Warning(ex, "[JOB] Nạp lại cache cấu hình lỗi — bỏ qua"); }
    }

    /// <summary>Lịch nghỉ ở T_TRADING_HOLIDAY. Nhớ theo NGÀY ⇒ tối đa 1 lượt hỏi DB mỗi ngày mỗi pod.</summary>
    private async Task<bool> IsBusinessDayAsync(DateTime day)
    {
        if (_bizDay == day) return _bizOk;
        _bizOk  = await _db.IsBusinessDateAsync(day);
        _bizDay = day;
        return _bizOk;
    }

    /// <summary>
    /// Message chỉ chở `jobRunId` — không payload, không trạng thái. Worker cầm id rồi hỏi DB mọi
    /// thứ khác; chở bản chụp cấu hình trong message là mở đường cho một job vừa bị sửa/tắt vẫn
    /// chạy theo bản cũ đang nằm trong topic (Kafka có lưu, nên bản cũ đó sống rất dai).
    ///
    /// Produce hụt KHÔNG phải thảm hoạ: dòng `T_JOB_RUN` đã `READY` và nhịp RECOVER sau sẽ produce lại.
    /// Vì thế chỉ log WARNING, không ném — ném là biến một sự cố tự hồi phục thành lượt chạy FAILED.
    /// </summary>
    private async Task NotifyAsync(long jobRunId, string jobCode, string src, CancellationToken ct)
    {
        try
        {
            var dr = await _producer.ProduceAsync(JobTopicKeys.Topic,
                new Message<string, string>
                {
                    Key   = JobTopicKeys.KeyOf(jobRunId),   // rải đều partition — xem JobTopicKeys
                    Value = jobRunId.ToString()
                }, ct);
            Log.Information("[JOB] {Src} → {Code} runId={Id} vào {TP}@{Off}", src, jobCode, jobRunId, dr.TopicPartition, dr.Offset);
        }
        catch (Exception ex)
        {
            Log.Warning(ex, "[JOB] Produce hụt cho runId={Id} — RECOVER sẽ produce lại", jobRunId);
        }
    }
}
