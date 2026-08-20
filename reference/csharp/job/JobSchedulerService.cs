using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
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
///   Redis mất khoá ⇒ nhiều pod cùng gọi `SP_JOB_CLAIM_SLOT` ⇒ `UQ (job_code, fire_key)` cho đúng
///   một pod thắng, còn lại `err=4`. Tệ nhất: vài lượt gọi DB thừa. Không job nào chạy hai lần.
///   Và `SP_JOB_CLAIM_SLOT` còn **đối chiếu mốc** pod gửi lên với `UDF_JOB_SLOT_AT` — pod tính sai
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

            // ③ ★ CHỐT MỐC BẰNG REDIS. Đặt được khoá = pod này là pod đầu tiên thấy mốc đó.
            //    Redis LỖI ⇒ DỪNG NHỊP, KHÔNG đi tiếp. Cố ý:
            //      · Không có gì để "đi tiếp xuống" nữa — bộ quét KHÔNG ghi DB, nó chỉ produce.
            //        Bỏ qua khoá mà cứ produce thì 10 pod produce 10 message cho cùng một mốc;
            //        DB vẫn chặn (UQ) nên không sai, nhưng đó là 10 lần gọi DB và 9 lần vô ích,
            //        đúng vào lúc hạ tầng đang có sự cố — thời điểm tệ nhất để tự tạo thêm tải.
            //      · Dữ liệu là snapshot: mất một nhịp thì nhịp sau bù. Không có gì phải cứu.
            var fireKey = FireKey(slot);
            bool mine;
            try
            {
                mine = await _redis.StringSetAsync(JobRedisKeys.SlotLock(j.JobCode, fireKey),
                    _owner, JobRedisKeys.SlotTtl(j.IntervalSec), When.NotExists);
            }
            catch (Exception ex)
            {
                Log.Error(ex, "[JOB] Redis lỗi khi chốt mốc {Code}/{Key} — DỪNG nhịp này, " +
                              "không produce. Nhịp sau thử lại.", j.JobCode, fireKey);
                return;   // dừng cả vòng quét, không chỉ job này
            }
            if (!mine) continue;   // pod khác đã lo mốc này

            // ④ Produce THẲNG — KHÔNG ghi DB. Dòng T_JOB_RUN do chính pod nhận message tạo ra,
            //    qua SP_JOB_CLAIM_SLOT, và sinh ra đã ở trạng thái RUNNING.
            //    ★ Produce HỤT ⇒ PHẢI TRẢ LẠI KHOÁ MỐC. Từ khi bộ quét thôi ghi DB, khoá Redis là
            //      thứ DUY NHẤT ghi nhận "mốc này đã có người lo" — mà lúc này thì KHÔNG có ai lo cả:
            //      không dòng T_JOB_RUN nào để SP_JOB_RECOVER tìm, và khoá còn sống 2× chu kỳ nên
            //      mọi nhịp quét sau (của MỌI pod) đều thấy "đã có người" rồi bỏ qua. Giữ khoá =
            //      mất hẳn mốc đó. Trả khoá = nhịp sau (10 giây) thử lại — đúng như chú thích hứa.
            if (!await NotifyAsync(j.JobCode, slot, fireKey, JobTopicKeys.SourceKafka, ct))
                await ReleaseSlotAsync(j.JobCode, fireKey);
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
    ///
    /// ★ PHẢI kiểm cả NGÀY GD, không chỉ giờ. Thiếu vế đó thì mỗi thứ Bảy/Chủ nhật, từ 09:00 đến
    ///   15:10, RECOVER vẫn nã DB mỗi 30 giây để tìm thứ không thể tồn tại — đúng cái mà chú thích
    ///   ngay trên đã hứa là không xảy ra. `IsBusinessDayAsync` nhớ theo ngày nên tốn tối đa 1 lượt
    ///   hỏi DB mỗi ngày mỗi pod.
    /// </summary>
    private async Task<bool> AnyWindowOpenAsync(DateTime now)
    {
        foreach (var j in _jobs)
        {
            if (j.WindowFrom is not { } from || j.WindowTo is not { } to) return true;  // không khai khung ⇒ luôn mở
            var until = to + TimeSpan.FromSeconds(j.MaxDelaySec ?? j.IntervalSec);
            if (now.TimeOfDay < from || now.TimeOfDay > until) continue;
            if (j.BusinessDayOnly && !await IsBusinessDayAsync(now.Date)) continue;
            return true;
        }
        return false;
    }

    private async Task RecoverAsync(CancellationToken ct)
    {
        // Không có khung nào đang mở ⇒ không thể có lượt chạy nào sống ⇒ khỏi hỏi DB.
        if (!await AnyWindowOpenAsync(TradingWindowGuard.NowVn())) return;

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

        // SP_JOB_RECOVER thu về hai loại: lượt RUNNING của pod đã chết (lease hết hạn) và lượt
        //   READY nằm quá lâu không ai nhặt. Cả hai đều đã có dòng T_JOB_RUN nên đẩy lại được.
        // ⚠️ Cái nó KHÔNG bắt được: message thất lạc TRƯỚC khi có pod nào giành mốc — lúc đó chưa
        //   có dòng nào để tìm. Đó là cái giá đã biết của việc bỏ ghi DB lúc sinh job.
        var revived = await _db.RecoverAsync(staleSec: 30, ct);
        if (revived.Count > 0)
            Log.Warning("[JOB] RECOVER: {N} lượt bị bỏ rơi (pod chết / không ai nhặt) — produce lại", revived.Count);
        foreach (var r in revived)
            await NotifyAsync(r.JobCode, r.SlotAt, r.FireKey, JobTopicKeys.SourceRecover, ct);
        // Ở đây KHÔNG trả khoá mốc khi produce hụt: dòng T_JOB_RUN vẫn còn, nhịp RECOVER sau
        //   (30 giây) sẽ thấy lại và đẩy lại. Khác hẳn đường sinh mốc ở ScanAsync ④.
    }

    /// <summary>
    /// Nạp cấu hình lịch cho nhịp quét. Đây là ĐƯỜNG ĐỌC; đường GHI (lúc ops đổi lịch) nằm ở
    /// <see cref="JobConfigService"/>, và cả hai ghi cache qua cùng một <see cref="JobConfigCache"/>.
    ///
    /// Cache trống KHÔNG phải lỗi — đó là trạng thái ngay sau khi ai đó đổi cấu hình, hoặc sau khi
    /// TTL hết. Nạp từ DB rồi ghi lại cache cho các pod khác đỡ phải hỏi.
    /// </summary>
    private async Task RefreshConfigAsync(CancellationToken ct)
    {
        _lastConfig = DateTime.UtcNow;
        try
        {
            var cached = await JobConfigCache.ReadAsync(_redis);
            if (cached != null) { _jobs = cached; return; }
        }
        catch (Exception ex)
        {
            Log.Warning(ex, "[JOB] Đọc cache cấu hình lỗi — nạp thẳng từ DB");
        }

        // Cache trống/hỏng ⇒ nạp từ DB (nguồn sự thật) rồi ghi lại cache cho các pod khác.
        var fromDb = await _db.GetSchedulableJobsAsync(ct);
        _jobs = fromDb;
        try { await JobConfigCache.WriteAsync(_redis, fromDb); }
        catch (Exception ex) { Log.Warning(ex, "[JOB] Ghi lại cache cấu hình lỗi — bỏ qua"); }
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
    /// Message chỉ chở `{code, slot, key}` — MỐC, không payload, không trạng thái. Worker cầm mốc
    /// rồi hỏi DB mọi thứ khác; chở bản chụp cấu hình trong message là mở đường cho một job vừa bị
    /// sửa/tắt vẫn chạy theo bản cũ đang nằm trong topic (Kafka có lưu, nên bản cũ đó sống rất dai).
    ///
    /// Trả về `true` nếu đã produce được. Produce hụt thì KHÔNG ném — ném là biến một sự cố tự hồi
    /// phục thành lượt chạy FAILED — nhưng người gọi PHẢI trả lại khoá mốc, xem ScanAsync ④.
    /// </summary>
    private async Task<bool> NotifyAsync(string jobCode, DateTime slotAt, string fireKey, string src, CancellationToken ct)
    {
        try
        {
            var dr = await _producer.ProduceAsync(JobTopicKeys.Topic,
                new Message<string, string>
                {
                    Key   = JobTopicKeys.KeyOf(fireKey),               // rải đều partition — lý do ở JobTopicKeys.KeyOf
                    Value = JobMessage.Serialize(jobCode, slotAt, fireKey)
                }, ct);
            Log.Information("[JOB] {Src} → {Code} mốc {Key} vào {TP}@{Off}", src, jobCode, fireKey, dr.TopicPartition, dr.Offset);
            return true;
        }
        catch (Exception ex)
        {
            Log.Warning(ex, "[JOB] Produce hụt cho {Code} mốc {Key} — trả lại khoá, nhịp sau thử lại",
                jobCode, fireKey);
            return false;
        }
    }

    /// <summary>
    /// Trả lại khoá mốc sau khi produce hụt. Bản thân việc trả khoá cũng có thể hụt (Redis đang
    /// trục trặc — rất có thể chính là lý do produce hụt); lúc đó đành chịu mất mốc, và khoá tự hết
    /// hạn sau 2× chu kỳ. Không có gì để cứu thêm: đây là số snapshot, mốc sau bù.
    /// </summary>
    private async Task ReleaseSlotAsync(string jobCode, string fireKey)
    {
        try { await _redis.KeyDeleteAsync(JobRedisKeys.SlotLock(jobCode, fireKey)); }
        catch (Exception ex)
        {
            Log.Warning(ex, "[JOB] Không trả được khoá mốc {Code}/{Key} — mốc này mất, "
                          + "khoá tự hết hạn sau 2× chu kỳ", jobCode, fireKey);
        }
    }
}
