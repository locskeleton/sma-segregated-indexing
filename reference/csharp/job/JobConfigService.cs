using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Serilog;
using StackExchange.Redis;

namespace SdiCoreMessagingProcess.Jobs;

/// <summary>
/// CACHE CẤU HÌNH LỊCH trên Redis — nơi DUY NHẤT biết cách đọc/ghi `SDI:JOB:CFG`.
///
/// Gom vào một chỗ vì có HAI người đụng vào nó và họ PHẢI ghi giống hệt nhau:
///   · <see cref="JobSchedulerService"/> — nạp khi cache trống (đường đọc);
///   · <see cref="JobConfigService"/>    — nạp lại ngay khi đổi cấu hình (đường ghi).
/// Hai bản sao của cùng một đoạn ghi là hai chỗ để lệch nhau, và lệch cache cấu hình thì
/// biểu hiện ra ngoài là "job chạy sai tần suất" — không ai nghĩ tới cache khi đi tìm.
/// </summary>
public static class JobConfigCache
{
    /// <summary>
    /// Ghi ĐÈ TOÀN BỘ cache: XOÁ khoá rồi mới ghi danh sách mới.
    ///
    /// ★ PHẢI XOÁ TRƯỚC, KHÔNG ĐƯỢC `HashSet` CHỒNG LÊN. `HashSet` chỉ thêm/sửa field, KHÔNG xoá
    ///   field cũ. Ops xoá chu kỳ của một job (hoặc tắt job) ⇒ job đó biến mất khỏi
    ///   `SP_GET_SCHEDULABLE_JOBS`, nhưng field cũ của nó VẪN NẰM trong hash ⇒ bộ quét vẫn đọc được
    ///   chu kỳ cũ và vẫn sinh mốc cho một job đáng lẽ đã dừng.
    ///   Và ca đó KHÔNG bị cửa err=20 chặn: job không còn chu kỳ thì `UDF_JOB_SLOT_AT` trả NULL,
    ///   phép đối chiếu lưới tự bỏ qua ⇒ mốc đi thẳng tới guard giờ giấc và có thể CHẠY THẬT.
    ///   ⇒ "Xoá chu kỳ" mà job vẫn chạy. Xoá-rồi-ghi là thứ chặn ca này.
    ///
    /// Pod nào đọc đúng khe giữa XOÁ và GHI sẽ thấy cache trống ⇒ tự nạp từ DB ⇒ vẫn đúng.
    /// Vì thế không cần Lua/transaction: trạng thái xấu nhất của khe đó là một lượt đọc DB thừa.
    /// </summary>
    public static async Task WriteAsync(IDatabase redis, IReadOnlyList<SchedulableJob> jobs)
    {
        await redis.KeyDeleteAsync(JobRedisKeys.ConfigHash);
        if (jobs.Count == 0) return;          // không job nào có lịch ⇒ để cache TRỐNG, đúng nghĩa

        var entries = new HashEntry[jobs.Count];
        for (var i = 0; i < jobs.Count; i++)
            entries[i] = new HashEntry(jobs[i].JobCode, JsonConvert.SerializeObject(jobs[i]));

        await redis.HashSetAsync(JobRedisKeys.ConfigHash, entries);
        await redis.KeyExpireAsync(JobRedisKeys.ConfigHash, JobRedisKeys.ConfigTtl);
    }

    /// <summary>Đọc cache. Trả null nếu cache trống hoặc không đọc được ⇒ người gọi nạp từ DB.</summary>
    public static async Task<List<SchedulableJob>?> ReadAsync(IDatabase redis)
    {
        var cached = await redis.HashGetAllAsync(JobRedisKeys.ConfigHash);
        if (cached.Length == 0) return null;

        var list = new List<SchedulableJob>(cached.Length);
        foreach (var e in cached)
        {
            var j = JsonConvert.DeserializeObject<SchedulableJob>(e.Value!);
            if (j != null) list.Add(j);
        }
        return list.Count > 0 ? list : null;
    }
}

/// <summary>
/// CỔNG ĐỔI LỊCH JOB phía ứng dụng — API/màn hình vận hành PHẢI đi qua đây, đừng gọi thẳng
/// <c>ISdiJobGateway.SetJobScheduleAsync</c>.
///
/// ★ VÌ SAO PHẢI CÓ LỚP NÀY: `SP_SET_JOB_SCHEDULE` chỉ với tới được DB. Nó dọn sạch lượt chờ của
///   cấu hình cũ và ghi cấu hình mới — chuẩn — nhưng nó KHÔNG biết Redis tồn tại. Mà bộ quét thì
///   đọc chu kỳ từ **cache Redis**, không đọc DB. Đổi cấu hình mà không ai đụng vào cache thì:
///
///     · DB nói 30 phút, Redis vẫn nói 15 phút, và pod tin Redis.
///     · Cache chỉ tự hết hạn theo TTL ⇒ cấu hình mới có hiệu lực SAU TỚI HÀNG GIỜ.
///     · 15p→30p: nửa số mốc lệch lưới ⇒ `SP_JOB_CLAIM_SLOT` trả err=20, chặn đúng nhưng cấu hình
///       mới coi như chưa có hiệu lực.
///     · 30p→15p: lưới 1800 nằm gọn trong lưới 900 ⇒ **mọi mốc đều lọt, không một lỗi nào**, job
///       chỉ lặng lẽ chạy nửa tần suất. Đây mới là ca tệ: sai mà không kêu.
///
///   Cửa err=20 ở DB cứu được chuyện *chạy sai mốc*; nó KHÔNG cứu được chuyện *cấu hình mới không
///   tới nơi*. Việc đó phải làm ở đây.
///
/// Trình tự (đúng thứ tự này, không đảo):
///   ① `SP_SET_JOB_SCHEDULE` — DB là nguồn sự thật, ghi vào đó TRƯỚC.
///   ② Nạp lại cache từ DB (xoá + ghi). Nếu ghi cache trước rồi DB lỗi thì cache đang quảng cáo
///      một cấu hình chưa từng tồn tại.
///   ③ Pod thấy cấu hình mới ở nhịp `ConfigEvery` kế tiếp (≤ 60 giây).
///
/// Bước ② hụt (Redis trục trặc) KHÔNG làm hỏng gì: DB đã đúng, và `JobRedisKeys.ConfigTtl` là lưới
/// an toàn — chậm nhất sau TTL là cache tự chết và pod nạp lại từ DB.
/// </summary>
public sealed class JobConfigService
{
    private readonly ISdiJobGateway _db;
    private readonly IDatabase      _redis;

    public JobConfigService(ISdiJobGateway db, IDatabase redis)
    {
        _db = db; _redis = redis;
    }

    /// <summary>
    /// Đổi chu kỳ quét (15'/30'/1h…). `intervalSec = null` + `clearInterval = true` ⇒ chuyển job về
    /// chế độ chạy-theo-yêu-cầu: bộ quét thôi sinh mốc cho nó.
    /// </summary>
    public Task<(int Err, string? Msg, int PurgedRuns)> SetPeriodAsync(
        string jobCode, int? intervalSec, string user, CancellationToken ct)
        => ApplyAsync(jobCode, intervalSec, clearInterval: intervalSec is null,
                      windowFrom: null, windowTo: null, clearWindow: false,
                      businessDayOnly: null, enabled: null, maxDelaySec: null, payload: null, user, ct);

    /// <summary>Đổi bất kỳ mặt nào của lịch. Mọi tham số null = GIỮ NGUYÊN (xem SP_SET_JOB_SCHEDULE).</summary>
    public async Task<(int Err, string? Msg, int PurgedRuns)> ApplyAsync(
        string jobCode, int? intervalSec, bool clearInterval, TimeSpan? windowFrom, TimeSpan? windowTo,
        bool clearWindow, bool? businessDayOnly, bool? enabled, int? maxDelaySec, string? payload,
        string user, CancellationToken ct)
    {
        // ① DB trước — nguồn sự thật.
        var r = await _db.SetJobScheduleAsync(jobCode, intervalSec, clearInterval, windowFrom, windowTo,
                                              clearWindow, businessDayOnly, enabled, maxDelaySec,
                                              payload, user, ct);
        if (r.Err != 0)
        {
            Log.Warning("[JOB] Đổi lịch {Code} KHÔNG thành (err={Err}): {Msg} — cache giữ nguyên",
                jobCode, r.Err, r.Msg);
            return r;   // DB không đổi ⇒ TUYỆT ĐỐI không đụng cache
        }

        // `PurgedRuns` = số lượt chờ của cấu hình CŨ đã bị dọn. Log ra, đừng nuốt: đây là bằng chứng
        //   cấu hình mới đã có hiệu lực ở tầng DB, và là con số đầu tiên cần nhìn khi có người hỏi
        //   "sao job vẫn chạy theo giờ cũ".
        Log.Information("[JOB] Đổi lịch {Code} bởi {User}: dọn {N} lượt chờ của cấu hình cũ",
            jobCode, user, r.PurgedRuns);

        // ② Nạp lại cache NGAY từ DB. Không suy cấu hình mới từ tham số đầu vào — đọc lại thứ DB
        //    thật sự đang giữ, vì SP có thể chuẩn hoá/từ chối một phần (và job KHÁC cũng có thể
        //    vừa bị đổi bởi người khác).
        await ReloadCacheAsync(ct);
        return r;
    }

    /// <summary>
    /// Đọc lại toàn bộ lịch từ DB và ghi đè cache. Gọi được độc lập — dùng khi ai đó đã `UPDATE`
    /// thẳng `T_JOB_DEFINITION` bằng script vận hành (trigger dọn lượt chờ, nhưng không ai dọn cache).
    /// </summary>
    public async Task ReloadCacheAsync(CancellationToken ct)
    {
        try
        {
            var fromDb = await _db.GetSchedulableJobsAsync(ct);
            await JobConfigCache.WriteAsync(_redis, fromDb);
            Log.Information("[JOB] Nạp lại cache cấu hình: {N} job có lịch", fromDb.Count);
        }
        catch (Exception ex)
        {
            // Không ném: DB đã đúng rồi. Cache sai chỉ làm cấu hình mới tới chậm, và TTL là lưới cuối.
            Log.Warning(ex, "[JOB] Nạp lại cache cấu hình HỤT — cấu hình mới sẽ tới pod chậm nhất "
                          + "sau {Ttl}, khi cache tự hết hạn", JobRedisKeys.ConfigTtl);
        }
    }
}
