using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace SdiCoreMessagingProcess.Jobs;

/// <summary>
/// HỢP ĐỒNG DUY NHẤT giữa khung job và nghiệp vụ.
///
/// Khung (JobSchedulerService + JobDispatcherService) KHÔNG biết FO là gì, không biết snapshot là
/// gì. Nó chỉ biết: có một <see cref="JobCode"/>, và khi tới lượt thì gọi <see cref="RunAsync"/>.
/// Thêm loại job mới = viết 1 class implement interface này + INSERT 1 dòng T_JOB_DEFINITION.
/// KHÔNG sửa dispatcher, KHÔNG sửa scheduler, KHÔNG sửa SP nào ở tầng A của db/11_JOB.sql.
/// </summary>
public interface IJobHandler
{
    /// <summary>Khớp với T_JOB_DEFINITION.C_HANDLER.</summary>
    string HandlerKey { get; }

    /// <summary>
    /// Trả về SỐ ĐƠN VỊ đã xử lý (ghi vào T_JOB_RUN.C_ROWS — để người trực nhìn là biết job có
    /// thật sự làm gì không, hay chỉ chạy cho có).
    /// NÉM exception ⇒ lượt chạy được đánh FAILED và (nếu còn lượt thử) tự xếp lại hàng đợi.
    /// </summary>
    Task<long> RunAsync(JobContext ctx, CancellationToken ct);
}

/// <summary>Thông tin một lượt chạy, khung truyền xuống handler.</summary>
public sealed class JobContext
{
    public long     JobRunId     { get; init; }
    public string   JobCode      { get; init; } = "";
    public string   FireKey      { get; init; } = "";
    public DateTime? BusinessDate { get; init; }
    public string?  PayloadJson  { get; init; }
    public int      Attempt      { get; init; }

    /// <summary>
    /// Báo tiến độ + GIA HẠN LEASE. Trả false ⇒ pod này KHÔNG CÒN LÀ CHỦ của lượt chạy
    /// (lease đã bị thu hồi, pod khác đang chạy) ⇒ handler PHẢI dừng ngay và KHÔNG ghi thêm gì.
    /// Vòng lặp dài (1000 batch) BẮT BUỘC gọi hàm này định kỳ, nếu không lease hết hạn giữa chừng
    /// và sẽ có pod thứ hai chạy song song cùng một chu kỳ.
    /// </summary>
    public Func<long, Task<bool>> HeartbeatAsync { get; init; } = _ => Task.FromResult(true);
}

/// <summary>Tra handler theo khoá. Đăng ký bằng DI, không hard-code switch-case.</summary>
public interface IJobRegistry
{
    IJobHandler? Resolve(string handlerKey);
}

public sealed class JobRegistry : IJobRegistry
{
    private readonly Dictionary<string, IJobHandler> _map;

    public JobRegistry(IEnumerable<IJobHandler> handlers)
    {
        _map = new Dictionary<string, IJobHandler>(StringComparer.OrdinalIgnoreCase);
        foreach (var h in handlers) _map[h.HandlerKey] = h;
    }

    public IJobHandler? Resolve(string handlerKey)
        => _map.TryGetValue(handlerKey, out var h) ? h : null;
}

/// <summary>Một lượt chạy đã claim được (kết quả SP_JOB_CLAIM).</summary>
public sealed class JobClaim
{
    public int       Err          { get; init; }   // 0 OK · 3 ngoài khung (SKIPPED) · 5 pod khác giữ · 6 DEAD
    public string?   Msg          { get; init; }
    public long      JobRunId     { get; init; }
    public string    JobCode      { get; init; } = "";
    public string    HandlerKey   { get; init; } = "";
    public string    FireKey      { get; init; } = "";
    public DateTime? BusinessDate { get; init; }
    public string?   PayloadJson  { get; init; }
    public int       Attempt      { get; init; }
}

public sealed class DueJob
{
    public long    JobRunId { get; init; }
    public string  JobCode  { get; init; } = "";
}

/// <summary>
/// Cầu nối DB — map 1-1 sang proc trong db/11_JOB.sql. Cài bằng Dapper/ADO theo repo sẵn có.
/// KHÔNG nhét logic vào đây: mọi quyết định (ai được chạy, còn trong giờ không) nằm TRONG SP,
/// vì đó là nơi duy nhất mọi pod nhìn thấy cùng một sự thật tại cùng một thời điểm.
/// </summary>
public interface ISdiJobGateway
{
    Task<IReadOnlyList<DueJob>> EnqueueDueAsync(CancellationToken ct);                       // SP_JOB_ENQUEUE_DUE
    Task<IReadOnlyList<DueJob>> ReapAsync(int staleSec, CancellationToken ct);               // SP_JOB_REAP
    Task<JobClaim>              ClaimAsync(long jobRunId, string owner, string streamId, CancellationToken ct); // SP_JOB_CLAIM
    Task<bool>                  HeartbeatAsync(long jobRunId, string owner, long? rows, CancellationToken ct);  // SP_JOB_HEARTBEAT
    Task                        CompleteAsync(long jobRunId, string owner, bool ok, long? rows, string? msg, CancellationToken ct); // SP_JOB_COMPLETE
    Task<(long Id, int Err)>    EnqueueAsync(string jobCode, string? fireKey, string? payload, DateTime? businessDate, string user, CancellationToken ct); // SP_JOB_ENQUEUE

    /// <summary>SELECT dbo.UDF_IS_BUSINESS_DATE(@d) — lịch nghỉ nằm ở DB (T_TRADING_HOLIDAY),
    /// KHÔNG hard-code trong C#. TradingWindowGuard gọi hàm này (nhớ theo ngày, 1 lượt/chu kỳ).</summary>
    Task<bool>                  IsBusinessDateAsync(DateTime d);

    /// <summary>
    /// Khung giờ HIỆU LỰC của job, đọc thẳng T_JOB_DEFINITION:
    ///   SELECT C_ENABLED, C_WINDOW_FROM, C_WINDOW_TO, C_BUSINESS_DAY_ONLY
    ///   FROM T_JOB_DEFINITION WHERE C_JOB_CODE = @code
    /// TradingWindowGuard gọi hàm này (nhớ tạm 60s). KHÔNG nhận khung giờ qua hằng số DI: đổi cấu
    /// hình trong DB mà tầng 4 vẫn gác theo khung cũ thì đúng cái tầng chạm FO là tầng hiểu sai luật.
    /// </summary>
    Task<JobWindow>             GetJobWindowAsync(string jobCode, CancellationToken ct);

    /// <summary>
    /// SP_SET_JOB_SCHEDULE — cổng đổi lịch. Trả về số lượt chờ của cấu hình CŨ đã bị dọn;
    /// hãy LOG con số đó, đừng nuốt: nó là bằng chứng cấu hình mới đã có hiệu lực.
    /// </summary>
    Task<(int Err, string? Msg, int PurgedRuns)> SetJobScheduleAsync(
        string jobCode, int? intervalSec, bool clearInterval, TimeSpan? windowFrom, TimeSpan? windowTo,
        bool clearWindow, bool? businessDayOnly, bool? enabled, int? maxDelaySec, string? payload,
        string user, CancellationToken ct);

    // --- nghiệp vụ FO snapshot (tầng B của 11_JOB.sql) ---
    Task<IReadOnlyList<ScopeRow>> GetFoSnapshotScopeAsync(string? masterCode, CancellationToken ct); // SP_GET_FO_SNAPSHOT_SCOPE
    Task<(int Err, string? Msg, long Rows, int SkippedEod)> IngestFoSnapshotRtAsync(
        string json, DateTime businessDate, DateTime snapshotAt, CancellationToken ct);             // SP_INGEST_FO_SNAPSHOT_RT
    Task<(int Err, string? Msg, long Rows)> RtMasterAggAsync(DateTime businessDate, CancellationToken ct); // SP_RT_MASTER_AGG
}

public sealed class ScopeRow
{
    public string CustCode     { get; init; } = "";
    public string SiAccount    { get; init; } = "";
    public string? SubAccountNo { get; init; }
    public string MasterCode   { get; init; } = "";
}
