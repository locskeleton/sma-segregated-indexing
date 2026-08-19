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
    /// <summary>MỐC mà lượt này đáng lẽ chạy — handler truyền vào TradingWindowGuard.</summary>
    public DateTime SlotAt       { get; init; }

    /// <summary>
    /// BÁO TIẾN ĐỘ — chỉ ghi vào một biến trong RAM. KHÔNG chạm DB, không await, không khoá.
    /// Gọi thoải mái trong vòng lặp nóng: 1000 batch gọi 1000 lần cũng bằng 0 lượt truy vấn.
    /// Con số này được nhịp tim nền đẩy xuống `T_JOB_RUN.C_ROWS` mỗi ~20-30 giây.
    ///
    /// ⚠️ Đây là hàm handler NÊN dùng. Bản đầu chỉ có `HeartbeatAsync` và
    ///    `FoSnapshotJobHandler` gọi nó sau MỖI batch ⇒ 1000 câu UPDATE vào ĐÚNG MỘT DÒNG mỗi
    ///    chu kỳ, phát ra từ 4 luồng song song ⇒ tự tạo điểm nghẽn khoá dòng, và tất cả chỉ để
    ///    ghi một con số không ai đọc trong lúc job đang chạy.
    /// </summary>
    public Action<long> ReportProgress { get; init; } = _ => { };

    /// <summary>
    /// CÒN LÀ CHỦ LƯỢT CHẠY NÀY KHÔNG — đọc từ RAM, cập nhật bởi nhịp tim nền.
    /// false ⇒ lease đã bị thu hồi (pod này treo quá lâu) HOẶC job vừa bị TẮT ⇒ handler phải
    /// dừng ngay và KHÔNG ghi thêm gì. Vòng lặp dài nên kiểm cờ này mỗi vòng — nó miễn phí.
    /// </summary>
    public Func<bool> IsStillMine { get; init; } = () => true;

    /// <summary>
    /// ÉP một nhịp tim NGAY (chạm DB) và trả lời "còn là chủ không".
    /// Handler thường KHÔNG cần gọi — nhịp tim nền đã lo cả việc gia hạn lease lẫn cập nhật cờ.
    /// Chỉ dùng khi cần một câu trả lời TƯƠI ngay trước một hành động không thể hoàn tác.
    /// CÓ CHẶN TẦN SUẤT: gọi dày hơn ngưỡng thì trả lại kết quả gần nhất, không bắn thêm query —
    /// để một handler viết ẩu cũng không thể biến hàm này thành vòi tưới DB.
    /// </summary>
    public Func<Task<bool>> HeartbeatAsync { get; init; } = () => Task.FromResult(true);
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
    /// <summary>T_JOB_DEFINITION.C_TIMEOUT_SEC — worker suy nhịp tim từ đây (nhịp = timeout/4).</summary>
    public int       TimeoutSec   { get; init; } = 300;
    /// <summary>Nguồn đánh thức lượt này ('kafka' | 'recover' | 'manual').</summary>
    public string?   ClaimSource  { get; init; }
    /// <summary>
    /// T_JOB_RUN.C_SLOT_AT — MỐC mà lượt này đáng lẽ chạy (09:00, 09:15…). NEO của mọi phép kiểm
    /// giờ giấc: hạn tươi đo từ đây, KHÔNG đo từ lúc pod nhận được message.
    /// </summary>
    public DateTime  SlotAt       { get; init; }
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
    /// <summary>
    /// SP_GET_SCHEDULABLE_JOBS — cấu hình lịch cho bộ quét. Đọc khi cache Redis trống; ở trạng
    /// thái ổn định gần như không gọi tới.
    /// </summary>
    Task<IReadOnlyList<SchedulableJob>> GetSchedulableJobsAsync(CancellationToken ct);
    Task<IReadOnlyList<DueJob>> RecoverAsync(int staleSec, CancellationToken ct);               // SP_JOB_RECOVER
    /// <summary>SP_JOB_CLAIM. `source` = 'notify' | 'recover' | 'manual' → ghi vào T_JOB_RUN.C_CLAIM_SOURCE.
    /// Đừng bỏ tham số này: nó là thứ duy nhất phân biệt "chuông Pub/Sub đang chạy" với "chuông
    /// đã tắt từ lâu mà bộ hồi phục vẫn gánh" — hai trạng thái nhìn từ ngoài giống hệt nhau.</summary>
    Task<JobClaim>              ClaimAsync(long jobRunId, string owner, string source, CancellationToken ct);
    Task<bool>                  HeartbeatAsync(long jobRunId, string owner, long? rows, CancellationToken ct);  // SP_JOB_HEARTBEAT
    Task                        CompleteAsync(long jobRunId, string owner, bool ok, long? rows, string? msg, CancellationToken ct); // SP_JOB_COMPLETE
    /// <summary>
    /// SP_JOB_ENQUEUE. `slotAt` = MỐC mà lượt này đáng lẽ chạy (bộ quét tự tính); truyền null cho
    /// job đẩy tay. `fireKey` để null khi đã có `slotAt` — SQL tự suy khoá bằng UDF_JOB_FIRE_KEY,
    /// bớt một chỗ để C# và SQL có thể định dạng khác nhau.
    /// err: 0 OK · 1 job lạ · 2 job tắt · 3 mốc ngoài khung · 4 mốc đã có (idempotent) · 7 singleton
    ///      đang chạy · 20 mốc không khớp lưới (bộ quét tính sai).
    /// </summary>
    Task<(long Id, int Err)>    EnqueueAsync(string jobCode, string? fireKey, string? payload,
                                             DateTime? businessDate, DateTime? slotAt, string user, CancellationToken ct);

    /// <summary>SELECT dbo.UDF_IS_BUSINESS_DATE(@d) — lịch nghỉ nằm ở DB (T_TRADING_HOLIDAY),
    /// KHÔNG hard-code trong C#. TradingWindowGuard gọi hàm này (nhớ theo ngày, 1 lượt/chu kỳ).</summary>
    Task<bool>                  IsBusinessDateAsync(DateTime d);

    /// <summary>
    /// Khung giờ HIỆU LỰC của job, đọc thẳng T_JOB_DEFINITION:
    ///   SELECT C_ENABLED, C_WINDOW_FROM, C_WINDOW_TO, C_BUSINESS_DAY_ONLY,
    ///          COALESCE(C_MAX_DELAY_SEC, C_INTERVAL_SEC) AS MaxDelaySec
    ///   FROM T_JOB_DEFINITION WHERE C_JOB_CODE = @code
    /// TradingWindowGuard gọi hàm này (nhớ tạm 60s). KHÔNG nhận khung giờ qua hằng số DI: đổi cấu
    /// hình trong DB mà tầng 4 vẫn gác theo khung cũ thì đúng cái tầng chạm FO là tầng hiểu sai luật.
    /// MaxDelaySec = CÙNG con số SP_JOB_CLAIM dùng ⇒ tầng 2 và tầng 4 không thể lệch pha.
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
