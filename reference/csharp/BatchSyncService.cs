using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Serilog;
using StackExchange.Redis;

namespace SdiCoreMessagingProcess.Services;

/// <summary>Kết quả xử lý 1 batch.</summary>
public sealed class BatchSyncResult
{
    public string JobId    { get; init; } = "";   // = requestId
    public bool   BatchDone { get; init; }        // batch này đã ghi DB xong
    public bool   JobDone   { get; init; }        // job đã đủ dòng ⇒ ASSET_NAV = READY
    public long   Rows      { get; init; }        // tổng dòng đã nhận
    public long   Total     { get; init; }        // totalRow Asset khai
}

/// <summary>Ném ra khi ghi DB lỗi ⇒ consumer KHÔNG commit offset ⇒ Kafka giao lại.</summary>
public sealed class BatchIngestException : Exception
{
    public BatchIngestException(string m) : base(m) { }
}

/// <summary>
/// THAY THẾ HOÀN TOÀN <c>BaseService.SyncEodDataGeneric</c>.
///
/// ĐÃ XOÁ (và vì sao):
///   • <c>COUNTER</c> + <c>if (counter == 1)</c> tạo JOB_ID
///       → ĐIỂM CHẾT ĐƠN: pod tăng COUNTER lên 1 rồi chết trước khi ghi JOB_ID ⇒ KHÔNG CÒN AI
///         thấy counter==1 nữa ⇒ mọi message sau đều không có JOB_ID ⇒ job chết vĩnh viễn.
///       → Nay: JOB_ID = requestId, Asset gửi sẵn trong MỌI message.
///   • <c>INITIAL_LOCK</c> (LockTake 5s, KHÔNG có else)
///       → Lấy hụt lock (lock sót từ lần chạy trước) ⇒ im lặng bỏ qua ⇒ JOB_ID không bao giờ được tạo.
///   • Vòng retry <c>1000 × 100ms</c> chờ JOB_ID
///       → Block consumer 100 GIÂY ⇒ vượt max.poll.interval ⇒ Kafka ĐÁ POD khỏi group.
///         Kafka KHÔNG giết thread — pod cũ thành ZOMBIE, vẫn ghi DB, vẫn INCR Redis, trong khi
///         pod mới xử lý LẠI cùng message ⇒ cộng dồn 2 lần ⇒ chốt job SỚM khi còn thiếu batch.
///   • <c>TOTAL_PROCESSED >= totalRow</c> (cộng SỐ DÒNG GHI ĐƯỢC)
///       → numProcessed = @p_rows của SP_INGEST_ASSET_NAV = số dòng GHI ĐƯỢC (đã lọc acc không thuộc
///         SDI qua INNER JOIN registry), còn totalRow = số dòng GỬI ĐI ⇒ HAI TẬP KHÁC NHAU ⇒ chỉ cần
///         Asset có 1 tài khoản lạ là tổng KHÔNG BAO GIỜ đạt ⇒ JobDone luôn false ⇒ CHAIN LUÔN BỊ BỎ.
///   • <c>COUNTER</c> làm điều kiện chốt (bản sau)
///       → INCR chạy TRƯỚC executeFunc ⇒ batch INSERT LỖI vẫn được tính là "đã xong"
///         ⇒ job chốt DONE trên dữ liệu THIẾU. Tệ hơn treo — treo thì ít ra còn phát hiện được.
///   • <c>END_LOCK</c> + double-check + <c>JobDone</c> (bool trong RAM)
///       → Chain gắn vào một biến RAM của ĐÚNG MỘT message may mắn. Message đó lỗi/trùng/pod chết
///         ⇒ SỰ KIỆN MẤT LUÔN, không ai dựng lại được ⇒ "thường xuyên bỏ qua chain job phía sau".
///
/// NGUYÊN TẮC MỚI:
///   ① Đừng ĐẾM LƯỢT GHÉ — hãy ĐÁNH DẤU DANH TÍNH rồi mới cộng (Lua nguyên tử).
///   ② "Đủ chưa" là một PHÉP HỎI (đọc Redis/DB), KHÔNG phải một SỰ KIỆN (bool trong RAM).
///   ③ totalRow là GỢI Ý. Cửa khoá thật là SP_EOD_RUN err=12 (mọi SI ACTIVE của SDI phải có dòng).
/// </summary>
public class BatchSyncService
{
    private readonly IDatabase      _redis;
    private readonly ISdiDbGateway  _db;         // wrapper gọi SP (Dapper/ADO — dùng repo sẵn có)
    private readonly TimeSpan       _ttl = TimeSpan.FromHours(48);

    public BatchSyncService(IDatabase redis, ISdiDbGateway db)
    {
        _redis = redis;
        _db    = db;
    }

    /// <param name="requestId">★ Asset gửi, CHUNG cho mọi batch của job ⇒ dùng thẳng làm JOB_ID.</param>
    /// <param name="siAccounts">Danh sách si_account trong batch — DANH TÍNH nghiệp vụ để dedup.</param>
    /// <param name="totalRow">Số dòng Asset KHAI cho cả ngày. GỢI Ý, không phải cổng chặn.</param>
    /// <param name="rowsInBatch">Số dòng trong batch này (đếm từ payload — KHÔNG phải số dòng ghi được).</param>
    /// <param name="executeFunc">Ghi DB. Trả false ⇒ NÉM ⇒ không commit offset ⇒ Kafka giao lại.</param>
    public async Task<BatchSyncResult> SyncBatchAsync(
        string requestId,
        string tranDate,
        string redisKeyPrefix,
        string bizType,
        IEnumerable<string> siAccounts,
        long   totalRow,
        int    rowsInBatch,
        Func<Task<bool>> executeFunc)
    {
        if (string.IsNullOrEmpty(requestId))
            throw new BatchIngestException("requestId rỗng — Asset phải gửi requestId cho mọi batch");

        var p = KafkaSyncKeys.Scope(redisKeyPrefix, requestId);

        // ── 1) GHI DB TRƯỚC ────────────────────────────────────────────────────────────────
        //    SP_INGEST_ASSET_NAV: all-or-nothing + idempotent (DELETE+INSERT theo (date, si)).
        //    Xử lý lại bao nhiêu lần cũng ra cùng kết quả ⇒ zombie pod ghi song song là VÔ HẠI.
        if (!await executeFunc())
            throw new BatchIngestException($"[{bizType}] {tranDate} ingest FAIL req={requestId}");
        //    ⚠️ NÉM chứ không "return false": phải để Kafka giao lại. Nuốt lỗi = MẤT DỮ LIỆU.

        // ── 2) CỘNG DỒN CÓ DEDUP — nguyên tử, 1 round-trip ────────────────────────────────
        //    Đánh dấu SAU khi DB đã commit. Đánh dấu TRƯỚC mà pod chết giữa chừng ⇒ batch coi như
        //    xong dù chưa ghi ⇒ tổng không bao giờ đạt ⇒ treo. KHÔNG BAO GIỜ đánh dấu trước.
        var batchKey = KafkaSyncKeys.BatchKey(siAccounts);
        long rows = (long)await _redis.ScriptEvaluateAsync(
            KafkaSyncKeys.LuaSumOnce,
            new RedisKey[]   { KafkaSyncKeys.Msg(p), KafkaSyncKeys.Rows(p) },
            new RedisValue[] { batchKey, rowsInBatch, (long)_ttl.TotalSeconds });

        // metadata cho watchdog
        var batch = _redis.CreateBatch();
        _ = batch.StringSetAsync(KafkaSyncKeys.Total(p),  totalRow, _ttl, When.NotExists);
        _ = batch.StringSetAsync(KafkaSyncKeys.Date(p),   tranDate, _ttl, When.NotExists);
        _ = batch.StringSetAsync(KafkaSyncKeys.LastAt(p), DateTime.UtcNow.Ticks, _ttl);
        _ = batch.SetAddAsync(KafkaSyncKeys.ActiveJobs(redisKeyPrefix), requestId);
        _ = batch.KeyExpireAsync(KafkaSyncKeys.ActiveJobs(redisKeyPrefix), _ttl);
        batch.Execute();

        // ── 3) ĐỦ CHƯA → BẬT CỜ. HẾT. ─────────────────────────────────────────────────────
        bool jobDone = rows >= totalRow;
        if (jobDone)
            await TrySetReadyAsync(redisKeyPrefix, requestId, tranDate, bizType, totalRow, rows, byTimeout: false);

        return new BatchSyncResult
        {
            JobId = requestId, BatchDone = true, JobDone = jobDone, Rows = rows, Total = totalRow
        };
    }

    /// <summary>
    /// Bật cờ ASSET_NAV=READY. SET NX ⇒ chỉ 1 pod gọi SP dù nhiều pod cùng thấy đủ.
    /// Gọi từ 2 nơi: (a) batch cuối chạm totalRow; (b) watchdog khi TIMEOUT.
    /// </summary>
    public async Task TrySetReadyAsync(
        string redisKeyPrefix, string requestId, string tranDate, string bizType,
        long total, long rows, bool byTimeout)
    {
        var p = KafkaSyncKeys.Scope(redisKeyPrefix, requestId);

        if (!await _redis.StringSetAsync(KafkaSyncKeys.Ready(p), "1", _ttl, When.NotExists))
            return;   // pod khác đã bật rồi

        if (byTimeout)
            Log.Warning("[{Biz}] {Date} req={Req} chốt theo TIMEOUT: {Rows}/{Total} dòng. " +
                        "Asset có thể gửi thiếu hoặc totalRow sai. ĐỦ/THIẾU THẬT do SP_EOD_RUN quyết (err=12).",
                        bizType, tranDate, requestId, rows, total);

        // Ghi cờ vào T_EOD_PIPELINE. SP KHÔNG còn tự đếm đủ/thiếu (commit 65cf008) — nó chỉ ghi cờ.
        // Cửa khoá thật: SP_EOD_RUN err=12 nếu SI ACTIVE nào của SDI thiếu dòng T_SI_BALANCE @d.
        await _db.SetSourceReadyAsync(tranDate, "ASSET_NAV", total);

        // job xong → gỡ khỏi danh sách watchdog phải canh
        await _redis.SetRemoveAsync(KafkaSyncKeys.ActiveJobs(redisKeyPrefix), requestId);

        Log.Information("[{Biz}] {Date} req={Req} ASSET_NAV READY: {Rows}/{Total}",
                        bizType, tranDate, requestId, rows, total);
    }
}
