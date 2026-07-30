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
    public long   Rows      { get; init; }        // ★ TỔNG DÒNG đã nhận (lũy kế cả job, sau batch này)
    public long   Total     { get; init; }        // totalRow Asset khai

    /// <summary>★ TỔNG AUM (VND) đã nhận — lũy kế cả job, sau batch này. Chỉ có nghĩa khi <see cref="AumTracked"/>.</summary>
    public long   Aum        { get; init; }
    /// <summary>false ⇒ batch này không đóng góp AUM (tổng batch vượt tầm int64 — xem log ERROR).</summary>
    public bool   AumTracked { get; init; }

    /// <summary>Tổng AUM của RIÊNG batch này (phần đóng góp mới, chưa tính phần sửa lại của batch cũ).</summary>
    public long   AumInBatch { get; init; }
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

    /// <summary>
    /// ★ CHÍNH SÁCH DỌN DẸP: TTL — TUYỆT ĐỐI KHÔNG KeyDelete SAU KHI JOB XONG.
    ///
    /// Code cũ xoá key khi job kết thúc:
    ///     var keysToDelete = new RedisKey[] { JOB_ID, COUNTER_SUCCESS, COUNTER_FAIL,
    ///                                         TOTAL_PROCESSED, EOD_DATE, LAST_TRY_DEQUEUE };
    ///     await db.KeyDeleteAsync(keysToDelete);
    /// ⇒ Batch TỚI MUỘN (rebalance / retry chậm) vào hàm, không tìm thấy JOB_ID → spin 100s →
    ///   return BatchDone=false → executeFunc KHÔNG ĐƯỢC GỌI → DỮ LIỆU BATCH ĐÓ KHÔNG BAO GIỜ VÀO DB.
    ///   Việc "dọn dẹp" đã GIẾT DỮ LIỆU, mà job vẫn báo DONE.
    ///
    /// Vì sao TTL an toàn hơn:
    ///   • Giữ {P}:MSG ⇒ batch trùng tới muộn KHÔNG bị đếm lại (dedup còn hiệu lực).
    ///   • Giữ {P}:ROWS/{P}:AUM/{P}:TOTAL ⇒ còn BẰNG CHỨNG để debug đúng lúc cần nhất (job hỏng):
    ///     "nhận đủ dòng chưa" VÀ "tổng tiền có khớp DB không".
    ///   • Giữ {P}:BATCH_AUM ⇒ batch gửi lại (đã SỬA giá trị) vẫn cộng đúng phần CHÊNH, không cộng lại từ đầu.
    ///   • Chi phí ~75 KB/job (MSG set 500×64B ≈ 35 KB + BATCH_AUM hash 500×(64B+~12B) ≈ 40 KB).
    ///     Vài job/ngày × 48h = vài MB. Không đáng đánh đổi.
    ///
    /// Thứ DUY NHẤT được dọn ngay: tư cách thành viên trong SET {prefix}:JOBS (SREM khi READY) —
    /// nếu không watchdog sẽ quét mãi job đã xong.
    /// </summary>
    private readonly TimeSpan _ttl = TimeSpan.FromHours(48);

    public BatchSyncService(IDatabase redis, ISdiDbGateway db)
    {
        _redis = redis;
        _db    = db;
    }

    /// <param name="requestId">★ Asset gửi, CHUNG cho mọi batch của job ⇒ dùng thẳng làm JOB_ID.</param>
    /// <param name="siAccounts">Danh sách si_account trong batch — DANH TÍNH nghiệp vụ để dedup.</param>
    /// <param name="totalRow">Số dòng Asset KHAI cho cả ngày. GỢI Ý, không phải cổng chặn.</param>
    /// <param name="rowsInBatch">Số dòng trong batch này (đếm từ payload — KHÔNG phải số dòng ghi được).</param>
    /// <param name="aumInBatch">
    ///   ★ Tổng AUM (VND) của batch này — tính bằng <see cref="KafkaSyncKeys.SumAum"/> tại call-site
    ///   (làm tròn TỪNG DÒNG y như <c>DECIMAL(20,0)</c> của DB).
    ///   <c>null</c> = không theo dõi AUM (job không có khái niệm AUM, hoặc tổng vượt tầm int64).
    ///
    ///   ⚠️ Đây là số để QUAN SÁT/ĐỐI SOÁT, TUYỆT ĐỐI không phải cổng chặn — đúng nguyên tắc
    ///   "Redis lo TỐC ĐỘ, DB lo TÍNH ĐÚNG". Sự thật về AUM là <c>SUM(C_AUM) FROM T_SI_BALANCE @d</c>.
    /// </param>
    /// <param name="executeFunc">Ghi DB. Trả false ⇒ NÉM ⇒ không commit offset ⇒ Kafka giao lại.</param>
    public async Task<BatchSyncResult> SyncBatchAsync(
        string requestId,
        string tranDate,
        string redisKeyPrefix,
        string bizType,
        IEnumerable<string> siAccounts,
        long   totalRow,
        int    rowsInBatch,
        long?  aumInBatch,
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
        //    ★ Một script Lua cộng CẢ HAI: số dòng (cổng chặn, dedup thuần) và tổng AUM (số quan sát,
        //      cộng theo delta để batch gửi lại-đã-sửa vẫn ra số đúng). Cùng 1 round-trip, cùng 1
        //      lát cắt nguyên tử ⇒ ROWS và AUM KHÔNG BAO GIỜ lệch pha nhau.
        var batchKey = KafkaSyncKeys.BatchKey(siAccounts);
        var res = (RedisValue[])await _redis.ScriptEvaluateAsync(
            KafkaSyncKeys.LuaSumOnce,
            new RedisKey[]   { KafkaSyncKeys.Msg(p),  KafkaSyncKeys.Rows(p),
                               KafkaSyncKeys.Aum(p),  KafkaSyncKeys.BatchAum(p) },
            new RedisValue[] { batchKey, rowsInBatch,
                               // ⚠️ ĐỪNG dùng long.ToString(): phụ thuộc CurrentCulture (dấu âm/nhóm số).
                               //    Ép sang RedisValue để thư viện format bất biến. '' = không theo dõi AUM.
                               aumInBatch.HasValue ? (RedisValue)aumInBatch.Value : RedisValue.EmptyString,
                               (long)_ttl.TotalSeconds });

        long rows = (long)res[0];
        long aum  = (long)res[1];

        // metadata cho watchdog
        var batch = _redis.CreateBatch();
        _ = batch.StringSetAsync(KafkaSyncKeys.Total(p),  totalRow, _ttl, When.NotExists);
        _ = batch.StringSetAsync(KafkaSyncKeys.Date(p),   tranDate, _ttl, When.NotExists);
        _ = batch.StringSetAsync(KafkaSyncKeys.LastAt(p), DateTime.UtcNow.Ticks, _ttl);
        _ = batch.SetAddAsync(KafkaSyncKeys.ActiveJobs(redisKeyPrefix), requestId);
        _ = batch.KeyExpireAsync(KafkaSyncKeys.ActiveJobs(redisKeyPrefix), _ttl);
        batch.Execute();

        // ── 3) SỔ LŨY KẾ SAU MỖI BATCH — dòng log này là thứ trực ban nhìn để biết job đang chảy tới đâu
        Log.Information("[{Biz}] {Date} req={Req} +{N} dòng / +{AumN} AUM → LŨY KẾ {Rows}/{Total} dòng, AUM {Aum}",
                        bizType, tranDate, requestId, rowsInBatch, aumInBatch, rows, totalRow, aum);

        // ── 4) ĐỦ CHƯA → BẬT CỜ. HẾT. ─────────────────────────────────────────────────────
        bool jobDone = rows >= totalRow;
        if (jobDone)
            await TrySetReadyAsync(redisKeyPrefix, requestId, tranDate, bizType, totalRow, rows, byTimeout: false);

        return new BatchSyncResult
        {
            JobId      = requestId, BatchDone = true, JobDone = jobDone, Rows = rows, Total = totalRow,
            Aum        = aum,
            AumTracked = aumInBatch.HasValue,
            AumInBatch = aumInBatch ?? 0
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

        // Đọc SAU khi thắng SET NX ⇒ đúng 1 lần/job, không thêm round-trip vào đường nóng của batch.
        var aumVal = await _redis.StringGetAsync(KafkaSyncKeys.Aum(p));

        if (byTimeout)
            Log.Warning("[{Biz}] {Date} req={Req} chốt theo TIMEOUT: {Rows}/{Total} dòng. " +
                        "Asset có thể gửi thiếu hoặc totalRow sai. ĐỦ/THIẾU THẬT do SP_EOD_RUN quyết (err=12).",
                        bizType, tranDate, requestId, rows, total);

        // Ghi cờ vào T_EOD_PIPELINE. SP KHÔNG còn tự đếm đủ/thiếu (commit 65cf008) — nó chỉ ghi cờ.
        // Cửa khoá thật: SP_EOD_RUN err=12 nếu SI ACTIVE nào của SDI thiếu dòng T_SI_BALANCE @d.
        await _db.SetSourceReadyAsync(tranDate, "ASSET_NAV", total);

        // job xong → gỡ khỏi danh sách watchdog phải canh
        await _redis.SetRemoveAsync(KafkaSyncKeys.ActiveJobs(redisKeyPrefix), requestId);

        // ★ Chốt sổ: dòng + AUM. So AUM này với SUM(C_AUM) FROM T_SI_BALANCE @d là phát hiện ngay
        //   "Asset gửi đủ dòng nhưng lệch tiền" — thứ mà đếm dòng KHÔNG BAO GIỜ thấy được.
        Log.Information("[{Biz}] {Date} req={Req} ASSET_NAV READY: {Rows}/{Total} dòng, tổng AUM {Aum}",
                        bizType, tranDate, requestId, rows, total,
                        aumVal.HasValue ? (long)aumVal : 0);
    }
}
