using System;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Serilog;
using StackExchange.Redis;

namespace SdiCoreMessagingProcess.Services;

/// <summary>
/// ★ BẢN DROP-IN — thay THẲNG method cũ trong BaseService&lt;T&gt;.
///   Giữ NGUYÊN kiểu trả về JobResult. Chỉ THÊM 1 tham số optional: jobId.
/// </summary>
public partial class BaseService<T>
{
    /// <param name="jobId">
    ///   ★ TRUYỀN TỪ NGOÀI VÀO — Asset gửi sẵn trong message (<c>model.requestId</c>), CHUNG cho mọi batch của job.
    ///
    ///   Vì có sẵn nên KHÔNG CẦN đi tạo ⇒ xoá sổ toàn bộ cụm:
    ///       counter = INCR(COUNTER); if (counter == 1) { LockTake(5s) → LogJobKafkaStarting }
    ///       while (jobId empty &amp;&amp; retry &lt; 1000) { Task.Delay(100); read JOB_ID; }
    ///   • counter==1 là ĐIỂM CHẾT ĐƠN: pod INCR lên 1 rồi chết trước khi ghi JOB_ID ⇒ KHÔNG AI còn
    ///     thấy counter==1 ⇒ mọi message sau đều mồ côi ⇒ JOB CHẾT VĨNH VIỄN.
    ///   • LockTake không có else: lấy hụt lock (lock sót từ lần trước) ⇒ im lặng bỏ qua ⇒ chết y hệt.
    ///   • Vòng chờ 1000×100ms = 100 GIÂY ⇒ vượt max.poll.interval ⇒ KAFKA ĐÁ POD (nhưng KHÔNG giết
    ///     thread — pod cũ thành ZOMBIE, vẫn ghi DB) ⇒ pod mới xử lý LẠI ⇒ duplicate ⇒ vòng xoáy.
    ///
    ///   Job KHÔNG có requestId (fee accrue/charge) → để null ⇒ dùng ngày làm định danh.
    ///   Xác định trước, không race, không cần điều phối gì cả.
    /// </param>
    /// <param name="executeFunc">
    ///   Trả về: (Thành công, Tổng record EOD, <b>SỐ RECORD TRONG BATCH</b>)
    ///
    ///   ⚠️⚠️ THAM SỐ THỨ 3 ĐỔI NGHĨA — ĐÂY LÀ SỬA QUAN TRỌNG NHẤT:
    ///       CŨ : RowsProcessed = <c>@p_rows</c> của SP = SỐ DÒNG <b>GHI ĐƯỢC</b>
    ///       MỚI: RowsInBatch   = <c>model.data.Count</c> = SỐ DÒNG <b>TRONG PAYLOAD</b>
    ///
    ///   VÌ SAO: SP_INGEST_ASSET_NAV có INNER JOIN registry ⇒ acc KHÔNG thuộc SDI bị LỌC BỎ
    ///   ⇒ <c>@p_rows</c> &lt; số item Asset gửi. Còn totalRow là số Asset GỬI ĐI.
    ///   ⇒ Σ(@p_rows) KHÔNG BAO GIỜ ĐẠT totalRow ⇒ JobDone luôn false ⇒ CHAIN LUÔN BỊ BỎ.
    ///   Chỉ cần Asset có MỘT tài khoản lạ là job treo vĩnh viễn. Đây chính là bug đang gặp.
    /// </param>
    public async Task<JobResult> SyncEodDataGeneric(
        string msg,
        string tranDate,
        string redisKeyPrefix,
        string bizType,
        Func<string, Task<(bool IsSuccess, long TotalRow, int RowsInBatch)>> executeFunc,
        string jobId = null)
    {
        var db          = _cachingService.GetCurrentDbActive();
        var timeExpired = _cachingService.GetTimeOutKeyRedis(redisKeyPrefix);

        string dateSuffix = string.IsNullOrEmpty(tranDate) ? "REALTIME" : tranDate;

        // jobId từ NGOÀI. Không có (job fee) → lấy ngày làm định danh.
        if (string.IsNullOrEmpty(jobId)) jobId = dateSuffix;

        // Scope theo JOB ⇒ Asset gửi lại với requestId MỚI = job mới, bộ đếm sạch, không dẫm job cũ.
        string p = $"{redisKeyPrefix}:{jobId}";

        try
        {
            // ═══ 1. Ghi log "job bắt đầu" ĐÚNG MỘT LẦN ══════════════════════════════════════
            //   Ai đặt được START_AT thì người đó ghi. Thua thì đi tiếp. KHÔNG AI CHỜ AI.
            if (await db.StringSetAsync($"{p}:START_AT", DateTime.UtcNow.Ticks, timeExpired, When.NotExists))
                LogJobKafkaStarting(bizType, tranDate);

            // ═══ 2. GHI DB — LUÔN CHẠY, KHÔNG PHỤ THUỘC GÌ ═════════════════════════════════
            //   CŨ: jobId rỗng → spin 100s → return, executeFunc KHÔNG được gọi
            //       ⇒ batch tới muộn thì DỮ LIỆU KHÔNG BAO GIỜ VÀO DB, mà job vẫn báo DONE.
            //   MỚI: jobId chỉ để LOG. TUYỆT ĐỐI không để nó chặn việc ghi dữ liệu.
            var (isSuccess, totalRow, rowsInBatch) = await executeFunc(msg);

            if (!isSuccess)
            {
                Log.Error("[{Biz}] {Date} job={Job} batch THẤT BẠI — consumer PHẢI không commit offset " +
                          "để Kafka giao lại (SP idempotent nên xử lý lại vô hại).", bizType, tranDate, jobId);
                return new JobResult { JobId = jobId, BatchDone = false, JobDone = false };
            }

            if (totalRow <= 0)   // không có dữ liệu cần đồng bộ
            {
                Log.Information("[{Biz}] {Date} job={Job} totalRow=0 → coi như XONG", bizType, tranDate, jobId);
                await FinishJobAsync(db, p, jobId, bizType, tranDate, 0, 0);
                return new JobResult { JobId = jobId, BatchDone = true, JobDone = true };
            }

            // ═══ 3. CỘNG DỒN CÓ DEDUP — Lua NGUYÊN TỬ, 1 round-trip ════════════════════════
            //   CŨ: INCR(TOTAL_PROCESSED, n) — PHÉP CỘNG KHÔNG IDEMPOTENT.
            //       Kafka chỉ hứa "ÍT NHẤT một lần" ⇒ giao lại ⇒ cộng 2 lần ⇒ đủ SỚM
            //       ⇒ chốt job khi batch cuối CHƯA HỀ TỚI ⇒ chain chạy trên data thiếu.
            //   MỚI: đánh dấu DANH TÍNH batch rồi mới cộng. Trùng ⇒ SADD trả 0 ⇒ KHÔNG cộng.
            string batchKey = Sha256(msg);   // danh tính NỘI DUNG — cùng batch gửi lại (offset/PID khác) → cùng khoá

            long rows = (long)await db.ScriptEvaluateAsync(LuaSumOnce,
                new RedisKey[]   { $"{p}:MSG", $"{p}:ROWS" },
                new RedisValue[] { batchKey, rowsInBatch, (long)timeExpired.TotalSeconds });

            Log.Information("[{Biz}] {Date} job={Job} +{N} dòng → {Rows}/{Total}",
                            bizType, tranDate, jobId, rowsInBatch, rows, totalRow);

            if (rows < totalRow)
                return new JobResult { JobId = jobId, BatchDone = true, JobDone = false };

            // ═══ 4. CHỐT JOB — SET NX, ai chạm mốc trước thì người đó chốt ══════════════════
            //   CŨ: END_LOCK + double-check. Không cần khoá khi mọi thao tác đã idempotent theo danh tính.
            if (await db.StringSetAsync($"{p}:DONE", "1", timeExpired, When.NotExists))
                await FinishJobAsync(db, p, jobId, bizType, tranDate, rows, totalRow);

            return new JobResult { JobId = jobId, BatchDone = true, JobDone = true };
        }
        catch (Exception ex)
        {
            Log.Error($"SyncEodDataGeneric [BizType: {bizType}] Lỗi: {ex.Message} | StackTrace: {ex.StackTrace}");
            return new JobResult { JobId = jobId, BatchDone = false, JobDone = false };
        }
    }

    private async Task FinishJobAsync(IDatabase db, string p, string jobId,
                                      string bizType, string tranDate, long rows, long totalRow)
    {
        var startTicks = await db.StringGetAsync($"{p}:START_AT");
        long totalTime = startTicks.HasValue
            ? (long)(DateTime.UtcNow - new DateTime((long)startTicks, DateTimeKind.Utc)).TotalMilliseconds
            : 0;

        updateJob(jobId, bizType, totalRow, rows, 0, totalTime, jobId);

        Log.Information("[{Biz}] {Date} job={Job} HOÀN THÀNH — {Rows}/{Total} dòng, {Ms}ms",
                        bizType, tranDate, jobId, rows, totalRow, totalTime);

        // ⚠️⚠️ TUYỆT ĐỐI KHÔNG KeyDeleteAsync Ở ĐÂY.
        //   CŨ: xoá JOB_ID / COUNTER_* / TOTAL_PROCESSED / EOD_DATE khi job xong
        //   ⇒ batch TỚI MUỘN (rebalance / retry chậm) không thấy JOB_ID → spin 100s →
        //     return BatchDone=false → executeFunc KHÔNG ĐƯỢC GỌI
        //     ⇒ DỮ LIỆU BATCH ĐÓ KHÔNG BAO GIỜ VÀO DB, mà job vẫn báo DONE.
        //   ⇒ VIỆC "DỌN DẸP" ĐÃ GIẾT DỮ LIỆU.
        //
        //   Cách dọn ĐÚNG: TTL tự hết hạn. Giữ {p}:MSG ⇒ batch trùng tới muộn KHÔNG bị đếm lại,
        //   và còn BẰNG CHỨNG để debug đúng lúc cần nhất. ~35KB/job.
    }

    private static string Sha256(string s) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(s)));

    /// <summary>
    /// CỘNG DỒN NGUYÊN TỬ. KEYS[1]=MSG(set) KEYS[2]=ROWS · ARGV[1]=batchKey ARGV[2]=rows ARGV[3]=ttl(giây)
    ///   Batch MỚI   → SADD trả 1 → INCRBY → trả tổng mới
    ///   Batch TRÙNG → SADD trả 0 → KHÔNG cộng → trả tổng hiện tại
    ///
    /// PHẢI là Lua (không tách 2 lệnh): SADD → ⚡pod chết → INCRBY không chạy
    /// ⇒ batch coi như xong mà dòng không được cộng ⇒ tổng KHÔNG BAO GIỜ đạt ⇒ TREO VĨNH VIỄN.
    /// </summary>
    private const string LuaSumOnce = @"
if redis.call('SADD', KEYS[1], ARGV[1]) == 1 then
    local n = redis.call('INCRBY', KEYS[2], ARGV[2])
    redis.call('EXPIRE', KEYS[1], ARGV[3])
    redis.call('EXPIRE', KEYS[2], ARGV[3])
    return n
else
    return tonumber(redis.call('GET', KEYS[2]) or '0')
end";
}

/* ══════════════════════════════════════════════════════════════════════════════════════════
   CALL-SITE SỬA 3 CHỖ:

   ① Job 01 — TRUYỀN jobId TỪ NGOÀI (requestId của Asset):
        var job01 = await SyncEodDataGeneric(
            msg:            rawJson,
            tranDate:       tranDate,
            redisKeyPrefix: RedisKeyDefine.EOD_ASSET,
            bizType:        BizTypeDefine.JOB_EOD_ASSET,
            executeFunc:    BulkInsertAssetToSdiCore,
            jobId:          model.requestId);            // ★ THÊM

   ② BulkInsertAssetToSdiCore — đổi giá trị thứ 3 + CỘNG SỔ LŨY KẾ NGAY TẠI ĐÂY:
        - return (true, totalRow, rowsWritten);        // ❌ @p_rows — số dòng GHI ĐƯỢC
        + // ★ Tổng số bản ghi + tổng AUM cộng Ở ĐÂY, trong hàm nghiệp vụ — KHÔNG nhét vào
        + //   script Lua của SyncEodDataGeneric: hàm đó dùng chung cho job phí (job 02/03) và
        + //   mọi luồng batch sau này, phần lớn KHÔNG có khái niệm AUM. Xem BatchStatAggregator.
        + var stat = await _stat.AccumulateAsync(
        +     RedisKeyDefine.EOD_ASSET, model.requestId,
        +     batchKey: Sha256(msg),                    // ⚠️ ĐÚNG khoá mà generic dùng để dedup
        +     rows:     model.data.Count,
        +     aum:      BatchStatAggregator.SumAum(model.data.Select(x => (decimal?)x.aum)));
        +
        + return (true, totalRow, model.data.Count);   // ✅ số dòng TRONG PAYLOAD

   ③ ProcessDataSyncAssetSdiCore — JobDone thì BẬT CỜ rồi DỪNG:
        if (!job01.JobDone) return;
        await _bo.SetSourceReady(tranDate, "ASSET_NAV", totalRow);   // ✅ SP_EOD_SET_SOURCE_READY

        - var job02 = await SyncEodDataGeneric(... ProcessJobAumFeeAcccrue ...);   // ❌ BỎ
        - var job03 = await SyncEodDataGeneric(... ProcessJobAumFeeCharge ...);    // ❌ BỎ
        // Chain trong handler = block consumer = Kafka đá pod = zombie = duplicate.
        // Chuyển sang cron: SP_FEE_RUN_DAILY @d → SP_EOD_RUN_INDEX @d → SP_EOD_RUN @d

   JOB 02/03 (fee) — KHÔNG SỬA GÌ:
        Không truyền jobId → dùng ngày làm định danh.
        Không gọi BatchStatAggregator → không có khoá STAT_* nào được tạo. Chạy y như cũ.
        ★ ĐÂY LÀ LÝ DO sổ lũy kế nằm ở hàm nghiệp vụ chứ không nằm trong SyncEodDataGeneric:
          hàm generic KHÔNG được biết đến "AUM", và job không có AUM KHÔNG phải mang theo
          tham số vô nghĩa với nó.

   ĐỌC SỔ TỪ NGOÀI (dashboard / kiểm tra tay):
        GET {redisKeyPrefix}:{requestId}:STAT_ROWS   → tổng số bản ghi đã nhận
        GET {redisKeyPrefix}:{requestId}:STAT_AUM    → tổng AUM đã nhận (VND)
   Đối chiếu: SELECT COUNT(*), SUM(C_AUM) FROM T_SI_BALANCE WHERE C_BUSINESS_DATE = @d
        STAT_ROWS >= COUNT(*) là BÌNH THƯỜNG (acc không thuộc SDI bị INNER JOIN registry lọc).
        STAT_AUM − SUM(C_AUM) = ĐÚNG tổng aum của đám acc lạ đó (có thể ÂM nếu acc lạ có aum<0
        — dấu không nói lên gì, chỉ ĐỘ LỚN mới nói). Lệch khác con số đó ⇒ có chuyện.
   ══════════════════════════════════════════════════════════════════════════════════════════ */
