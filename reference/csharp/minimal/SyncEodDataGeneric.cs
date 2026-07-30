using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Serilog;
using StackExchange.Redis;

namespace SdiCoreMessagingProcess.Services;

/// <summary>
/// ★ BẢN DROP-IN — thay THẲNG method cũ trong BaseService&lt;T&gt;.
///   Giữ NGUYÊN kiểu trả về JobResult. Chỉ THÊM 2 tham số optional: jobId, aumInBatch.
///   Job không truyền gì thêm (fee accrue/charge) chạy y như cũ — cả 2 đều có default.
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
    /// <param name="aumInBatch">
    ///   ★ Tổng AUM (VND) của batch này — tính tại call-site bằng <c>SumAum(model.data.Select(x =&gt; x.aum))</c>.
    ///   Redis cộng dồn để lúc nào cũng có sẵn "tổng dòng + tổng tiền" đã nhận của job.
    ///   <c>null</c> (mặc định) = job không có khái niệm AUM (fee accrue/charge) ⇒ KHÔNG tạo key AUM.
    ///
    ///   ⚠️ SỐ ĐỂ QUAN SÁT/ĐỐI SOÁT, KHÔNG phải cổng chặn. Cổng chặn vẫn chỉ là số DÒNG
    ///   (<c>rows &gt;= totalRow</c>) và cửa khoá thật vẫn là <c>SP_EOD_RUN</c> err=12.
    ///   Sự thật về tiền là <c>SUM(C_AUM) FROM T_SI_BALANCE @d</c>.
    /// </param>
    public async Task<JobResult> SyncEodDataGeneric(
        string msg,
        string tranDate,
        string redisKeyPrefix,
        string bizType,
        Func<string, Task<(bool IsSuccess, long TotalRow, int RowsInBatch)>> executeFunc,
        string jobId = null,
        long?  aumInBatch = null)
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
                await FinishJobAsync(db, p, jobId, bizType, tranDate, 0, 0, 0);
                return new JobResult { JobId = jobId, BatchDone = true, JobDone = true };
            }

            // ═══ 3. CỘNG DỒN CÓ DEDUP — Lua NGUYÊN TỬ, 1 round-trip ════════════════════════
            //   CŨ: INCR(TOTAL_PROCESSED, n) — PHÉP CỘNG KHÔNG IDEMPOTENT.
            //       Kafka chỉ hứa "ÍT NHẤT một lần" ⇒ giao lại ⇒ cộng 2 lần ⇒ đủ SỚM
            //       ⇒ chốt job khi batch cuối CHƯA HỀ TỚI ⇒ chain chạy trên data thiếu.
            //   MỚI: đánh dấu DANH TÍNH batch rồi mới cộng. Trùng ⇒ SADD trả 0 ⇒ KHÔNG cộng.
            //   ★ CÙNG script này cộng luôn TỔNG AUM (nếu job có AUM) — 1 round-trip, 1 lát cắt
            //     nguyên tử ⇒ "tổng dòng" và "tổng tiền" của job KHÔNG BAO GIỜ lệch pha nhau.
            string batchKey = Sha256(msg);   // danh tính NỘI DUNG — cùng batch gửi lại (offset/PID khác) → cùng khoá

            var res = (RedisValue[])await db.ScriptEvaluateAsync(LuaSumOnce,
                new RedisKey[]   { $"{p}:MSG", $"{p}:ROWS", $"{p}:AUM", $"{p}:BATCH_AUM" },
                new RedisValue[] { batchKey, rowsInBatch,
                                   // ⚠️ ĐỪNG dùng long.ToString(): phụ thuộc CurrentCulture. Ép sang
                                   //    RedisValue để thư viện format bất biến. '' = không theo dõi AUM.
                                   aumInBatch.HasValue ? (RedisValue)aumInBatch.Value : RedisValue.EmptyString,
                                   (long)timeExpired.TotalSeconds });

            long rows = (long)res[0];
            long aum  = (long)res[1];

            // Sổ lũy kế SAU MỖI BATCH — nhìn 1 dòng log là biết job đang chảy tới đâu, cả dòng lẫn tiền.
            Log.Information("[{Biz}] {Date} job={Job} +{N} dòng / +{AumN} AUM → LŨY KẾ {Rows}/{Total} dòng, AUM {Aum}",
                            bizType, tranDate, jobId, rowsInBatch, aumInBatch, rows, totalRow, aum);

            if (rows < totalRow)
                return new JobResult { JobId = jobId, BatchDone = true, JobDone = false };

            // ═══ 4. CHỐT JOB — SET NX, ai chạm mốc trước thì người đó chốt ══════════════════
            //   CŨ: END_LOCK + double-check. Không cần khoá khi mọi thao tác đã idempotent theo danh tính.
            if (await db.StringSetAsync($"{p}:DONE", "1", timeExpired, When.NotExists))
                await FinishJobAsync(db, p, jobId, bizType, tranDate, rows, totalRow, aum);

            return new JobResult { JobId = jobId, BatchDone = true, JobDone = true };
        }
        catch (Exception ex)
        {
            Log.Error($"SyncEodDataGeneric [BizType: {bizType}] Lỗi: {ex.Message} | StackTrace: {ex.StackTrace}");
            return new JobResult { JobId = jobId, BatchDone = false, JobDone = false };
        }
    }

    private async Task FinishJobAsync(IDatabase db, string p, string jobId,
                                      string bizType, string tranDate, long rows, long totalRow, long aum)
    {
        var startTicks = await db.StringGetAsync($"{p}:START_AT");
        long totalTime = startTicks.HasValue
            ? (long)(DateTime.UtcNow - new DateTime((long)startTicks, DateTimeKind.Utc)).TotalMilliseconds
            : 0;

        updateJob(jobId, bizType, totalRow, rows, 0, totalTime, jobId);

        // ★ Chốt sổ có TIỀN: so tổng AUM này với SUM(C_AUM) FROM T_SI_BALANCE @d là bắt được ngay
        //   "đủ dòng nhưng lệch tiền" — thứ mà đếm dòng KHÔNG BAO GIỜ thấy.
        Log.Information("[{Biz}] {Date} job={Job} HOÀN THÀNH — {Rows}/{Total} dòng, tổng AUM {Aum}, {Ms}ms",
                        bizType, tranDate, jobId, rows, totalRow, aum, totalTime);

        // ⚠️⚠️ TUYỆT ĐỐI KHÔNG KeyDeleteAsync Ở ĐÂY.
        //   CŨ: xoá JOB_ID / COUNTER_* / TOTAL_PROCESSED / EOD_DATE khi job xong
        //   ⇒ batch TỚI MUỘN (rebalance / retry chậm) không thấy JOB_ID → spin 100s →
        //     return BatchDone=false → executeFunc KHÔNG ĐƯỢC GỌI
        //     ⇒ DỮ LIỆU BATCH ĐÓ KHÔNG BAO GIỜ VÀO DB, mà job vẫn báo DONE.
        //   ⇒ VIỆC "DỌN DẸP" ĐÃ GIẾT DỮ LIỆU.
        //
        //   Cách dọn ĐÚNG: TTL tự hết hạn. Giữ {p}:MSG ⇒ batch trùng tới muộn KHÔNG bị đếm lại;
        //   giữ {p}:ROWS + {p}:AUM ⇒ còn BẰNG CHỨNG (đủ dòng chưa VÀ khớp tiền không) đúng lúc cần nhất.
        //   ~75KB/job (MSG set ≈35KB + BATCH_AUM hash ≈40KB).
    }

    private static string Sha256(string s) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(s)));

    /// <summary>
    /// TỔNG AUM CỦA MỘT BATCH — làm tròn ĐÚNG NHƯ DB (<c>DECIMAL(20,0)</c> ⇒ tròn TỪNG DÒNG,
    /// half away from zero). Cộng thập phân rồi mới tròn một lần sẽ lệch DB vài đồng — đúng loại
    /// chênh vô nghĩa mà lúc đối soát tốn cả buổi để truy.
    /// Trả <c>null</c> nếu vượt tầm int64 ⇒ caller log ERROR, BỎ theo dõi AUM (KHÔNG làm treo ingest).
    /// </summary>
    public static long? SumAum(IEnumerable<decimal?> aums)
    {
        decimal sum = 0m;
        foreach (var a in aums)
        {
            if (a is null) continue;
            sum += Math.Round(a.Value, 0, MidpointRounding.AwayFromZero);
        }
        return sum > long.MaxValue || sum < long.MinValue ? null : (long)sum;
    }

    /// <summary>
    /// CỘNG DỒN NGUYÊN TỬ — cộng CẢ số dòng LẪN tổng AUM trong một lát cắt.
    /// KEYS[1]=MSG(set) KEYS[2]=ROWS KEYS[3]=AUM KEYS[4]=BATCH_AUM(hash)
    /// ARGV[1]=batchKey ARGV[2]=rows ARGV[3]=aum ('' = job không có AUM) ARGV[4]=ttl(giây)
    ///
    /// PHẢI là Lua (không tách nhiều lệnh): SADD → ⚡pod chết → INCRBY không chạy
    /// ⇒ batch coi như xong mà dòng không được cộng ⇒ tổng KHÔNG BAO GIỜ đạt ⇒ TREO VĨNH VIỄN.
    ///
    /// ── HAI PHÉP CỘNG, HAI LUẬT — ĐỪNG TRỘN ──────────────────────────────────────────────
    /// • ROWS (cổng chặn): dedup THUẦN. Batch MỚI → SADD trả 1 → cộng. TRÙNG → trả 0 → KHÔNG cộng.
    ///   Giữ NGUYÊN như cũ vì toàn bộ lập luận "rows >= totalRow" dựa vào nó.
    /// • AUM (số quan sát): cộng theo DELTA — nhớ AUM của từng batchKey trong HASH {BATCH_AUM},
    ///   gặp lại chính batchKey đó thì cộng phần CHÊNH (mới − cũ).
    ///   ⇒ Asset gửi lại batch ĐÃ SỬA giá trị (luồng re-ingest chính thức, SP idempotent DELETE+INSERT)
    ///     → DB ra số mới → AUM Redis CŨNG ra số mới. Dedup thuần thì AUM đứng ở số CŨ, tức là sai
    ///     đúng vào lúc người ta cần nó nhất: vừa sửa dữ liệu xong, đang ngồi đối soát.
    ///   Gửi lại y nguyên → delta = 0 → vô hại.
    ///
    /// ⚠️ RIÊNG BẢN DROP-IN NÀY: batchKey = Sha256(TOÀN BỘ msg), không phải băm tập si_account.
    ///    Batch gửi lại ĐÃ SỬA ⇒ msg khác ⇒ khoá KHÁC ⇒ SADD trả 1 ⇒ cả ROWS lẫn AUM đều cộng THÊM
    ///    (không trừ số cũ). Đây là hạn chế SẴN CÓ của bản drop-in (đổi 1 ký tự trong payload là dedup mù),
    ///    KHÔNG phải do phần AUM sinh ra — AUM chỉ đi theo đúng luật mà ROWS đang chạy.
    ///    Muốn có tính chất "sửa số ra đúng số mới" thì dùng BatchSyncService (băm tập si_account).
    ///
    /// TTL được gia hạn ở MỌI lượt gọi (kể cả batch trùng), không chỉ lượt đầu: job còn message chảy vào
    /// thì bằng chứng còn sống. Trước đây chỉ gia hạn khi batch mới — không sai, nhưng chặt hơn mức cần.
    ///
    /// TRẢ VỀ CHUỖI (GET) chứ không trả số: số trong Lua là double (chỉ chính xác tới 2^53 ≈ 9,0e15),
    /// mà tổng AUM tính bằng ĐỒNG có thể tới 1e14–1e15. Đi qua GET thì tổng do INCRBY tính bằng int64
    /// trong Redis, C# parse lại từ chuỗi ⇒ chính xác tuyệt đối. Chỉ DELTA (≤ AUM 1 batch) qua double.
    /// </summary>
    private const string LuaSumOnce = @"
local isNew = redis.call('SADD', KEYS[1], ARGV[1])
if isNew == 1 then
    redis.call('INCRBY', KEYS[2], ARGV[2])
end

if ARGV[3] ~= '' then
    local prev = redis.call('HGET', KEYS[4], ARGV[1]) or '0'
    redis.call('INCRBY', KEYS[3], string.format('%d', tonumber(ARGV[3]) - tonumber(prev)))
    redis.call('HSET',   KEYS[4], ARGV[1], ARGV[3])
end

redis.call('EXPIRE', KEYS[1], ARGV[4])
redis.call('EXPIRE', KEYS[2], ARGV[4])
redis.call('EXPIRE', KEYS[3], ARGV[4])
redis.call('EXPIRE', KEYS[4], ARGV[4])

return { redis.call('GET', KEYS[2]) or '0', redis.call('GET', KEYS[3]) or '0' }";
}

/* ══════════════════════════════════════════════════════════════════════════════════════════
   CALL-SITE SỬA 3 CHỖ:

   ① Job 01 — TRUYỀN jobId TỪ NGOÀI (requestId của Asset) + TỔNG AUM CỦA BATCH:
        // ⚠️ ép (decimal?): IEnumerable<decimal> KHÔNG tự chuyển sang IEnumerable<decimal?> (kiểu giá trị
        //    không có covariance). Ép thế này thì aum khai decimal hay decimal? đều biên dịch được.
        var aumInBatch = SumAum(model.data.Select(x => (decimal?)x.aum));   // ★ THÊM — tròn TỪNG DÒNG như DECIMAL(20,0)
        if (aumInBatch is null)
            Log.Error("[{Biz}] {Date} req={Req} — tổng AUM batch VƯỢT TẦM int64 ({N} dòng). " +
                      "Batch VẪN ghi DB + đếm dòng bình thường, chỉ BỎ cộng AUM.",
                      BizTypeDefine.JOB_EOD_ASSET, tranDate, model.requestId, model.data.Count);

        var job01 = await SyncEodDataGeneric(
            msg:            rawJson,
            tranDate:       tranDate,
            redisKeyPrefix: RedisKeyDefine.EOD_ASSET,
            bizType:        BizTypeDefine.JOB_EOD_ASSET,
            executeFunc:    BulkInsertAssetToSdiCore,
            jobId:          model.requestId,             // ★ THÊM
            aumInBatch:     aumInBatch);                 // ★ THÊM

   ② BulkInsertAssetToSdiCore — đổi giá trị thứ 3:
        - return (true, totalRow, rowsWritten);        // ❌ @p_rows — số dòng GHI ĐƯỢC
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
        Không truyền aumInBatch → null → KHÔNG tạo key {p}:AUM / {p}:BATCH_AUM (job phí không có AUM batch).
        executeFunc trả (true, 1, 1) → 1>=1 → JobDone=true. Chạy y như cũ.

   ĐỌC SỐ LŨY KẾ TỪ NGOÀI (dashboard / lệnh kiểm tra thủ công):
        GET {redisKeyPrefix}:{requestId}:ROWS     → tổng số bản ghi đã nhận
        GET {redisKeyPrefix}:{requestId}:AUM      → tổng AUM đã nhận (VND)
        GET {redisKeyPrefix}:{requestId}:TOTAL    → totalRow Asset khai
   Đối chiếu:  SELECT COUNT(*), SUM(C_AUM) FROM T_SI_BALANCE WHERE C_BUSINESS_DATE = @d
        ROWS >= COUNT(*) là BÌNH THƯỜNG (acc không thuộc SDI bị INNER JOIN registry lọc).
        AUM − SUM(C_AUM) = ĐÚNG tổng aum của đám acc lạ đó (có thể ÂM nếu acc lạ có aum<0 —
        dấu không nói lên gì, chỉ ĐỘ LỚN mới nói). Lệch khác con số đó ⇒ có chuyện.
   ══════════════════════════════════════════════════════════════════════════════════════════ */
