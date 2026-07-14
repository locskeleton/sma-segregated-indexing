using System;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Serilog;

namespace SdiCoreMessagingProcess.Services;

/// <summary>
/// ★ BẢN DROP-IN — thay THẲNG method cũ trong BaseService&lt;T&gt;.
///   Giữ NGUYÊN chữ ký, NGUYÊN kiểu trả về JobResult. Call-site chỉ sửa 2 chỗ (xem cuối file).
/// </summary>
public partial class BaseService<T>
{
    /// <summary>
    /// Hàm callback nhận json msg thô, trả về: (Thành công, Tổng record EOD, SỐ RECORD TRONG BATCH)
    ///
    /// ⚠️⚠️ ĐỔI NGHĨA THAM SỐ THỨ 3 — ĐÂY LÀ SỬA QUAN TRỌNG NHẤT:
    ///     CŨ: RowsProcessed = @p_rows của SP = SỐ DÒNG GHI ĐƯỢC
    ///     MỚI: RowsInBatch  = model.data.Count = SỐ DÒNG TRONG PAYLOAD
    ///
    ///     VÌ SAO: SP_INGEST_ASSET_NAV có INNER JOIN registry ⇒ acc KHÔNG thuộc SDI bị LỌC BỎ
    ///     ⇒ @p_rows &lt; số item Asset gửi. Còn totalRow là số Asset GỬI ĐI.
    ///     ⇒ Σ(@p_rows) KHÔNG BAO GIỜ ĐẠT totalRow ⇒ JobDone luôn false ⇒ CHAIN LUÔN BỊ BỎ.
    ///     Đây chính là bug "job check finish không thành công".
    /// </summary>
    public async Task<JobResult> SyncEodDataGeneric(
        string msg,
        string tranDate,
        string redisKeyPrefix,
        string bizType,
        Func<string, Task<(bool IsSuccess, long TotalRow, int RowsInBatch)>> executeFunc)
    {
        var db          = _cachingService.GetCurrentDbActive();
        var timeExpired = _cachingService.GetTimeOutKeyRedis(redisKeyPrefix);

        string dateSuffix   = string.IsNullOrEmpty(tranDate) ? "REALTIME" : tranDate;
        string scopedPrefix = $"{redisKeyPrefix}:{dateSuffix}";
        string jobId        = "";

        try
        {
            // ═══ 1. JOB_ID — KHÔNG lock, KHÔNG counter==1, KHÔNG chờ ═════════════════════════
            //   CŨ: counter = INCR(COUNTER); if (counter == 1) { LockTake(5s) → tạo JOB_ID }
            //       → ĐIỂM CHẾT ĐƠN: pod INCR lên 1 rồi CHẾT trước khi ghi JOB_ID
            //         ⇒ KHÔNG AI còn thấy counter==1 ⇒ mọi message sau đều mồ côi ⇒ JOB CHẾT VĨNH VIỄN.
            //       → LockTake không có else: lấy hụt lock ⇒ im lặng bỏ qua ⇒ cũng chết như trên.
            //   MỚI: SET NX — ai đặt được placeholder thì người đó tạo. Thua thì đọc. KHÔNG AI CHỜ AI.
            string jobIdKey = $"{scopedPrefix}:JOB_ID";

            if (await db.StringSetAsync(jobIdKey, "CREATING", timeExpired, When.NotExists))
            {
                jobId = string.IsNullOrEmpty(tranDate)
                        ? LogJobKafkaStarting(bizType)
                        : LogJobKafkaStarting(bizType, tranDate);
                await db.StringSetAsync(jobIdKey, jobId, timeExpired);
            }
            else
            {
                jobId = await db.StringGetAsync(jobIdKey);
                if (jobId == "CREATING") jobId = "";   // người kia chưa ghi xong → KỆ, đi tiếp
            }

            // ═══ 2. GHI DB — LUÔN CHẠY ══════════════════════════════════════════════════════
            //   CŨ: JOB_ID rỗng → spin 1000×100ms = 100 GIÂY → return, KHÔNG gọi executeFunc
            //       ⇒ block consumer 100s ⇒ vượt max.poll.interval ⇒ KAFKA ĐÁ POD
            //         (nhưng KHÔNG giết thread! pod cũ thành ZOMBIE, vẫn ghi DB)
            //       ⇒ pod mới xử lý LẠI cùng message ⇒ cộng dồn 2 lần ⇒ chốt job SỚM khi còn thiếu batch.
            //       ⇒ Và batch tới muộn (sau khi key bị xoá) thì DỮ LIỆU KHÔNG BAO GIỜ VÀO DB.
            //   MỚI: jobId chỉ để LOG. KHÔNG BAO GIỜ để nó chặn việc ghi dữ liệu. Bỏ hẳn vòng chờ.
            var (isSuccess, totalRow, rowsInBatch) = await executeFunc(msg);

            if (!isSuccess)
            {
                Log.Error("[{Biz}] {Date} batch xử lý THẤT BẠI — KHÔNG commit offset để Kafka giao lại. " +
                          "(SP idempotent nên xử lý lại là vô hại.)", bizType, tranDate);
                // ⚠️ Consumer PHẢI ném / không commit offset ở đây. Nuốt lỗi = MẤT DỮ LIỆU.
                return new JobResult { JobId = jobId, BatchDone = false, JobDone = false };
            }

            // Không có dữ liệu (totalRow = 0) → coi như xong luôn
            if (totalRow <= 0)
            {
                Log.Information("[{Biz}] {Date} không có dữ liệu cần đồng bộ (totalRow=0) → coi như XONG", bizType, tranDate);
                await FinishJobAsync(db, scopedPrefix, jobId, bizType, tranDate, 0, 0, timeExpired);
                return new JobResult { JobId = jobId, BatchDone = true, JobDone = true };
            }

            // ═══ 3. CỘNG DỒN CÓ DEDUP — Lua NGUYÊN TỬ, 1 round-trip ═════════════════════════
            //   CŨ: INCR(TOTAL_PROCESSED, numProcessed) — PHÉP CỘNG KHÔNG IDEMPOTENT.
            //       Kafka chỉ hứa "ÍT NHẤT một lần" ⇒ message giao lại ⇒ cộng 2 lần
            //       ⇒ đủ SỚM ⇒ chốt job khi batch cuối CHƯA HỀ TỚI ⇒ chain chạy trên data thiếu.
            //   MỚI: đánh dấu DANH TÍNH batch rồi mới cộng. Trùng ⇒ SADD trả 0 ⇒ KHÔNG cộng.
            //
            //   Vì sao PHẢI là Lua (không tách 2 lệnh):
            //       SADD → ⚡pod chết → INCRBY không chạy ⇒ batch coi như xong mà dòng không được cộng
            //       ⇒ tổng KHÔNG BAO GIỜ đạt ⇒ TREO VĨNH VIỄN.
            string batchKey = Sha256(msg);   // danh tính NỘI DUNG. Cùng batch gửi lại (offset/PID khác) → cùng khoá.

            long rows = (long)await db.ScriptEvaluateAsync(LuaSumOnce,
                new RedisKey[]   { $"{scopedPrefix}:MSG", $"{scopedPrefix}:ROWS" },
                new RedisValue[] { batchKey, rowsInBatch, (long)timeExpired.TotalSeconds });

            await db.StringSetAsync($"{scopedPrefix}:LAST_AT", DateTime.UtcNow.Ticks, timeExpired);

            Log.Information("[{Biz}] {Date} batch OK: +{N} dòng → {Rows}/{Total}",
                            bizType, tranDate, rowsInBatch, rows, totalRow);

            // ═══ 4. ĐỦ CHƯA? ═══════════════════════════════════════════════════════════════
            //   CŨ: END_LOCK + double-check + JobDone là bool trong RAM của ĐÚNG MỘT message may mắn.
            //       Message đó lỗi/trùng/pod chết ⇒ SỰ KIỆN MẤT LUÔN ⇒ "bỏ qua chain phía sau".
            //   MỚI: SET NX — ai chạm mốc trước thì người đó chốt. Idempotent, không cần lock.
            if (rows < totalRow)
                return new JobResult { JobId = jobId, BatchDone = true, JobDone = false };

            bool iAmTheOne = await db.StringSetAsync($"{scopedPrefix}:DONE", "1", timeExpired, When.NotExists);
            if (iAmTheOne)
                await FinishJobAsync(db, scopedPrefix, jobId, bizType, tranDate, rows, totalRow, timeExpired);

            return new JobResult { JobId = jobId, BatchDone = true, JobDone = true };
        }
        catch (Exception ex)
        {
            Log.Error($"SyncEodDataGeneric [BizType: {bizType}] Lỗi: {ex.Message} | StackTrace: {ex.StackTrace}");
            return new JobResult { JobId = jobId, BatchDone = false, JobDone = false };
        }
    }

    /// <summary>Ghi nhận job xong. ⚠️ KHÔNG XOÁ KEY — xem lý do bên dưới.</summary>
    private async Task FinishJobAsync(IDatabase db, string scopedPrefix, string jobId,
                                      string bizType, string tranDate, long rows, long totalRow,
                                      TimeSpan timeExpired)
    {
        var startTicks = await db.StringGetAsync($"{scopedPrefix}:START_AT");
        long totalTime = startTicks.HasValue
            ? (long)(DateTime.UtcNow - new DateTime((long)startTicks, DateTimeKind.Utc)).TotalMilliseconds
            : 0;

        if (!string.IsNullOrEmpty(jobId))
            updateJob(jobId, bizType, totalRow, rows, 0, totalTime, jobId);

        Log.Information("[{Biz}] {Date} HOÀN THÀNH — {Rows}/{Total} dòng, {Ms}ms",
                        bizType, tranDate, rows, totalRow, totalTime);

        // ⚠️⚠️ TUYỆT ĐỐI KHÔNG KeyDelete Ở ĐÂY.
        //   CŨ: db.KeyDeleteAsync(JOB_ID, COUNTER_SUCCESS, COUNTER_FAIL, TOTAL_PROCESSED, EOD_DATE, ...)
        //   ⇒ Batch TỚI MUỘN (rebalance / retry chậm) vào hàm, không thấy JOB_ID → spin 100s →
        //     return BatchDone=false → executeFunc KHÔNG ĐƯỢC GỌI
        //     ⇒ DỮ LIỆU BATCH ĐÓ KHÔNG BAO GIỜ VÀO DB, mà job vẫn báo DONE.
        //   ⇒ VIỆC "DỌN DẸP" ĐÃ GIẾT DỮ LIỆU.
        //
        //   Cách dọn ĐÚNG: để TTL tự hết hạn. Giữ {scopedPrefix}:MSG thì batch trùng tới muộn
        //   KHÔNG bị đếm lại, và còn BẰNG CHỨNG để debug đúng lúc cần nhất. ~35KB/job.
    }

    /// <summary>Danh tính NỘI DUNG của batch. Cùng nội dung → cùng khoá, bất kể offset/PID/broker nào.</summary>
    private static string Sha256(string s) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(s)));

    /// <summary>
    /// CỘNG DỒN NGUYÊN TỬ. KEYS[1]=MSG(set) KEYS[2]=ROWS  ARGV[1]=batchKey ARGV[2]=rows ARGV[3]=ttl(s)
    /// Batch MỚI  → SADD trả 1 → INCRBY → trả tổng mới.
    /// Batch TRÙNG → SADD trả 0 → KHÔNG cộng → trả tổng hiện tại.
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
   CALL-SITE CHỈ SỬA 2 CHỖ:

   ① BulkInsertAssetToSdiCore — đổi giá trị thứ 3 trả về:
          - return (true, totalRow, rowsWritten);        // ❌ @p_rows — số dòng GHI ĐƯỢC
          + return (true, totalRow, model.data.Count);   // ✅ số dòng TRONG PAYLOAD

   ② ProcessDataSyncAssetSdiCore — sau khi JobDone thì BẬT CỜ, KHÔNG chain:
          var job01 = await SyncEodDataGeneric(rawJson, tranDate, RedisKeyDefine.EOD_ASSET,
                                               BizTypeDefine.JOB_EOD_ASSET, BulkInsertAssetToSdiCore);
          if (!job01.JobDone) return;

          await _bo.SetSourceReady(tranDate, "ASSET_NAV", totalRow);   // ✅ SP_EOD_SET_SOURCE_READY

          - var job02 = await SyncEodDataGeneric(... ProcessJobAumFeeAcccrue ...);   // ❌ BỎ
          - var job03 = await SyncEodDataGeneric(... ProcessJobAumFeeCharge ...);    // ❌ BỎ
          // → Chain chạy TRONG handler = block consumer = bị Kafka đá = zombie = duplicate.
          //   Chuyển sang cron/scheduler gọi SP_FEE_RUN_DAILY + SP_EOD_RUN.

   JOB 02/03 (fee) DÙNG NGUYÊN HÀM NÀY KHÔNG SỬA GÌ:
          executeFunc trả (true, 1, 1) → totalRow=1, rows=1 → 1>=1 → JobDone=true. Chạy y như cũ.
   ══════════════════════════════════════════════════════════════════════════════════════════ */
