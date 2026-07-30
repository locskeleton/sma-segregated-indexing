using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace SdiCoreMessagingProcess.Services;

/// <summary>
/// Khoá Redis + script Lua cho luồng đồng bộ batch từ Kafka.
///
/// SCOPE = requestId (Asset gửi, CHUNG cho mọi batch của 1 job).
///   ⇒ Không cần "message đầu tiên" tạo JOB_ID. Không cần lock. Không cần chờ.
///   ⇒ Asset gửi lại cả ngày với requestId MỚI = job mới, bộ đếm sạch, không dẫm lên job cũ.
/// </summary>
public static class KafkaSyncKeys
{
    /// <summary>Scope của 1 job: {redisKeyPrefix}:{requestId}</summary>
    public static string Scope(string redisKeyPrefix, string requestId) => $"{redisKeyPrefix}:{requestId}";

    public static string Msg(string p)    => $"{p}:MSG";      // SET các batchKey đã xử lý (dedup)
    public static string Rows(string p)   => $"{p}:ROWS";     // tổng số DÒNG đã nhận (cộng dồn CÓ dedup)
    public static string Total(string p)  => $"{p}:TOTAL";    // totalRow Asset khai — GỢI Ý, không phải cổng chặn
    public static string Date(string p)   => $"{p}:DATE";     // tranDate (cho watchdog)
    public static string LastAt(string p) => $"{p}:LAST_AT";  // tick message cuối (timeout fallback)
    public static string Ready(string p)  => $"{p}:READY";    // "1" — chống gọi SP lặp

    /// <summary>SET các requestId đang chạy của 1 bizType — để watchdog quét (không dùng KEYS/SCAN).</summary>
    public static string ActiveJobs(string redisKeyPrefix) => $"{redisKeyPrefix}:JOBS";

    /// <summary>
    /// DANH TÍNH CỦA BATCH = băm TẬP si_account (đã sắp xếp) mà batch này mang theo.
    ///
    /// ⚠️ KHÔNG băm chuỗi JSON thô: chỉ cần Asset đổi thứ tự field, đổi format số, hay nhét thêm
    ///    timestamp/traceId vào payload là hash ĐỔI ⇒ dedup mù ⇒ cộng dồn 2 lần.
    ///    Băm DANH TÍNH NGHIỆP VỤ thì bất biến với mọi thay đổi hình thức.
    ///
    /// ⚠️ KHÔNG dùng (partition, offset): đó là danh tính của CÁI PHONG BÌ.
    ///    enable.idempotence=true (bên Asset — TA KHÔNG KIỂM SOÁT) chỉ chặn trùng trong CÙNG một
    ///    phiên producer: sống sót qua leader failover, nhưng KHÔNG sống sót qua producer RESTART
    ///    (redeploy/OOM/evict → PID mới) ⇒ cùng nội dung nằm ở OFFSET KHÁC ⇒ dedup theo offset mù.
    ///
    /// Vì sao tập si_account là danh tính đúng:
    ///   • Asset gửi LẠI cùng batch (kể cả đã SỬA giá trị) → cùng tập tài khoản → cùng khoá
    ///     → KHÔNG cộng dồn 2 lần.  (Dữ liệu SỬA vẫn được ghi: executeFunc chạy TRƯỚC dedup.)
    ///   • Batch khác → tập tài khoản khác → khoá khác → cộng bình thường.
    /// </summary>
    public static string BatchKey(IEnumerable<string> siAccounts)
    {
        var canonical = string.Join(",", siAccounts.Select(s => s.Trim().ToUpperInvariant())
                                                   .OrderBy(s => s, StringComparer.Ordinal));
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(canonical));
        return Convert.ToHexString(bytes);   // 64 ký tự
    }

    /// <summary>
    /// CỘNG DỒN NGUYÊN TỬ — 1 round-trip.
    ///
    /// Vì sao PHẢI là Lua (không tách 2 lệnh):
    ///     SADD  {MSG} batchKey     → đánh dấu đã xử lý
    ///     ⚡ pod chết ĐÚNG ở đây
    ///     INCRBY {ROWS} n          → KHÔNG BAO GIỜ CHẠY
    ///     ⇒ message coi như xong nhưng n dòng không được cộng ⇒ TREO VĨNH VIỄN.
    /// Redis chạy Lua nguyên tử ⇒ không có cửa sổ ghi-dở.
    ///
    /// KEYS[1]=MSG  KEYS[2]=ROWS   ARGV[1]=batchKey  ARGV[2]=rowsInBatch  ARGV[3]=ttlSeconds
    /// Trả về: tổng số dòng hiện tại của job.
    /// </summary>
    public const string LuaSumOnce = @"
if redis.call('SADD', KEYS[1], ARGV[1]) == 1 then
    local n = redis.call('INCRBY', KEYS[2], ARGV[2])
    redis.call('EXPIRE', KEYS[1], ARGV[3])
    redis.call('EXPIRE', KEYS[2], ARGV[3])
    return n
else
    return tonumber(redis.call('GET', KEYS[2]) or '0')
end";
}
