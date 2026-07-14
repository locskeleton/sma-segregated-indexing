using System;
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
    /// DANH TÍNH CỦA BATCH = băm phần DỮ LIỆU.
    ///
    /// ⚠️ VÌ SAO KHÔNG DÙNG (partition, offset):
    ///   enable.idempotence=true chỉ chặn trùng khi retry trong CÙNG một phiên producer.
    ///   Nó SỐNG SÓT qua leader-partition failover (producer state nằm trong log của partition),
    ///   nhưng KHÔNG sống sót qua producer RESTART (redeploy / OOM / evict → PID mới, broker
    ///   không nhận ra sequence cũ) ⇒ cùng nội dung nằm ở OFFSET KHÁC ⇒ dedup theo offset MÙ.
    ///   Băm nội dung thì miễn nhiễm: cùng data → cùng khoá, bất kể offset nào.
    ///
    /// ⚠️ CHỈ băm phần `data`. KHÔNG băm requestId/timestamp/envelope — nếu Asset sinh lại
    ///    những trường đó khi gửi lại thì hash sẽ đổi và dedup mất tác dụng.
    /// </summary>
    public static string BatchKey(string dataJson)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(dataJson));
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
