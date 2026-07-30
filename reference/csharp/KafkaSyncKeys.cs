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
///
/// ⚠️ REDIS CLUSTER: 4 khoá dưới đây đi CHUNG một script Lua ⇒ phải cùng slot.
///    Bản này chạy trên Redis standalone/sentinel (không cluster) nên không cần hash-tag.
///    Nếu về sau chuyển sang cluster: đổi <see cref="Scope"/> thành "{prefix:requestId}" (có ngoặc nhọn)
///    để cả 4 khoá cùng slot — KHÔNG được tách script thành nhiều lệnh rời (mất tính nguyên tử).
/// </summary>
public static class KafkaSyncKeys
{
    /// <summary>Scope của 1 job: {redisKeyPrefix}:{requestId}</summary>
    public static string Scope(string redisKeyPrefix, string requestId) => $"{redisKeyPrefix}:{requestId}";

    public static string Msg(string p)      => $"{p}:MSG";        // SET các batchKey đã xử lý (dedup)
    public static string Rows(string p)     => $"{p}:ROWS";       // tổng số DÒNG đã nhận (cộng dồn CÓ dedup)
    public static string Aum(string p)      => $"{p}:AUM";        // ★ tổng AUM (VND) đã nhận — cộng dồn CÓ dedup + CÓ SỬA
    public static string BatchAum(string p) => $"{p}:BATCH_AUM";  // ★ HASH batchKey → AUM của batch đó (để sửa số khi gửi lại)
    public static string Total(string p)    => $"{p}:TOTAL";      // totalRow Asset khai — GỢI Ý, không phải cổng chặn
    public static string Date(string p)     => $"{p}:DATE";       // tranDate (cho watchdog)
    public static string LastAt(string p)   => $"{p}:LAST_AT";    // tick message cuối (timeout fallback)
    public static string Ready(string p)    => $"{p}:READY";      // "1" — chống gọi SP lặp

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
    ///
    /// ★ Chính đặc tính "gửi lại (đã SỬA) → CÙNG khoá" là lý do <see cref="BatchAum"/> phải tồn tại:
    ///   số DÒNG không đổi khi sửa giá trị, nhưng AUM thì ĐỔI. Xem <see cref="LuaSumOnce"/>.
    /// </summary>
    public static string BatchKey(IEnumerable<string> siAccounts)
    {
        var canonical = string.Join(",", siAccounts.Select(s => s.Trim().ToUpperInvariant())
                                                   .OrderBy(s => s, StringComparer.Ordinal));
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(canonical));
        return Convert.ToHexString(bytes);   // 64 ký tự
    }

    /// <summary>
    /// TỔNG AUM CỦA MỘT BATCH — làm tròn ĐÚNG NHƯ DB.
    ///
    /// <c>T_SI_BALANCE.C_AUM</c> là <c>DECIMAL(20,0)</c> ⇒ SQL Server làm tròn TỪNG DÒNG (half away from zero)
    /// khi ghi. Nếu ở đây cộng dồn thập phân rồi mới làm tròn MỘT LẦN thì số Redis và số DB lệch nhau
    /// vài đồng — đúng loại chênh vô nghĩa mà lúc đối soát tốn cả buổi để truy. Nên: làm tròn TỪNG DÒNG.
    ///
    /// Trả <c>null</c> nếu tổng vượt tầm <see cref="long"/> (Redis INCRBY là số nguyên 64-bit):
    /// dữ liệu Asset bất thường ⇒ caller log ERROR và BỎ theo dõi AUM cho batch đó.
    /// KHÔNG ném: AUM ở Redis là số để QUAN SÁT, không phải cổng chặn — không được phép làm treo ingest.
    /// </summary>
    public static long? SumAum(IEnumerable<decimal?> aums)
    {
        decimal sum = 0m;
        foreach (var a in aums)
        {
            if (a is null) continue;   // Asset không gửi aum cho dòng này → coi như 0, không phá tổng
            sum += Math.Round(a.Value, 0, MidpointRounding.AwayFromZero);
        }
        return sum > long.MaxValue || sum < long.MinValue ? null : (long)sum;
    }

    /// <summary>
    /// CỘNG DỒN NGUYÊN TỬ — 1 round-trip, cộng CẢ số dòng LẪN tổng AUM.
    ///
    /// Vì sao PHẢI là Lua (không tách nhiều lệnh):
    ///     SADD  {MSG} batchKey     → đánh dấu đã xử lý
    ///     ⚡ pod chết ĐÚNG ở đây
    ///     INCRBY {ROWS} n          → KHÔNG BAO GIỜ CHẠY
    ///     ⇒ message coi như xong nhưng n dòng không được cộng ⇒ TREO VĨNH VIỄN.
    /// Redis chạy Lua nguyên tử ⇒ không có cửa sổ ghi-dở.
    ///
    /// ── HAI PHÉP CỘNG, HAI LUẬT KHÁC NHAU — ĐỪNG TRỘN ────────────────────────────────────────
    /// • ROWS (cổng chặn READY): dedup THUẦN — batch trùng thì KHÔNG cộng lại. Giữ nguyên như cũ,
    ///   không đụng vào, vì toàn bộ lập luận "rows >= totalRow" dựa trên nó.
    /// • AUM  (số quan sát):     cộng theo DELTA — lưu AUM của từng batchKey vào HASH {BATCH_AUM},
    ///   lần sau gặp lại chính batchKey đó thì cộng phần CHÊNH (mới − cũ).
    ///   ⇒ Asset gửi lại batch với giá trị ĐÃ SỬA (luồng re-ingest chính thức: SP idempotent
    ///     DELETE+INSERT theo (date,si)) → DB đúng số mới → AUM ở Redis CŨNG đúng số mới.
    ///   Nếu chỉ dedup thuần như ROWS thì AUM sẽ đứng ở số CŨ — sai đúng vào lúc người ta cần nó nhất
    ///   (vừa sửa dữ liệu xong, đang ngồi đối soát). Batch gửi lại y nguyên → delta = 0 → vô hại.
    ///
    /// ── VÌ SAO TRẢ VỀ CHUỖI (GET) CHỨ KHÔNG TRẢ SỐ ───────────────────────────────────────────
    /// Số trong Lua là double (chính xác tới 2^53 ≈ 9,0e15). Tổng AUM toàn hệ tính bằng ĐỒNG có thể
    /// tới 1e14–1e15 — chưa vỡ nhưng biên an toàn mỏng. Trả bằng GET (chuỗi) thì tổng KHÔNG bao giờ
    /// đi qua double: INCRBY tính bằng int64 trong Redis, C# parse lại từ chuỗi ⇒ chính xác tuyệt đối.
    /// Chỉ có DELTA (≤ AUM của 1 batch) đi qua double — nhỏ hơn 2^53 rất xa.
    ///
    /// TTL được gia hạn ở MỌI lượt gọi (kể cả batch trùng), không chỉ lượt đầu: job còn message chảy vào
    /// thì bằng chứng còn sống. Trước đây chỉ gia hạn khi batch mới — không sai, nhưng chặt hơn mức cần.
    ///
    /// KEYS[1]=MSG(set) KEYS[2]=ROWS KEYS[3]=AUM KEYS[4]=BATCH_AUM(hash)
    /// ARGV[1]=batchKey ARGV[2]=rowsInBatch ARGV[3]=aumInBatch ('' = job không có AUM, vd job phí)
    /// ARGV[4]=ttlSeconds
    /// Trả về: { tổng dòng (chuỗi), tổng AUM (chuỗi) }
    /// </summary>
    public const string LuaSumOnce = @"
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
