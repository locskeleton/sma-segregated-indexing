using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Serilog;
using StackExchange.Redis;

namespace SdiCoreMessagingProcess.Services;

/// <summary>Sổ lũy kế của MỘT job, đọc ra sau mỗi batch.</summary>
public sealed record BatchStat(long Rows, long Aum);

/// <summary>
/// SỔ LŨY KẾ NGHIỆP VỤ — tổng SỐ BẢN GHI + tổng AUM của một job, cập nhật sau mỗi batch.
///
/// ══ VÌ SAO NẰM RIÊNG, KHÔNG NHÉT VÀO SCRIPT LUA CỦA <see cref="BatchSyncService"/> ══════════
/// <see cref="BatchSyncService"/> là hạ tầng DÙNG CHUNG cho mọi luồng batch (Asset NAV hôm nay,
/// giá BO / holdings / bất cứ thứ gì mai mốt). PHẦN LỚN các luồng đó KHÔNG CÓ khái niệm "AUM".
/// Nhét AUM vào script Lua của nó nghĩa là:
///   • Mọi luồng đều phải mang theo một tham số vô nghĩa với nó (aum = null / '' / 0).
///   • Script Lua — thứ giữ TÍNH ĐÚNG của cổng chặn READY — phải mọc nhánh if cho một con số
///     chỉ để NGẮM. Sửa phần thống kê là động vào code quyết định "job xong chưa". Không đáng.
///   • Thêm luồng thứ 3 có "tổng khối lượng" thay vì "tổng tiền" là lại sửa Lua lần nữa.
///
/// ⇒ Tách hẳn: hàm nghiệp vụ (<c>BulkInsertAssetToSdiCore</c>) tự gọi lớp này SAU KHI ghi DB xong.
///   Ai có số để cộng thì gọi. Ai không có thì không biết lớp này tồn tại.
///   <see cref="BatchSyncService"/> và script Lua của nó KHÔNG ĐỔI MỘT DÒNG.
///
/// ══ VÌ SAO KHÔNG DÙNG LẠI ĐƯỢC DEDUP CỦA {P}:MSG ═══════════════════════════════════════════
/// Dedup kia nằm TRONG script Lua của cổng chặn, và nó chạy SAU <c>executeFunc</c>. Lớp này chạy
/// TRONG <c>executeFunc</c> ⇒ không thấy kết quả dedup đó. Nên nó phải TỰ idempotent — và cách
/// làm ở đây (cộng theo DELTA qua HASH) còn ĐÚNG HƠN dedup thuần, xem dưới.
///
/// ══ CỘNG THEO DELTA, KHÔNG PHẢI CỘNG DỒN ═══════════════════════════════════════════════════
/// Lưu số của TỪNG batch vào HASH <c>{P}:STAT_BATCH</c> (batchKey → "rows|aum"), lần sau gặp lại
/// chính batchKey đó thì cộng phần CHÊNH (mới − cũ). Hệ quả:
///   • Kafka giao lại y nguyên  → delta = 0 → không đổi gì. (Bằng dedup thuần.)
///   • Asset gửi lại ĐÃ SỬA số → delta = chênh → tổng ra SỐ MỚI, khớp DB. (Dedup thuần thì ĐỨNG
///     Ở SỐ CŨ — sai đúng vào lúc cần nhất: vừa re-ingest sửa dữ liệu xong, đang ngồi đối soát.)
///   Điều này ĐÚNG vì <c>SP_INGEST_ASSET_NAV</c> ghi <c>DELETE+INSERT</c> theo <c>(date, si)</c>:
///   gửi lại cùng tập tài khoản = GHI ĐÈ, không phải cộng thêm. Sổ phải phản ánh đúng phép ghi đó.
///
/// ══ SỐ NÀY ĐỂ NHÌN, KHÔNG ĐỂ CHẶN ══════════════════════════════════════════════════════════
/// Đúng nguyên tắc ①: Redis lo TỐC ĐỘ, DB lo TÍNH ĐÚNG. Không có nhánh code nào được rẽ theo nó.
/// Sự thật là <c>SELECT COUNT(*), SUM(C_AUM) FROM T_SI_BALANCE WHERE C_BUSINESS_DATE=@d</c>.
/// Cổng chặn vẫn là <c>rows >= totalRow</c> + <c>SP_EOD_RUN</c> err=12.
/// Vì vậy lớp này KHÔNG BAO GIỜ ném: Redis hỏng thì mất sổ, KHÔNG được phép làm hỏng ingest.
/// </summary>
public class BatchStatAggregator
{
    private readonly IDatabase _redis;

    /// <summary>TTL 48h — giống mọi khoá khác của luồng batch. Dọn bằng TTL, KHÔNG KeyDelete.</summary>
    private readonly TimeSpan _ttl = TimeSpan.FromHours(48);

    public BatchStatAggregator(IDatabase redis) => _redis = redis;

    public static string StatRows(string p)  => $"{p}:STAT_ROWS";    // tổng SỐ BẢN GHI đã nhận
    public static string StatAum(string p)   => $"{p}:STAT_AUM";     // tổng AUM (VND) đã nhận
    public static string StatBatch(string p) => $"{p}:STAT_BATCH";   // HASH batchKey → "rows|aum"

    /// <summary>
    /// TỔNG AUM CỦA MỘT BATCH — làm tròn ĐÚNG NHƯ DB.
    ///
    /// <c>SP_INGEST_ASSET_NAV</c> đọc payload bằng <c>OPENJSON(...) WITH (aum DECIMAL(20,0) '$.aum')</c>
    /// ⇒ SQL Server làm tròn TỪNG DÒNG (half away from zero). Cộng thập phân rồi mới tròn MỘT LẦN
    /// sẽ lệch DB vài đồng — đúng loại chênh vô nghĩa mà lúc đối soát tốn cả buổi để truy.
    ///
    /// Trả <c>null</c> nếu vượt tầm <see cref="long"/> (Redis INCRBY là số nguyên 64-bit) ⇒ caller
    /// log ERROR và BỎ cộng batch đó. KHÔNG ném: một con số để ngắm không được làm treo ingest.
    /// </summary>
    public static long? SumAum(IEnumerable<decimal?> aums)
    {
        decimal sum = 0m;
        foreach (var a in aums)
        {
            if (a is null) continue;   // Asset không gửi aum dòng này → coi như 0
            sum += Math.Round(a.Value, 0, MidpointRounding.AwayFromZero);
        }
        return sum > long.MaxValue || sum < long.MinValue ? null : (long)sum;
    }

    /// <summary>
    /// Cộng số của batch này vào sổ job, trả về LŨY KẾ sau batch. Gọi SAU KHI ghi DB thành công.
    /// </summary>
    /// <param name="batchKey">
    ///   PHẢI là đúng khoá mà tầng cổng chặn dùng (<see cref="KafkaSyncKeys.BatchKey"/> —
    ///   băm tập si_account). Dùng khoá khác thì "gửi lại đã sửa" không nhận ra nhau ⇒ cộng chồng.
    /// </param>
    /// <param name="aum">
    ///   <c>null</c> ⇒ chỉ cộng số bản ghi, KHÔNG động vào tổng AUM (luồng batch không có tiền,
    ///   hoặc tổng batch vượt tầm int64 — xem <see cref="SumAum"/>).
    /// </param>
    /// <returns><c>null</c> nếu Redis lỗi — ĐÃ NUỐT và log. Caller cứ đi tiếp, đừng chặn ingest.</returns>
    public async Task<BatchStat?> AccumulateAsync(
        string redisKeyPrefix, string requestId, string batchKey, long rows, long? aum)
    {
        var p = KafkaSyncKeys.Scope(redisKeyPrefix, requestId);

        try
        {
            var res = (RedisValue[])await _redis.ScriptEvaluateAsync(
                LuaAccumulate,
                new RedisKey[]   { StatBatch(p), StatRows(p), StatAum(p) },
                // ⚠️ ĐỪNG dùng long.ToString(): phụ thuộc CurrentCulture (dấu âm). Ép sang RedisValue
                //    để thư viện format bất biến. '' = không động vào tổng AUM.
                new RedisValue[] { batchKey, rows,
                                   aum.HasValue ? (RedisValue)aum.Value : RedisValue.EmptyString,
                                   (long)_ttl.TotalSeconds });

            return new BatchStat((long)res[0], (long)res[1]);
        }
        catch (Exception ex)
        {
            // Sổ hỏng ≠ dữ liệu hỏng. Dữ liệu ĐÃ vào DB rồi (gọi hàm này sau khi ghi xong).
            Log.Warning(ex, "[Stat] req={Req} cộng sổ lũy kế THẤT BẠI (+{Rows} dòng, +{Aum} AUM). " +
                            "Ingest KHÔNG bị ảnh hưởng — chỉ mất số để ngắm.", requestId, rows, aum);
            return null;
        }
    }

    /// <summary>Đọc sổ lũy kế hiện tại (dashboard / watchdog). Chưa có số nào → (0,0).</summary>
    public async Task<BatchStat> ReadAsync(string redisKeyPrefix, string requestId)
    {
        var p    = KafkaSyncKeys.Scope(redisKeyPrefix, requestId);
        var vals = await _redis.StringGetAsync(new RedisKey[] { StatRows(p), StatAum(p) });
        return new BatchStat(vals[0].HasValue ? (long)vals[0] : 0,
                             vals[1].HasValue ? (long)vals[1] : 0);
    }

    /// <summary>
    /// CỘNG THEO DELTA — nguyên tử, 1 round-trip.
    ///
    /// PHẢI là Lua: đọc số cũ → tính chênh → cộng → ghi số mới là 4 lệnh. Tách ra thì hai pod cùng
    /// xử lý một batch (zombie sau rebalance) sẽ đọc cùng số cũ rồi cộng chênh HAI LẦN.
    ///
    /// TRẢ VỀ CHUỖI (GET) chứ không trả số: số trong Lua là double, chỉ chính xác tới 2^53 ≈ 9,0e15,
    /// mà tổng AUM tính bằng ĐỒNG có thể tới 1e14–1e15. Qua GET thì tổng do INCRBY tính bằng int64
    /// trong Redis, C# parse lại từ chuỗi ⇒ chính xác tuyệt đối. Chỉ DELTA (≤ 1 batch) qua double.
    ///
    /// ⚠️ REDIS CLUSTER: 3 khoá phải cùng slot. Bản hiện tại chạy standalone/sentinel nên không cần
    ///    hash-tag; nếu chuyển sang cluster thì đổi Scope() thành "{prefix:requestId}" (có ngoặc nhọn).
    ///
    /// KEYS[1]=STAT_BATCH(hash) KEYS[2]=STAT_ROWS KEYS[3]=STAT_AUM
    /// ARGV[1]=batchKey ARGV[2]=rows ARGV[3]=aum ('' = không động vào AUM) ARGV[4]=ttl(giây)
    /// Trả về: { tổng dòng (chuỗi), tổng AUM (chuỗi) }
    /// </summary>
    private const string LuaAccumulate = @"
local prevRows, prevAum = '0', '0'
local prev = redis.call('HGET', KEYS[1], ARGV[1])
if prev then
    local sep = string.find(prev, '|', 1, true)
    if sep then
        prevRows = string.sub(prev, 1, sep - 1)
        prevAum  = string.sub(prev, sep + 1)
    end
end

redis.call('INCRBY', KEYS[2], string.format('%d', tonumber(ARGV[2]) - tonumber(prevRows)))

-- ARGV[3]='' ⇒ luồng không có tiền: GIỮ NGUYÊN số aum cũ của batch, không tạo khoá STAT_AUM rác
local aum = prevAum
if ARGV[3] ~= '' then
    aum = ARGV[3]
    redis.call('INCRBY', KEYS[3], string.format('%d', tonumber(ARGV[3]) - tonumber(prevAum)))
    redis.call('EXPIRE', KEYS[3], ARGV[4])
end

redis.call('HSET',   KEYS[1], ARGV[1], ARGV[2] .. '|' .. aum)
redis.call('EXPIRE', KEYS[1], ARGV[4])
redis.call('EXPIRE', KEYS[2], ARGV[4])

return { redis.call('GET', KEYS[2]) or '0', redis.call('GET', KEYS[3]) or '0' }";
}
