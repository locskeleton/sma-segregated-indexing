using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Serilog;
using StackExchange.Redis;

namespace SdiCoreMessagingProcess.Services;

/// <summary>
/// WATCHDOG — chạy trên MỌI pod, 15 giây/lần.
///
/// Giải quyết 2 chuyện mà consumer KHÔNG thể tự làm:
///
/// ① TIMEOUT FALLBACK — "đừng để hệ mình chết vì hệ người khác lỗi".
///    totalRow là số Asset KHAI. Asset có thể khai sai / gửi thiếu / gửi trùng si_account.
///    Nếu để `rows >= totalRow` làm CỔNG CHẶN CỨNG thì một lỗi của Asset = job TREO VĨNH VIỄN.
///    ⇒ Không có message mới trong 5 phút mà đã nhận được dòng nào đó → VẪN bật cờ READY (log WARNING).
///    ⇒ Đủ/thiếu THẬT do SP_EOD_RUN quyết (err=12) — thứ duy nhất SDI kiểm soát 100%.
///
///    ⚠️ Phải nằm ở đây chứ KHÔNG phải trong consumer: lúc job treo thì ĐÚNG LÀ không còn
///       message nào tới nữa ⇒ code trong handler sẽ không bao giờ chạy.
///
/// ② HẾT TREO IM LẶNG — job kẹt thì phải KÊU. Hiện tại nó chết im, chỉ phát hiện khi đi soi log.
/// </summary>
public class EodJobWatchdogService : BackgroundService
{
    private static readonly TimeSpan Interval   = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan QuietAfter = TimeSpan.FromMinutes(5);   // im lặng bao lâu thì chốt

    private readonly IDatabase        _redis;
    private readonly BatchSyncService _sync;

    // Mỗi bizType 1 dòng: (redisKeyPrefix, bizType)
    private static readonly (string Prefix, string Biz)[] Jobs =
    {
        (RedisKeyDefine.EOD_ASSET, BizTypeDefine.JOB_EOD_ASSET),
    };

    public EodJobWatchdogService(IDatabase redis, BatchSyncService sync)
    {
        _redis = redis;
        _sync  = sync;
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try { await ScanAsync(); }
            catch (Exception ex) { Log.Error(ex, "[Watchdog] lỗi vòng quét"); }

            await Task.Delay(Interval, ct);
        }
    }

    private async Task ScanAsync()
    {
        foreach (var (prefix, biz) in Jobs)
        {
            // SET các requestId đang chạy — KHÔNG dùng KEYS/SCAN (O(N), block Redis)
            var actives = await _redis.SetMembersAsync(KafkaSyncKeys.ActiveJobs(prefix));

            foreach (var reqId in actives)
            {
                var p = KafkaSyncKeys.Scope(prefix, reqId!);

                // Đã READY (batch cuối bật) → gỡ khỏi danh sách canh
                if (await _redis.KeyExistsAsync(KafkaSyncKeys.Ready(p)))
                {
                    await _redis.SetRemoveAsync(KafkaSyncKeys.ActiveJobs(prefix), reqId);
                    continue;
                }

                var rowsVal  = await _redis.StringGetAsync(KafkaSyncKeys.Rows(p));
                var totalVal = await _redis.StringGetAsync(KafkaSyncKeys.Total(p));
                var lastVal  = await _redis.StringGetAsync(KafkaSyncKeys.LastAt(p));
                var dateVal  = await _redis.StringGetAsync(KafkaSyncKeys.Date(p));
                var aumVal   = await _redis.StringGetAsync(KafkaSyncKeys.Aum(p));   // ★ chỉ để LOG

                // Redis mất key giữa chừng (evict/restart) → không đủ dữ liệu để quyết → gỡ ra, kêu.
                if (!rowsVal.HasValue || !totalVal.HasValue || !lastVal.HasValue || !dateVal.HasValue)
                {
                    Log.Warning("[Watchdog][{Biz}] req={Req} MẤT KEY Redis (evict/restart). " +
                                "Dữ liệu trong DB VẪN ĐỦ — chỉ là cờ không bật. " +
                                "Bật cờ thủ công hoặc để Asset gửi lại (SP idempotent).", biz, reqId);
                    await _redis.SetRemoveAsync(KafkaSyncKeys.ActiveJobs(prefix), reqId);
                    continue;
                }

                // ⚠️ AUM CỐ TÌNH KHÔNG nằm trong guard "mất key" ở trên: nó là số QUAN SÁT.
                //    Thiếu nó thì log xấu một chút, KHÔNG được phép làm đổi quyết định chốt job.
                long rows  = (long)rowsVal;
                long total = (long)totalVal;
                long aum   = aumVal.HasValue ? (long)aumVal : 0;
                var  last  = new DateTime((long)lastVal, DateTimeKind.Utc);
                var  quiet = DateTime.UtcNow - last;

                if (quiet < QuietAfter) continue;   // vẫn đang chảy, chưa im lặng đủ lâu

                if (rows <= 0)
                {
                    // Job chết hẳn: Asset không gửi được dòng nào. KÊU ĐÚNG MỘT LẦN rồi gỡ khỏi danh sách canh.
                    // ⚠️ Nếu để nguyên trong JOBS thì watchdog log ERROR mỗi 15 GIÂY suốt 48h (TTL) →
                    //    SPAM LOG → người trực nhờn → đến lúc lỗi thật thì không ai để ý.
                    //    Gỡ ra KHÔNG mất gì: message mới tới sẽ SADD requestId vào JOBS lại (xem BatchSyncService).
                    Log.Error("[Watchdog][{Biz}] {Date} req={Req} — {Min:F0} phút KHÔNG nhận được dòng nào. " +
                              "Kiểm tra producer/Asset. (Gỡ khỏi danh sách canh; tự quay lại nếu có message mới.)",
                              biz, (string)dateVal!, reqId, quiet.TotalMinutes);
                    await _redis.SetRemoveAsync(KafkaSyncKeys.ActiveJobs(prefix), reqId);
                    continue;
                }

                // ★ CHỐT THEO TIMEOUT — không để Asset lỗi làm treo hệ mình.
                //   TrySetReadyAsync tự SREM khỏi JOBS sau khi bật cờ (và tự log tổng AUM).
                Log.Information("[Watchdog][{Biz}] {Date} req={Req} im lặng {Min:F0} phút — chốt với " +
                                "{Rows}/{Total} dòng, tổng AUM {Aum}",
                                biz, (string)dateVal!, reqId, quiet.TotalMinutes, rows, total, aum);

                await _sync.TrySetReadyAsync(prefix, reqId!, (string)dateVal!, biz, total, rows, byTimeout: true);
            }
        }
    }
}
