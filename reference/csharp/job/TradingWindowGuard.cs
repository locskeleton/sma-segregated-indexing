using System;
using System.Threading;
using System.Threading.Tasks;
using Serilog;

namespace SdiCoreMessagingProcess.Jobs;

/// <summary>Ném ra khi phiên đã đóng ⇒ dừng chu kỳ, KHÔNG gọi FO thêm lần nào.</summary>
public sealed class TradingWindowClosedException : Exception
{
    public TradingWindowClosedException(string m) : base(m) { }
}

/// <summary>Khung giờ hiệu lực của một job, đọc từ T_JOB_DEFINITION.</summary>
public sealed class JobWindow
{
    public bool      Enabled         { get; init; }
    public TimeSpan? From            { get; init; }   // null = mọi giờ
    public TimeSpan? To              { get; init; }
    public bool      BusinessDayOnly { get; init; }
}

/// <summary>
/// ★ GUARD TẦNG 4 — hàng rào cuối cùng, đứng NGAY TRƯỚC MỖI LƯỢT GỌI HTTP SANG FO.
///
/// VÌ SAO 3 TẦNG TRONG DB CHƯA ĐỦ:
///   Tầng 1 (sinh job) và tầng 2 (nhận job) kiểm tra tại MỘT THỜI ĐIỂM — lúc chu kỳ BẮT ĐẦU.
///   Nhưng một chu kỳ quét ~1000 batch có thể chạy nhiều phút. Chu kỳ khởi động hợp lệ lúc 14:45
///   mà tới batch thứ 700 thì đồng hồ đã 15:02. Cả hai tầng kia đều đã "duyệt" nó từ trước.
///   ⇒ Không kiểm LẠI trong vòng lặp thì hệ vẫn bắn hàng trăm request sang FO sau giờ đóng cửa.
///
/// KIỂM Ở ĐÂU: ngay trước mỗi call, KHÔNG phải mỗi N batch. Kiểm thưa ra là mở lại một khe hở
///   đúng bằng N batch — và khe đó sẽ được lấp vào đúng ngày chu kỳ chạy chậm nhất.
///
/// ★★ KHUNG GIỜ ĐỌC TỪ DB, KHÔNG NHẬN QUA HẰNG SỐ LÚC KHỞI ĐỘNG.
///   Bản đầu nhận `from`/`to` qua DI và giữ nguyên suốt đời tiến trình. Nó SAI ngay khi người vận
///   hành đổi khung giờ trong `T_JOB_DEFINITION`: tầng 1/2/3 (nằm trong SQL) đổi theo tức thì, còn
///   tầng 4 vẫn gác theo khung CŨ cho tới lần deploy sau. Hai tầng hiểu luật khác nhau — mà tầng
///   sai lại đúng là tầng duy nhất trực tiếp chạm vào FO.
///   Nay: đọc `T_JOB_DEFINITION`, nhớ tạm 60 giây (đủ để không bắn query mỗi batch, đủ nhanh để
///   đổi cấu hình có hiệu lực trong vòng một phút). Đây là thứ giữ cho lời hứa "luật giờ định
///   nghĩa ĐÚNG MỘT CHỖ" là sự thật chứ không phải khẩu hiệu trong tài liệu.
///
/// ĐỌC DB HỎNG THÌ SAO: giữ giá trị đọc được lần cuối và ĐI TIẾP (fail-safe theo hướng bảo thủ —
///   xem `_last`). Nếu chưa từng đọc được lần nào thì TỪ CHỐI chạy: chưa biết luật thì không gọi
///   hệ ngoài, đó là ý nghĩa của chữ "tuyệt đối" trong yêu cầu.
///
/// ĐỒNG HỒ: giờ VIỆT NAM từ TimeZoneInfo, không dùng DateTime.Now. Pod chạy UTC là khung 9h–15h
///   lệch 7 tiếng. Cùng lý do với UDF_JOB_NOW() phía SQL — hai bên phải chung khái niệm "bây giờ".
/// </summary>
public sealed class TradingWindowGuard
{
    private static readonly TimeZoneInfo Vn = ResolveVnZone();
    private static readonly TimeSpan     CacheTtl = TimeSpan.FromSeconds(60);

    private readonly ISdiJobGateway _db;
    private readonly string         _jobCode;

    private readonly SemaphoreSlim _lock = new(1, 1);
    private JobWindow? _last;                       // giá trị đọc được gần nhất (fallback khi DB lỗi)
    private DateTime   _lastAt = DateTime.MinValue;
    private DateTime   _cachedDay = DateTime.MinValue;
    private bool       _cachedIsGd;

    public TradingWindowGuard(ISdiJobGateway db, string jobCode = "FO_SNAPSHOT_RT")
    {
        _db = db; _jobCode = jobCode;
    }

    public static DateTime NowVn() => TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, Vn);

    /// <summary>
    /// Ném <see cref="TradingWindowClosedException"/> nếu ĐÃ ra ngoài khung. Gọi trước MỖI call FO.
    /// Biên [from, to): 15:00:00 chẵn là đã đóng — "đến 3h chiều" nghĩa là phiên hết lúc 3h.
    /// </summary>
    public async Task EnsureOpenAsync(CancellationToken ct)
    {
        var w   = await GetWindowAsync(ct);
        var now = NowVn();

        if (!w.Enabled)
            throw new TradingWindowClosedException($"Job {_jobCode} đang TẮT — DỪNG gọi FO.");

        if (w.From is { } from && w.To is { } to)
        {
            var tod = now.TimeOfDay;
            if (tod < from || tod >= to)
                throw new TradingWindowClosedException(
                    $"Phiên đã đóng: {now:yyyy-MM-dd HH:mm:ss} (giờ VN), khung cho phép " +
                    $"{from:hh\\:mm}–{to:hh\\:mm}. DỪNG gọi FO.");
        }

        if (w.BusinessDayOnly)
        {
            // Lịch nghỉ nằm ở T_TRADING_HOLIDAY, không hard-code trong C#. Nhớ theo NGÀY: một chu kỳ
            //   chỉ nằm trong một ngày nên tối đa 1 lượt hỏi DB cho cả chu kỳ.
            var day = now.Date;
            if (_cachedDay != day)
            {
                _cachedIsGd = await _db.IsBusinessDateAsync(day);
                _cachedDay  = day;
            }
            if (!_cachedIsGd)
                throw new TradingWindowClosedException($"{day:yyyy-MM-dd} KHÔNG phải ngày giao dịch. DỪNG gọi FO.");
        }
    }

    private async Task<JobWindow> GetWindowAsync(CancellationToken ct)
    {
        if (_last != null && DateTime.UtcNow - _lastAt < CacheTtl) return _last;

        await _lock.WaitAsync(ct);
        try
        {
            if (_last != null && DateTime.UtcNow - _lastAt < CacheTtl) return _last;   // pod nhiều luồng: chỉ 1 lượt đọc
            try
            {
                _last   = await _db.GetJobWindowAsync(_jobCode, ct);
                _lastAt = DateTime.UtcNow;
            }
            catch (Exception ex)
            {
                if (_last == null)
                    // Chưa từng đọc được ⇒ KHÔNG BIẾT luật ⇒ không được gọi FO. Từ chối là lựa chọn
                    //   duy nhất đúng ở đây; đoán bừa 9h–15h là tự ý viết lại cấu hình của người khác.
                    throw new TradingWindowClosedException(
                        $"Chưa đọc được khung giờ của {_jobCode} từ DB ({ex.Message}) — TỪ CHỐI gọi FO.");
                Log.Warning(ex, "[FO-RT] Không làm mới được khung giờ, dùng giá trị đọc lúc {At}", _lastAt);
            }
            return _last!;
        }
        finally { _lock.Release(); }
    }

    private static TimeZoneInfo ResolveVnZone()
    {
        // Windows dùng 'SE Asia Standard Time', Linux dùng 'Asia/Ho_Chi_Minh'. Thử cả hai thay vì
        //   giả định môi trường — deploy sang container Linux mà crash vì tên múi giờ là lỗi ngớ
        //   ngẩn nhất có thể gặp, và nó chỉ lộ ra lúc chạy thật.
        foreach (var id in new[] { "SE Asia Standard Time", "Asia/Ho_Chi_Minh" })
        {
            try { return TimeZoneInfo.FindSystemTimeZoneById(id); } catch { }
        }
        Log.Warning("[JOB] Không tìm thấy múi giờ VN — dùng UTC+7 cố định");
        return TimeZoneInfo.CreateCustomTimeZone("VN", TimeSpan.FromHours(7), "VN", "VN");
    }
}
