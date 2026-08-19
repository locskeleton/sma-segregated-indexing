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

/// <summary>
/// ★ GUARD TẦNG 4 — hàng rào cuối cùng, đứng NGAY TRƯỚC MỖI LƯỢT GỌI HTTP SANG FO.
///
/// VÌ SAO 3 TẦNG TRONG DB CHƯA ĐỦ:
///   Tầng 1 (sinh job) và tầng 2 (nhận job) kiểm tra tại MỘT THỜI ĐIỂM — lúc chu kỳ BẮT ĐẦU.
///   Nhưng một chu kỳ quét ~1000 batch có thể chạy nhiều phút. Chu kỳ khởi động hợp lệ lúc 14:45
///   mà tới batch thứ 700 thì đồng hồ đã 15:02. Cả hai tầng kia đều đã "duyệt" nó từ trước.
///   ⇒ Nếu không kiểm LẠI trong vòng lặp, hệ vẫn bắn hàng trăm request sang FO sau giờ đóng cửa —
///     đúng cái điều BRD cấm tuyệt đối, và cấm bằng chữ "tuyệt đối".
///
/// KIỂM Ở ĐÂU: ngay trước mỗi call, KHÔNG phải mỗi N batch. Kiểm thưa ra là lại mở một khe hở
///   đúng bằng N batch — và khe hở đó sẽ được lấp đầy vào đúng ngày thị trường biến động, khi
///   chu kỳ chạy chậm nhất.
///
/// ĐỒNG HỒ: dùng giờ VIỆT NAM lấy từ TimeZoneInfo, không dùng DateTime.Now. Pod chạy UTC là
///   khung 9h–15h lệch 7 tiếng. Cùng lý do với UDF_JOB_NOW() phía SQL — hai bên phải chung một
///   khái niệm "bây giờ", nếu không thì tầng 2 và tầng 4 sẽ bất đồng và không ai biết bên nào đúng.
/// </summary>
public sealed class TradingWindowGuard
{
    private static readonly TimeZoneInfo Vn = ResolveVnZone();

    private readonly TimeSpan _from;
    private readonly TimeSpan _to;
    private readonly Func<DateTime, Task<bool>> _isBusinessDate;   // hỏi DB: UDF_IS_BUSINESS_DATE
    private DateTime _cachedDay = DateTime.MinValue;
    private bool     _cachedIsGd;

    public TradingWindowGuard(TimeSpan from, TimeSpan to, Func<DateTime, Task<bool>> isBusinessDate)
    {
        _from = from; _to = to; _isBusinessDate = isBusinessDate;
    }

    public static DateTime NowVn() => TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, Vn);

    /// <summary>
    /// Ném <see cref="TradingWindowClosedException"/> nếu ĐÃ ra ngoài khung. Gọi trước MỖI call FO.
    /// Biên [from, to): 15:00:00 chẵn là đã đóng — "đến 3h chiều" nghĩa là phiên hết lúc 3h.
    /// </summary>
    public async Task EnsureOpenAsync(CancellationToken ct)
    {
        var now = NowVn();
        var tod = now.TimeOfDay;

        if (tod < _from || tod >= _to)
            throw new TradingWindowClosedException(
                $"Phiên đã đóng: {now:yyyy-MM-dd HH:mm:ss} (giờ VN), khung cho phép {_from:hh\\:mm}–{_to:hh\\:mm}. DỪNG gọi FO.");

        // Ngày GD hỏi DB (lịch nghỉ nằm ở T_TRADING_HOLIDAY, không hard-code trong code C#).
        //   Nhớ theo NGÀY: một chu kỳ chỉ nằm trong một ngày nên tối đa 1 lượt hỏi DB/chu kỳ.
        var day = now.Date;
        if (_cachedDay != day)
        {
            _cachedIsGd = await _isBusinessDate(day);
            _cachedDay  = day;
        }
        if (!_cachedIsGd)
            throw new TradingWindowClosedException($"{day:yyyy-MM-dd} KHÔNG phải ngày giao dịch. DỪNG gọi FO.");
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
