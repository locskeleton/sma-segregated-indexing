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
    public TimeSpan? To              { get; init; }   // MỐC CUỐI được SINH job (đóng: 15:00 hợp lệ)
    public bool      BusinessDayOnly { get; init; }
    /// <summary>
    /// HẠN TƯƠI của một lượt chạy, tính từ MỐC SLOT của nó.
    /// = COALESCE(C_MAX_DELAY_SEC, C_INTERVAL_SEC) — CÙNG con số SP_JOB_CLAIM_SLOT dùng, nên tầng 2 và
    /// tầng 4 không thể lệch pha. Null = không hết hạn (job on-demand không chu kỳ).
    /// </summary>
    /// <summary>⚠️ KHÔNG CÒN DÙNG (đã bỏ hạn tươi). Giữ để không phải sửa tầng đọc DB.</summary>
    public int?      MaxDelaySec     { get; init; }
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
    /// Ném <see cref="TradingWindowClosedException"/> nếu đã quá HẠN CHÓT CHẠY. Gọi trước MỖI call FO.
    /// Hạn chót = To + GraceSec (không phải To) — xem chú thích trong thân hàm.
    /// </summary>
    /// <summary>
    /// Ném <see cref="TradingWindowClosedException"/> nếu lượt chạy này KHÔNG còn được phép chạm FO.
    /// Gọi TRƯỚC KHI lấy dữ liệu / chia batch, và TRƯỚC MỖI call FO trong vòng lặp.
    ///
    /// Ba câu hỏi, không có câu nào là "bây giờ mấy giờ so với 15:00":
    ///   ① Job còn bật không?
    ///   ② MỐC của lượt này có nằm trong khung giờ không? (mốc, KHÔNG phải `now` — lượt 15:00
    ///      claim lúc 15:00:03 vẫn hợp lệ; áp khung lên `now` là giết ảnh chụp đóng cửa)
    ///   ③ ⚠️ ĐÃ BỎ — "lượt này còn tươi không". Xem chú thích trong thân hàm.
    ///
    /// ⚠️ Bỏ ③ nghĩa là chấp nhận: một lượt trễ 40 phút vẫn gọi FO, và một chu kỳ của mốc cuối
    /// phiên vẫn có thể gọi FO sau 15h. Cận trên duy nhất còn lại là timeout của HTTP client.
    /// </summary>
    /// <param name="slotAt">Mốc mà lượt chạy này đáng lẽ chạy (JobContext.SlotAt).</param>
    public async Task EnsureOpenAsync(DateTime slotAt, CancellationToken ct)
    {
        var w   = await GetWindowAsync(ct);
        var now = NowVn();

        // ① Tắt job là dừng được cả chu kỳ đang chạy (~1 phút, bằng TTL cache).
        if (!w.Enabled)
            throw new TradingWindowClosedException($"Job {_jobCode} dang TAT - DUNG goi FO.");

        // ② Khung giờ áp lên MỐC, không phải lên `now`.
        if (w.From is { } from && w.To is { } to)
        {
            var slotTod = slotAt.TimeOfDay;
            if (slotTod < from || slotTod > to)
                throw new TradingWindowClosedException(
                    $"Moc {slotAt:yyyy-MM-dd HH:mm:ss} nam ngoai khung {from}-{to}. DUNG goi FO.");
        }

        // ③ ⚠️ ĐÃ BỎ HẠN TƯƠI (C_MAX_DELAY_SEC) — quyết định nghiệp vụ, và nó có giá:
        //
        //   Guard ② áp lên MỐC, không áp lên `now` — ĐÚNG như thiết kế: mốc 15:00 là mốc cuối
        //   cùng cần lấy dữ liệu, và chu kỳ của nó HOÀN THÀNH SAU 15h LÀ HỢP LỆ. Cái bị cấm là
        //   SINH thêm mốc mới sau phiên, và bộ quét đã lo việc đó.
        //   Hạn tươi không trả lời "mốc này có hợp lệ không" — nó trả lời "MUỘN THẾ NÀY THÌ THÔI".
        //   Bỏ nó ⇒ câu hỏi đó không còn ai trả lời: chu kỳ của mốc 15:00 gặp FO chậm vẫn gọi FO
        //   lúc 16h, 17h, và không có gì nói được đâu là quá.
        //
        //   ⇒ CẬN TRÊN THỜI LƯỢNG CHU KỲ NAY NẰM HOÀN TOÀN Ở TIMEOUT CỦA HTTP CLIENT:
        //         thời lượng ≈ ⌈số batch / parallel⌉ × timeout HTTP
        //     và nó phải nhỏ hơn C_INTERVAL_SEC, nếu không các chu kỳ chồng lên nhau và SDI tự
        //     nhân tải lên chính FO. KHÔNG có ràng buộc nào gác phép tính này — đặt timeout HTTP
        //     quá lớn (hoặc 0 = vô hạn) là mở đúng cái cửa đó.

        // Lịch nghỉ nằm ở T_TRADING_HOLIDAY, không hard-code trong C#. Nhớ theo NGÀY: một chu kỳ
        //   chỉ nằm trong một ngày nên tối đa 1 lượt hỏi DB cho cả chu kỳ.
        if (w.BusinessDayOnly)
        {
            var day = slotAt.Date;
            if (_cachedDay != day)
            {
                _cachedIsGd = await _db.IsBusinessDateAsync(day);
                _cachedDay  = day;
            }
            if (!_cachedIsGd)
                throw new TradingWindowClosedException($"{day:yyyy-MM-dd} KHONG phai ngay giao dich. DUNG goi FO.");
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
