using System;

namespace SdiCoreMessagingProcess.Jobs;

/// <summary>
/// KHOÁ REDIS của khung job. Redis ở đây làm ĐÚNG MỘT VIỆC: **lọc trước để khỏi hỏi DB**.
///
/// ★ VÌ SAO CẦN — đo thật, không phải cảm tính:
///   Bộ quét chạy 10 giây/lần trên MỌI pod. Với chu kỳ 15 phút thì **89/90 nhịp quét không có gì
///   tới hạn**. Trước khi có lớp lọc này, mỗi nhịp vẫn là một lượt gọi DB
///   (~50 logical reads): 10 pod × 6 lần/phút × 24 giờ = **~86.400 lượt quét/ngày để sinh 25 job**
///   — khoảng **3.400 lượt quét cho mỗi job**. Tải tuyệt đối thì nhỏ, nhưng đó là 3.400 lần hỏi
///   một câu đã biết trước câu trả lời.
///
///   Nay: pod tự tính mốc (không chạm DB) rồi `SET NX` khoá mốc đó. Ai đặt được khoá thì mới đi
///   tiếp xuống DB. ⇒ DB chỉ bị gọi **đúng một lần cho mỗi mốc** (~25 lượt/ngày).
///
/// ★ REDIS KHÔNG GIỮ MẢNH TÍNH ĐÚNG NÀO — vẫn nguyên tắc ①:
///   Mất khoá slot (Redis restart, evict, FLUSHALL) ⇒ nhiều pod cùng thấy "chưa ai làm" ⇒ cùng
///   gọi `SP_JOB_CLAIM_SLOT` ⇒ **`UQ (C_JOB_CODE, C_FIRE_KEY)` cho đúng một pod thắng**, số còn lại
///   nhận `err=5`. Hậu quả tệ nhất của việc mất KHOÁ: vài lượt gọi DB thừa. Không job nào chạy hai
///   lần, không job nào mất.
///   ⇒ Đó là lý do lớp này được phép "sai": nó chỉ trả lời *"có đáng hỏi DB không"*, không trả lời
///     *"ai được chạy"*.
///
/// ★ NHƯNG MẤT KHOÁ ≠ MẤT REDIS. Redis LỖI thì bộ quét DỪNG cả nhịp (JobSchedulerService.ScanAsync
///   `return`), KHÔNG produce và KHÔNG rơi về quét DB: mất khoá là vài query thừa, còn mất Redis là
///   KHÔNG CÒN AI LỌC — 10 pod × 6 nhịp/phút bắn message cho mọi mốc tới hạn.
/// </summary>
public static class JobRedisKeys
{
    /// <summary>
    /// Khoá MỐC đã được xử lý: `SET NX` thành công = pod này là pod đầu tiên thấy mốc đó.
    /// TTL = 2× chu kỳ: đủ dài để mọi pod trong cùng một mốc đều thấy khoá, đủ ngắn để không tích
    /// rác. Ngắn hơn chu kỳ là vô nghĩa (khoá hết hạn trước khi mốc kế tiếp tới ⇒ không lọc được gì).
    /// </summary>
    public static string SlotLock(string jobCode, string fireKey) => $"SDI:JOB:SLOT:{jobCode}:{fireKey}";


    /// <summary>
    /// Cache cấu hình lịch (HASH: jobCode → JSON) — thứ bộ quét thật sự đọc để tính mốc.
    ///
    /// ★ HAI ĐƯỜNG GHI, đều đi qua <see cref="JobConfigCache.WriteAsync"/>:
    ///   · <see cref="JobConfigService"/> — ngay sau khi ops đổi lịch (đường CHÍNH);
    ///   · <see cref="JobSchedulerService"/> — nạp lại khi thấy cache trống (đường lấp).
    /// KHÔNG được `HashSet` chồng lên khoá cũ — lý do ở JobConfigCache.WriteAsync.
    /// </summary>
    public const string ConfigHash = "SDI:JOB:CFG";

    /// <summary>
    /// TTL của cache cấu hình = LƯỚI CUỐI, không phải cơ chế chính.
    ///
    /// Đường chính là nạp lại ngay lúc đổi lịch (JobConfigService). TTL chỉ đỡ cho ca cấu hình bị
    /// đổi mà KHÔNG đi qua cổng đó — `UPDATE T_JOB_DEFINITION` thẳng bằng script vận hành / tool DB.
    /// Lúc đó trigger vẫn dọn lượt chờ ở DB, nhưng KHÔNG ai dọn cache, và bộ quét vẫn sinh mốc theo
    /// chu kỳ cũ cho tới khi khoá này hết hạn.
    ///
    /// Vì thế 5 phút, không phải 1 giờ: 1 giờ là con số hợp lý hồi TTL còn là đường DUY NHẤT, nay
    /// nó chỉ còn là thời gian TỐI ĐA một thay đổi "đi cửa sau" nằm im. Giá phải trả cho 5 phút gần
    /// như bằng 0 — cache hết hạn thì đúng một pod đọc DB một lần rồi ghi lại cho cả cụm.
    /// </summary>
    public static readonly TimeSpan ConfigTtl = TimeSpan.FromMinutes(5);

    public static TimeSpan SlotTtl(int intervalSec) => TimeSpan.FromSeconds(Math.Max(60, intervalSec * 2));
}
