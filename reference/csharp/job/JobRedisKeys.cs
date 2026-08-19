using System;

namespace SdiCoreMessagingProcess.Jobs;

/// <summary>
/// KHOÁ REDIS của khung job. Redis ở đây làm ĐÚNG MỘT VIỆC: **lọc trước để khỏi hỏi DB**.
///
/// ★ VÌ SAO CẦN — đo thật, không phải cảm tính:
///   Bộ quét chạy 10 giây/lần trên MỌI pod. Với chu kỳ 15 phút thì **89/90 nhịp quét không có gì
///   tới hạn**. Trước khi có lớp lọc này, mỗi nhịp vẫn là một lượt gọi `SP_JOB_ENQUEUE_DUE`
///   (~50 logical reads): 10 pod × 6 lần/phút × 24 giờ = **~86.400 lượt quét/ngày để sinh 25 job**
///   — khoảng **3.400 lượt quét cho mỗi job**. Tải tuyệt đối thì nhỏ, nhưng đó là 3.400 lần hỏi
///   một câu đã biết trước câu trả lời.
///
///   Nay: pod tự tính mốc (không chạm DB) rồi `SET NX` khoá mốc đó. Ai đặt được khoá thì mới đi
///   tiếp xuống DB. ⇒ DB chỉ bị gọi **đúng một lần cho mỗi mốc** (~25 lượt/ngày).
///
/// ★ REDIS KHÔNG GIỮ MẢNH TÍNH ĐÚNG NÀO — vẫn nguyên tắc ①:
///   Mất khoá slot (Redis restart, evict, FLUSHALL) ⇒ nhiều pod cùng thấy "chưa ai làm" ⇒ cùng
///   gọi `SP_JOB_ENQUEUE` ⇒ **`UQ (C_JOB_CODE, C_FIRE_KEY)` cho đúng một pod thắng**, số còn lại
///   nhận `err=4`. Hậu quả tệ nhất của việc mất sạch Redis: vài lượt gọi DB thừa. Không job nào
///   chạy hai lần, không job nào mất.
///   ⇒ Đó là lý do lớp này được phép "sai": nó chỉ trả lời *"có đáng hỏi DB không"*, không trả lời
///     *"ai được chạy"*.
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
    /// Vé chạy `SP_JOB_RECOVER`. RECOVER idempotent nên chạy trùng KHÔNG sai — vé này thuần tuý để 10 pod
    /// không cùng làm một việc mỗi 30 giây (20 lượt/phút → 2). Lấy hụt vé thì bỏ qua nhịp, không chờ.
    /// </summary>
    public const string RecoverTicket = "SDI:JOB:RECOVER";

    /// <summary>
    /// Cache cấu hình lịch (HASH: jobCode → JSON). App XOÁ khoá này ngay sau khi đổi cấu hình qua
    /// `SP_SET_JOB_SCHEDULE`, nên nhịp quét kế tiếp đã thấy cấu hình mới.
    /// TTL 1 giờ là lưới an toàn: quên xoá thì cấu hình mới vẫn có hiệu lực chậm nhất sau 1 giờ,
    /// thay vì kẹt vĩnh viễn cho tới lần restart pod.
    /// </summary>
    public const string ConfigHash = "SDI:JOB:CFG";

    public static readonly TimeSpan ConfigTtl = TimeSpan.FromHours(1);
    public static readonly TimeSpan RecoverTicketTtl = TimeSpan.FromSeconds(25);   // < nhịp RECOVER 30s

    public static TimeSpan SlotTtl(int intervalSec) => TimeSpan.FromSeconds(Math.Max(60, intervalSec * 2));
}
