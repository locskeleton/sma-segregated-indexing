using System;
using StackExchange.Redis;

namespace SdiCoreMessagingProcess.Jobs;

/// <summary>
/// KHOÁ REDIS STREAMS cho hàng đợi job.
///
/// VAI TRÒ CỦA REDIS Ở ĐÂY — đọc kỹ trước khi sửa:
///   Stream là ĐƯỜNG VẬN CHUYỂN + CHUÔNG CỬA. Nó trả lời "có việc mới, dậy đi" trong vài ms
///   (XREADGROUP BLOCK) — thứ mà DB polling không cho rẻ như vậy.
///   Nó KHÔNG phải sổ cái. Sổ cái là T_JOB_RUN.
///
///   ⇒ Message trong stream chỉ chở ĐÚNG MỘT THỨ: <c>jobRunId</c>. Không chở payload, không chở
///     trạng thái, không chở gì có thể "cũ" so với DB. Worker cầm id rồi HỎI DB mọi thứ khác.
///     Nếu chở payload trong message thì một job bị sửa cấu hình / bị huỷ vẫn chạy theo bản chụp
///     cũ nằm trong stream — sai mà không ai thấy.
///
///   ⇒ Consumer group cho ta "mỗi message giao cho một consumer". Nhưng nó KHÔNG hứa exactly-once:
///     XAUTOCLAIM sau timeout, pod restart, hay đơn giản là XADD hai lần đều làm một jobRunId tới
///     hai nơi. Chống trùng THẬT nằm ở SP_JOB_CLAIM (một UPDATE có điều kiện). Xem db/11_JOB.sql.
///
///   ⇒ Mất sạch Redis (FLUSHALL / cụm không bền / TTL) ⇒ message biến mất trong khi T_JOB_RUN vẫn
///     ghi READY. SP_JOB_REAP quét đúng những dòng đó và trả lại để XADD. Chậm một nhịp, không mất.
/// </summary>
public static class JobStreamKeys
{
    /// <summary>Một stream duy nhất cho MỌI loại job — worker nào cũng chạy được job nào.</summary>
    public const string Stream = "SDI:JOB:STREAM";

    /// <summary>Consumer group dùng chung cho toàn bộ pod.</summary>
    public const string Group = "sdi-job-workers";

    /// <summary>Tên field trong entry. CHỈ có id — xem chú thích trên.</summary>
    public const string FieldJobRunId = "jobRunId";

    /// <summary>
    /// TRẦN CỨNG cho bộ nhớ stream. Dùng kèm `useApproximateMaxLength: true` (`MAXLEN ~`) để việc
    /// cắt bớt là O(1) theo biên node, thay vì O(N) như MAXLEN chính xác.
    ///
    /// Entry chỉ chở một trường `jobRunId`, các entry cùng node dùng chung tên trường ⇒ chi phí biên
    /// vài chục byte/entry ⇒ **trần ~2–5 MB**. Với job FO (24 XADD/ngày) thì phải hơn 5 năm mới chạm
    /// trần; nhưng nếu sau này có luồng fan-out (1000 job con/chu kỳ) thì chạm sau ~2 ngày rồi ĐỨNG
    /// YÊN. Đặt trần ngay từ đầu chính là để lúc đó không phải sửa gì.
    ///
    /// ⚠️ Cắt bớt KHÔNG dọn tham chiếu PEL trỏ tới entry vừa bị cắt — cái bẫy kinh điển của Streams.
    ///   Hệ này luôn XACK (kể cả nhánh bỏ qua) nên PEL gần như rỗng; đừng bỏ XACK ở bất kỳ nhánh nào.
    ///
    /// Lịch sử THẬT nằm ở T_JOB_RUN (tra được, có index, biết ai chạy khi nào) — stream không phải sổ.
    /// </summary>
    public const int MaxLen = 50_000;

    /// <summary>Entry treo quá lâu trong pending list ⇒ pod nhận nó đã chết ⇒ XAUTOCLAIM.</summary>
    public static readonly TimeSpan PendingIdle = TimeSpan.FromMinutes(2);

    /// <summary>Tạo consumer group nếu chưa có. Gọi lúc pod khởi động; chạy nhiều pod vẫn an toàn.</summary>
    public static async System.Threading.Tasks.Task EnsureGroupAsync(IDatabase redis)
    {
        try
        {
            // "$" = chỉ nhận message MỚI. Nếu dùng "0" thì pod đầu tiên khởi động sẽ hút lại toàn bộ
            //   lịch sử stream và chạy lại hàng nghìn lượt job cũ — tất cả đều bị SP_JOB_CLAIM chặn
            //   (trạng thái đã DONE), nhưng vẫn tốn một trận bão query vô nghĩa lúc deploy.
            await redis.StreamCreateConsumerGroupAsync(Stream, Group, StreamPosition.NewMessages, createStream: true);
        }
        catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP"))
        {
            // Group đã tồn tại (pod khác tạo trước) — đúng ý đồ, không phải lỗi.
        }
    }
}
