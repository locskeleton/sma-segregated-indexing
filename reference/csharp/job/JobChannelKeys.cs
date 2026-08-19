using StackExchange.Redis;

namespace SdiCoreMessagingProcess.Jobs;

/// <summary>
/// KÊNH REDIS PUB/SUB — chuông cửa đánh thức worker. Chỉ có thế.
///
/// VAI TRÒ (đọc kỹ trước khi sửa):
///   Pub/Sub là **bắn-rồi-quên**. Nó KHÔNG lưu gì, KHÔNG có hàng đợi, KHÔNG có pending list.
///   Thông báo phát ra lúc không pod nào đang nghe là **mất luôn** — và ở thiết kế này điều đó
///   HOÀN TOÀN CHẤP NHẬN ĐƯỢC, vì sổ cái là `T_JOB_RUN` chứ không phải Redis: `SP_JOB_REAP`
///   (30 giây/lần) thấy dòng `READY` không ai chạy thì phát lại. Mất chuông ⇒ chậm ≤30 giây.
///
///   ⇒ Redis ở đây có thể `FLUSHALL`, restart, mất kết nối, hay biến mất hẳn — job **không mất,
///     không chạy hai lần**. Đó là ý nghĩa của nguyên tắc ①.
///
/// VÌ SAO PUB/SUB CHỨ KHÔNG PHẢI STREAMS (đổi 2026-08-19):
///   Bản đầu dùng Redis Streams + consumer group. Nhưng thứ DUY NHẤT Streams cho thêm là khả năng
///   cứu message đã giao cho một pod rồi pod đó chết (PEL + `XAUTOCLAIM`) — mà `SP_JOB_REAP` đã
///   làm đúng việc đó, và làm tốt hơn vì nó dựng lại từ sổ cái thật chứ không từ bản sao trong
///   Redis. Toàn bộ bộ máy consumer group / PEL / `XAUTOCLAIM` / dọn consumer chết là **trùng lặp**.
///   Trong khi đó `StackExchange.Redis` KHÔNG hỗ trợ lệnh chặn ⇒ Streams buộc phải hỏi thăm
///   500ms/lần, còn Pub/Sub là **đẩy thật** (kết nối subscriber riêng, callback gọi về).
///
///   Kết quả của việc đổi:
///     · độ trễ đánh thức: ≤500ms → **~1ms**
///     · tải Redis lúc rỗi (10 pod): 20 lệnh/giây → **0**
///     · bộ nhớ Redis: ~5 MB + phải dọn consumer định kỳ → **0**
///     · code phải nuôi: group, XACK, PEL, XAUTOCLAIM, dọn consumer → **Subscribe + Publish**
///     · tính đúng: **không đổi một chút nào** (nó vốn không nằm ở Redis)
///
/// ĐÁNH ĐỔI DUY NHẤT: Pub/Sub phát cho MỌI pod, nên cả N pod cùng lao vào `SP_JOB_CLAIM` và
///   N−1 pod nhận `err=5`. Với 24 lượt job/ngày thì đó là ~240 lượt claim hụt/ngày — không đáng
///   kể. `JobDispatcherService` còn chặn bớt bằng hạn mức job chạy đồng thời: pod đang bận thì
///   không thèm claim, nhường pod rảnh.
///
/// ⚠️ REDIS CLUSTER: Pub/Sub thường (`PUBLISH`) phát tới MỌI node trong cụm. Chạy cụm lớn thì
///   nên đổi sang Sharded Pub/Sub (`SPUBLISH`, Redis 7+) để thông báo không lan ra toàn cụm.
///   Một node đơn / sentinel thì không cần bận tâm.
/// </summary>
public static class JobChannelKeys
{
    /// <summary>Một kênh duy nhất cho MỌI loại job — pod nào cũng chạy được job nào.</summary>
    public const string Notify = "SDI:JOB:NOTIFY";

    public static RedisChannel NotifyChannel => RedisChannel.Literal(Notify);

    /// <summary>Nguồn đánh thức, ghi vào T_JOB_RUN.C_CLAIM_SOURCE để soi khi mổ xẻ sự cố.</summary>
    public const string SourceNotify = "notify";
    public const string SourceReap   = "reap";
}
