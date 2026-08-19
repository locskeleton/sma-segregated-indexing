namespace SdiCoreMessagingProcess.Jobs;

/// <summary>
/// TOPIC KAFKA — chuông cửa đánh thức worker. Chỉ có thế.
///
/// VÌ SAO KAFKA CHỨ KHÔNG PHẢI REDIS (chốt 2026-08-19):
///   Hệ đã có sẵn Kafka để xử lý event, đã có consumer pattern, monitoring, alerting, người trực
///   quen mặt. Dựng thêm một tầng nhắn tin thứ hai bằng Redis là bắt cả tổ chức phải nuôi hai thứ
///   làm cùng một việc. Đổi sang Kafka **bớt đi** một công nghệ, không phải đổi ngang.
///
///   Và Kafka còn TỐT HƠN Redis Pub/Sub ở đúng chỗ quan trọng: nó **bền**. Pub/Sub bắn-rồi-quên
///   nên `SP_JOB_RECOVER` phải gánh vai đường hồi phục chính; Kafka là log có lưu nên message sống
///   qua restart pod/broker, và `SP_JOB_RECOVER` trở lại đúng vai **lưới an toàn**.
///
/// ⚠️⚠️ LUẬT SỐ MỘT — ĐỌC TRƯỚC KHI SỬA `JobDispatcherService`:
///   **KHÔNG BAO GIỜ chạy job bên trong vòng poll của consumer.**
///   `docs/SDI-kafka-batch-sync-design.md` §6 đã ghi bằng máu bài học này:
///     "chain chạy trong handler → block consumer → vượt max.poll.interval → Kafka đá pod (nhưng
///      KHÔNG giết thread — nó vẫn ghi DB!) → zombie ghi song song → rebalance → giao lại →
///      duplicate → vòng xoáy tự khuếch đại."
///   Một chu kỳ quét FO chạy VÀI PHÚT (1000 batch), `max.poll.interval.ms` mặc định 5 phút.
///   ⇒ Consume → claim → **commit offset NGAY** → ném job sang Task nền. Vòng poll luôn rảnh.
///
///   (Khung job hiện tại có thứ luồng cũ không có: lease + heartbeat. Pod bị đá vẫn heartbeat nên
///    vẫn giữ lease, pod mới claim nhận err=5 ⇒ vòng xoáy KHÔNG hình thành. Nhưng đó là lưới cứu,
///    không phải lý do để cố tình nhảy xuống vực.)
///
/// CẤU HÌNH CONSUMER — ba dòng không được đổi:
///   enable.auto.commit  = false     → commit TAY, ngay sau claim
///   auto.offset.reset   = latest    → group mới KHÔNG phát lại cả topic. Với `earliest`, một
///                                     consumer group mới sẽ dội về hàng nghìn jobRunId cũ; tất cả
///                                     đều bị SP_JOB_CLAIM chặn (đã DONE) hoặc C_MAX_DELAY_SEC
///                                     đóng dấu SKIPPED — vô hại, nhưng là một trận bão query
///                                     mỗi lần ai đó đổi tên group.
///   max.poll.interval.ms = mặc định → giữ nguyên; ta không cần nới vì không chạy job trong poll.
/// </summary>
public static class JobTopicKeys
{
    /// <summary>
    /// Topic RIÊNG cho khung job, tách khỏi luồng ingest Asset. Cách ly hai chiều: một chu kỳ FO
    /// chậm không đẩy lag sang luồng ingest, và ngược lại. Chỉ ~25 message/ngày nên chi phí một
    /// topic gần như bằng 0.
    /// </summary>
    public const string Topic = "sdi.job.notify";

    /// <summary>Consumer group dùng chung cho mọi pod.</summary>
    public const string Group = "sdi-job-workers";

    /// <summary>
    /// KEY = jobRunId (rải đều partition). KHÔNG dùng jobCode làm key: như thế mọi lượt FO dồn vào
    /// một partition ⇒ một consumer ⇒ một pod gánh hết. Thứ tự message KHÔNG quan trọng ở đây vì
    /// mọi quyết định đều nằm ở DB (SP_JOB_CLAIM), nên rải đều là lựa chọn đúng.
    /// Số partition nên ≥ số pod muốn chạy job song song.
    /// </summary>
    public static string KeyOf(long jobRunId) => jobRunId.ToString();

    /// <summary>Nguồn đánh thức, ghi vào T_JOB_RUN.C_CLAIM_SOURCE để soi khi mổ xẻ sự cố.</summary>
    public const string SourceKafka = "kafka";
    public const string SourceRecover  = "recover";
}
