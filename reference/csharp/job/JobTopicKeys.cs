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
///                                     đều bị SP_JOB_CLAIM_SLOT chặn (UQ/hạn tươi) hoặc C_MAX_DELAY_SEC
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
    /// KEY = fireKey của mốc (rải đều partition). KHÔNG dùng jobCode làm key: như thế mọi lượt FO
    /// dồn vào một partition ⇒ một consumer ⇒ một pod gánh hết. Thứ tự message KHÔNG quan trọng vì
    /// mọi quyết định nằm ở DB (SP_JOB_CLAIM_SLOT), nên rải đều là lựa chọn đúng.
    /// Số partition nên ≥ số pod muốn chạy job song song.
    /// </summary>
    public static string KeyOf(string fireKey) => fireKey;

    /// <summary>Nguồn đánh thức, ghi vào T_JOB_RUN.C_CLAIM_SOURCE để soi khi mổ xẻ sự cố.</summary>
    public const string SourceKafka = "kafka";
    public const string SourceRecover  = "recover";

}

/// <summary>
/// NỘI DUNG MESSAGE — chở MỐC, không chở id.
///
/// ★ VÌ SAO KHÔNG CHỞ jobRunId NỮA: bộ quét lịch KHÔNG còn ghi DB lúc sinh job, nên lúc produce
///   chưa có dòng T_JOB_RUN nào và cũng chưa có id nào tồn tại. Dòng đó do chính pod nhận được
///   message tạo ra, qua SP_JOB_CLAIM_SLOT, và sinh ra đã ở trạng thái RUNNING.
///
/// ★ VÌ SAO KHÔNG CHỞ THÊM GÌ (payload, khung giờ, chu kỳ): Kafka CÓ LƯU, nên một message chở bản
///   chụp cấu hình sẽ sống rất dai — job vừa bị sửa hay tắt vẫn chạy theo bản cũ đang nằm trong
///   topic. Chở đúng danh tính của MỐC rồi hỏi DB mọi thứ khác là cách duy nhất để cấu hình mới
///   luôn thắng.
///
/// `fireKey` chở kèm dù suy được từ `slotAt`: nó là khoá phân vùng và là thứ hiện trong log, nên
/// có sẵn thì khỏi phải tính lại ở mỗi chặng. DB vẫn tự suy lại và đối chiếu.
/// </summary>
public sealed class JobMessage
{
    [Newtonsoft.Json.JsonProperty("code")] public string JobCode { get; set; } = "";
    [Newtonsoft.Json.JsonProperty("key")]  public string FireKey { get; set; } = "";

    /// <summary>
    /// ★ MỐC ĐI TRÊN ĐƯỜNG TRUYỀN DƯỚI DẠNG CHUỖI, KHÔNG PHẢI `DateTime`. Cố ý.
    ///
    /// Mốc ở hệ này là GIỜ TƯỜNG VN — `UDF_JOB_NOW()` trả giờ VN, `T_JOB_RUN.C_SLOT_AT` là
    /// `DATETIME` không mang múi giờ. Để `DateTime` chạy qua JSON là mời trình tuần tự hoá làm
    /// phép cộng trừ múi giờ ở HAI đầu:
    ///   · pod sinh có `Kind=Local` ⇒ Newtonsoft ghi kèm `+07:00`;
    ///   · pod nhận (container thường đặt `TZ=UTC`) đọc lại ⇒ đổi về giờ máy nó ⇒ **09:15 thành 02:15**.
    /// Và ca đó KHÔNG kêu: lệch 7 tiếng vẫn chia hết cho mọi chu kỳ ≤ 1 giờ nên qua được cửa đối
    /// chiếu lưới (`err=20`), rồi chết lặng ở hạn tươi (`err=3`) — job không bao giờ chạy, không ai biết.
    ///
    /// Chuỗi `yyyy-MM-dd HH:mm:ss` + `ParseExact` với `DateTimeStyles.None` ⇒ `Kind=Unspecified`,
    /// không phép biến đổi nào ở cả hai đầu. Hôm nay `NowVn()` trả `Unspecified` nên bản `DateTime`
    /// cũng chạy đúng — nhưng nó đúng NHỜ MAY, và chỉ cần ai đó đổi sang `DateTime.Now` là hỏng.
    /// </summary>
    [Newtonsoft.Json.JsonProperty("slot")] public string SlotRaw { get; set; } = "";

    private const string SlotFormat = "yyyy-MM-dd HH:mm:ss";

    public DateTime SlotAt => DateTime.ParseExact(SlotRaw, SlotFormat,
        System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None);

    public static string Serialize(string jobCode, DateTime slotAt, string fireKey)
        => Newtonsoft.Json.JsonConvert.SerializeObject(new JobMessage {
               JobCode = jobCode,
               SlotRaw = slotAt.ToString(SlotFormat, System.Globalization.CultureInfo.InvariantCulture),
               FireKey = fireKey });

    /// <summary>Trả null nếu message không đọc được HOẶC mốc không đúng định dạng — người gọi bỏ qua.</summary>
    public static JobMessage? Parse(string raw)
    {
        try
        {
            var m = Newtonsoft.Json.JsonConvert.DeserializeObject<JobMessage>(raw);
            if (m == null || string.IsNullOrEmpty(m.JobCode)) return null;
            _ = m.SlotAt;              // ném ngay tại đây nếu mốc sai định dạng
            return m;
        }
        catch { return null; }
    }
}
