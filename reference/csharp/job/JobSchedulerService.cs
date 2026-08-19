using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Serilog;

namespace SdiCoreMessagingProcess.Jobs;

/// <summary>
/// BỘ QUÉT LỊCH + LƯỚI AN TOÀN — chạy trên MỌI pod (không cần bầu leader, không cần lock phân tán).
///
/// Hai việc, hai nhịp riêng:
///   ① 10s/lần — `SP_JOB_ENQUEUE_DUE`: mốc nào tới hạn thì tạo lượt chạy rồi produce Kafka.
///      Chạy trên mọi pod mà không sinh trùng, vì `UQ (job_code, fire_key)` trong DB quyết ai
///      thắng. Không cần leader election — thứ mà mỗi lần hỏng lại hỏng vào lúc không ai để ý.
///   ② 30s/lần — `SP_JOB_REAP`: thu hồi lượt của pod đã chết, đóng dấu lượt quá hạn, và produce
///      lại những lượt `READY` chưa ai chạy.
///
/// ★ ② LÀ LƯỚI AN TOÀN — và với Kafka nó thực sự chỉ là lưới, khác hẳn thời dùng Redis Pub/Sub.
///   Pub/Sub bắn-rồi-quên nên mọi trục trặc kết nối đều làm mất tin, và ② phải gánh vai đường
///   hồi phục CHÍNH. Kafka có lưu: message sống qua restart pod/broker, consumer đọc tiếp từ
///   offset cũ. ② chỉ còn phải lo ba ca hiếm: produce hụt, mọi pod đều bận, và lượt bị REAP thu
///   hồi từ pod chết.
///
/// ★ KHUNG GIỜ: `SP_JOB_ENQUEUE_DUE` tự lọc, và nó lọc trên **MỐC SLOT** chứ không phải trên thời
///   điểm quét — nhờ vậy mốc 15:00 vẫn sinh được dù bộ quét chạy lúc 15:00:04. Scheduler KHÔNG tự
///   kiểm giờ: luật giờ nằm ĐÚNG MỘT CHỖ (`UDF_JOB_IN_WINDOW`). Cài luật ở cả C# lẫn SQL là cách
///   chắc chắn để một ngày nào đó hai bên hiểu khác nhau, và bên sai sẽ là bên gọi FO lúc 15h05.
/// </summary>
public class JobSchedulerService : BackgroundService
{
    private static readonly TimeSpan ScanEvery = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan ReapEvery = TimeSpan.FromSeconds(30);

    private readonly IProducer<string, string> _producer;
    private readonly ISdiJobGateway            _db;
    private readonly string                    _owner;

    private DateTime _lastReap = DateTime.MinValue;

    public JobSchedulerService(IProducer<string, string> producer, ISdiJobGateway db)
    {
        _producer = producer;
        _db       = db;
        _owner    = $"{Environment.MachineName}#{Environment.ProcessId}";
    }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        Log.Information("[JOB] Scheduler khởi động trên pod {Owner}", _owner);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                // ① tới hạn → tạo lượt → rung chuông
                var due = await _db.EnqueueDueAsync(ct);
                await NotifyAsync(due, JobTopicKeys.SourceKafka, ct);

                // ② lưới an toàn
                if (DateTime.UtcNow - _lastReap > ReapEvery)
                {
                    _lastReap = DateTime.UtcNow;
                    var revived = await _db.ReapAsync(staleSec: 30, ct);
                    if (revived.Count > 0)
                        Log.Warning("[JOB] REAP: {N} lượt READY không ai chạy — produce lại", revived.Count);
                    await NotifyAsync(revived, JobTopicKeys.SourceReap, ct);
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex, "[JOB] Scheduler lỗi một nhịp — bỏ qua, nhịp sau chạy tiếp");
            }

            try { await Task.Delay(ScanEvery, ct); } catch { break; }
        }
    }

    /// <summary>
    /// Message chỉ chở `jobRunId` — không payload, không trạng thái. Worker cầm id rồi hỏi DB mọi
    /// thứ khác; chở bản chụp cấu hình trong message là mở đường cho một job vừa bị sửa/tắt vẫn
    /// chạy theo bản cũ đang nằm trong topic (Kafka có lưu, nên bản cũ đó sống rất dai).
    ///
    /// Produce hụt KHÔNG phải thảm hoạ: dòng `T_JOB_RUN` đã nằm đó ở trạng thái `READY` và nhịp
    /// REAP sau sẽ produce lại. Vì thế chỉ log WARNING, không ném — ném là biến một sự cố tự hồi
    /// phục thành một lượt chạy FAILED.
    /// </summary>
    private async Task NotifyAsync(IReadOnlyList<DueJob> jobs, string src, CancellationToken ct)
    {
        foreach (var j in jobs)
        {
            try
            {
                var dr = await _producer.ProduceAsync(JobTopicKeys.Topic,
                    new Message<string, string>
                    {
                        Key   = JobTopicKeys.KeyOf(j.JobRunId),   // rải đều partition — xem JobTopicKeys
                        Value = j.JobRunId.ToString()
                    }, ct);

                Log.Information("[JOB] {Src} → {Code} runId={Id} vào {TP}@{Off}",
                    src, j.JobCode, j.JobRunId, dr.TopicPartition, dr.Offset);
            }
            catch (ProduceException<string, string> ex)
            {
                Log.Warning(ex, "[JOB] Produce hụt cho runId={Id} ({Reason}) — REAP sẽ produce lại",
                    j.JobRunId, ex.Error.Reason);
            }
            catch (Exception ex)
            {
                Log.Warning(ex, "[JOB] Produce hụt cho runId={Id} — REAP sẽ produce lại", j.JobRunId);
            }
        }
    }
}
