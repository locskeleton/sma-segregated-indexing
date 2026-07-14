using System;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Serilog;

namespace SdiCoreMessagingProcess.Services.Implement;

/// <summary>
/// THAY THẾ <c>SyncAssetDataService.ProcessDataSyncAssetSdiCore</c>.
///
/// ★ ĐÃ NGẮT KHỎI EOD/CHAIN.
///   Consumer CHỈ làm 1 việc: ghi DB → cộng dồn → bật cờ. XONG.
///   KHÔNG gọi ProcessJobAumFeeAcccrue / ProcessJobAumFeeCharge / SP_EOD_RUN.
///
///   VÌ SAO KHÔNG BAO GIỜ CHẠY CHAIN TRONG HANDLER:
///     chain (accrue phí toàn bộ SI + chốt phí) chạy vài phút TRONG handler
///       → consumer không kịp poll → vượt max.poll.interval
///       → Kafka ĐÁ POD khỏi group (nhưng KHÔNG GIẾT THREAD — pod cũ vẫn ghi DB = ZOMBIE)
///       → partition giao pod khác → message được xử lý LẠI song song
///       → duplicate → hỏng bộ đếm → rebalance tiếp → VÒNG XOÁY TỰ KHUẾCH ĐẠI.
///
///     Và: JobDone là bool trong RAM của ĐÚNG MỘT message. Message đó lỗi/trùng/pod chết
///     ⇒ sự kiện MẤT LUÔN, không ai dựng lại ⇒ đúng triệu chứng "bỏ qua chain phía sau".
///
///   Các step sau chạy RIÊNG, tường minh (xem EodStepRunner):
///     1. Asset ingest  → consumer này            → C_ASSET_NAV_STATUS = READY
///     2. Phí           → SP_FEE_RUN_DAILY  @d    (MỌI ngày lịch, độc lập)
///     3. Index         → SP_EOD_RUN_INDEX  @d    (khi BO có giá)
///     4. EOD           → SP_EOD_RUN        @d    (err=12 nếu thiếu SI của SDI ⇐ CỬA KHOÁ THẬT)
/// </summary>
public class SyncAssetDataService : ISyncAssetDataService
{
    private readonly BatchSyncService _sync;
    private readonly IBoServices      _bo;

    public SyncAssetDataService(BatchSyncService sync, IBoServices bo)
    {
        _sync = sync;
        _bo   = bo;
    }

    /// <summary>Xử lý event đồng bộ dữ liệu cuối ngày của các tài khoản tham gia SDI.</summary>
    public async Task ProcessDataSyncAssetSdiCore(AssetSdiCoreConsumerKafkaModel model)
    {
        var assetJsonString   = JsonConvert.SerializeObject(model.data);   // ⚠️ CHỈ data — để băm danh tính batch
        var assetResponseData = JsonConvert.DeserializeObject<List<IndexingDataSyncModel>>(assetJsonString);

        if (assetResponseData == null || assetResponseData.Count == 0)
        {
            Log.Error("[ProcessDataSyncAssetSdiCore] request id: {ReqId} — batch RỖNG", model.requestId);
            return;
        }

        var tranDate = assetResponseData.Select(m => m.date).FirstOrDefault();
        if (string.IsNullOrWhiteSpace(tranDate) || !Utils.IsValidDate(tranDate))
        {
            Log.Error("[ProcessDataSyncAssetSdiCore] request id: {ReqId} — dữ liệu transDate: {Date} " +
                      "nhận đồng bộ từ asset không hợp lệ", model.requestId, tranDate);
            return;
        }
        if (string.IsNullOrWhiteSpace(model.requestId))
        {
            Log.Error("[ProcessDataSyncAssetSdiCore] {Date} — THIẾU requestId. Asset PHẢI gửi requestId " +
                      "(chung cho mọi batch của job) — nó là JOB_ID.", tranDate);
            return;
        }

        var rawJson = JsonConvert.SerializeObject(model);

        // ── Ghi DB + cộng dồn + bật cờ. KHÔNG CHAIN. ──────────────────────────────────────
        var result = await _sync.SyncBatchAsync(
            requestId:      model.requestId,                    // ★ JOB_ID — không cần tạo, không cần chờ
            tranDate:       tranDate,
            redisKeyPrefix: RedisKeyDefine.EOD_ASSET,
            bizType:        BizTypeDefine.JOB_EOD_ASSET,
            dataJson:       assetJsonString,                    // băm CHỈ phần data
            totalRow:       model.totalRow,                     // GỢI Ý (Asset khai) — không phải cổng chặn
            rowsInBatch:    assetResponseData.Count,            // ⚠️ đếm từ PAYLOAD, KHÔNG dùng @p_rows (số dòng
                                                                //    GHI ĐƯỢC — đã lọc acc lạ ⇒ không bao giờ khớp)
            executeFunc:    () => _bo.BulkInsertAssetToSdiCore(rawJson));

        if (result.JobDone)
            Log.Information("[{Biz}] {Date} req={Req} — nhận đủ {Rows}/{Total} dòng. " +
                            "Chạy tiếp bằng EodStepRunner (KHÔNG chain trong handler).",
                            BizTypeDefine.JOB_EOD_ASSET, tranDate, model.requestId, result.Rows, result.Total);

        // ❌ KHÔNG: ProcessJobAumFeeAcccrue(tranDate)
        // ❌ KHÔNG: ProcessJobAumFeeCharge(tranDate)
        // ❌ KHÔNG: SP_EOD_RUN(tranDate)
        // → return → consumer commit offset. Vài chục ms. KHÔNG BAO GIỜ bị Kafka đá.
    }
}
