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
    private readonly BatchSyncService    _sync;
    private readonly BatchStatAggregator _stat;   // ★ sổ lũy kế: tổng bản ghi + tổng AUM
    private readonly IBoServices         _bo;

    public SyncAssetDataService(BatchSyncService sync, BatchStatAggregator stat, IBoServices bo)
    {
        _sync = sync;
        _stat = stat;
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

        // ── Chuẩn bị cho SỔ LŨY KẾ (tổng bản ghi + tổng AUM) ──────────────────────────────
        //    ⚠️ PHẢI dùng ĐÚNG batchKey mà BatchSyncService dùng (băm tập si_account). Khoá khác
        //       thì batch "gửi lại đã sửa" không nhận ra nhau ⇒ cộng chồng thay vì cộng phần chênh.
        //       Tính lại ở đây = 1 lần SHA256 trên chuỗi ngắn — rẻ hơn nhiều so với việc kéo thêm
        //       tham số ra/vào cái generic chỉ để chuyền một chuỗi.
        var batchKey = KafkaSyncKeys.BatchKey(assetResponseData.Select(x => x.si_account));

        //    ⚠️ ép (decimal?): IEnumerable<decimal> KHÔNG tự chuyển sang IEnumerable<decimal?>
        //       (kiểu giá trị không có covariance) ⇒ ép thế này thì aum khai decimal hay decimal? đều chạy.
        var aumInBatch = BatchStatAggregator.SumAum(assetResponseData.Select(x => (decimal?)x.aum));
        if (aumInBatch is null)
            Log.Error("[ProcessDataSyncAssetSdiCore] {Date} req={ReqId} — tổng AUM batch VƯỢT TẦM int64 " +
                      "({N} dòng). Batch VẪN ghi DB + đếm dòng bình thường, chỉ BỎ cộng AUM. " +
                      "Kiểm tra dữ liệu Asset gửi.", tranDate, model.requestId, assetResponseData.Count);

        // ── Ghi DB + cộng dồn + bật cờ. KHÔNG CHAIN. ──────────────────────────────────────
        var result = await _sync.SyncBatchAsync(
            requestId:      model.requestId,                    // ★ JOB_ID — không cần tạo, không cần chờ
            tranDate:       tranDate,
            redisKeyPrefix: RedisKeyDefine.EOD_ASSET,
            bizType:        BizTypeDefine.JOB_EOD_ASSET,
            siAccounts:     assetResponseData.Select(x => x.si_account),   // DANH TÍNH nghiệp vụ để dedup —
                                                                //    KHÔNG băm JSON thô (Asset đổi format/thứ tự
                                                                //    field là hash đổi ⇒ dedup mù)
            totalRow:       model.totalRow,                     // GỢI Ý (Asset khai) — không phải cổng chặn
            rowsInBatch:    assetResponseData.Count,            // ⚠️ đếm từ PAYLOAD, KHÔNG dùng @p_rows (số dòng
                                                                //    GHI ĐƯỢC — đã lọc acc lạ ⇒ không bao giờ khớp)
            // ★ SỔ LŨY KẾ NẰM Ở ĐÂY — trong hàm nghiệp vụ, KHÔNG trong script Lua của cái generic.
            //   BatchSyncService dùng chung cho mọi luồng batch, phần lớn KHÔNG có khái niệm AUM.
            //   Luồng nào có số để cộng thì tự cộng, ngay sau khi ghi DB xong. Xem BatchStatAggregator.
            executeFunc: async () =>
            {
                if (!await _bo.BulkInsertAssetToSdiCore(rawJson))
                    return false;   // BatchSyncService sẽ NÉM ⇒ không commit offset ⇒ Kafka giao lại

                // Cộng SAU KHI DB đã commit. Cộng trước mà pod chết giữa chừng ⇒ sổ có, dữ liệu không.
                // Lỗi Redis ở đây trả null + log WARNING, KHÔNG ném — mất sổ ≠ mất dữ liệu.
                var stat = await _stat.AccumulateAsync(
                    RedisKeyDefine.EOD_ASSET, model.requestId, batchKey,
                    rows: assetResponseData.Count, aum: aumInBatch);

                if (stat is not null)
                    Log.Information("[{Biz}] {Date} req={Req} +{N} dòng / +{AumN} AUM → " +
                                    "LŨY KẾ {Rows} dòng, AUM {Aum}",
                                    BizTypeDefine.JOB_EOD_ASSET, tranDate, model.requestId,
                                    assetResponseData.Count, aumInBatch, stat.Rows, stat.Aum);
                return true;
            });

        if (result.JobDone)
        {
            var final = await _stat.ReadAsync(RedisKeyDefine.EOD_ASSET, model.requestId);
            // ★ Chốt sổ: so với SELECT COUNT(*), SUM(C_AUM) FROM T_SI_BALANCE WHERE C_BUSINESS_DATE=@d
            //   là bắt được ngay "đủ dòng nhưng lệch tiền" — thứ mà đếm dòng KHÔNG BAO GIỜ thấy.
            //   Hiệu số phải ĐÚNG BẰNG phần của các acc không thuộc SDI (INNER JOIN registry lọc).
            Log.Information("[{Biz}] {Date} req={Req} — nhận đủ {Rows}/{Total} dòng, " +
                            "sổ lũy kế {StatRows} dòng / AUM {Aum}. " +
                            "Chạy tiếp bằng EodStepRunner (KHÔNG chain trong handler).",
                            BizTypeDefine.JOB_EOD_ASSET, tranDate, model.requestId,
                            result.Rows, result.Total, final.Rows, final.Aum);
        }

        // ❌ KHÔNG: ProcessJobAumFeeAcccrue(tranDate)
        // ❌ KHÔNG: ProcessJobAumFeeCharge(tranDate)
        // ❌ KHÔNG: SP_EOD_RUN(tranDate)
        // → return → consumer commit offset. Vài chục ms. KHÔNG BAO GIỜ bị Kafka đá.
    }
}
