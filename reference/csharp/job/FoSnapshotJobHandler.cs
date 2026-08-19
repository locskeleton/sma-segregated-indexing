using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Serilog;

namespace SdiCoreMessagingProcess.Jobs;

/// <summary>Một dòng tài sản FO trả về cho 1 tiểu khoản (khớp JSON của SP_INGEST_FO_SNAPSHOT_RT).</summary>
public sealed class FoSnapshotRow
{
    [JsonProperty("si_account")]       public string  SiAccount        { get; set; } = "";
    [JsonProperty("aum")]              public decimal Aum              { get; set; }
    [JsonProperty("cash")]             public decimal Cash             { get; set; }
    [JsonProperty("cash_available")]   public decimal? CashAvailable   { get; set; }
    [JsonProperty("dividend_pending")] public decimal? DividendPending { get; set; }
    [JsonProperty("sell_pending")]     public decimal? SellPending     { get; set; }
}

/// <summary>Cổng gọi FO. Cài đặt HTTP thật nằm ở repo dịch vụ; ở đây chỉ là ranh giới.</summary>
public interface IFoSnapshotClient
{
    Task<IReadOnlyList<FoSnapshotRow>> GetSnapshotAsync(IReadOnlyList<ScopeRow> batch, CancellationToken ct);
}

/// <summary>
/// JOB QUÉT SNAPSHOT FO — near-realtime cho dashboard PM.
///
/// LUỒNG (mô hình "một worker chạy cả chu kỳ"):
///   1. Lấy toàn bộ tiểu khoản indexing đang mở (SP_GET_FO_SNAPSHOT_SCOPE).
///   2. Gom theo KHÁCH HÀNG rồi cắt 50 KH/batch (đúng BRD).
///      ⚠️ 50 KH ≠ 50 tiểu khoản: một KH đầu tư K master thì mang theo K tiểu khoản
///        (UQ_SI_PORTFOLIO_ACTIVE cho phép mỗi KH 1 tiểu khoản ACTIVE TRÊN MỖI master).
///        Scope trả về đã sắp theo KH nên cắt batch không bao giờ xẻ đôi một khách hàng — nếu xẻ
///        thì cùng một KH bị hỏi ở hai batch, hai thời điểm, và số của họ khớp nhau chỉ do may mắn.
///   3. Chạy song song có giới hạn (mặc định 4). Trước MỖI call: TradingWindowGuard (tầng 4).
///   4. Mỗi batch trả về → ghi ngay (SP_INGEST_FO_SNAPSHOT_RT). KHÔNG gom hết rồi ghi một lần:
///      gom hết nghĩa là hỏng ở batch 999 thì mất trắng 998 batch trước đó.
///   5. Cuối chu kỳ → SP_RT_MASTER_AGG gộp cấp master ĐÚNG MỘT LẦN.
///      Gộp sau mỗi batch sẽ cho ra con số master "nửa cũ nửa mới" mà dashboard không phân biệt được.
///
/// HẾT GIỜ GIỮA CHỪNG: đây là kết cục BÌNH THƯỜNG, không phải lỗi. Dừng vòng lặp, VẪN gộp master
///   trên phần đã ghi được (dashboard hiện coverage n/N để người dùng biết là chưa đủ), trả về số
///   dòng đã xử lý. Không ném exception — ném thì lượt chạy bị đánh FAILED rồi retry, và retry
///   sau 15h chỉ tổ đập vào guard tầng 2 rồi SKIPPED, làm nhật ký đầy tiếng ồn màu đỏ vô nghĩa.
/// </summary>
public class FoSnapshotJobHandler : IJobHandler
{
    public string HandlerKey => "FoSnapshotJobHandler";

    private readonly ISdiJobGateway    _db;
    private readonly IFoSnapshotClient _fo;
    private readonly TradingWindowGuard _window;

    public FoSnapshotJobHandler(ISdiJobGateway db, IFoSnapshotClient fo, TradingWindowGuard window)
    {
        _db = db; _fo = fo; _window = window;
    }

    private sealed class Payload
    {
        [JsonProperty("batchSize")]  public int BatchSize { get; set; } = 50;
        [JsonProperty("parallel")]   public int Parallel  { get; set; } = 4;
        [JsonProperty("masterCode")] public string? MasterCode { get; set; }
    }

    public async Task<long> RunAsync(JobContext ctx, CancellationToken ct)
    {
        var cfg = string.IsNullOrWhiteSpace(ctx.PayloadJson)
            ? new Payload()
            : JsonConvert.DeserializeObject<Payload>(ctx.PayloadJson!) ?? new Payload();

        var bizDate = ctx.BusinessDate ?? TradingWindowGuard.NowVn().Date;

        // ★★ CHẶN TRƯỚC MỌI THỨ — trước khi đọc scope (~50k dòng), trước khi chia batch, trước khi
        //   chạm FO. Ngoài giờ giao dịch thì số near-realtime không có ý nghĩa gì với PM, nên không
        //   có lý do gì để tốn một câu query 50k dòng rồi mới phát hiện ra điều đó.
        //   BẮT exception ở đây thay vì để nó bay lên: phiên đóng KHÔNG phải lỗi của job. Ném lên
        //   thì dispatcher đánh FAILED → retry → đập vào guard tầng 2 → SKIPPED, và nhật ký có một
        //   vệt đỏ mỗi ngày lúc 15h mà không ai cần. Trả 0 là mô tả đúng chuyện đã xảy ra.
        try { await _window.EnsureOpenAsync(ctx.SlotAt, ct); }
        catch (TradingWindowClosedException ex)
        {
            Log.Warning("[FO-RT] Không bắt đầu chu kỳ: {Msg}", ex.Message);
            return 0;
        }

        var scope = await _db.GetFoSnapshotScopeAsync(cfg.MasterCode, ct);
        if (scope.Count == 0)
        {
            Log.Warning("[FO-RT] Không có tiểu khoản indexing nào đang mở — bỏ qua chu kỳ");
            return 0;
        }

        var batches = ChunkByCustomer(scope, cfg.BatchSize);
        Log.Information("[FO-RT] {Si} tiểu khoản / {Cust} KH → {N} batch ({B} KH/batch), song song {P}",
            scope.Count, scope.Select(s => s.CustCode).Distinct().Count(), batches.Count, cfg.BatchSize, cfg.Parallel);

        long written = 0, skippedEod = 0, failedBatch = 0;
        var closed = false;

        using var gate = new SemaphoreSlim(Math.Max(1, cfg.Parallel));
        var tasks = new List<Task>();

        foreach (var batch in batches)
        {
            // IsStillMine đọc từ RAM (0 query): mất lease hoặc job vừa bị TẮT thì dừng ngay ở
            //   ranh giới batch, không chờ tới lúc token bị huỷ giữa một lời gọi HTTP.
            if (ct.IsCancellationRequested || Volatile.Read(ref closed) || !ctx.IsStillMine()) break;

            await gate.WaitAsync(ct);
            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    if (Volatile.Read(ref closed)) return;

                    // ★★ GUARD TẦNG 4 — ngay trước khi chạm FO. Không có dòng này thì một chu kỳ
                    //    bắt đầu hợp lệ nhưng FO chậm vẫn bắn request sau khi số đã hết ý nghĩa.
                    //    Hạn tươi neo vào MỐC nên nó là mốc TUYỆT ĐỐI, không trôi theo tiến độ.
                    await _window.EnsureOpenAsync(ctx.SlotAt, ct);

                    var rows = await _fo.GetSnapshotAsync(batch, ct);
                    if (rows.Count == 0) return;

                    var json = JsonConvert.SerializeObject(rows);
                    var (err, msg, n, skipped) = await _db.IngestFoSnapshotRtAsync(
                        json, bizDate, TradingWindowGuard.NowVn(), ct);

                    if (err != 0)
                    {
                        // Một batch hỏng KHÔNG giết cả chu kỳ: 49 KH khác trong batch sau vẫn cần số.
                        //   Nhịp 15 phút sau sẽ ghi đè lại toàn bộ, nên thiệt hại tối đa = một nhịp
                        //   thiếu vài chục KH, và coverage n/N trên dashboard nói rõ điều đó.
                        Interlocked.Increment(ref failedBatch);
                        Log.Error("[FO-RT] Batch {Cust} ghi lỗi err={Err}: {Msg}", batch[0].CustCode, err, msg);
                        return;
                    }

                    Interlocked.Add(ref written, n);
                    if (skipped > 0) Interlocked.Add(ref skippedEod, skipped);

                    // ★ CHỈ ghi vào RAM — 0 câu truy vấn. Nhịp tim nền tự đẩy con số này xuống DB
                    //   mỗi ~30 giây. Bản đầu gọi thẳng heartbeat ở đây ⇒ 1000 câu UPDATE vào ĐÚNG
                    //   MỘT DÒNG mỗi chu kỳ, phát ra từ 4 luồng ⇒ điểm nghẽn khoá dòng tự chế.
                    ctx.ReportProgress(Interlocked.Read(ref written));
                }
                catch (TradingWindowClosedException ex)
                {
                    // Chuông 15h00 điểm giữa chu kỳ. Bật cờ để các batch còn lại không xuất phát nữa.
                    if (!Volatile.Read(ref closed))
                    {
                        Volatile.Write(ref closed, true);
                        Log.Warning("[FO-RT] {Msg} Dừng chu kỳ giữa chừng — phần đã ghi vẫn giữ.", ex.Message);
                    }
                }
                catch (Exception ex)
                {
                    Interlocked.Increment(ref failedBatch);
                    Log.Error(ex, "[FO-RT] Batch {Cust} lỗi", batch[0].CustCode);
                }
                finally { gate.Release(); }
            }, ct));
        }

        await Task.WhenAll(tasks);

        // Gộp master MỘT LẦN, kể cả khi chu kỳ dừng sớm — dashboard thà có số của 60% khách hàng
        //   kèm nhãn PARTIAL, còn hơn đứng im ở số của 15 phút trước mà không ai biết là số cũ.
        var (aggErr, aggMsg, aggRows) = await _db.RtMasterAggAsync(bizDate, CancellationToken.None);
        if (aggErr != 0)
            Log.Error("[FO-RT] Gộp master lỗi err={Err}: {Msg}", aggErr, aggMsg);

        Log.Information("[FO-RT] Chu kỳ {Fk}: ghi {W} tiểu khoản · {M} master · bỏ qua {S} (đã có số chốt) · {F} batch lỗi{C}",
            ctx.FireKey, written, aggRows, skippedEod, failedBatch, closed ? " · DỪNG SỚM do hết giờ" : "");

        if (skippedEod > 0)
            Log.Information("[FO-RT] {S} tiểu khoản đã có số CHỐT của hôm nay ⇒ RT không đè (đúng thiết kế)", skippedEod);

        return written;
    }

    /// <summary>
    /// Cắt batch theo KHÁCH HÀNG. Scope đã ORDER BY cust nên chỉ cần đếm số KH đã gặp — một KH
    /// không bao giờ bị chia vào hai batch.
    /// </summary>
    internal static List<List<ScopeRow>> ChunkByCustomer(IReadOnlyList<ScopeRow> scope, int custPerBatch)
    {
        var result  = new List<List<ScopeRow>>();
        var current = new List<ScopeRow>();
        string? lastCust = null;
        var custCount = 0;

        foreach (var r in scope)
        {
            if (r.CustCode != lastCust)
            {
                // Ranh giới KH mới: chỉ ở ĐÂY mới được đóng batch (không đóng giữa các tiểu khoản
                //   của cùng một khách hàng).
                if (custCount >= custPerBatch && current.Count > 0)
                {
                    result.Add(current);
                    current = new List<ScopeRow>();
                    custCount = 0;
                }
                lastCust = r.CustCode;
                custCount++;
            }
            current.Add(r);
        }
        if (current.Count > 0) result.Add(current);
        return result;
    }
}
