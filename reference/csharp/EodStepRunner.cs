using System;
using System.Threading.Tasks;
using Serilog;

namespace SdiCoreMessagingProcess.Services;

/// <summary>
/// CHẠY TỪNG STEP TƯỜNG MINH — thay cho chain nối trong Kafka handler.
///
/// Gọi từ Controller / lệnh CLI / Hangfire / cron — CHỖ NÀO CŨNG ĐƯỢC, MIỄN KHÔNG PHẢI TRONG
/// KAFKA HANDLER. Mỗi step tự kiểm tra tiền đề của nó và trả err rõ ràng ⇒ nhìn err là biết kẹt ở đâu,
/// không còn cảnh "chain im lặng bị bỏ, không ai biết vì sao".
///
/// MỌI STEP ĐỀU IDEMPOTENT ⇒ gọi lại bao nhiêu lần cũng an toàn:
///   • SP_FEE_RUN_DAILY   — MERGE upsert; kỳ ĐÃ/ĐANG THU thì khoá, không đè
///   • SP_EOD_RUN_INDEX   — DELETE+INSERT index ngày đó
///   • SP_EOD_RUN         — SP_EOD_STEP có resume-gate: job đã DONE thì BỎ QUA
/// ⇒ Không cần distributed lock để bảo vệ tính đúng. (Lock chỉ để đỡ tốn CPU — nếu muốn.)
/// </summary>
public class EodStepRunner
{
    private readonly ISdiDbGateway _db;

    public EodStepRunner(ISdiDbGateway db) => _db = db;

    /// <summary>Step 2 — PHÍ. Chạy MỌI NGÀY LỊCH (kể cả T7/CN/lễ), độc lập với EOD.</summary>
    /// <remarks>
    /// Phí theo NGÀY DƯƠNG LỊCH (rate/365): tiền nằm trong TK ngày T7 thì vẫn chịu phí ngày T7.
    /// Trong khi EOD/index chỉ chạy NGÀY GD. Để phí trong EOD ⇒ MẤT PHÍ ~115 ngày nghỉ/năm.
    /// </remarks>
    public async Task<StepResult> RunFeeAsync(DateTime d)
    {
        var (err, msg) = await _db.FeeRunDailyAsync(d);
        Log(nameof(RunFeeAsync), d, err, msg);
        return new StepResult(err, msg);
    }

    /// <summary>Step 3 — INDEX danh mục mẫu. Chỉ NGÀY GD (err=13 nếu T7/CN/lễ).</summary>
    /// <remarks>err: 10=MKT chưa READY · 11=thiếu giá mã rổ · 13=không phải ngày GD</remarks>
    public async Task<StepResult> RunIndexAsync(DateTime d)
    {
        var (err, msg) = await _db.EodRunIndexAsync(d);
        Log(nameof(RunIndexAsync), d, err, msg);
        return new StepResult(err, msg);
    }

    /// <summary>Step 4 — EOD. ★ CỬA KHOÁ THẬT của toàn bộ luồng.</summary>
    /// <remarks>
    /// err: 10 = precondition chưa đủ (MKT/FO/ASSET_NAV chưa READY hoặc INDEX chưa DONE)
    ///      12 = ★ THIẾU Asset NAV cho N tiểu khoản ACTIVE của SDI  ⇐ POST-CHECK
    ///      13 = không phải ngày GD
    ///      -2 = reconcile BREAK (xem T_EOD_RECON_BREAK)
    ///
    /// err=12 là thứ DUY NHẤT SDI kiểm soát 100%: nó đọc registry của CHÍNH MÌNH (T_SI_PORTFOLIO)
    /// và đòi mọi SI ACTIVE phải có dòng T_SI_BALANCE @d. KHÔNG phụ thuộc bất kỳ con số nào Asset khai.
    /// ⇒ Cờ ASSET_NAV=READY bật SỚM (do totalRow sai / dedup thủng) là VÔ HẠI: EOD vẫn bị chặn tại đây.
    /// </remarks>
    public async Task<StepResult> RunEodAsync(DateTime d)
    {
        var (err, msg) = await _db.EodRunAsync(d);
        Log(nameof(RunEodAsync), d, err, msg);

        if (err == 12)
            Log.Warning("[EOD] {Date} — Asset gửi THIẾU tài khoản của SDI. EOD bị chặn (ĐÚNG). " +
                        "Chờ batch còn lại tới rồi gọi lại RunEodAsync. {Msg}", d, msg);

        return new StepResult(err, msg);
    }

    /// <summary>Chạy tuần tự 3 step, dừng ngay khi có step lỗi. KHÔNG gọi từ Kafka handler.</summary>
    public async Task<StepResult> RunAllAsync(DateTime d)
    {
        var fee = await RunFeeAsync(d);
        if (!fee.Ok) return fee;

        var idx = await RunIndexAsync(d);
        if (!idx.Ok) return idx;

        return await RunEodAsync(d);
    }

    private static void Log(string step, DateTime d, int err, string? msg)
    {
        if (err == 0) Serilog.Log.Information("[Step] {Step} {Date} OK", step, d);
        else          Serilog.Log.Warning("[Step] {Step} {Date} err={Err} {Msg}", step, d, err, msg);
    }
}

public readonly record struct StepResult(int Err, string? Msg)
{
    public bool Ok => Err == 0;
}
