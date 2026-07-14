using System;
using System.Threading.Tasks;
using Serilog;

namespace SdiCoreMessagingProcess.Services;

/// <summary>
/// CHẠY TỪNG STEP TƯỜNG MINH — thay cho chain nối trong Kafka handler.
///
/// Gọi từ Controller / CLI / Hangfire / cron — CHỖ NÀO CŨNG ĐƯỢC, MIỄN KHÔNG PHẢI TRONG KAFKA HANDLER.
/// Mỗi step tự kiểm tra tiền đề và trả err rõ ràng ⇒ nhìn err là biết kẹt ở đâu, không còn cảnh
/// "chain im lặng bị bỏ, không ai biết vì sao".
///
/// Mỗi step chạy qua JobRunner ⇒ vẫn có log job (LogJobKafkaStarting/updateJob) + đo thời gian
/// + khoá best-effort — GIỮ NGUYÊN mọi thứ hàm generic cũ đang cho, chỉ bỏ phần đếm-batch mà mấy
/// job này vốn KHÔNG cần (trước phải nhét msg="{}", totalRow=1, numProcessed=1 để lừa máy đếm).
/// </summary>
public class EodStepRunner
{
    private readonly ISdiDbGateway _db;
    private readonly JobRunner     _job;

    public EodStepRunner(ISdiDbGateway db, JobRunner job)
    {
        _db  = db;
        _job = job;
    }

    /// <summary>Step 2 — PHÍ. Chạy MỌI NGÀY LỊCH (kể cả T7/CN/lễ), độc lập với EOD.</summary>
    /// <remarks>
    /// Phí theo NGÀY DƯƠNG LỊCH (rate/365): tiền nằm trong TK ngày T7 vẫn chịu phí ngày T7.
    /// EOD/index thì chỉ chạy NGÀY GD. Để phí trong EOD ⇒ MẤT PHÍ ~115 ngày nghỉ/năm.
    /// </remarks>
    public Task<JobRunResult> RunFeeAsync(DateTime d) =>
        _job.RunOnceAsync(BizTypeDefine.JOB_EOD_AUM_FEE_ACCRUE, D(d), async () =>
        {
            var (err, msg, rows) = await _db.FeeRunDailyAsync(d);
            return (err == 0, rows, rows, err == 0 ? 0 : 1, msg);
        });

    /// <summary>Step 3 — INDEX danh mục mẫu. Chỉ NGÀY GD.</summary>
    /// <remarks>err: 10=MKT chưa READY · 11=thiếu giá mã rổ · 13=không phải ngày GD</remarks>
    public Task<JobRunResult> RunIndexAsync(DateTime d) =>
        _job.RunOnceAsync(BizTypeDefine.JOB_EOD_INDEX, D(d), async () =>
        {
            var (err, msg, rows) = await _db.EodRunIndexAsync(d);
            return (err == 0, rows, rows, err == 0 ? 0 : 1, msg);
        });

    /// <summary>Step 4 — EOD. ★ CỬA KHOÁ THẬT của toàn bộ luồng.</summary>
    /// <remarks>
    /// err: 10 = precondition chưa đủ (MKT/FO/ASSET_NAV chưa READY hoặc INDEX chưa DONE)
    ///      12 = ★ THIẾU Asset NAV cho N tiểu khoản ACTIVE của SDI   ⇐ POST-CHECK
    ///      13 = không phải ngày GD
    ///      -2 = reconcile BREAK (xem T_EOD_RECON_BREAK)
    ///
    /// err=12 là thứ DUY NHẤT SDI kiểm soát 100%: nó đọc registry của CHÍNH MÌNH (T_SI_PORTFOLIO)
    /// và đòi mọi SI ACTIVE phải có dòng T_SI_BALANCE @d. KHÔNG phụ thuộc con số nào Asset khai.
    /// ⇒ Cờ ASSET_NAV=READY bật SỚM (totalRow sai / dedup thủng) là VÔ HẠI: EOD vẫn bị chặn tại đây.
    /// </remarks>
    public async Task<JobRunResult> RunEodAsync(DateTime d)
    {
        var r = await _job.RunOnceAsync(BizTypeDefine.JOB_EOD_ASSET, D(d), async () =>
        {
            var (err, msg, rows) = await _db.EodRunAsync(d);

            if (err == 12)
                Log.Warning("[EOD] {Date} — Asset gửi THIẾU tài khoản của SDI. EOD bị chặn (ĐÚNG). " +
                            "Chờ batch còn lại tới rồi gọi lại RunEodAsync. {Msg}", d, msg);

            return (err == 0, rows, rows, err == 0 ? 0 : 1, msg);
        });
        return r;
    }

    /// <summary>Chạy tuần tự 3 step, dừng ngay khi có step lỗi. ⚠️ KHÔNG gọi từ Kafka handler.</summary>
    public async Task<JobRunResult> RunAllAsync(DateTime d)
    {
        var fee = await RunFeeAsync(d);
        if (!fee.Ok && fee.Status != JobRunStatus.SkippedRunningElsewhere) return fee;

        var idx = await RunIndexAsync(d);
        if (!idx.Ok && idx.Status != JobRunStatus.SkippedRunningElsewhere) return idx;

        return await RunEodAsync(d);
    }

    private static string D(DateTime d) => d.ToString("yyyy-MM-dd");
}
