/*==============================================================================
  SDI — SMOKE TEST KHUNG JOB + SNAPSHOT NEAR-REALTIME FO (11_JOB.sql).
  Chạy SAU 01_TABLES + 02_SP_ENGINE + 11_JOB (09_FEE nếu muốn ca "RT không vào phí").

  TRỌNG TÂM — 4 yêu cầu BRD, mỗi cái phải có ca chứng minh:
    (1) Khung chạy được với MỌI loại job  → seed 1 job giả (không phải FO) chạy qua cùng bộ proc.
    (2) Đẩy job vào là xử lý ngay         → SP_JOB_CLAIM_SLOT sinh dòng ĐÃ Ở 'RUNNING' ⇒ chạy NGAY, không chờ.
    (3) Nhiều pod KHÔNG xử lý trùng       → 2 pod claim cùng 1 lượt: đúng 1 thắng.
    (4) FO chỉ chạy 9h–15h ngày GD        → chặn ở CẢ 3 tầng (sinh job / nhận job / ghi dữ liệu).
  Và quan trọng nhất, phần dễ hỏng âm thầm nhất:
    (5) Dòng RT KHÔNG lây sang đường EOD  → err=12, phí, báo cáo AUM đều phải MÙ với dòng RT.

  ⚠️ PHỤ THUỘC THỜI GIAN THẬT: proc RT cố tình chỉ nhận NGÀY HÔM NAY và chỉ ngày GD (guard tầng 3).
     Nên khối (B) chỉ chạy được vào NGÀY GIAO DỊCH. Chạy vào T7/CN thì khối đó tự BỎ QUA và in
     cảnh báo — KHÔNG báo FAIL giả. Khối (A) khung job chạy được mọi lúc (tự chỉnh khung giờ job giả).
==============================================================================*/
SET NOCOUNT ON; SET QUOTED_IDENTIFIER ON; SET ANSI_NULLS ON;
GO
/*  SP_SMK_SCAN — mô phỏng MỘT vòng của hệ thật: bộ quét trên pod tính mốc → produce Kafka →
      một pod nhận message và gọi SP_JOB_CLAIM_SLOT.
    Ghi các lượt GIÀNH ĐƯỢC vào #scan (temp của batch gọi — nested proc nhìn thấy được).
    Ở prod còn có Redis SET NX lọc trước và Kafka ở giữa; bỏ qua ở đây vì cả hai chỉ là bộ lọc/đường
    vận chuyển, KHÔNG tham gia tính đúng — UQ(job_code, fire_key) mới là thứ quyết định ai chạy.
    @p_owner: đổi owner giữa hai lần gọi để mô phỏng HAI POD khác nhau cùng nhận một mốc.          */
CREATE OR ALTER PROCEDURE SP_SMK_SCAN @p_owner VARCHAR(64) = 'pod-A', @p_now DATETIME = NULL
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @now DATETIME = ISNULL(@p_now, dbo.UDF_JOB_NOW());
    DECLARE @code VARCHAR(40), @slot DATETIME, @ec INT, @em NVARCHAR(400), @id BIGINT;
    DECLARE c CURSOR LOCAL FAST_FORWARD FOR
        SELECT C_JOB_CODE FROM T_JOB_DEFINITION WHERE C_ENABLED=1 AND C_INTERVAL_SEC IS NOT NULL;
    OPEN c; FETCH NEXT FROM c INTO @code;
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @slot = dbo.UDF_JOB_SLOT_AT(@code, @now);
        SET @id = NULL;
        EXEC SP_JOB_CLAIM_SLOT @p_job_code=@code, @p_slot_at=@slot, @p_owner=@p_owner, @p_source='kafka',
             @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_job_run_id=@id OUTPUT;
        IF @ec = 0 AND @id IS NOT NULL INSERT #scan (id, code) VALUES (@id, @code);
        FETCH NEXT FROM c INTO @code;
    END
    CLOSE c; DEALLOCATE c;
END
GO

-- 3 cột: khớp CẢ result set của SP_SMK_SCAN (id,code) lẫn SP_JOB_RECOVER (id,code,payload).
--   Lệch cột thì INSERT..EXEC lỗi, và vì SP_JOB_RECOVER có XACT_ABORT ON nên nó ROLLBACK luôn cả
--   phần UPDATE đã chạy trước đó — hỏng im lặng, khó lần.
CREATE TABLE #scan (id BIGINT, code VARCHAR(40), payload NVARCHAR(MAX));
-- SP_JOB_RECOVER nay trả MỐC (code, slot_at, fire_key) chứ không trả job_run_id — bộ quét không
--   còn ghi DB lúc sinh job nên không có id nào để chở trong message Kafka.
CREATE TABLE #rec (code VARCHAR(40), slot_at DATETIME, fire_key VARCHAR(64));
DECLARE @R TABLE (id INT IDENTITY, name VARCHAR(90), ok BIT, detail NVARCHAR(300));
DECLARE @ec INT, @em NVARCHAR(400), @rows BIGINT, @id BIGINT, @id2 BIGINT, @mine BIT, @skip INT;
DECLARE @now DATETIME = dbo.UDF_JOB_NOW();
DECLARE @today DATE = CAST(@now AS DATE);
DECLARE @isGD BIT = dbo.UDF_IS_BUSINESS_DATE(@today);

/*--- dọn dữ liệu test cũ ---*/
DELETE FROM T_JOB_RUN        WHERE C_JOB_CODE IN ('SMK_ANY','SMK_WIN','SMK_SGL','SMK_CFG','SMK_OND','SMK_FRESH','SMK_LATE','FO_SNAPSHOT_RT');
DELETE FROM T_JOB_DEFINITION WHERE C_JOB_CODE IN ('SMK_ANY','SMK_WIN','SMK_SGL','SMK_CFG','SMK_OND','SMK_FRESH','SMK_LATE');
DELETE FROM T_SI_BALANCE     WHERE C_SI_ACCOUNT LIKE 'RT%';
DELETE FROM T_SI_CURRENT     WHERE C_SI_ACCOUNT LIKE 'RT%';
DELETE FROM T_SI_PORTFOLIO   WHERE C_SI_ACCOUNT LIKE 'RT%';
DELETE FROM T_MASTER_BALANCE WHERE C_MASTER_CODE='RTM';
DELETE FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE='RTM';

/*==============================================================================
  (A) KHUNG JOB — generic, KHÔNG dính dáng gì tới FO
==============================================================================*/

-- Job giả loại "bất kỳ": không chu kỳ, không khung giờ. Chứng minh khung không cưới FO.
INSERT INTO T_JOB_DEFINITION (C_JOB_CODE,C_JOB_NAME,C_HANDLER,C_ENABLED,C_INTERVAL_SEC,
        C_WINDOW_FROM,C_WINDOW_TO,C_BUSINESS_DAY_ONLY,C_TIMEOUT_SEC,C_MAX_ATTEMPT,C_RETRY_DELAY_SEC,C_SINGLETON)
VALUES ('SMK_ANY',N'Job bất kỳ (smoke)','SmokeHandler',1,NULL, NULL,NULL,0, 60, 2, 0, 0);
-- Hạn tươi rộng: ca A7/A8 đo retry/quyền sở hữu, không nên vỡ vì số học mốc.
UPDATE T_JOB_DEFINITION SET C_MAX_DELAY_SEC=86400 WHERE C_JOB_CODE='SMK_ANY';

-- Job có khung giờ KHÔNG BAO GIỜ chứa hiện tại → dùng để test guard, độc lập giờ chạy smoke.
DECLARE @wf TIME(0) = CAST(DATEADD(MINUTE, 5, @now) AS TIME(0));
DECLARE @wt TIME(0) = CAST(DATEADD(MINUTE,10, @now) AS TIME(0));
-- (nếu rơi qua nửa đêm thì lấy khung cố định 00:00–00:01 — vẫn "ngoài" trừ đúng 1 phút đầu ngày)
IF @wf >= @wt SELECT @wf='00:00:00', @wt='00:01:00';
INSERT INTO T_JOB_DEFINITION (C_JOB_CODE,C_JOB_NAME,C_HANDLER,C_ENABLED,C_INTERVAL_SEC,
        C_WINDOW_FROM,C_WINDOW_TO,C_BUSINESS_DAY_ONLY,C_TIMEOUT_SEC,C_MAX_ATTEMPT,C_RETRY_DELAY_SEC,C_SINGLETON)
VALUES ('SMK_WIN',N'Job có khung giờ (smoke)','SmokeHandler',1,NULL, @wf,@wt,0, 60, 2, 0, 0);

-- A1. UDF khung giờ
INSERT INTO @R SELECT 'A1 UDF_JOB_IN_WINDOW: job không khai khung ⇒ 1 (mọi giờ)',
  CASE WHEN dbo.UDF_JOB_IN_WINDOW('SMK_ANY',@now)=1 THEN 1 ELSE 0 END, NULL;
INSERT INTO @R SELECT 'A1 UDF_JOB_IN_WINDOW: ngoài khung ⇒ 0',
  CASE WHEN dbo.UDF_JOB_IN_WINDOW('SMK_WIN',@now)=0 THEN 1 ELSE 0 END,
  CONCAT('now=',CONVERT(VARCHAR(8),@now,108),' window=',CAST(@wf AS VARCHAR(8)),'-',CAST(@wt AS VARCHAR(8)));
INSERT INTO @R SELECT 'A1 UDF_JOB_IN_WINDOW: job_code lạ ⇒ 0 (không tồn tại thì không được chạy)',
  CASE WHEN dbo.UDF_JOB_IN_WINDOW('KHONG_CO_JOB_NAY',@now)=0 THEN 1 ELSE 0 END, NULL;
UPDATE T_JOB_DEFINITION SET C_ENABLED=0 WHERE C_JOB_CODE='SMK_ANY';
INSERT INTO @R SELECT 'A1 UDF_JOB_IN_WINDOW: job TẮT ⇒ 0',
  CASE WHEN dbo.UDF_JOB_IN_WINDOW('SMK_ANY',@now)=0 THEN 1 ELSE 0 END, NULL;
UPDATE T_JOB_DEFINITION SET C_ENABLED=1 WHERE C_JOB_CODE='SMK_ANY';

-- A2. Nhận mốc là chạy ngay (yêu cầu BRD #2). MỘT proc duy nhất: SP_JOB_CLAIM_SLOT vừa tạo dòng
--     vừa giành quyền chạy. Bộ quét KHÔNG ghi DB — nó chỉ đặt khoá Redis rồi produce Kafka.
-- SMK_ANY là job ON-DEMAND (C_INTERVAL_SEC NULL) ⇒ không có lưới mốc, UDF_JOB_SLOT_AT trả NULL.
--   Mốc của job đẩy tay = chính thời điểm đẩy. SP_JOB_CLAIM_SLOT bỏ qua bước đối chiếu lưới khi
--   job không có chu kỳ (@ref IS NULL), nên không cần khai gì thêm.
DECLARE @slotAny DATETIME = @now;
EXEC SP_JOB_CLAIM_SLOT @p_job_code='SMK_ANY', @p_slot_at=@slotAny, @p_owner='pod-A', @p_source='kafka',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_job_run_id=@id OUTPUT;
INSERT INTO @R SELECT 'A2 ★ nhận mốc ⇒ err=0 + dòng sinh ra ĐÃ ở RUNNING (không qua trạng thái READY)',
  CASE WHEN @ec=0 AND @id IS NOT NULL
        AND EXISTS(SELECT 1 FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@id AND C_STATUS='RUNNING'
                     AND C_ATTEMPT=1 AND C_OWNER='pod-A' AND C_LEASE_UNTIL IS NOT NULL)
       THEN 1 ELSE 0 END, CONCAT('err=',@ec,' id=',@id);

INSERT INTO @R SELECT 'A2 ★ fire_key tự suy từ MỐC (pod không phải tự format khoá)',
  CASE WHEN (SELECT C_FIRE_KEY FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@id)=dbo.UDF_JOB_FIRE_KEY(@slotAny)
       THEN 1 ELSE 0 END, (SELECT C_FIRE_KEY FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@id);

EXEC SP_JOB_CLAIM_SLOT @p_job_code='KHONG_CO', @p_slot_at=@now, @p_owner='pod-A',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'A2 job_code lạ ⇒ err=1', CASE WHEN @ec=1 THEN 1 ELSE 0 END, CONCAT('err=',@ec);

-- A3. ★ GUARD KHUNG GIỜ — mốc ngoài khung thì KHÔNG tạo dòng nào
DECLARE @slotWin DATETIME = @now;   -- SMK_WIN cũng on-demand: mốc = lúc đẩy
EXEC SP_JOB_CLAIM_SLOT @p_job_code='SMK_WIN', @p_slot_at=@slotWin, @p_owner='pod-A',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_job_run_id=@id2 OUTPUT;
INSERT INTO @R SELECT 'A3 ★ mốc NGOÀI khung ⇒ err=3 + KHÔNG có dòng T_JOB_RUN nào được tạo',
  CASE WHEN @ec=3 AND (SELECT COUNT(*) FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_WIN')=0 THEN 1 ELSE 0 END,
  CONCAT('err=',@ec);

-- A4. ★ NHIỀU POD KHÔNG XỬ LÝ TRÙNG (yêu cầu BRD #3).
--     Kafka giao cùng một mốc cho 2 pod (rebalance / bộ hồi phục produce trùng / pod zombie).
--     Cả hai gọi SP_JOB_CLAIM_SLOT ⇒ pod thứ hai vỡ UQ(job_code, fire_key) ⇒ err=5.
--     ĐÂY LÀ CHỐT CHẶN DUY NHẤT — Redis và Kafka không giữ mảnh tính đúng nào.
DECLARE @idB BIGINT;
EXEC SP_JOB_CLAIM_SLOT @p_job_code='SMK_ANY', @p_slot_at=@slotAny, @p_owner='pod-B', @p_source='kafka',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_job_run_id=@idB OUTPUT;
INSERT INTO @R SELECT 'A4 ★★ pod thứ HAI nhận cùng mốc ⇒ err=5, KHÔNG chạy, KHÔNG sinh dòng thứ hai',
  CASE WHEN @ec=5
        AND (SELECT C_OWNER FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@id)='pod-A'
        AND (SELECT COUNT(*) FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_ANY')=1
       THEN 1 ELSE 0 END, CONCAT('podB err=',@ec);

-- A4 ★★★ ...VÀ PHẢI LÀM ĐƯỢC THẾ KHI NGƯỜI GỌI ĐANG MỞ TRANSACTION.
--   Đây là ca đã cắn thật: SP_JOB_CLAIM_SLOT bắt lỗi vỡ khoá 2601 để rẽ sang đường (b), nhưng với
--   SET XACT_ABORT ON thì chính việc BẮT lỗi đó làm transaction của người gọi thành KHÔNG THỂ
--   COMMIT (XACT_STATE = -1) ⇒ UPDATE ở đường (b) chết ⇒ err=-1 "The current transaction cannot be
--   committed" thay vì err=5. Và 9/10 pod đi vào đúng nhánh này MỖI MỐC, nên nó là đường THƯỜNG.
--   Ca này chốt: err=5 và transaction vẫn COMMIT được.
DECLARE @xs INT, @idC BIGINT;
BEGIN TRAN;
  EXEC SP_JOB_CLAIM_SLOT @p_job_code='SMK_ANY', @p_slot_at=@slotAny, @p_owner='pod-C', @p_source='kafka',
       @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_job_run_id=@idC OUTPUT;
  SET @xs = XACT_STATE();
IF @xs = -1 ROLLBACK; ELSE COMMIT;
INSERT INTO @R SELECT 'A4 ★★★ pod thứ BA gọi TRONG transaction của caller ⇒ err=5 + transaction VẪN COMMIT được',
  CASE WHEN @ec=5 AND @xs=1 THEN 1 ELSE 0 END,
  CONCAT('podC err=',@ec,' XACT_STATE=',@xs,' (−1 = transaction bị doom)');

INSERT INTO @R SELECT 'A4 lượt giành được ghi cả MỐC (C_SLOT_AT) — neo của mọi phép kiểm giờ giấc',
  CASE WHEN (SELECT C_SLOT_AT FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@id)=@slotAny THEN 1 ELSE 0 END,
  (SELECT CONVERT(VARCHAR(19),C_SLOT_AT,120) FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@id);
-- Nguồn đánh thức phải được ghi lại. Kafka hỏng thì hệ VẪN CHẠY ĐÚNG nhờ bộ hồi phục, chỉ chậm ~30s
--   — và không ai nhận ra. Cột này là thứ duy nhất tố giác.
INSERT INTO @R SELECT 'A4 ★ ghi lại NGUỒN đánh thức (kafka/recover) — soi được chuông có còn sống không',
  CASE WHEN (SELECT C_CLAIM_SOURCE FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@id)='kafka' THEN 1 ELSE 0 END,
  (SELECT C_CLAIM_SOURCE FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@id);

-- A5. Heartbeat có hàng rào chủ sở hữu (chống pod zombie ghi song song)
EXEC SP_JOB_HEARTBEAT @p_job_run_id=@id, @p_owner='pod-A', @p_rows=10, @p_still_mine=@mine OUTPUT;
INSERT INTO @R SELECT 'A5 heartbeat ĐÚNG chủ ⇒ still_mine=1', CASE WHEN @mine=1 THEN 1 ELSE 0 END, NULL;
EXEC SP_JOB_HEARTBEAT @p_job_run_id=@id, @p_owner='pod-B', @p_still_mine=@mine OUTPUT;
INSERT INTO @R SELECT 'A5 ★ heartbeat SAI chủ (pod zombie) ⇒ still_mine=0 ⇒ pod đó phải tự dừng',
  CASE WHEN @mine=0 THEN 1 ELSE 0 END, NULL;

-- A6. Lease hết hạn = pod chết → SP_JOB_RECOVER thu hồi, pod khác giành được
UPDATE T_JOB_RUN SET C_LEASE_UNTIL=DATEADD(MINUTE,-5,@now) WHERE C_JOB_RUN_ID=@id;
EXEC SP_JOB_RECOVER @p_stale_sec=0, @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'A6 SP_JOB_RECOVER: lease quá hạn ⇒ RUNNING→READY (không tăng attempt)',
  CASE WHEN EXISTS(SELECT 1 FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@id AND C_STATUS='READY'
                     AND C_ATTEMPT=1 AND C_OWNER IS NULL) THEN 1 ELSE 0 END,
  (SELECT CONCAT(C_STATUS,' attempt=',C_ATTEMPT) FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@id);

-- A7. Lỗi → retry; hết lượt thử → DEAD (max_attempt=2)
-- Cùng MỐC ⇒ SP_JOB_CLAIM_SLOT đi đường (b): dòng đã tồn tại, giành lại (READY→RUNNING, attempt+1).
EXEC SP_JOB_CLAIM_SLOT @p_job_code='SMK_ANY', @p_slot_at=@slotAny, @p_owner='pod-C',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
EXEC SP_JOB_COMPLETE @p_job_run_id=@id, @p_owner='pod-C', @p_ok=0, @p_message=N'lỗi giả lập',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'A7 COMPLETE(lỗi) lần 2/2 ⇒ DEAD (không quay vòng vô tận)',
  CASE WHEN EXISTS(SELECT 1 FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@id AND C_STATUS='DEAD') THEN 1 ELSE 0 END,
  (SELECT CONCAT(C_STATUS,' attempt=',C_ATTEMPT) FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@id);
EXEC SP_JOB_CLAIM_SLOT @p_job_code='SMK_ANY', @p_slot_at=@slotAny, @p_owner='pod-D',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'A7 nhận lại mốc đã DEAD ⇒ err=6, KHÔNG chạy lại',
  CASE WHEN @ec=6 THEN 1 ELSE 0 END, CONCAT('err=',@ec);

-- A8. COMPLETE bởi pod KHÔNG phải chủ ⇒ từ chối (err=5)
-- Mốc KHÁC (lùi 1 chu kỳ) ⇒ dòng mới, đi đường (a) INSERT.
DECLARE @slotPrev DATETIME = DATEADD(SECOND, -900, @slotAny);   -- mốc khác ⇒ dòng khác
EXEC SP_JOB_CLAIM_SLOT @p_job_code='SMK_ANY', @p_slot_at=@slotPrev, @p_owner='pod-A',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_job_run_id=@id2 OUTPUT;
EXEC SP_JOB_COMPLETE @p_job_run_id=@id2, @p_owner='pod-XX', @p_ok=1,
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'A8 COMPLETE bởi pod KHÔNG phải chủ ⇒ err=5, lượt vẫn RUNNING',
  CASE WHEN @ec=5 AND EXISTS(SELECT 1 FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@id2 AND C_STATUS='RUNNING')
       THEN 1 ELSE 0 END, CONCAT('err=',@ec);
EXEC SP_JOB_COMPLETE @p_job_run_id=@id2, @p_owner='pod-A', @p_ok=1, @p_rows=123,
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'A8 COMPLETE bởi ĐÚNG chủ ⇒ DONE + ghi số dòng',
  CASE WHEN @ec=0 AND EXISTS(SELECT 1 FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@id2 AND C_STATUS='DONE' AND C_ROWS=123)
       THEN 1 ELSE 0 END, NULL;

-- A9. Scheduler: job định kỳ, 2 lần quét trong CÙNG slot ⇒ đúng 1 lượt (10 pod cũng vậy)
UPDATE T_JOB_DEFINITION SET C_INTERVAL_SEC=900, C_SINGLETON=0 WHERE C_JOB_CODE='SMK_ANY';
DELETE FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_ANY';
DELETE #scan; DELETE #rec;
EXEC SP_SMK_SCAN;                       -- pod A quét
DECLARE @pass1 INT = (SELECT COUNT(*) FROM #scan WHERE code='SMK_ANY');
DELETE #scan; DELETE #rec;
EXEC SP_SMK_SCAN;                       -- pod B quét CÙNG slot, ngay sau đó
DECLARE @pass2 INT = (SELECT COUNT(*) FROM #scan WHERE code='SMK_ANY');
INSERT INTO @R SELECT 'A9 ★ 2 pod cùng quét slot ⇒ pod đầu tạo 1, pod sau tạo 0 (UQ fire_key quyết)',
  CASE WHEN @pass1=1 AND @pass2=0
        AND (SELECT COUNT(*) FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_ANY')=1 THEN 1 ELSE 0 END,
  CONCAT('pass1=',@pass1,' pass2=',@pass2);
-- fire_key phải có GIÂY: chu kỳ nhỏ nhất cho phép là 30s ⇒ mốc chỉ tới phút thì 2 slot/phút
--   trùng khoá và job 30s lặng lẽ chạy 60s/lần.
INSERT INTO @R SELECT 'A9 ★ fire_key = mốc slot yyyyMMddHHmmss (có GIÂY; không trôi theo giờ pod khởi động)',
  CASE WHEN EXISTS(SELECT 1 FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_ANY'
                     AND C_FIRE_KEY LIKE CONVERT(CHAR(8),@today,112)+'[0-9][0-9][0-9][0-9][0-9][0-9]'
                     AND CAST(SUBSTRING(C_FIRE_KEY,11,2) AS INT) % 15 = 0   -- phút chia hết 15 (interval 900s)
                     AND RIGHT(C_FIRE_KEY,2) = '00') THEN 1 ELSE 0 END,
  (SELECT TOP 1 C_FIRE_KEY FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_ANY');
-- Chứng minh trực tiếp bug đã sửa: chu kỳ 30s ⇒ 2 slot LIỀN NHAU trong cùng một phút PHẢI khác khoá.
DECLARE @m0 DATETIME = CAST(CAST(@now AS DATE) AS DATETIME);
DECLARE @k1 VARCHAR(64) = CONVERT(CHAR(8), DATEADD(SECOND,(DATEDIFF(SECOND,@m0,@m0)/30)*30,@m0),112)
                        + FORMAT(DATEADD(SECOND,(DATEDIFF(SECOND,@m0,@m0)/30)*30,@m0),'HHmmss');
DECLARE @k2 VARCHAR(64) = CONVERT(CHAR(8), DATEADD(SECOND,(DATEDIFF(SECOND,@m0,DATEADD(SECOND,30,@m0))/30)*30,@m0),112)
                        + FORMAT(DATEADD(SECOND,(DATEDIFF(SECOND,@m0,DATEADD(SECOND,30,@m0))/30)*30,@m0),'HHmmss');
INSERT INTO @R SELECT 'A9 ★ chu kỳ 30s: 2 slot trong CÙNG một phút ra 2 fire_key KHÁC nhau',
  CASE WHEN @k1 <> @k2 THEN 1 ELSE 0 END, CONCAT(@k1,' vs ',@k2);

-- A10. Singleton — dùng JOB RIÊNG (SMK_SGL), KHÔNG dùng lại SMK_ANY.
--   Vì sao: hai chu kỳ khác nhau vẫn có thể cho ra CÙNG một mốc slot (900s và 60s trùng nhau tại
--   mọi phút chia hết cho 15) ⇒ dùng lại job của A9 thì ca này PASS/FAIL theo giờ chạy smoke.
--   Đó chính là cách bug fire_key-thiếu-giây lộ ra: ca A10 fail trên DB sạch, pass trên DB cũ.
--   Test phải tất định — nếu nó phụ thuộc đồng hồ thì lần đỏ tiếp theo sẽ bị cho là "flaky" và bỏ qua.
INSERT INTO T_JOB_DEFINITION (C_JOB_CODE,C_JOB_NAME,C_HANDLER,C_ENABLED,C_INTERVAL_SEC,
        C_WINDOW_FROM,C_WINDOW_TO,C_BUSINESS_DAY_ONLY,C_TIMEOUT_SEC,C_MAX_ATTEMPT,C_RETRY_DELAY_SEC,C_SINGLETON)
VALUES ('SMK_SGL',N'Job singleton (smoke)','SmokeHandler',1,60, NULL,NULL,0, 60, 2, 0, 1);

EXEC SP_SMK_SCAN;
INSERT INTO @R SELECT 'A10 job singleton mới ⇒ sinh đúng 1 lượt ở nhịp đầu',
  CASE WHEN (SELECT COUNT(*) FROM #scan WHERE code='SMK_SGL')=1 THEN 1 ELSE 0 END, NULL;

-- lượt đó đang chạy (lease còn hiệu lực) ⇒ nhịp sau KHÔNG mở lượt mới
UPDATE T_JOB_RUN SET C_STATUS='RUNNING', C_OWNER='pod-A', C_LEASE_UNTIL=DATEADD(MINUTE,5,@now)
 WHERE C_JOB_CODE='SMK_SGL';
DECLARE @cntBefore INT = (SELECT COUNT(*) FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_SGL');
DELETE #scan; DELETE #rec;
EXEC SP_SMK_SCAN;
INSERT INTO @R SELECT 'A10 singleton: lượt trước còn RUNNING ⇒ KHÔNG sinh lượt mới',
  CASE WHEN (SELECT COUNT(*) FROM #scan WHERE code='SMK_SGL')=0
        AND (SELECT COUNT(*) FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_SGL')=@cntBefore THEN 1 ELSE 0 END, NULL;

-- ... nhưng lease CHẾT thì slot kế tiếp KHÔNG bị chặn vĩnh viễn.
--   Lùi mốc slot của lượt cũ về quá khứ để nhịp này chắc chắn rơi vào slot KHÁC (tất định,
--   không phụ thuộc smoke chạy vào giây thứ mấy của phút).
UPDATE T_JOB_RUN SET C_LEASE_UNTIL=DATEADD(MINUTE,-1,@now), C_FIRE_KEY='19000101000000'
 WHERE C_JOB_CODE='SMK_SGL';
DELETE #scan; DELETE #rec;
EXEC SP_SMK_SCAN;
INSERT INTO @R SELECT 'A10 ★ singleton + pod chết (lease hết hạn) ⇒ slot mới VẪN mở (không kẹt vĩnh viễn)',
  CASE WHEN (SELECT COUNT(*) FROM #scan WHERE code='SMK_SGL')=1 THEN 1 ELSE 0 END,
  CONCAT('sinh=',(SELECT COUNT(*) FROM #scan WHERE code='SMK_SGL'));

-- A11. GUARD TẦNG 2 + DỌN XÁC. Một lượt RETRY nằm chờ backoff, tới lúc được đẩy lại thì mốc của nó
--   đã ra ngoài khung. Guard phải làm HAI việc: (1) chặn — err=3, và (2) ĐÓNG DẤU SKIPPED cho chính
--   dòng đó. Thiếu (2) thì dòng nằm READY vĩnh viễn: cứ bị chặn hoài, không bao giờ kết thúc, và
--   bảng theo dõi đếm nó là "tồn đọng" mãi mãi.
DELETE FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_WIN';
-- SMK_WIN là job on-demand (C_INTERVAL_SEC NULL) ⇒ không có lưới mốc, mốc = lúc đẩy.
DECLARE @slotLate DATETIME   = @now;
DECLARE @fkLate   VARCHAR(64) = dbo.UDF_JOB_FIRE_KEY(@slotLate);
INSERT INTO T_JOB_RUN (C_JOB_CODE,C_FIRE_KEY,C_STATUS,C_BUSINESS_DATE,C_RUN_AFTER,C_SLOT_AT,C_ENQUEUED_AT,C_ATTEMPT)
VALUES ('SMK_WIN',@fkLate,'READY',@today,@now,@slotLate,@now,1);   -- retry vừa hết backoff
SET @id2 = SCOPE_IDENTITY();
EXEC SP_JOB_CLAIM_SLOT @p_job_code='SMK_WIN', @p_slot_at=@slotLate, @p_owner='pod-A',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'A11 ★ TẦNG 2: lượt retry tới lúc chạy đã ngoài khung ⇒ err=3 + đóng dấu SKIPPED',
  CASE WHEN @ec=3 AND EXISTS(SELECT 1 FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@id2 AND C_STATUS='SKIPPED')
       THEN 1 ELSE 0 END,
  CONCAT('err=',@ec,' status=',(SELECT C_STATUS FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@id2));

-- ...nhưng KHÔNG được đụng vào dòng RUNNING của pod khác: đó là lượt đang chạy dở, không phải xác.
--   Dùng fire_key KHÁC để dòng SKIPPED ở trên còn nguyên cho ca SP_JOB_PURGE bên dưới.
DECLARE @idRun BIGINT;
INSERT INTO T_JOB_RUN (C_JOB_CODE,C_FIRE_KEY,C_STATUS,C_BUSINESS_DATE,C_RUN_AFTER,C_SLOT_AT,
                       C_ENQUEUED_AT,C_ATTEMPT,C_OWNER,C_LEASE_UNTIL)
VALUES ('SMK_WIN','DANGCHAY','RUNNING',@today,@now,@slotLate,@now,1,'pod-Z',DATEADD(MINUTE,5,@now));
SET @idRun = SCOPE_IDENTITY();
EXEC SP_JOB_CLAIM_SLOT @p_job_code='SMK_WIN', @p_slot_at=@slotLate, @p_owner='pod-A',
     @p_fire_key='DANGCHAY', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'A11 ★ ...nhưng KHÔNG đụng dòng RUNNING của pod khác (chỉ dọn READY/FAILED)',
  CASE WHEN @ec=3 AND (SELECT C_STATUS FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@idRun)='RUNNING'
        AND (SELECT C_OWNER  FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@idRun)='pod-Z'
       THEN 1 ELSE 0 END,
  CONCAT('err=',@ec,' status=',(SELECT C_STATUS FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@idRun));

-- A12. Job FO thật: khung 09:00–15:00 + chỉ ngày GD (đọc từ seed, không hard-code lại)
INSERT INTO @R SELECT 'A12 seed FO_SNAPSHOT_RT: 15 phút · 09:00–15:00 · chỉ ngày GD · singleton',
  CASE WHEN EXISTS(SELECT 1 FROM T_JOB_DEFINITION WHERE C_JOB_CODE='FO_SNAPSHOT_RT'
                     AND C_INTERVAL_SEC=900 AND C_WINDOW_FROM='09:00:00' AND C_WINDOW_TO='15:00:00'
                     AND C_BUSINESS_DAY_ONLY=1 AND C_SINGLETON=1 AND C_TIMEOUT_SEC<900)
       THEN 1 ELSE 0 END, NULL;
-- Biên khung giờ tính TAY (không phụ thuộc lúc chạy smoke): 08:59 ngoài · 09:00 trong · 14:59 trong · 15:00 NGOÀI
DECLARE @gd DATE = @today;
WHILE dbo.UDF_IS_BUSINESS_DATE(@gd)=0 SET @gd = DATEADD(DAY,1,@gd);   -- lấy 1 ngày GD bất kỳ để test biên
-- SINH job: biên [09:00, 15:00] ĐÓNG HAI ĐẦU ⇒ 15:00 là mốc CUỐI CÙNG hợp lệ (ảnh chụp đóng cửa).
INSERT INTO @R SELECT 'A12 ★ SINH job: 08:59 NGOÀI · 09:00 TRONG · 15:00 TRONG (mốc cuối) · 15:01 NGOÀI',
  CASE WHEN dbo.UDF_JOB_IN_WINDOW('FO_SNAPSHOT_RT', DATEADD(MINUTE, 8*60+59, CAST(@gd AS DATETIME)))=0
        AND dbo.UDF_JOB_IN_WINDOW('FO_SNAPSHOT_RT', DATEADD(MINUTE, 9*60,    CAST(@gd AS DATETIME)))=1
        AND dbo.UDF_JOB_IN_WINDOW('FO_SNAPSHOT_RT', DATEADD(MINUTE,15*60,    CAST(@gd AS DATETIME)))=1
        AND dbo.UDF_JOB_IN_WINDOW('FO_SNAPSHOT_RT', DATEADD(MINUTE,15*60+1,  CAST(@gd AS DATETIME)))=0
       THEN 1 ELSE 0 END, CONCAT('ngày GD dùng để test = ', CONVERT(VARCHAR(10),@gd,23));
-- CHẠY: KHÔNG còn khái niệm "khung + ân hạn". Chỉ còn HẠN TƯƠI (C_MAX_DELAY_SEC=600s) đo từ MỐC.
--   ⇒ lượt 15:00 hết hiệu lực lúc 15:10, bất kể nó nằm chờ hay đã thử lại mấy lần.
INSERT INTO @R SELECT 'A12 ★★ hạn tươi neo vào MỐC: 15:00 mốc hợp lệ, hết hiệu lực sau 600s (15:10)',
  CASE WHEN (SELECT C_MAX_DELAY_SEC FROM T_JOB_DEFINITION WHERE C_JOB_CODE='FO_SNAPSHOT_RT')=600
        AND dbo.UDF_JOB_IN_WINDOW('FO_SNAPSHOT_RT', DATEADD(MINUTE,15*60, CAST(@gd AS DATETIME)))=1
       THEN 1 ELSE 0 END, NULL;
-- Đếm ĐÚNG số mốc trong phiên — ví dụ nghiệp vụ đã chốt: khung 9h-15h, chu kỳ 1 tiếng ⇒ 7 mốc.
DECLARE @n1h INT=0, @n15m INT=0, @sec INT;
SET @sec = 9*3600;
WHILE @sec <= 15*3600 BEGIN
    IF dbo.UDF_JOB_IN_WINDOW('FO_SNAPSHOT_RT', DATEADD(SECOND,@sec,CAST(@gd AS DATETIME)))=1 SET @n1h += 1;
    SET @sec += 3600; END
SET @sec = 9*3600;
WHILE @sec <= 15*3600 BEGIN
    IF dbo.UDF_JOB_IN_WINDOW('FO_SNAPSHOT_RT', DATEADD(SECOND,@sec,CAST(@gd AS DATETIME)))=1 SET @n15m += 1;
    SET @sec += 900; END
INSERT INTO @R SELECT 'A12 ★ khung 09:00-15:00: chu kỳ 1 tiếng ⇒ 7 mốc · chu kỳ 15 phút ⇒ 25 mốc',
  CASE WHEN @n1h=7 AND @n15m=25 THEN 1 ELSE 0 END, CONCAT('1h=',@n1h,' 15m=',@n15m);
-- Ngày NGHỈ: đúng 10h sáng nhưng là Chủ nhật ⇒ vẫn 0
DECLARE @sun DATE = @today;
WHILE DATEPART(WEEKDAY, @sun) <> 1 SET @sun = DATEADD(DAY,1,@sun);   -- (DATEFIRST mặc định 7 ⇒ 1 = Chủ nhật)
INSERT INTO @R SELECT 'A12 ★ 10h sáng CHỦ NHẬT ⇒ 0 (đúng giờ nhưng sai ngày ⇒ vẫn không gọi FO)',
  CASE WHEN dbo.UDF_JOB_IN_WINDOW('FO_SNAPSHOT_RT', DATEADD(HOUR,10,CAST(@sun AS DATETIME)))=0 THEN 1 ELSE 0 END,
  CONCAT('CN = ', CONVERT(VARCHAR(10),@sun,23));

-- A13. Vận hành: chạy tay ngoài khung (cửa hậu cho job VÔ HẠI), theo dõi, dọn nhật ký.
--   4 proc này lúc đầu KHÔNG có ca test nào — tự review mới lòi ra. Proc không có test là proc
--   chưa từng chạy, và nó sẽ chạy lần đầu vào lúc có sự cố, tức là lúc tệ nhất.
-- ★ KHÔNG CÒN CỬA HẬU. Tham số @p_ignore_window đã bị BỎ: một cửa hậu mà job gọi hệ ngoài cũng
--   đi qua được thì nó không phải cửa hậu, nó là cái lỗ. Đẩy tay ngoài khung ⇒ err=3, KHÔNG tạo
--   lượt nào. Cần chạy ngoài khung thì sửa khung bằng SP_SET_JOB_SCHEDULE (có dấu vết, có người
--   chịu trách nhiệm, và tự dọn lượt chờ của cấu hình cũ).
SET @id2 = NULL;
EXEC SP_JOB_CLAIM_SLOT @p_job_code='SMK_WIN', @p_slot_at=@slotLate, @p_owner='ops', @p_fire_key='IGN',
     @p_source='manual', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_job_run_id=@id2 OUTPUT;
INSERT INTO @R SELECT 'A13 ★ KHÔNG còn cửa hậu: đẩy tay ngoài khung ⇒ err=3 + 0 lượt được tạo',
  CASE WHEN @ec=3 AND @id2 IS NULL
        AND NOT EXISTS(SELECT 1 FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_WIN' AND C_FIRE_KEY='IGN')
       THEN 1 ELSE 0 END, CONCAT('err=',@ec,' id=',ISNULL(CAST(@id2 AS VARCHAR(20)),'(null)'));

-- SP_GET_JOB_STATUS trả HAI result set (RS1 sức khoẻ + RS2 lượt cần xử lý) đúng quy ước read API
--   của repo ⇒ KHÔNG hứng được bằng INSERT..EXEC. Chạy thẳng và soi @p_err_code.
EXEC SP_GET_JOB_STATUS @p_job_code='FO_SNAPSHOT_RT', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'A13 SP_GET_JOB_STATUS chạy được (2 result set) ⇒ err=0',
  CASE WHEN @ec=0 THEN 1 ELSE 0 END, CONCAT('err=',@ec,' msg=',ISNULL(@em,'(null)'));
INSERT INTO @R SELECT 'A13 cột C_IN_WINDOW_NOW phản ánh ĐÚNG khung giờ hiện tại của từng job',
  CASE WHEN dbo.UDF_JOB_IN_WINDOW('SMK_ANY',@now)=1 AND dbo.UDF_JOB_IN_WINDOW('SMK_WIN',@now)=0
       THEN 1 ELSE 0 END, NULL;

-- Dựng 1 lượt DEAD để chứng minh SP_JOB_PURGE KHÔNG bao giờ xoá nó.
--   (Ca cũ dùng lượt DEAD của A7 — nhưng A9 đã DELETE sạch SMK_ANY trước đó, nên nó chỉ PASS
--    do may mắn chứ không đo được gì. Tự review bắt được, dựng dữ liệu tường minh tại chỗ.)
INSERT INTO T_JOB_RUN (C_JOB_CODE,C_FIRE_KEY,C_STATUS,C_RUN_AFTER,C_ENQUEUED_AT,C_ENDED_AT,C_MESSAGE)
VALUES ('SMK_ANY','DEADONE','DEAD',@now,DATEADD(DAY,-99,@now),DATEADD(DAY,-99,@now),N'lỗi cũ, cần người xem');
-- SP_JOB_PURGE có SÀN 1 NGÀY (chống dọn lượt hôm nay rồi bị sinh lại ⇒ chạy 2 lần trong 1 slot),
--   nên phải LÀM CŨ các lượt đã đóng thì mới dọn được. Gọi keep_days=0 trên lượt vừa xong là
--   KHÔNG dọn gì cả — và đó chính là hành vi đúng.
UPDATE T_JOB_RUN SET C_ENDED_AT=DATEADD(DAY,-3,@now)
 WHERE C_JOB_CODE IN ('SMK_ANY','SMK_WIN') AND C_STATUS IN ('DONE','SKIPPED');
DECLARE @doneBefore INT = (SELECT COUNT(*) FROM T_JOB_RUN
                           WHERE C_JOB_CODE IN ('SMK_ANY','SMK_WIN') AND C_STATUS IN ('DONE','SKIPPED'));
DECLARE @purged BIGINT;
EXEC SP_JOB_PURGE @p_keep_days=1, @p_rows=@purged OUTPUT;
INSERT INTO @R SELECT 'A13 ★ SP_JOB_PURGE: dọn hết DONE/SKIPPED nhưng GIỮ NGUYÊN DEAD (thứ cần người xem)',
  CASE WHEN @doneBefore > 0 AND @purged = @doneBefore
        AND NOT EXISTS(SELECT 1 FROM T_JOB_RUN WHERE C_JOB_CODE IN ('SMK_ANY','SMK_WIN') AND C_STATUS IN ('DONE','SKIPPED'))
        AND EXISTS(SELECT 1 FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_ANY' AND C_FIRE_KEY='DEADONE' AND C_STATUS='DEAD')
       THEN 1 ELSE 0 END, CONCAT('trước=',@doneBefore,' đã dọn=',@purged);
INSERT INTO @R SELECT 'A13 SP_JOB_PURGE KHÔNG đụng lượt đang RUNNING/READY (chỉ dọn lượt đã đóng)',
  CASE WHEN EXISTS(SELECT 1 FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_ANY' AND C_STATUS IN ('RUNNING','READY'))
       THEN 1 ELSE 0 END, NULL;

/*==============================================================================
  (B) NGHIỆP VỤ FO SNAPSHOT — cần NGÀY GD (guard tầng 3 chỉ nhận hôm nay + ngày GD)
==============================================================================*/
INSERT INTO T_MASTER_PORTFOLIO (C_MASTER_CODE,C_MASTER_NAME,C_STATUS,C_INCEPTION_DATE,C_BENCHMARK_CODE)
 VALUES ('RTM',N'RT smoke master','ACTIVE','2026-01-01','VNINDEX');
-- ⚠️ UQ_SI_PORTFOLIO_ACTIVE (filtered ACTIVE) = 1 KH chỉ có 1 tiểu khoản ACTIVE TRÊN MỖI MASTER.
--   ⇒ "50 khách hàng / batch" KHÔNG bằng "50 tiểu khoản / batch": một KH đầu tư K master thì mang
--     theo K tiểu khoản. Worker cắt batch theo KHÁCH HÀNG (đúng BRD) nên payload gửi FO có thể
--     tới 50×K dòng — đó là lý do SP_GET_FO_SNAPSHOT_SCOPE trả per-tiểu-khoản và sắp theo KH.
INSERT INTO T_SI_PORTFOLIO (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_JOIN_DATE,C_STATUS) VALUES
 ('RT01','RTC9','RTM','2026-01-01','ACTIVE'),   -- ★ mã KH cố tình ĐẢO so với mã tiểu khoản:
 ('RT02','RTC5','RTM','2026-01-01','ACTIVE'),   --   xếp theo KH ra RT03,RT02,RT01 còn xếp theo
 ('RT03','RTC1','RTM','2026-01-01','ACTIVE');   --   master/si ra RT01,RT02,RT03 ⇒ ca B0 phân biệt được

-- Phiên chốt HÔM QUA (mốc so sánh) — ghi tay, C_SRC mặc định 'EOD'.
--   ⚠️ DÙNG UDF_PREV_BUSINESS_DAY (thuần LỊCH), KHÔNG dùng UDF_PREV_BUSINESS_DATE — hàm kia đòi
--     ngày đó phải CÓ GIÁ trong T_PRICE_DAILY. Smoke này tự seed dữ liệu của nó và KHÔNG seed giá,
--     nên hàm kia trả NULL ⇒ toàn bộ khối (B) seed hỏng im lặng (INSERT lỗi NOT NULL, các ca sau
--     fail với thông điệp khó hiểu). Đây là phụ thuộc ngầm vào 03_SMOKE — đã cắt.
DECLARE @prev DATE = dbo.UDF_PREV_BUSINESS_DAY(@today);
INSERT INTO T_SI_BALANCE (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_AUM,C_DAILY_RETURN,C_CASH) VALUES
 (@prev,'RT01','RTC9','RTM',1000000000,0.010000,100000000),
 (@prev,'RT02','RTC5','RTM', 500000000,0.010000, 50000000),
 (@prev,'RT03','RTC1','RTM', 200000000,0.010000, 20000000);
INSERT INTO T_MASTER_BALANCE (C_BUSINESS_DATE,C_MASTER_CODE,C_CASH,C_AUM,C_DAILY_RETURN,C_TOTAL_ACCOUNT)
 VALUES (@prev,'RTM',170000000,1700000000,0.010000,3);

-- B0. Scope cho worker chia batch
-- Cột seq giữ ĐÚNG thứ tự SP trả về — không có nó thì ca kiểm thứ tự chỉ là tautology
--   (bản trước tự ORDER BY lại trong chính câu assert, nên nó không đo được gì).
CREATE TABLE #scope (seq INT IDENTITY(1,1), cust VARCHAR(10), si VARCHAR(20), sub VARCHAR(30), master VARCHAR(20));
INSERT #scope (cust, si, sub, master) EXEC SP_GET_FO_SNAPSHOT_SCOPE @p_master_code='RTM',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'B0 ★ SCOPE xếp theo MASTER rồi tiểu khoản (KHÔNG theo khách hàng)',
  CASE WHEN (SELECT COUNT(*) FROM #scope)=3
        -- thứ tự thật phải là RT01,RT02,RT03; nếu SP còn xếp theo KH thì ra RT03,RT02,RT01
        AND (SELECT si FROM #scope WHERE seq=1)='RT01'
        AND (SELECT si FROM #scope WHERE seq=3)='RT03'
        -- và không có chỗ nào master bị xen kẽ (kiểm tổng quát, đúng cả khi thêm master sau này)
        AND NOT EXISTS (SELECT 1 FROM #scope a JOIN #scope b ON b.seq > a.seq
                        WHERE b.master < a.master)
       THEN 1 ELSE 0 END,
  (SELECT STRING_AGG(si, '>') WITHIN GROUP (ORDER BY seq) FROM #scope);
DROP TABLE #scope;

-- B1. ★ GUARD TẦNG 3 — ngày quá khứ bị từ chối (chạy được MỌI ngày, kể cả T7/CN)
EXEC SP_INGEST_FO_SNAPSHOT_RT @p_json=N'[{"si_account":"RT01","aum":1,"cash":1}]',
     @p_business_date='2026-01-05', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_rows=@rows OUTPUT;
INSERT INTO @R SELECT 'B1 ★ TẦNG 3: ghi RT cho ngày QUÁ KHỨ ⇒ err=22, 0 dòng (không đụng lịch sử)',
  CASE WHEN @ec=22 AND @rows=0 THEN 1 ELSE 0 END, CONCAT('err=',@ec);

IF @isGD = 0
BEGIN
    INSERT INTO @R SELECT 'B2..B9 BỎ QUA: hôm nay KHÔNG phải ngày GD — chạy lại smoke vào ngày GD', 1,
      CONCAT(N'today=',CONVERT(VARCHAR(10),@today,23),N' (T7/CN/lễ). Guard tầng 3 đã được kiểm ở B1.');
END
ELSE
BEGIN
    -- B2. Ghi 1 batch RT + acc lạ bị lọc
    EXEC SP_INGEST_FO_SNAPSHOT_RT
         @p_json=N'[{"si_account":"RT01","aum":1100000000,"cash":110000000,"cash_available":90000000,"dividend_pending":5000000,"sell_pending":10000000},
                    {"si_account":"RT02","aum":510000000,"cash":51000000},
                    {"si_account":"KHONG_THUOC_SDI","aum":999,"cash":9}]',
         @p_business_date=@today, @p_user='smoke',
         @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_rows=@rows OUTPUT, @p_skipped_eod=@skip OUTPUT;
    INSERT INTO @R SELECT 'B2 ingest RT: 2 dòng ghi, acc KHÔNG thuộc SDI bị lọc (không reject cả batch)',
      CASE WHEN @ec=0 AND @rows=2
            AND (SELECT COUNT(*) FROM T_SI_BALANCE WHERE C_BUSINESS_DATE=@today AND C_SRC='RT')=2
            AND NOT EXISTS(SELECT 1 FROM T_SI_BALANCE WHERE C_SI_ACCOUNT='KHONG_THUOC_SDI')
           THEN 1 ELSE 0 END, CONCAT('err=',@ec,' rows=',@rows);
    INSERT INTO @R SELECT 'B2 ★ dòng RT có C_DAILY_RETURN = NULL (KHÔNG bịa 0% cho giữa phiên)',
      CASE WHEN NOT EXISTS(SELECT 1 FROM T_SI_BALANCE WHERE C_BUSINESS_DATE=@today AND C_SRC='RT'
                             AND C_DAILY_RETURN IS NOT NULL) THEN 1 ELSE 0 END, NULL;
    INSERT INTO @R SELECT 'B2 dòng RT có C_RT_AT (dashboard hiện "cập nhật lúc HH:mm")',
      CASE WHEN NOT EXISTS(SELECT 1 FROM T_SI_BALANCE WHERE C_BUSINESS_DATE=@today AND C_SRC='RT'
                             AND C_RT_AT IS NULL) THEN 1 ELSE 0 END, NULL;

    -- B3. UPSERT: gọi lại nhịp sau ⇒ ĐÈ LÊN, không nhân đôi
    EXEC SP_INGEST_FO_SNAPSHOT_RT
         @p_json=N'[{"si_account":"RT01","aum":1200000000,"cash":120000000}]',
         @p_business_date=@today, @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_rows=@rows OUTPUT;
    INSERT INTO @R SELECT 'B3 ★ nhịp 15 phút sau ⇒ UPSERT đè (1 dòng/tiểu khoản/ngày, KHÔNG trùng lặp)',
      CASE WHEN (SELECT COUNT(*) FROM T_SI_BALANCE WHERE C_BUSINESS_DATE=@today AND C_SI_ACCOUNT='RT01')=1
            AND (SELECT C_AUM FROM T_SI_BALANCE WHERE C_BUSINESS_DATE=@today AND C_SI_ACCOUNT='RT01')=1200000000
           THEN 1 ELSE 0 END,
      CONCAT('aum=',(SELECT C_AUM FROM T_SI_BALANCE WHERE C_BUSINESS_DATE=@today AND C_SI_ACCOUNT='RT01'));

    -- B4. ★★ LUẬT BẤT DI BẤT DỊCH: RT KHÔNG BAO GIỜ ĐÈ SỐ CHỐT
    INSERT INTO T_SI_BALANCE (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_AUM,C_DAILY_RETURN,C_CASH,C_SRC)
     VALUES (@today,'RT03','RTC1','RTM',222000000,0.020000,22000000,'EOD');   -- Asset đã chốt RT03
    EXEC SP_INGEST_FO_SNAPSHOT_RT
         @p_json=N'[{"si_account":"RT03","aum":999999999,"cash":1}]',
         @p_business_date=@today, @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT,
         @p_rows=@rows OUTPUT, @p_skipped_eod=@skip OUTPUT;
    INSERT INTO @R SELECT 'B4 ★★ RT tới SAU khi đã có số CHỐT ⇒ KHÔNG đè, đếm vào skipped_eod',
      CASE WHEN @ec=0 AND @skip=1
            AND (SELECT C_AUM FROM T_SI_BALANCE WHERE C_BUSINESS_DATE=@today AND C_SI_ACCOUNT='RT03')=222000000
            AND (SELECT C_SRC FROM T_SI_BALANCE WHERE C_BUSINESS_DATE=@today AND C_SI_ACCOUNT='RT03')='EOD'
            AND (SELECT C_DAILY_RETURN FROM T_SI_BALANCE WHERE C_BUSINESS_DATE=@today AND C_SI_ACCOUNT='RT03')=0.020000
           THEN 1 ELSE 0 END, CONCAT('skipped_eod=',@skip,' aum=',(SELECT C_AUM FROM T_SI_BALANCE WHERE C_BUSINESS_DATE=@today AND C_SI_ACCOUNT='RT03'));

    -- B4b. FO trả TRÙNG tiểu khoản trong cùng batch ⇒ thông điệp đọc được, KHÔNG phải lỗi PK thô
    EXEC SP_INGEST_FO_SNAPSHOT_RT
         @p_json=N'[{"si_account":"RT01","aum":1,"cash":1},{"si_account":"RT01","aum":2,"cash":2}]',
         @p_business_date=@today, @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_rows=@rows OUTPUT;
    INSERT INTO @R SELECT 'B4b FO trả TRÙNG si trong 1 batch ⇒ err=21 + thông điệp nêu đích danh mã',
      CASE WHEN @ec=21 AND @rows=0 AND @em LIKE N'%RT01%' THEN 1 ELSE 0 END, CONCAT('err=',@ec);
    -- ...và batch trùng KHÔNG làm hỏng dòng đã ghi đúng trước đó
    INSERT INTO @R SELECT 'B4b batch bị từ chối KHÔNG đụng dòng RT đã ghi (all-or-nothing)',
      CASE WHEN (SELECT C_AUM FROM T_SI_BALANCE WHERE C_BUSINESS_DATE=@today AND C_SI_ACCOUNT='RT01')=1200000000
           THEN 1 ELSE 0 END, NULL;

    -- B4c. JSON rỗng ⇒ không lỗi, không ghi (FO trả batch rỗng là chuyện bình thường)
    EXEC SP_INGEST_FO_SNAPSHOT_RT @p_json=N'[]', @p_business_date=@today,
         @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_rows=@rows OUTPUT;
    INSERT INTO @R SELECT 'B4c JSON rỗng ⇒ err=0, rows=0 (không coi là sự cố)',
      CASE WHEN @ec=0 AND @rows=0 THEN 1 ELSE 0 END, CONCAT('err=',@ec);

    -- B5. Gộp cấp master
    EXEC SP_RT_MASTER_AGG @p_business_date=@today, @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_rows=@rows OUTPUT;
    INSERT INTO @R SELECT 'B5 SP_RT_MASTER_AGG: ΣAUM RT = 1.2e9 + 0.51e9 (KHÔNG cộng RT03 vì nó là dòng EOD)',
      CASE WHEN @ec=0
            AND (SELECT C_AUM FROM T_MASTER_BALANCE WHERE C_BUSINESS_DATE=@today AND C_MASTER_CODE='RTM' AND C_SRC='RT')
                = 1710000000 THEN 1 ELSE 0 END,
      CONCAT('aum=',(SELECT C_AUM FROM T_MASTER_BALANCE WHERE C_BUSINESS_DATE=@today AND C_MASTER_CODE='RTM' AND C_SRC='RT'));
    INSERT INTO @R SELECT 'B5 ★ coverage: 2/3 tiểu khoản có số RT ⇒ dashboard biết là ĐANG CẬP NHẬT',
      CASE WHEN EXISTS(SELECT 1 FROM T_MASTER_BALANCE WHERE C_BUSINESS_DATE=@today AND C_MASTER_CODE='RTM'
                         AND C_SRC='RT' AND C_RT_SI_COUNT=2 AND C_TOTAL_ACCOUNT=3) THEN 1 ELSE 0 END, NULL;
    INSERT INTO @R SELECT 'B5 ★ dòng master RT có C_DAILY_RETURN NULL (không bịa lợi suất giữa phiên)',
      CASE WHEN (SELECT C_DAILY_RETURN FROM T_MASTER_BALANCE WHERE C_BUSINESS_DATE=@today
                   AND C_MASTER_CODE='RTM' AND C_SRC='RT') IS NULL THEN 1 ELSE 0 END, NULL;

    -- B6. API dashboard: biến động tính TRÊN CÙNG TẬP tiểu khoản
    CREATE TABLE #rt (mcode VARCHAR(20), mname NVARCHAR(200), aum DECIMAL(20,0), cash DECIMAL(20,0),
                      nrt INT, ntot INT, snap DATETIME, stale INT, prevd DATE, aumprev DECIMAL(20,0),
                      chabs DECIMAL(20,0), chpct DECIMAL(18,8), cov VARCHAR(10), inwin BIT);
    INSERT #rt EXEC SP_GET_PM_RT_OVERVIEW @p_master_code='RTM', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
    -- mốc so sánh = 1.0e9 + 0.5e9 = 1.5e9 (CHỈ RT01+RT02, KHÔNG gồm RT03) ⇒ biến động = +0.21e9 = +14%
    INSERT INTO @R SELECT 'B6 ★ mốc so sánh dùng CÙNG TẬP KH (1.5e9, không phải 1.7e9 của cả master)',
      CASE WHEN (SELECT aumprev FROM #rt)=1500000000 AND (SELECT chabs FROM #rt)=210000000
           THEN 1 ELSE 0 END,
      CONCAT('aum_prev=',(SELECT aumprev FROM #rt),' change=',(SELECT chabs FROM #rt));
    INSERT INTO @R SELECT 'B6 coverage=PARTIAL khi FO chưa trả đủ',
      CASE WHEN (SELECT cov FROM #rt)='PARTIAL' THEN 1 ELSE 0 END, (SELECT cov FROM #rt);
    DROP TABLE #rt;

    -- B7. ★★★ DÒNG RT KHÔNG LÂY SANG ĐƯỜNG EOD — phần dễ hỏng âm thầm nhất
    --   Cổng khoá err=12 phải VẪN thấy thiếu, dù RT01/RT02 đã "có dòng" @hôm nay.
    DECLARE @missSI INT = (SELECT COUNT(*) FROM T_SI_PORTFOLIO p
        WHERE p.C_STATUS='ACTIVE' AND p.C_MASTER_CODE='RTM'
          AND NOT EXISTS (SELECT 1 FROM T_SI_BALANCE a WHERE a.C_SI_ACCOUNT=p.C_SI_ACCOUNT
                          AND a.C_BUSINESS_DATE=@today AND a.C_SRC='EOD'));
    DECLARE @missNoFilter INT = (SELECT COUNT(*) FROM T_SI_PORTFOLIO p
        WHERE p.C_STATUS='ACTIVE' AND p.C_MASTER_CODE='RTM'
          AND NOT EXISTS (SELECT 1 FROM T_SI_BALANCE a WHERE a.C_SI_ACCOUNT=p.C_SI_ACCOUNT
                          AND a.C_BUSINESS_DATE=@today));
    INSERT INTO @R SELECT 'B7 ★★★ cổng err=12 VẪN thấy thiếu 2 KH (bỏ lọc C_SRC thì chỉ thấy 0 ⇒ PASS GIẢ)',
      CASE WHEN @missSI=2 AND @missNoFilter=0 THEN 1 ELSE 0 END,
      CONCAT('có lọc=',@missSI,' (đúng) · không lọc=',@missNoFilter,' (sẽ mở cổng oan)');

    -- B8. Phí: dòng RT KHÔNG được làm base tính phí
    IF OBJECT_ID('SP_EOD_FEE_ACCRUE') IS NOT NULL
    BEGIN
        DECLARE @feeSrc INT = (SELECT COUNT(*) FROM T_SI_BALANCE b
            INNER JOIN T_SI_PORTFOLIO p ON p.C_SI_ACCOUNT=b.C_SI_ACCOUNT AND p.C_STATUS='ACTIVE'
            WHERE b.C_BUSINESS_DATE=@today AND b.C_SRC='EOD' AND b.C_MASTER_CODE='RTM');
        INSERT INTO @R SELECT 'B8 ★ nguồn tính phí @hôm nay = 1 dòng CHỐT (2 dòng RT bị loại khỏi base)',
          CASE WHEN @feeSrc=1 THEN 1 ELSE 0 END, CONCAT('rows_EOD=',@feeSrc);
    END

    -- B9. Báo cáo AUM: mặc định "ngày mới nhất" phải là phiên CHỐT, không nhảy sang hôm nay vì có RT
    DECLARE @maxEod DATE = (SELECT MAX(C_BUSINESS_DATE) FROM T_SI_BALANCE WHERE C_SRC='EOD');
    DECLARE @maxAll DATE = (SELECT MAX(C_BUSINESS_DATE) FROM T_SI_BALANCE);
    INSERT INTO @R SELECT 'B9 ★ MAX(ngày) có lọc = phiên chốt; không lọc = hôm nay (bằng chứng vì sao phải lọc)',
      CASE WHEN @maxEod=@today AND @maxAll=@today THEN 1     -- RT03 đã chốt hôm nay ⇒ 2 mốc trùng nhau, hợp lệ
           WHEN @maxEod=@prev  AND @maxAll=@today THEN 1
           ELSE 0 END,
      CONCAT('maxEOD=',CONVERT(VARCHAR(10),@maxEod,23),' maxALL=',CONVERT(VARCHAR(10),@maxAll,23));
END

/*==============================================================================
  (C) ĐỔI CẤU HÌNH THỜI GIAN + CÁC ĐIỂM RỦI RO CAO
      Mỗi ca dưới đây trả lời MỘT câu hỏi cụ thể, không phải test cho có.
==============================================================================*/
DELETE FROM T_JOB_RUN        WHERE C_JOB_CODE IN ('SMK_CFG','SMK_OND');
DELETE FROM T_JOB_DEFINITION WHERE C_JOB_CODE IN ('SMK_CFG','SMK_OND');

-- Job có chu kỳ, KHÔNG khung giờ, KHÔNG singleton (cô lập đúng thứ đang đo).
INSERT INTO T_JOB_DEFINITION (C_JOB_CODE,C_JOB_NAME,C_HANDLER,C_ENABLED,C_INTERVAL_SEC,
        C_WINDOW_FROM,C_WINDOW_TO,C_BUSINESS_DAY_ONLY,C_TIMEOUT_SEC,C_MAX_ATTEMPT,C_RETRY_DELAY_SEC,C_SINGLETON)
VALUES ('SMK_CFG',N'Job đổi cấu hình (smoke)','SmokeHandler',1,900, NULL,NULL,0, 60, 3, 0, 0);
-- Job CHƯA CÓ cấu hình chu kỳ (chạy theo yêu cầu).
INSERT INTO T_JOB_DEFINITION (C_JOB_CODE,C_JOB_NAME,C_HANDLER,C_ENABLED,C_INTERVAL_SEC,
        C_WINDOW_FROM,C_WINDOW_TO,C_BUSINESS_DAY_ONLY,C_TIMEOUT_SEC,C_MAX_ATTEMPT,C_RETRY_DELAY_SEC,C_SINGLETON)
VALUES ('SMK_OND',N'Job chạy theo yêu cầu (smoke)','SmokeHandler',1,NULL, NULL,NULL,0, 60, 3, 0, 0);

DECLARE @purgedC INT, @cntC INT;

/*---- C1..C3. ĐỔI CẤU HÌNH ⇒ CẤU HÌNH CŨ KHÔNG ĐƯỢC CHẠY NỮA ----
  ⚠️ NGỮ NGHĨA ĐÃ ĐỔI từ khi bộ quét thôi ghi DB. Trước đây mỗi mốc để lại một dòng READY nằm chờ,
     nên "dọn cấu hình cũ" = xoá hàng đợi READY đó. Nay dòng sinh ra ĐÃ Ở RUNNING (do chính pod
     sắp chạy nó tạo), nên hàng đợi READY gần như luôn RỖNG.
  ⇒ Luật "cấu hình cũ chết theo cấu hình cũ" nay được giữ bởi HAI cơ chế KHÁC NHAU:
       (a) message mang mốc của LƯỚI CŨ còn nằm trong Kafka  → SP_JOB_CLAIM_SLOT đối chiếu với
           UDF_JOB_SLOT_AT của lưới MỚI ⇒ err=20, TỰ TỪ CHỐI. Đây mới là cơ chế chính.
       (b) dòng READY còn sót (retry đang chờ backoff)        → SP_SET_JOB_SCHEDULE / trigger dọn.
  Cả hai đều phải có ca kiểm.                                                                    */

-- C1. (a) Mốc của LƯỚI CŨ tự bị từ chối sau khi đổi chu kỳ — cơ chế chính.
DECLARE @slotOld DATETIME = dbo.UDF_JOB_SLOT_AT('SMK_CFG', @now);   -- lưới 900s
EXEC SP_SET_JOB_SCHEDULE @p_job_code='SMK_CFG', @p_interval_sec=1800, @p_user='ops',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_purged_runs=@purgedC OUTPUT;
DECLARE @slotNew DATETIME = dbo.UDF_JOB_SLOT_AT('SMK_CFG', @now);   -- lưới 1800s
EXEC SP_JOB_CLAIM_SLOT @p_job_code='SMK_CFG', @p_slot_at=@slotOld, @p_owner='pod-A', @p_source='kafka',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'C1 ★★ đổi 15p→30p: message mang mốc LƯỚI CŨ còn trong Kafka ⇒ err=20, tự từ chối',
  CASE WHEN (SELECT C_INTERVAL_SEC FROM T_JOB_DEFINITION WHERE C_JOB_CODE='SMK_CFG')=1800
        AND (@slotOld = @slotNew OR @ec = 20)   -- trùng mốc thì hợp lệ cả hai lưới; lệch thì phải bị chặn
        AND NOT EXISTS(SELECT 1 FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_CFG' AND C_FIRE_KEY=dbo.UDF_JOB_FIRE_KEY(@slotOld)
                         AND @slotOld <> @slotNew)
       THEN 1 ELSE 0 END,
  CONCAT('slot cũ=',CONVERT(VARCHAR(19),@slotOld,120),' slot mới=',CONVERT(VARCHAR(19),@slotNew,120),' err=',@ec);

-- C1b. Mốc của lưới MỚI thì chạy được bình thường.
--   Dọn trước: khi hai lưới TRÙNG mốc (now rơi vào nửa đầu bucket 30 phút) thì C1 đã tạo dòng đó
--   rồi, và claim lại chính nó sẽ ra err=5 — không phải điều C1b muốn đo.
DELETE FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_CFG';
EXEC SP_JOB_CLAIM_SLOT @p_job_code='SMK_CFG', @p_slot_at=@slotNew, @p_owner='pod-A', @p_source='kafka',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'C1b mốc của lưới MỚI ⇒ err=0, chạy bình thường',
  CASE WHEN @ec=0 THEN 1 ELSE 0 END, CONCAT('err=',@ec);

-- C2. (b) Dòng READY còn sót (retry đang chờ backoff) PHẢI bị dọn khi đổi cấu hình.
DELETE FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_CFG';
INSERT INTO T_JOB_RUN (C_JOB_CODE,C_FIRE_KEY,C_STATUS,C_RUN_AFTER,C_SLOT_AT,C_ENQUEUED_AT,C_ATTEMPT)
VALUES ('SMK_CFG','RETRY1','READY',DATEADD(SECOND,30,@now),@now,@now,1);   -- retry đang chờ backoff
SET @cntC = (SELECT COUNT(*) FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_CFG' AND C_STATUS='READY');
EXEC SP_SET_JOB_SCHEDULE @p_job_code='SMK_CFG', @p_interval_sec=900, @p_user='ops',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_purged_runs=@purgedC OUTPUT;
INSERT INTO @R SELECT 'C2 ★ đổi cấu hình ⇒ dọn dòng READY còn sót (retry đang chờ backoff)',
  CASE WHEN @cntC=1 AND @purgedC=1
        AND NOT EXISTS(SELECT 1 FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_CFG' AND C_STATUS='READY')
       THEN 1 ELSE 0 END, CONCAT('trước=',@cntC,' đã dọn=',@purgedC);

-- C2b. LƯỚI CHẶN: UPDATE THẲNG bảng cấu hình (bỏ qua cổng) cũng phải dọn.
INSERT INTO T_JOB_RUN (C_JOB_CODE,C_FIRE_KEY,C_STATUS,C_RUN_AFTER,C_SLOT_AT,C_ENQUEUED_AT,C_ATTEMPT)
VALUES ('SMK_CFG','RETRY2','READY',DATEADD(SECOND,30,@now),@now,@now,1);
UPDATE T_JOB_DEFINITION SET C_INTERVAL_SEC=3600 WHERE C_JOB_CODE='SMK_CFG';   -- gõ tay, bỏ qua cổng
INSERT INTO @R SELECT 'C2b ★ UPDATE THẲNG bảng cấu hình (bỏ qua cổng) ⇒ trigger vẫn dọn',
  CASE WHEN NOT EXISTS(SELECT 1 FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_CFG' AND C_STATUS='READY')
       THEN 1 ELSE 0 END, NULL;

/*---- C3. Trigger KHÔNG được dọn OAN ----*/
INSERT INTO T_JOB_RUN (C_JOB_CODE,C_FIRE_KEY,C_STATUS,C_RUN_AFTER,C_SLOT_AT,C_ENQUEUED_AT,C_ATTEMPT)
VALUES ('SMK_CFG','RETRY3','READY',DATEADD(SECOND,30,@now),@now,@now,1);
UPDATE T_JOB_DEFINITION SET C_TIMEOUT_SEC=120 WHERE C_JOB_CODE='SMK_CFG';       -- cột không liên quan lịch
INSERT INTO @R SELECT 'C3 đổi cột KHÔNG liên quan lịch (timeout) ⇒ KHÔNG dọn hàng đợi',
  CASE WHEN EXISTS(SELECT 1 FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_CFG' AND C_STATUS='READY')
       THEN 1 ELSE 0 END, NULL;
UPDATE T_JOB_DEFINITION SET C_INTERVAL_SEC=C_INTERVAL_SEC WHERE C_JOB_CODE='SMK_CFG';  -- ghi lại ĐÚNG giá trị cũ
INSERT INTO @R SELECT 'C3 ★ SET cột lịch = CHÍNH GIÁ TRỊ CŨ (ORM ghi cả hàng) ⇒ KHÔNG dọn oan',
  CASE WHEN EXISTS(SELECT 1 FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_CFG' AND C_STATUS='READY')
       THEN 1 ELSE 0 END, NULL;
DELETE FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_CFG';
EXEC SP_SET_JOB_SCHEDULE @p_job_code='SMK_CFG', @p_interval_sec=900, @p_user='ops',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_purged_runs=@purgedC OUTPUT;

/*---- C4. Lượt ĐANG CHẠY không bị đổi cấu hình giết giữa chừng ----*/
DELETE FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_CFG';
INSERT INTO T_JOB_RUN (C_JOB_CODE,C_FIRE_KEY,C_STATUS,C_OWNER,C_LEASE_UNTIL,
                       C_RUN_AFTER,C_SLOT_AT,C_ENQUEUED_AT,C_STARTED_AT,C_ATTEMPT)
VALUES ('SMK_CFG','DANGCHAY','RUNNING','pod-A',DATEADD(MINUTE,5,@now),
        @now,@now,@now,@now,1);
EXEC SP_SET_JOB_SCHEDULE @p_job_code='SMK_CFG', @p_interval_sec=900, @p_user='ops',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_purged_runs=@purgedC OUTPUT;
INSERT INTO @R SELECT 'C4 đổi cấu hình KHÔNG giết lượt đang RUNNING (không DELETE được pod đang gọi FO)',
  CASE WHEN @purgedC=0 AND EXISTS(SELECT 1 FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_CFG' AND C_STATUS='RUNNING')
       THEN 1 ELSE 0 END, CONCAT('purged=',@purgedC);

/*---- C5. TẮT job: chu kỳ ĐANG CHẠY phải dừng (nút tắt khẩn cấp có tác dụng thật) ----*/
DECLARE @runId BIGINT = (SELECT TOP 1 C_JOB_RUN_ID FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_CFG' AND C_STATUS='RUNNING');
EXEC SP_JOB_HEARTBEAT @p_job_run_id=@runId, @p_owner='pod-A', @p_still_mine=@mine OUTPUT;
DECLARE @beatOn BIT = @mine;
EXEC SP_SET_JOB_SCHEDULE @p_job_code='SMK_CFG', @p_enabled=0, @p_user='ops',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_purged_runs=@purgedC OUTPUT;
EXEC SP_JOB_HEARTBEAT @p_job_run_id=@runId, @p_owner='pod-A', @p_still_mine=@mine OUTPUT;
INSERT INTO @R SELECT 'C5 * TẮT job: heartbeat trả still_mine=0 -> chu kỳ đang chạy TỰ DỪNG (~20s)',
  CASE WHEN @beatOn=1 AND @mine=0 THEN 1 ELSE 0 END, CONCAT('trước=',@beatOn,' sau=',@mine);
EXEC SP_SET_JOB_SCHEDULE @p_job_code='SMK_CFG', @p_enabled=1, @p_user='ops',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_purged_runs=@purgedC OUTPUT;

/*---- C6. *** JOB QUÁ GIỜ KHÔNG ĐƯỢC CHẠY LẠI (lượt 14:59 hôm qua sống tới 9h hôm nay) ----*/
DELETE FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_CFG';
INSERT INTO T_JOB_RUN (C_JOB_CODE,C_FIRE_KEY,C_STATUS,C_RUN_AFTER,C_ENQUEUED_AT)
VALUES ('SMK_CFG','HOMQUA1459','READY',DATEADD(HOUR,-18,@now),DATEADD(HOUR,-18,@now));
DECLARE @oldId BIGINT = SCOPE_IDENTITY();
DELETE #scan; DELETE #rec;
INSERT #rec EXEC SP_JOB_RECOVER @p_stale_sec=0, @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'C6 *** lượt 18 tiếng trước: RECOVER đóng dấu SKIPPED, KHÔNG đẩy lại để chạy',
  CASE WHEN (SELECT C_STATUS FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@oldId)='SKIPPED'
        AND NOT EXISTS(SELECT 1 FROM #rec WHERE fire_key='HOMQUA1459') THEN 1 ELSE 0 END,
  (SELECT CONCAT(C_STATUS,' | ',LEFT(C_MESSAGE,55)) FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@oldId);
-- Chốt chặn THỨ HAI, độc lập với bộ hồi phục: giả sử RECOVER bỏ sót và message của lượt 18 tiếng
--   trước vẫn được đẩy lại vào Kafka, thì SP_JOB_CLAIM_SLOT vẫn phải chặn ở HẠN TƯƠI.
DECLARE @slotHQ DATETIME = dbo.UDF_JOB_SLOT_AT('SMK_CFG', DATEADD(HOUR,-18,@now));
UPDATE T_JOB_RUN SET C_STATUS='READY', C_ENDED_AT=NULL, C_SLOT_AT=@slotHQ,
       C_FIRE_KEY=dbo.UDF_JOB_FIRE_KEY(@slotHQ) WHERE C_JOB_RUN_ID=@oldId;
EXEC SP_JOB_CLAIM_SLOT @p_job_code='SMK_CFG', @p_slot_at=@slotHQ, @p_owner='pod-A',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'C6 * CLAIM lượt quá hạn: err=3 + SKIPPED (chốt chặn thứ hai, độc lập RECOVER)',
  CASE WHEN @ec=3 AND (SELECT C_STATUS FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@oldId)='SKIPPED'
       THEN 1 ELSE 0 END, CONCAT('err=',@ec);

/*---- C7. CHƯA CÓ cấu hình chu kỳ: scheduler KHÔNG sinh, nhưng đẩy tay VẪN chạy ----*/
DELETE #scan; DELETE #rec;
EXEC SP_SMK_SCAN;
INSERT INTO @R SELECT 'C7 * chưa cấu hình chu kỳ (interval NULL): scheduler KHÔNG sinh lượt nào',
  CASE WHEN NOT EXISTS(SELECT 1 FROM #scan WHERE code='SMK_OND') THEN 1 ELSE 0 END, NULL;
-- Job on-demand KHÔNG có chu kỳ ⇒ không có lưới mốc. Người vận hành truyền @now làm mốc và tự đặt
--   fire_key (requestId) ⇒ vẫn đi qua ĐÚNG MỘT cổng SP_JOB_CLAIM_SLOT, không có đường tắt riêng.
SET @id2 = NULL;
EXEC SP_JOB_CLAIM_SLOT @p_job_code='SMK_OND', @p_slot_at=@now, @p_owner='pod-A', @p_fire_key='TAY1',
     @p_source='manual', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_job_run_id=@id2 OUTPUT;
INSERT INTO @R SELECT 'C7 * ...nhưng ĐẨY TAY thì vẫn chạy ngay (err=0, dòng vào thẳng RUNNING)',
  CASE WHEN @ec=0 AND @id2 IS NOT NULL
        AND (SELECT C_STATUS FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@id2)='RUNNING'
       THEN 1 ELSE 0 END, CONCAT('err=',@ec);

/*---- C8. Job chạy-theo-yêu-cầu KHÔNG được tự hết hạn ----*/
INSERT INTO T_JOB_RUN (C_JOB_CODE,C_FIRE_KEY,C_STATUS,C_RUN_AFTER,C_ENQUEUED_AT)
VALUES ('SMK_OND','CHOLAU','READY',DATEADD(DAY,-3,@now),DATEADD(DAY,-3,@now));
DECLARE @ondId BIGINT = SCOPE_IDENTITY();
EXEC SP_JOB_RECOVER @p_stale_sec=0, @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'C8 * job on-demand: lượt đẩy tay KHÔNG tự bốc hơi dù đã chờ 3 ngày',
  CASE WHEN (SELECT C_STATUS FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@ondId)='READY' THEN 1 ELSE 0 END,
  (SELECT C_STATUS FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@ondId);

/*---- C9. XOÁ chu kỳ (đang có -> clear): dừng sinh + dọn lượt chờ ----*/
DELETE FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_CFG';
-- Hàng đợi READY nay CHỈ gồm retry (bộ quét không để lại dòng chờ nào) ⇒ dựng tay một dòng như vậy.
INSERT INTO T_JOB_RUN (C_JOB_CODE,C_FIRE_KEY,C_STATUS,C_RUN_AFTER,C_SLOT_AT,C_ENQUEUED_AT,C_ATTEMPT)
VALUES ('SMK_CFG','RETRY9','READY',DATEADD(SECOND,30,@now),@now,@now,1);
SET @cntC = (SELECT COUNT(*) FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_CFG' AND C_STATUS='READY');
EXEC SP_SET_JOB_SCHEDULE @p_job_code='SMK_CFG', @p_clear_interval=1, @p_user='ops',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_purged_runs=@purgedC OUTPUT;
DELETE #scan; DELETE #rec;
EXEC SP_SMK_SCAN;
INSERT INTO @R SELECT 'C9 * XOÁ chu kỳ: dọn lượt chờ + scheduler ngừng sinh (job về chạy-theo-yêu-cầu)',
  CASE WHEN @cntC=1 AND @purgedC=1
        AND (SELECT C_INTERVAL_SEC FROM T_JOB_DEFINITION WHERE C_JOB_CODE='SMK_CFG') IS NULL
        AND NOT EXISTS(SELECT 1 FROM #scan WHERE code='SMK_CFG')
        AND NOT EXISTS(SELECT 1 FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_CFG' AND C_STATUS='READY')
       THEN 1 ELSE 0 END, CONCAT('trước=',@cntC,' dọn=',@purgedC);

-- C9 ★★ ...NHƯNG DB KHÔNG CHẶN ĐƯỢC POD CÒN GIỮ CACHE CŨ — và đó là CỐ Ý.
--   Xoá chu kỳ xong, một pod chưa kịp nạp lại cache vẫn tính mốc theo lưới 900 cũ và vẫn produce.
--   Mốc đó tới SP_JOB_CLAIM_SLOT: job nay KHÔNG còn chu kỳ ⇒ UDF_JOB_SLOT_AT trả NULL ⇒ phép đối
--   chiếu lưới TỰ BỎ QUA (err=20 không bắn) ⇒ mốc đi thẳng tới guard giờ giấc và CHẠY THẬT.
--   Không thể sửa ở DB: bỏ qua đối chiếu khi interval NULL chính là thứ cho phép job
--   chạy-theo-yêu-cầu đẩy tay với mốc bất kỳ. Hai nhu cầu dùng chung một đường.
--   ⇒ Chốt chặn nằm ở tầng ứng dụng: JobConfigService PHẢI xoá+nạp lại SDI:JOB:CFG ngay khi đổi
--     lịch, và PHẢI xoá khoá trước khi ghi (HashSet chồng lên KHÔNG xoá được field của job vừa bị
--     gỡ lịch). Ca kiểm này tồn tại để ai định bỏ bước đó thì thấy trước hậu quả.
DECLARE @slotClr DATETIME = DATEADD(SECOND, -(DATEDIFF(SECOND, CAST(CAST(@now AS DATE) AS DATETIME), @now) % 900), @now);
DECLARE @idClr BIGINT;
DELETE FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_CFG';
EXEC SP_JOB_CLAIM_SLOT @p_job_code='SMK_CFG', @p_slot_at=@slotClr, @p_owner='pod-cu', @p_source='kafka',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_job_run_id=@idClr OUTPUT;
INSERT INTO @R SELECT 'C9 ★★ xoá chu kỳ mà pod còn cache cũ VẪN chạy được ⇒ dọn cache là BẮT BUỘC',
  CASE WHEN @ec=0 AND (SELECT C_STATUS FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@idClr)='RUNNING'
       THEN 1 ELSE 0 END,
  CONCAT('err=',@ec,' status=',ISNULL((SELECT C_STATUS FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@idClr),'(khong tao dong)'));
DELETE FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_CFG';

EXEC SP_SET_JOB_SCHEDULE @p_job_code='SMK_CFG', @p_interval_sec=900, @p_user='ops',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_purged_runs=@purgedC OUTPUT;

/*---- C10. Pod mới / mất message: lượt READY vẫn được nhặt lại (không mất job) ----*/
DELETE FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_CFG';
INSERT INTO T_JOB_RUN (C_JOB_CODE,C_FIRE_KEY,C_STATUS,C_RUN_AFTER,C_ENQUEUED_AT)
VALUES ('SMK_CFG','MATMSG','READY',DATEADD(SECOND,-120,@now),DATEADD(SECOND,-120,@now));
DECLARE @lostId BIGINT = SCOPE_IDENTITY();
DELETE #scan; DELETE #rec;
INSERT #rec EXEC SP_JOB_RECOVER @p_stale_sec=60, @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'C10 * mất message Kafka ⇒ RECOVER trả MỐC để produce lại (lượt đã chạy dở không mất)',
  CASE WHEN EXISTS(SELECT 1 FROM #rec WHERE fire_key='MATMSG') THEN 1 ELSE 0 END, NULL;
DELETE FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_CFG';
INSERT INTO T_JOB_RUN (C_JOB_CODE,C_FIRE_KEY,C_STATUS,C_RUN_AFTER,C_ENQUEUED_AT)
VALUES ('SMK_CFG','VUASINH','READY',@now,@now);
DELETE #scan; DELETE #rec;
INSERT #rec EXEC SP_JOB_RECOVER @p_stale_sec=60, @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'C10 lượt VỪA sinh (chưa quá stale_sec): RECOVER KHÔNG đẩy lại (khỏi produce trùng mỗi 30s)',
  CASE WHEN NOT EXISTS(SELECT 1 FROM #rec WHERE code='SMK_CFG') THEN 1 ELSE 0 END, NULL;

/*---- C11. KHÔNG back-fill: dừng dịch vụ nửa ngày rồi bật lại -> chỉ 1 lượt ----*/
DELETE FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_CFG';
DELETE #scan; DELETE #rec;
EXEC SP_SMK_SCAN;
INSERT INTO @R SELECT 'C11 * bật lại dịch vụ sau nhiều giờ: CHỈ sinh slot HIỆN TẠI, không dồn slot đã lỡ',
  CASE WHEN (SELECT COUNT(*) FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_CFG')=1 THEN 1 ELSE 0 END,
  CONCAT('sinh=',(SELECT COUNT(*) FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_CFG'));

/*---- C12. Dọn nhật ký KHÔNG được làm job chạy lại trong cùng slot ----*/
UPDATE T_JOB_RUN SET C_STATUS='DONE', C_ENDED_AT=@now WHERE C_JOB_CODE='SMK_CFG';
EXEC SP_JOB_PURGE @p_keep_days=0, @p_rows=@rows OUTPUT;   -- gọi ẩu: 0 ngày
DELETE #scan; DELETE #rec;
EXEC SP_SMK_SCAN;
INSERT INTO @R SELECT 'C12 * PURGE keep_days=0 bị ép sàn 1 ngày: lượt hôm nay còn -> KHÔNG sinh lại slot vừa chạy',
  CASE WHEN EXISTS(SELECT 1 FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_CFG' AND C_STATUS='DONE')
        AND NOT EXISTS(SELECT 1 FROM #scan WHERE code='SMK_CFG') THEN 1 ELSE 0 END,
  CONCAT('purged=',@rows);

/*---- C13. Cổng đổi lịch phải VALIDATE, không nhận cấu hình vô nghĩa ----*/
EXEC SP_SET_JOB_SCHEDULE @p_job_code='SMK_CFG', @p_interval_sec=5, @p_user='ops',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_purged_runs=@purgedC OUTPUT;
DECLARE @e1 INT = @ec;
EXEC SP_SET_JOB_SCHEDULE @p_job_code='SMK_CFG', @p_window_from='15:00', @p_window_to='09:00', @p_user='ops',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_purged_runs=@purgedC OUTPUT;
DECLARE @e2 INT = @ec;
EXEC SP_SET_JOB_SCHEDULE @p_job_code='SMK_CFG', @p_window_from='09:00', @p_user='ops',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_purged_runs=@purgedC OUTPUT;
DECLARE @e3 INT = @ec;
EXEC SP_SET_JOB_SCHEDULE @p_job_code='KHONG_CO', @p_interval_sec=900, @p_user='ops',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_purged_runs=@purgedC OUTPUT;
INSERT INTO @R SELECT 'C13 cổng validate: chu kỳ 5s=20 - khung đảo ngược=20 - nửa khung=20 - job lạ=1',
  CASE WHEN @e1=20 AND @e2=20 AND @e3=20 AND @ec=1 THEN 1 ELSE 0 END,
  CONCAT(@e1,'/',@e2,'/',@e3,'/',@ec);
INSERT INTO @R SELECT 'C13 * cấu hình KHÔNG bị sửa dở dang sau khi validate từ chối',
  CASE WHEN (SELECT C_INTERVAL_SEC FROM T_JOB_DEFINITION WHERE C_JOB_CODE='SMK_CFG')=900
        AND (SELECT C_WINDOW_FROM FROM T_JOB_DEFINITION WHERE C_JOB_CODE='SMK_CFG') IS NULL
       THEN 1 ELSE 0 END, NULL;

/*---- C14. Đổi cấu hình job FO thật: 15 phút -> 1 tiếng, rồi trả lại ----*/
EXEC SP_SET_JOB_SCHEDULE @p_job_code='FO_SNAPSHOT_RT', @p_interval_sec=3600, @p_user='ops',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_purged_runs=@purgedC OUTPUT;
INSERT INTO @R SELECT 'C14 đổi job FO sang 1 tiếng/lần: err=0, khung giờ 09:00-15:00 GIỮ NGUYÊN',
  CASE WHEN @ec=0
        AND (SELECT C_INTERVAL_SEC FROM T_JOB_DEFINITION WHERE C_JOB_CODE='FO_SNAPSHOT_RT')=3600
        AND (SELECT C_WINDOW_FROM  FROM T_JOB_DEFINITION WHERE C_JOB_CODE='FO_SNAPSHOT_RT')='09:00:00'
        AND (SELECT C_WINDOW_TO    FROM T_JOB_DEFINITION WHERE C_JOB_CODE='FO_SNAPSHOT_RT')='15:00:00'
       THEN 1 ELSE 0 END, ISNULL(@em,'');
EXEC SP_SET_JOB_SCHEDULE @p_job_code='FO_SNAPSHOT_RT', @p_interval_sec=900, @p_user='ops',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_purged_runs=@purgedC OUTPUT;
INSERT INTO @R SELECT 'C14 trả lại 15 phút: err=0',
  CASE WHEN @ec=0 AND (SELECT C_INTERVAL_SEC FROM T_JOB_DEFINITION WHERE C_JOB_CODE='FO_SNAPSHOT_RT')=900
       THEN 1 ELSE 0 END, NULL;

/*---- C15. HẠN TƯƠI neo vào MỐC SLOT — thay cho khái niệm "khung + ân hạn" đã bỏ ----*/
DELETE FROM T_JOB_RUN        WHERE C_JOB_CODE IN ('SMK_FRESH','SMK_LATE');
DELETE FROM T_JOB_DEFINITION WHERE C_JOB_CODE IN ('SMK_FRESH','SMK_LATE');
-- Khung 00:00-23:59 để mọi mốc đều nằm trong khung ⇒ cô lập đúng phép đo HẠN TƯƠI,
--   không phụ thuộc smoke chạy vào giờ nào trong ngày.
INSERT INTO T_JOB_DEFINITION (C_JOB_CODE,C_JOB_NAME,C_HANDLER,C_INTERVAL_SEC,
        C_WINDOW_FROM,C_WINDOW_TO,C_BUSINESS_DAY_ONLY,C_TIMEOUT_SEC,C_MAX_ATTEMPT,C_MAX_DELAY_SEC,C_SINGLETON)
--   Chu kỳ 60 giây: mốc lưới gần nhất luôn cũ dưới 60 giây ⇒ ca "còn tươi" KHÔNG phụ thuộc smoke
--   chạy vào lúc nào trong bucket. Để 900 thì mốc có thể cũ tới 899s và ca (a) đỏ theo giờ chạy.
VALUES ('SMK_FRESH',N'Hạn tươi (smoke)','SmokeHandler',60,'00:00:00','23:59:00',0,300,3,600,0);
-- Job có khung giờ HẸP: dùng để kiểm "mốc nằm ngoài khung thì chặn", tách khỏi phép đo hạn tươi.
INSERT INTO T_JOB_DEFINITION (C_JOB_CODE,C_JOB_NAME,C_HANDLER,C_INTERVAL_SEC,
        C_WINDOW_FROM,C_WINDOW_TO,C_BUSINESS_DAY_ONLY,C_TIMEOUT_SEC,C_MAX_ATTEMPT,C_MAX_DELAY_SEC,C_SINGLETON)
--   Chu kỳ 1 ngày ⇒ lưới mốc = đúng nửa đêm. Khung 12:00–12:01 ⇒ mốc nửa đêm CHẮC CHẮN ngoài khung.
--   Hạn tươi 2 ngày ⇒ mốc đó VẪN còn tươi ⇒ ca này đo ĐÚNG guard khung giờ, không lẫn hạn tươi.
VALUES ('SMK_LATE',N'Mốc ngoài khung (smoke)','SmokeHandler',86400,'12:00:00','12:01:00',0,300,3,172800,0);

-- (a) MỐC CÒN TƯƠI (60 giây trước) ⇒ CHẠY ĐƯỢC. Đây chính là ca "mốc 15:00 claim lúc 15:00:03":
--     nếu áp khung giờ lên @now thay vì lên MỐC thì ca này trượt và ảnh chụp đóng cửa chết yểu.
--   Mốc phải NẰM TRÊN LƯỚI của job, nếu không SP_JOB_CLAIM_SLOT từ chối err=20 (đúng như thiết kế).
DECLARE @slotFresh DATETIME = dbo.UDF_JOB_SLOT_AT('SMK_FRESH', @now);              -- cũ 0..59 giây
INSERT INTO T_JOB_RUN (C_JOB_CODE,C_FIRE_KEY,C_STATUS,C_RUN_AFTER,C_SLOT_AT,C_ENQUEUED_AT)
VALUES ('SMK_FRESH','TUOI','READY',@now,@slotFresh,@now);
DECLARE @freshId BIGINT = SCOPE_IDENTITY();
EXEC SP_JOB_CLAIM_SLOT @p_job_code='SMK_FRESH', @p_slot_at=@slotFresh, @p_owner='pod-A',
     @p_fire_key='TUOI', @p_source='kafka', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'C15 ★ mốc còn TƯƠI (60s) ⇒ claim OK (mốc cuối phiên KHÔNG chết yểu)',
  CASE WHEN @ec=0 AND (SELECT C_STATUS FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@freshId)='RUNNING'
       THEN 1 ELSE 0 END, CONCAT('err=',@ec);

-- (b) MỐC QUÁ HẠN TƯƠI (700 giây > 600) ⇒ SKIPPED, dù mốc vẫn nằm trong khung.
DECLARE @slotStale DATETIME = DATEADD(SECOND,-1200,@slotFresh);   -- vẫn trên lưới, cũ 1200..1259 giây
INSERT INTO T_JOB_RUN (C_JOB_CODE,C_FIRE_KEY,C_STATUS,C_RUN_AFTER,C_SLOT_AT,C_ENQUEUED_AT)
VALUES ('SMK_FRESH','HET','READY',@now,@slotStale,@now);
DECLARE @staleId BIGINT = SCOPE_IDENTITY();
EXEC SP_JOB_CLAIM_SLOT @p_job_code='SMK_FRESH', @p_slot_at=@slotStale, @p_owner='pod-A',
     @p_fire_key='HET', @p_source='kafka', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'C15 ★ mốc quá HẠN TƯƠI (1200s > 600s) ⇒ err=3 + SKIPPED, dù vẫn trong khung',
  CASE WHEN @ec=3 AND (SELECT C_STATUS FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@staleId)='SKIPPED'
       THEN 1 ELSE 0 END, CONCAT('err=',@ec);

-- (c) RETRY KHÔNG làm mới hạn tươi. Đẩy C_RUN_AFTER lên hiện tại (giả lập vừa hết backoff) nhưng
--     giữ nguyên mốc cũ ⇒ vẫn phải SKIPPED. Nếu đo hạn tươi từ C_RUN_AFTER thì ca này lọt, và một
--     lượt thử lại mãi sẽ tự làm mới hạn của chính nó rồi bò qua giờ đóng cửa.
UPDATE T_JOB_RUN SET C_STATUS='READY', C_ENDED_AT=NULL, C_RUN_AFTER=@now, C_ATTEMPT=1
 WHERE C_JOB_RUN_ID=@staleId;
EXEC SP_JOB_CLAIM_SLOT @p_job_code='SMK_FRESH', @p_slot_at=@slotStale, @p_owner='pod-A',
     @p_fire_key='HET', @p_source='kafka', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'C15 ★★ RETRY KHÔNG làm mới hạn tươi (đo từ MỐC, không phải C_RUN_AFTER)',
  CASE WHEN @ec=3 THEN 1 ELSE 0 END, CONCAT('err=',@ec);

-- (d) MỐC NGOÀI KHUNG ⇒ chặn, DÙ CÒN TƯƠI (hạn tươi của SMK_LATE để 2 ngày). Hai guard độc lập.
DECLARE @slotOut DATETIME = dbo.UDF_JOB_SLOT_AT('SMK_LATE', @now);   -- lưới 1 ngày ⇒ đúng nửa đêm
INSERT INTO T_JOB_RUN (C_JOB_CODE,C_FIRE_KEY,C_STATUS,C_RUN_AFTER,C_SLOT_AT,C_ENQUEUED_AT)
VALUES ('SMK_LATE','NGOAI','READY',@now,@slotOut,@now);
DECLARE @outId BIGINT = SCOPE_IDENTITY();
EXEC SP_JOB_CLAIM_SLOT @p_job_code='SMK_LATE', @p_slot_at=@slotOut, @p_owner='pod-A',
     @p_fire_key='NGOAI', @p_source='kafka', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'C15 ★ mốc NGOÀI khung (mốc nửa đêm vs khung 12:00-12:01) ⇒ chặn, dù còn tươi',
  CASE WHEN @ec=3 AND (SELECT C_STATUS FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@outId)='SKIPPED'
       THEN 1 ELSE 0 END, CONCAT('err=',@ec);

-- (e) Bộ quét ghi đúng MỐC SLOT (không phải thời điểm quét).
--   Chu kỳ SMK_FRESH là 60 giây nên mốc lưới gần nhất luôn cũ dưới 60 giây < hạn tươi 600 giây
--   ⇒ ca này KHÔNG phụ thuộc smoke chạy vào lúc nào trong ngày.
DELETE FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_FRESH';
DELETE #scan; DELETE #rec;
EXEC SP_SMK_SCAN;
INSERT INTO @R SELECT 'C15 ★ bộ quét ghi C_SLOT_AT = mốc slot (giây = 00, chia hết chu kỳ)',
  CASE WHEN EXISTS(SELECT 1 FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_FRESH'
                     AND C_SLOT_AT IS NOT NULL
                     AND DATEPART(SECOND, C_SLOT_AT)=0
                     AND DATEDIFF(SECOND, CAST(CAST(C_SLOT_AT AS DATE) AS DATETIME), C_SLOT_AT)
                         % (SELECT C_INTERVAL_SEC FROM T_JOB_DEFINITION WHERE C_JOB_CODE='SMK_FRESH') = 0)
       THEN 1 ELSE 0 END,
  (SELECT TOP 1 CONVERT(VARCHAR(19),C_SLOT_AT,120) FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_FRESH');

DELETE FROM T_JOB_RUN        WHERE C_JOB_CODE IN ('SMK_FRESH','SMK_LATE');
DELETE FROM T_JOB_DEFINITION WHERE C_JOB_CODE IN ('SMK_FRESH','SMK_LATE');

DELETE FROM T_JOB_RUN        WHERE C_JOB_CODE IN ('SMK_CFG','SMK_OND');
DELETE FROM T_JOB_DEFINITION WHERE C_JOB_CODE IN ('SMK_CFG','SMK_OND');

/*==============================================================================
  KẾT QUẢ
==============================================================================*/
SELECT name AS [Check], CASE WHEN ok=1 THEN 'PASS' ELSE 'FAIL' END AS [Result], detail AS [Detail] FROM @R ORDER BY id;
DECLARE @pass INT=(SELECT COUNT(*) FROM @R WHERE ok=1), @tot INT=(SELECT COUNT(*) FROM @R);
PRINT REPLICATE('=',70);
PRINT CONCAT('JOB/RT SMOKE: ',@pass,'/',@tot,' PASS  -> ', CASE WHEN @pass=@tot THEN 'ALL GREEN' ELSE 'HAS FAILURES' END);
IF @isGD=0 PRINT 'LUU Y: hom nay KHONG phai ngay GD -> khoi (B) da bo qua. Chay lai vao ngay GD de phu het.';
IF @pass<>@tot SELECT name AS [FAILED], detail FROM @R WHERE ok=0;
