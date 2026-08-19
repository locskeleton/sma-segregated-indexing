/*==============================================================================
  SDI — SMOKE TEST KHUNG JOB + SNAPSHOT NEAR-REALTIME FO (11_JOB.sql).
  Chạy SAU 01_TABLES + 02_SP_ENGINE + 11_JOB (09_FEE nếu muốn ca "RT không vào phí").

  TRỌNG TÂM — 4 yêu cầu BRD, mỗi cái phải có ca chứng minh:
    (1) Khung chạy được với MỌI loại job  → seed 1 job giả (không phải FO) chạy qua cùng bộ proc.
    (2) Đẩy job vào là xử lý ngay         → SP_JOB_ENQUEUE trả id ⇒ claim được NGAY, không chờ.
    (3) Nhiều pod KHÔNG xử lý trùng       → 2 pod claim cùng 1 lượt: đúng 1 thắng.
    (4) FO chỉ chạy 9h–15h ngày GD        → chặn ở CẢ 3 tầng (sinh job / nhận job / ghi dữ liệu).
  Và quan trọng nhất, phần dễ hỏng âm thầm nhất:
    (5) Dòng RT KHÔNG lây sang đường EOD  → err=12, phí, báo cáo AUM đều phải MÙ với dòng RT.

  ⚠️ PHỤ THUỘC THỜI GIAN THẬT: proc RT cố tình chỉ nhận NGÀY HÔM NAY và chỉ ngày GD (guard tầng 3).
     Nên khối (B) chỉ chạy được vào NGÀY GIAO DỊCH. Chạy vào T7/CN thì khối đó tự BỎ QUA và in
     cảnh báo — KHÔNG báo FAIL giả. Khối (A) khung job chạy được mọi lúc (tự chỉnh khung giờ job giả).
==============================================================================*/
SET NOCOUNT ON; SET QUOTED_IDENTIFIER ON; SET ANSI_NULLS ON;

DECLARE @R TABLE (id INT IDENTITY, name VARCHAR(90), ok BIT, detail NVARCHAR(300));
DECLARE @ec INT, @em NVARCHAR(400), @rows BIGINT, @id BIGINT, @id2 BIGINT, @mine BIT, @skip INT;
DECLARE @now DATETIME = dbo.UDF_JOB_NOW();
DECLARE @today DATE = CAST(@now AS DATE);
DECLARE @isGD BIT = dbo.UDF_IS_BUSINESS_DATE(@today);

/*--- dọn dữ liệu test cũ ---*/
DELETE FROM T_JOB_RUN        WHERE C_JOB_CODE IN ('SMK_ANY','SMK_WIN','SMK_SGL','FO_SNAPSHOT_RT');
DELETE FROM T_JOB_DEFINITION WHERE C_JOB_CODE IN ('SMK_ANY','SMK_WIN','SMK_SGL');
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

-- A2. Đẩy job vào là chạy ngay (yêu cầu BRD #2) + idempotent theo fire_key
EXEC SP_JOB_ENQUEUE @p_job_code='SMK_ANY', @p_fire_key='FK1', @p_payload=N'{"x":1}',
     @p_user='smoke', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_job_run_id=@id OUTPUT;
INSERT INTO @R SELECT 'A2 SP_JOB_ENQUEUE: đẩy job ⇒ err=0 + có job_run_id ngay',
  CASE WHEN @ec=0 AND @id IS NOT NULL THEN 1 ELSE 0 END, CONCAT('err=',@ec,' id=',@id);

EXEC SP_JOB_ENQUEUE @p_job_code='SMK_ANY', @p_fire_key='FK1',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_job_run_id=@id2 OUTPUT;
INSERT INTO @R SELECT 'A2 SP_JOB_ENQUEUE: cùng fire_key ⇒ err=4 + KHÔNG sinh lượt mới (idempotent)',
  CASE WHEN @ec=4 AND @id2=@id AND (SELECT COUNT(*) FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_ANY')=1
       THEN 1 ELSE 0 END, CONCAT('err=',@ec,' id2=',@id2);

EXEC SP_JOB_ENQUEUE @p_job_code='KHONG_CO', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'A2 SP_JOB_ENQUEUE: job_code lạ ⇒ err=1', CASE WHEN @ec=1 THEN 1 ELSE 0 END, CONCAT('err=',@ec);

-- A3. ★ GUARD TẦNG 1 — không sinh lượt chạy ngoài khung giờ
EXEC SP_JOB_ENQUEUE @p_job_code='SMK_WIN', @p_fire_key='W1',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_job_run_id=@id2 OUTPUT;
INSERT INTO @R SELECT 'A3 TẦNG 1: enqueue ngoài khung ⇒ err=3 + KHÔNG có dòng nào trong hàng đợi',
  CASE WHEN @ec=3 AND (SELECT COUNT(*) FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_WIN')=0 THEN 1 ELSE 0 END,
  CONCAT('err=',@ec);

-- A4. ★ NHIỀU POD KHÔNG XỬ LÝ TRÙNG (yêu cầu BRD #3) — hai pod tranh cùng 1 lượt
EXEC SP_JOB_CLAIM @p_job_run_id=@id, @p_owner='pod-A', @p_stream_id='1-1',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
DECLARE @ecA INT = @ec;
EXEC SP_JOB_CLAIM @p_job_run_id=@id, @p_owner='pod-B', @p_stream_id='1-1',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'A4 ★ 2 pod claim cùng 1 lượt ⇒ ĐÚNG 1 thắng (A=0, B=5)',
  CASE WHEN @ecA=0 AND @ec=5 AND (SELECT C_OWNER FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@id)='pod-A'
       THEN 1 ELSE 0 END, CONCAT('podA err=',@ecA,' podB err=',@ec);
INSERT INTO @R SELECT 'A4 claim ⇒ RUNNING + attempt=1 + có lease',
  CASE WHEN EXISTS(SELECT 1 FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@id AND C_STATUS='RUNNING'
                     AND C_ATTEMPT=1 AND C_LEASE_UNTIL IS NOT NULL) THEN 1 ELSE 0 END, NULL;

-- A5. Heartbeat có hàng rào chủ sở hữu (chống pod zombie ghi song song)
EXEC SP_JOB_HEARTBEAT @p_job_run_id=@id, @p_owner='pod-A', @p_rows=10, @p_still_mine=@mine OUTPUT;
INSERT INTO @R SELECT 'A5 heartbeat ĐÚNG chủ ⇒ still_mine=1', CASE WHEN @mine=1 THEN 1 ELSE 0 END, NULL;
EXEC SP_JOB_HEARTBEAT @p_job_run_id=@id, @p_owner='pod-B', @p_still_mine=@mine OUTPUT;
INSERT INTO @R SELECT 'A5 ★ heartbeat SAI chủ (pod zombie) ⇒ still_mine=0 ⇒ pod đó phải tự dừng',
  CASE WHEN @mine=0 THEN 1 ELSE 0 END, NULL;

-- A6. Lease hết hạn = pod chết → SP_JOB_REAP thu hồi, pod khác giành được
UPDATE T_JOB_RUN SET C_LEASE_UNTIL=DATEADD(MINUTE,-5,@now) WHERE C_JOB_RUN_ID=@id;
EXEC SP_JOB_REAP @p_stale_sec=0, @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'A6 SP_JOB_REAP: lease quá hạn ⇒ RUNNING→READY (không tăng attempt)',
  CASE WHEN EXISTS(SELECT 1 FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@id AND C_STATUS='READY'
                     AND C_ATTEMPT=1 AND C_OWNER IS NULL) THEN 1 ELSE 0 END,
  (SELECT CONCAT(C_STATUS,' attempt=',C_ATTEMPT) FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@id);

-- A7. Lỗi → retry; hết lượt thử → DEAD (max_attempt=2)
EXEC SP_JOB_CLAIM @p_job_run_id=@id, @p_owner='pod-C', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
EXEC SP_JOB_COMPLETE @p_job_run_id=@id, @p_owner='pod-C', @p_ok=0, @p_message=N'lỗi giả lập',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'A7 COMPLETE(lỗi) lần 2/2 ⇒ DEAD (không quay vòng vô tận)',
  CASE WHEN EXISTS(SELECT 1 FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@id AND C_STATUS='DEAD') THEN 1 ELSE 0 END,
  (SELECT CONCAT(C_STATUS,' attempt=',C_ATTEMPT) FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@id);
EXEC SP_JOB_CLAIM @p_job_run_id=@id, @p_owner='pod-D', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'A7 claim lượt DEAD ⇒ err=6, KHÔNG chạy lại',
  CASE WHEN @ec=6 THEN 1 ELSE 0 END, CONCAT('err=',@ec);

-- A8. COMPLETE bởi pod KHÔNG phải chủ ⇒ từ chối (err=5)
EXEC SP_JOB_ENQUEUE @p_job_code='SMK_ANY', @p_fire_key='FK2',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_job_run_id=@id2 OUTPUT;
EXEC SP_JOB_CLAIM @p_job_run_id=@id2, @p_owner='pod-A', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
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
CREATE TABLE #due1 (id BIGINT, code VARCHAR(40), payload NVARCHAR(MAX));
CREATE TABLE #due2 (id BIGINT, code VARCHAR(40), payload NVARCHAR(MAX));
INSERT #due1 EXEC SP_JOB_ENQUEUE_DUE @p_user='pod-A', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT #due2 EXEC SP_JOB_ENQUEUE_DUE @p_user='pod-B', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'A9 ★ 2 pod cùng quét slot ⇒ pod đầu tạo 1, pod sau tạo 0',
  CASE WHEN (SELECT COUNT(*) FROM #due1 WHERE code='SMK_ANY')=1
        AND (SELECT COUNT(*) FROM #due2 WHERE code='SMK_ANY')=0
        AND (SELECT COUNT(*) FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_ANY')=1 THEN 1 ELSE 0 END,
  CONCAT('pass1=',(SELECT COUNT(*) FROM #due1),' pass2=',(SELECT COUNT(*) FROM #due2));
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

CREATE TABLE #due3 (id BIGINT, code VARCHAR(40), payload NVARCHAR(MAX));
INSERT #due3 EXEC SP_JOB_ENQUEUE_DUE @p_user='pod-A', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'A10 job singleton mới ⇒ sinh đúng 1 lượt ở nhịp đầu',
  CASE WHEN (SELECT COUNT(*) FROM #due3 WHERE code='SMK_SGL')=1 THEN 1 ELSE 0 END, NULL;

-- lượt đó đang chạy (lease còn hiệu lực) ⇒ nhịp sau KHÔNG mở lượt mới
UPDATE T_JOB_RUN SET C_STATUS='RUNNING', C_OWNER='pod-A', C_LEASE_UNTIL=DATEADD(MINUTE,5,@now)
 WHERE C_JOB_CODE='SMK_SGL';
DECLARE @cntBefore INT = (SELECT COUNT(*) FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_SGL');
DELETE #due3;
INSERT #due3 EXEC SP_JOB_ENQUEUE_DUE @p_user='pod-A', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'A10 singleton: lượt trước còn RUNNING ⇒ KHÔNG sinh lượt mới',
  CASE WHEN (SELECT COUNT(*) FROM #due3 WHERE code='SMK_SGL')=0
        AND (SELECT COUNT(*) FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_SGL')=@cntBefore THEN 1 ELSE 0 END, NULL;

-- ... nhưng lease CHẾT thì slot kế tiếp KHÔNG bị chặn vĩnh viễn.
--   Lùi mốc slot của lượt cũ về quá khứ để nhịp này chắc chắn rơi vào slot KHÁC (tất định,
--   không phụ thuộc smoke chạy vào giây thứ mấy của phút).
UPDATE T_JOB_RUN SET C_LEASE_UNTIL=DATEADD(MINUTE,-1,@now), C_FIRE_KEY='19000101000000'
 WHERE C_JOB_CODE='SMK_SGL';
DELETE #due3;
INSERT #due3 EXEC SP_JOB_ENQUEUE_DUE @p_user='pod-A', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'A10 ★ singleton + pod chết (lease hết hạn) ⇒ slot mới VẪN mở (không kẹt vĩnh viễn)',
  CASE WHEN (SELECT COUNT(*) FROM #due3 WHERE code='SMK_SGL')=1 THEN 1 ELSE 0 END,
  CONCAT('sinh=',(SELECT COUNT(*) FROM #due3 WHERE code='SMK_SGL'));
DROP TABLE #due1, #due2, #due3;

-- A11. GUARD TẦNG 2: job sinh ra hợp lệ, nhưng tới lúc chạy đã ra ngoài khung ⇒ SKIPPED
DELETE FROM T_JOB_RUN WHERE C_JOB_CODE='SMK_WIN';
INSERT INTO T_JOB_RUN (C_JOB_CODE,C_FIRE_KEY,C_STATUS,C_BUSINESS_DATE,C_RUN_AFTER,C_ENQUEUED_AT)
VALUES ('SMK_WIN','LATE','READY',@today,@now,@now);   -- giả lập job nằm trong stream vắt qua giờ đóng
SET @id2 = SCOPE_IDENTITY();
EXEC SP_JOB_CLAIM @p_job_run_id=@id2, @p_owner='pod-A', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'A11 ★ TẦNG 2: tới lượt chạy đã ngoài khung ⇒ err=3 + SKIPPED (KHÔNG gọi hệ ngoài)',
  CASE WHEN @ec=3 AND EXISTS(SELECT 1 FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@id2 AND C_STATUS='SKIPPED')
       THEN 1 ELSE 0 END, CONCAT('err=',@ec);

-- A12. Job FO thật: khung 09:00–15:00 + chỉ ngày GD (đọc từ seed, không hard-code lại)
INSERT INTO @R SELECT 'A12 seed FO_SNAPSHOT_RT: 15 phút · 09:00–15:00 · chỉ ngày GD · singleton',
  CASE WHEN EXISTS(SELECT 1 FROM T_JOB_DEFINITION WHERE C_JOB_CODE='FO_SNAPSHOT_RT'
                     AND C_INTERVAL_SEC=900 AND C_WINDOW_FROM='09:00:00' AND C_WINDOW_TO='15:00:00'
                     AND C_BUSINESS_DAY_ONLY=1 AND C_SINGLETON=1 AND C_TIMEOUT_SEC<900)
       THEN 1 ELSE 0 END, NULL;
-- Biên khung giờ tính TAY (không phụ thuộc lúc chạy smoke): 08:59 ngoài · 09:00 trong · 14:59 trong · 15:00 NGOÀI
DECLARE @gd DATE = @today;
WHILE dbo.UDF_IS_BUSINESS_DATE(@gd)=0 SET @gd = DATEADD(DAY,1,@gd);   -- lấy 1 ngày GD bất kỳ để test biên
INSERT INTO @R SELECT 'A12 ★ biên khung: 08:59 NGOÀI · 09:00 TRONG · 14:59 TRONG · 15:00 NGOÀI (đóng cửa là hết)',
  CASE WHEN dbo.UDF_JOB_IN_WINDOW('FO_SNAPSHOT_RT', DATEADD(MINUTE, 8*60+59, CAST(@gd AS DATETIME)))=0
        AND dbo.UDF_JOB_IN_WINDOW('FO_SNAPSHOT_RT', DATEADD(MINUTE, 9*60,    CAST(@gd AS DATETIME)))=1
        AND dbo.UDF_JOB_IN_WINDOW('FO_SNAPSHOT_RT', DATEADD(MINUTE,14*60+59, CAST(@gd AS DATETIME)))=1
        AND dbo.UDF_JOB_IN_WINDOW('FO_SNAPSHOT_RT', DATEADD(MINUTE,15*60,    CAST(@gd AS DATETIME)))=0
       THEN 1 ELSE 0 END, CONCAT('ngày GD dùng để test = ', CONVERT(VARCHAR(10),@gd,23));
-- Ngày NGHỈ: đúng 10h sáng nhưng là Chủ nhật ⇒ vẫn 0
DECLARE @sun DATE = @today;
WHILE DATEPART(WEEKDAY, @sun) <> 1 SET @sun = DATEADD(DAY,1,@sun);   -- (DATEFIRST mặc định 7 ⇒ 1 = Chủ nhật)
INSERT INTO @R SELECT 'A12 ★ 10h sáng CHỦ NHẬT ⇒ 0 (đúng giờ nhưng sai ngày ⇒ vẫn không gọi FO)',
  CASE WHEN dbo.UDF_JOB_IN_WINDOW('FO_SNAPSHOT_RT', DATEADD(HOUR,10,CAST(@sun AS DATETIME)))=0 THEN 1 ELSE 0 END,
  CONCAT('CN = ', CONVERT(VARCHAR(10),@sun,23));

-- A13. Vận hành: chạy tay ngoài khung (cửa hậu cho job VÔ HẠI), theo dõi, dọn nhật ký.
--   4 proc này lúc đầu KHÔNG có ca test nào — tự review mới lòi ra. Proc không có test là proc
--   chưa từng chạy, và nó sẽ chạy lần đầu vào lúc có sự cố, tức là lúc tệ nhất.
EXEC SP_JOB_ENQUEUE @p_job_code='SMK_WIN', @p_fire_key='IGN', @p_ignore_window=1,
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT, @p_job_run_id=@id2 OUTPUT;
INSERT INTO @R SELECT 'A13 @p_ignore_window=1 ⇒ chạy tay ngoài khung được (job vô hại)',
  CASE WHEN @ec=0 AND @id2 IS NOT NULL THEN 1 ELSE 0 END, CONCAT('err=',@ec);
-- ...nhưng cửa hậu KHÔNG mở được đường gọi FO: tầng 2 vẫn chặn khi tới lượt chạy.
EXEC SP_JOB_CLAIM @p_job_run_id=@id2, @p_owner='pod-A', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'A13 ★ cửa hậu ignore_window KHÔNG qua được tầng 2 ⇒ vẫn SKIPPED',
  CASE WHEN @ec=3 THEN 1 ELSE 0 END, CONCAT('err=',@ec);

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
DECLARE @doneBefore INT = (SELECT COUNT(*) FROM T_JOB_RUN
                           WHERE C_JOB_CODE IN ('SMK_ANY','SMK_WIN') AND C_STATUS IN ('DONE','SKIPPED'));
DECLARE @purged BIGINT;
EXEC SP_JOB_PURGE @p_keep_days=0, @p_rows=@purged OUTPUT;
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
 ('RT01','RTC1','RTM','2026-01-01','ACTIVE'),
 ('RT02','RTC2','RTM','2026-01-01','ACTIVE'),
 ('RT03','RTC3','RTM','2026-01-01','ACTIVE');

-- Phiên chốt HÔM QUA (mốc so sánh) — ghi tay, C_SRC mặc định 'EOD'
DECLARE @prev DATE = dbo.UDF_PREV_BUSINESS_DATE(@today);
INSERT INTO T_SI_BALANCE (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_AUM,C_DAILY_RETURN,C_CASH) VALUES
 (@prev,'RT01','RTC1','RTM',1000000000,0.010000,100000000),
 (@prev,'RT02','RTC2','RTM', 500000000,0.010000, 50000000),
 (@prev,'RT03','RTC3','RTM', 200000000,0.010000, 20000000);
INSERT INTO T_MASTER_BALANCE (C_BUSINESS_DATE,C_MASTER_CODE,C_CASH,C_AUM,C_DAILY_RETURN,C_TOTAL_ACCOUNT)
 VALUES (@prev,'RTM',170000000,1700000000,0.010000,3);

-- B0. Scope cho worker chia batch
CREATE TABLE #scope (cust VARCHAR(10), si VARCHAR(20), sub VARCHAR(30), master VARCHAR(20));
INSERT #scope EXEC SP_GET_FO_SNAPSHOT_SCOPE @p_master_code='RTM',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT 'B0 SCOPE: 3 tiểu khoản, xếp theo KH (batch không xẻ đôi 1 khách hàng)',
  CASE WHEN (SELECT COUNT(*) FROM #scope)=3
        AND (SELECT TOP 1 cust FROM #scope ORDER BY cust, si)='RTC1' THEN 1 ELSE 0 END,
  CONCAT('rows=',(SELECT COUNT(*) FROM #scope));
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
     VALUES (@today,'RT03','RTC3','RTM',222000000,0.020000,22000000,'EOD');   -- Asset đã chốt RT03
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
  KẾT QUẢ
==============================================================================*/
SELECT name AS [Check], CASE WHEN ok=1 THEN 'PASS' ELSE 'FAIL' END AS [Result], detail AS [Detail] FROM @R ORDER BY id;
DECLARE @pass INT=(SELECT COUNT(*) FROM @R WHERE ok=1), @tot INT=(SELECT COUNT(*) FROM @R);
PRINT REPLICATE('=',70);
PRINT CONCAT('JOB/RT SMOKE: ',@pass,'/',@tot,' PASS  -> ', CASE WHEN @pass=@tot THEN 'ALL GREEN' ELSE 'HAS FAILURES' END);
IF @isGD=0 PRINT 'LUU Y: hom nay KHONG phai ngay GD -> khoi (B) da bo qua. Chay lai vao ngay GD de phu het.';
IF @pass<>@tot SELECT name AS [FAILED], detail FROM @R WHERE ok=0;
