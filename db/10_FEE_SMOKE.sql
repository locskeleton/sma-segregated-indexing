/*==============================================================================
  SDI — SMOKE TEST PHÍ QUẢN LÝ (09_FEE.sql). Chạy SAU 01_TABLES + 02_SP_ENGINE + 09_FEE.
  Tự seed dataset nhỏ, EXEC từng proc, assert kết quả (hand-computed).

  Trọng tâm: hạch toán MỖI NGÀY 1 DÒNG — Thứ 6 → 3 dòng T6/T7/CN; dải nghỉ vắt 2 tháng 30/4–1/5/2026
    → 5 dòng per-day (Apr29,30 kỳ 202604 + May1,2,3 kỳ 202605).
  Lịch 2026 (tính tay): Apr29=Thứ4(GD), Apr30=lễ, May1=lễ, May2/3=cuối tuần, May4=Thứ2(GD).
    → UDF_NEXT_BUSINESS_DATE('2026-04-29')='2026-05-04'; dải [Apr29,May4)=5 ngày dương lịch.

  Rate test: 0.015 (1.5%/năm), day_count 365, AUM 1,000,000,000 → phí/ngày = 41,095.890411.
==============================================================================*/
SET NOCOUNT ON; SET QUOTED_IDENTIFIER ON; SET ANSI_NULLS ON;

DECLARE @R TABLE (id INT IDENTITY, name VARCHAR(80), ok BIT, detail NVARCHAR(200));
DECLARE @ec INT, @em NVARCHAR(400), @rows BIGINT;
DECLARE @feeDay DECIMAL(20,6) = 41095.890411;   -- 1e9 × 0.015/365

/*--- reset bảng phí + phụ thuộc ---*/
DELETE FROM T_SI_FEE_CHARGE; DELETE FROM T_SI_FEE_BALANCE; DELETE FROM T_SI_FEE_RATE; DELETE FROM T_FEE_CONFIG;
DELETE FROM T_TRADING_HOLIDAY;
DELETE FROM T_SI_BALANCE   WHERE C_SI_ACCOUNT LIKE 'SUB%';
DELETE FROM T_SI_CURRENT   WHERE C_SI_ACCOUNT LIKE 'SUB%';
DELETE FROM T_SI_PORTFOLIO WHERE C_SI_ACCOUNT LIKE 'SUB%';
DELETE FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE='SDIF';

/*--- lịch nghỉ 2026 (lễ 30/4 + 1/5) ---*/
INSERT INTO T_TRADING_HOLIDAY (C_HOLIDAY_DATE,C_NOTE) VALUES
 ('2026-04-30',N'Thống nhất'), ('2026-05-01',N'Lao động');

/*--- master + config phí sản phẩm ---*/
INSERT INTO T_MASTER_PORTFOLIO (C_MASTER_CODE,C_MASTER_NAME,C_STATUS,C_INCEPTION_DATE,C_BENCHMARK_CODE)
 VALUES ('SDIF',N'Fee smoke master','ACTIVE','2026-01-01','VNINDEX');
EXEC SP_SET_FEE_CONFIG @p_master_code='SDIF',@p_rate=0.015,@p_day_count=365,@p_effective_from='2026-01-01',
     @p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;

/*--- 3 sub-account test accrual (SUBONE/SUBFRI/SUBX) ---*/
INSERT INTO T_SI_PORTFOLIO (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_JOIN_DATE,C_STATUS) VALUES
 ('SUBONE','KH01','SDIF','2026-01-01','ACTIVE'),
 ('SUBFRI','KH02','SDIF','2026-01-01','ACTIVE'),
 ('SUBX'  ,'KH03','SDIF','2026-01-01','ACTIVE');
-- rate per-SI: NULL → copy từ product (test default onboarding)
EXEC SP_SET_SI_FEE_RATE @p_si_account='SUBONE',@p_effective_from='2026-01-01',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
EXEC SP_SET_SI_FEE_RATE @p_si_account='SUBFRI',@p_effective_from='2026-01-01',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
EXEC SP_SET_SI_FEE_RATE @p_si_account='SUBX'  ,@p_effective_from='2026-01-01',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;

INSERT INTO @R SELECT 'SP_SET_SI_FEE_RATE default copy rate 0.015',
  CASE WHEN (SELECT COUNT(*) FROM T_SI_FEE_RATE WHERE C_RATE=0.015 AND C_DAY_COUNT=365)=3 THEN 1 ELSE 0 END,
  CONCAT('rows=',(SELECT COUNT(*) FROM T_SI_FEE_RATE));

/*==============================================================================
  0) UDF lịch GD
==============================================================================*/
INSERT INTO @R SELECT 'UDF_IS_BUSINESS_DATE Apr29=1 (Thứ4)',  CASE WHEN dbo.UDF_IS_BUSINESS_DATE('2026-04-29')=1 THEN 1 ELSE 0 END, NULL;
INSERT INTO @R SELECT 'UDF_IS_BUSINESS_DATE Apr30=0 (lễ)',     CASE WHEN dbo.UDF_IS_BUSINESS_DATE('2026-04-30')=0 THEN 1 ELSE 0 END, NULL;
INSERT INTO @R SELECT 'UDF_IS_BUSINESS_DATE May02=0 (Thứ7)',   CASE WHEN dbo.UDF_IS_BUSINESS_DATE('2026-05-02')=0 THEN 1 ELSE 0 END, NULL;
INSERT INTO @R SELECT 'UDF_NEXT_BUSINESS_DATE(Apr29)=May04',   CASE WHEN dbo.UDF_NEXT_BUSINESS_DATE('2026-04-29')='2026-05-04' THEN 1 ELSE 0 END,
  CONVERT(VARCHAR,dbo.UDF_NEXT_BUSINESS_DATE('2026-04-29'),23);

/*==============================================================================
  1) ACCRUE — MỖI NGÀY 1 DÒNG (per-day)
     SUBONE @Apr22 (Thứ4→Thứ5)  = 1 dòng (Apr22), kỳ 202604
     SUBFRI @Apr24 (Thứ6→Thứ2)  = 3 dòng (T6/T7/CN: Apr24,25,26), kỳ 202604
     SUBX   @Apr29 (vắt tháng)   = 5 dòng: Apr29,30 (202604) + May1,2,3 (202605)
     Mỗi dòng fee = 1×feeDay (dùng AUM ngày GD accrue).
==============================================================================*/
INSERT INTO T_SI_BALANCE (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_AUM,C_DAILY_RETURN,C_CASH,C_CASH_IN,C_CASH_OUT) VALUES
 ('2026-04-22','SUBONE','KH01','SDIF',1000000000,NULL,0,0,0),
 ('2026-04-24','SUBFRI','KH02','SDIF',1000000000,NULL,0,0,0),
 ('2026-04-29','SUBX'  ,'KH03','SDIF',1000000000,NULL,0,0,0);

EXEC SP_EOD_FEE_ACCRUE '2026-04-22',@p_rows=@rows OUTPUT;
EXEC SP_EOD_FEE_ACCRUE '2026-04-24',@p_rows=@rows OUTPUT;
EXEC SP_EOD_FEE_ACCRUE '2026-04-29',@p_rows=@rows OUTPUT;

-- SUBONE: 1 dòng (Apr22), kỳ 202604, fee = feeDay
INSERT INTO @R SELECT 'ACCRUE SUBONE 1 dòng (Apr22) kỳ202604',
  CASE WHEN (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBONE')=1
        AND (SELECT C_FEE_DATE FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBONE')='2026-04-22'
        AND (SELECT C_PERIOD FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBONE')='202604'
        AND (SELECT ROUND(C_FEE_AMOUNT,6) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBONE')=@feeDay THEN 1 ELSE 0 END,
  (SELECT CONCAT('fee=',C_FEE_AMOUNT) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBONE');

-- ★ SUBFRI: 3 dòng cho T6/T7/CN (Apr24,25,26) — mỗi dòng feeDay, tổng 3×feeDay
INSERT INTO @R SELECT '★ ACCRUE SUBFRI 3 dòng T6/T7/CN (mỗi ngày 1 dòng)',
  CASE WHEN (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBFRI')=3
        AND (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBFRI' AND C_FEE_DATE IN ('2026-04-24','2026-04-25','2026-04-26'))=3
        AND (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBFRI' AND ROUND(C_FEE_AMOUNT,6)<>@feeDay)=0
        AND (SELECT COUNT(DISTINCT C_ACCRUED_ON) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBFRI')=1 THEN 1 ELSE 0 END,   -- cả 3 accrue cùng ngày GD (T6)
  CONCAT('count=',(SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBFRI'),
         ' Σfee=',(SELECT SUM(C_FEE_AMOUNT) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBFRI'));

-- ★ SUBX: 5 dòng per-day (vắt tháng); 202604 = Apr29,30 (2) ; 202605 = May1,2,3 (3)
INSERT INTO @R SELECT '★ ACCRUE SUBX 5 dòng per-day (vắt tháng)',
  CASE WHEN (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX')=5
        AND (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202604')=2
        AND (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202605')=3 THEN 1 ELSE 0 END,
  CONCAT('count=',(SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX'));
-- kỳ 202604: đúng ngày Apr29,Apr30 ; mỗi dòng feeDay → Σ = 2×feeDay
INSERT INTO @R SELECT '★ SUBX 202604 = Apr29,Apr30 (Σ=2×feeDay)',
  CASE WHEN (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202604' AND C_FEE_DATE IN ('2026-04-29','2026-04-30'))=2
        AND (SELECT ROUND(SUM(C_FEE_AMOUNT),6) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202604')=ROUND(2*@feeDay,6) THEN 1 ELSE 0 END,
  (SELECT CONCAT('Σfee=',SUM(C_FEE_AMOUNT)) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202604');
-- kỳ 202605: đúng ngày May1,2,3 ; Σ = 3×feeDay
INSERT INTO @R SELECT '★ SUBX 202605 = May01,02,03 (Σ=3×feeDay)',
  CASE WHEN (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202605' AND C_FEE_DATE IN ('2026-05-01','2026-05-02','2026-05-03'))=3
        AND (SELECT ROUND(SUM(C_FEE_AMOUNT),6) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202605')=ROUND(3*@feeDay,6) THEN 1 ELSE 0 END,
  (SELECT CONCAT('Σfee=',SUM(C_FEE_AMOUNT)) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202605');

/*--- idempotent: re-accrue Apr29 → vẫn 5 dòng cho SUBX (không nhân đôi) ---*/
EXEC SP_EOD_FEE_ACCRUE '2026-04-29',@p_rows=@rows OUTPUT;
INSERT INTO @R SELECT 'ACCRUE idempotent re-run (SUBX vẫn 5)',
  CASE WHEN (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX')=5 THEN 1 ELSE 0 END,
  CONCAT('count=',(SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX'));

/*==============================================================================
  2) CLOSE PERIOD — Apr29 là ngày GD CUỐI tháng 4 (next=May4 sang tháng 5)
==============================================================================*/
-- close trên ngày GIỮA tháng (Apr22) → no-op (chưa cuối tháng)
EXEC SP_FEE_CLOSE_PERIOD '2026-04-22',@p_rows=@rows OUTPUT;
INSERT INTO @R SELECT 'CLOSE giữa tháng (Apr22) = no-op',
  CASE WHEN @rows=0 AND (SELECT COUNT(*) FROM T_SI_FEE_CHARGE)=0 THEN 1 ELSE 0 END, CONCAT('rows=',@rows);

-- close cuối tháng 4
EXEC SP_FEE_CLOSE_PERIOD '2026-04-29',@p_rows=@rows OUTPUT;
-- 3 charge kỳ 202604 (mỗi SI 1 dòng); 202605 KHÔNG chốt
INSERT INTO @R SELECT 'CLOSE 202604 → 3 charge (per-SI)',
  CASE WHEN @rows=3 AND (SELECT COUNT(*) FROM T_SI_FEE_CHARGE WHERE C_PERIOD='202604')=3
        AND (SELECT COUNT(*) FROM T_SI_FEE_CHARGE WHERE C_PERIOD='202605')=0 THEN 1 ELSE 0 END,
  CONCAT('rows=',@rows);
-- SUBX charge 202604: total=2×feeDay, due=ROUND
INSERT INTO @R SELECT 'CLOSE SUBX 202604 total/due đúng',
  CASE WHEN (SELECT ROUND(C_FEE_TOTAL,6) FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202604')=ROUND(2*@feeDay,6)
        AND (SELECT C_FEE_DUE FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202604')=ROUND(2*@feeDay,0)
        AND (SELECT C_STATUS FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202604')='UNPAID' THEN 1 ELSE 0 END,
  (SELECT CONCAT('total=',C_FEE_TOTAL,' due=',C_FEE_DUE) FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202604');
-- close KHÔNG đổi C_STATUS balance (vẫn =1 hợp lệ); không sinh invalid
INSERT INTO @R SELECT 'CLOSE giữ balance C_STATUS=1 (không sinh invalid)',
  CASE WHEN (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_PERIOD='202604' AND C_STATUS<>1)=0
        AND (SELECT C_STATUS FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202605')=1 THEN 1 ELSE 0 END, NULL;

/*==============================================================================
  2b) KHÓA THEO PAID — accrue check is_paid (ko chơi chạy lại đè dữ liệu đã thu):
      kỳ ĐÃ PAID bên BO → re-accrue KHÔNG đè (frozen). Kỳ CHƯA thu → recompute được.
      Contrast: SUBLOCK (đánh PAID) vs SUBLOCK2 (UNPAID). Đổi AUM ×2 rồi re-accrue cùng ngày.
      Mar31/2026 = Thứ3 = ngày GD cuối tháng 3 (next=Apr1) → close chốt 202603.
==============================================================================*/
INSERT INTO T_SI_PORTFOLIO (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_JOIN_DATE,C_STATUS) VALUES
 ('SUBLOCK' ,'KH07','SDIF','2026-01-01','ACTIVE'),
 ('SUBLOCK2','KH08','SDIF','2026-01-01','ACTIVE');
EXEC SP_SET_SI_FEE_RATE @p_si_account='SUBLOCK' ,@p_effective_from='2026-01-01',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
EXEC SP_SET_SI_FEE_RATE @p_si_account='SUBLOCK2',@p_effective_from='2026-01-01',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
INSERT INTO T_SI_BALANCE (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_AUM,C_DAILY_RETURN,C_CASH,C_CASH_IN,C_CASH_OUT) VALUES
 ('2026-03-31','SUBLOCK' ,'KH07','SDIF',1000000000,NULL,0,0,0),
 ('2026-03-31','SUBLOCK2','KH08','SDIF',1000000000,NULL,0,0,0);
EXEC SP_EOD_FEE_ACCRUE   '2026-03-31',@p_rows=@rows OUTPUT;   -- 1 dải kỳ 202603, fee=feeDay
EXEC SP_FEE_CLOSE_PERIOD '2026-03-31',@p_rows=@rows OUTPUT;   -- Mar31=cuối tháng 3 → chốt 202603 (UNPAID)
-- BO thu XONG SUBLOCK → charge PAID; SUBLOCK2 vẫn UNPAID
UPDATE T_SI_FEE_CHARGE SET C_STATUS='PAID', C_FEE_PAID=C_FEE_DUE, C_COLLECTED_AT=GETDATE()
 WHERE C_SI_ACCOUNT='SUBLOCK' AND C_PERIOD='202603';
-- mô phỏng re-ingest/re-run quá khứ AUM khác (×2) → re-accrue cùng ngày
UPDATE T_SI_BALANCE SET C_AUM=2000000000 WHERE C_BUSINESS_DATE='2026-03-31' AND C_SI_ACCOUNT IN ('SUBLOCK','SUBLOCK2');
EXEC SP_EOD_FEE_ACCRUE '2026-03-31',@p_rows=@rows OUTPUT;
-- ★ SUBLOCK (PAID) → KHÓA: fee + AUM GIỮ NGUYÊN 1e9-based (không đè)
INSERT INTO @R SELECT '★ PAID lock: SUBLOCK re-accrue KHÔNG đè (fee+AUM giữ)',
  CASE WHEN (SELECT ROUND(C_FEE_AMOUNT,6) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBLOCK' AND C_PERIOD='202603')=@feeDay
        AND (SELECT C_AUM FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBLOCK' AND C_PERIOD='202603')=1000000000 THEN 1 ELSE 0 END,
  (SELECT CONCAT('fee=',C_FEE_AMOUNT,' aum=',C_AUM) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBLOCK' AND C_PERIOD='202603');
-- ★ SUBLOCK2 (UNPAID) → recompute: fee ×2, AUM=2e9 (kỳ chưa thu sửa được)
INSERT INTO @R SELECT '★ Chưa thu: SUBLOCK2 re-accrue recompute (fee ×2)',
  CASE WHEN (SELECT ROUND(C_FEE_AMOUNT,6) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBLOCK2' AND C_PERIOD='202603')=ROUND(2*@feeDay,6)
        AND (SELECT C_AUM FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBLOCK2' AND C_PERIOD='202603')=2000000000 THEN 1 ELSE 0 END,
  (SELECT CONCAT('fee=',C_FEE_AMOUNT,' aum=',C_AUM) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBLOCK2' AND C_PERIOD='202603');
-- ★ luồng thường KHÔNG sinh invalid (C_STATUS=0)
INSERT INTO @R SELECT '★ No invalid (C_STATUS=0) trong luồng thường',
  CASE WHEN (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_STATUS=0)=0 THEN 1 ELSE 0 END,
  CONCAT('invalid=',(SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_STATUS=0));

/*==============================================================================
  3) COLLECT — FIFO theo SI, all-or-nothing per món, gác số dư cash
     SUBX : thêm món cũ 202603 due=50,000 + 202604 due=82,192. Cash=60,000
            → FIFO: 202603 (cum 50,000 ≤ 60,000)✓; 202604 (cum 132,192 > 60,000)✗ → chỉ 202603
     SUBONE: 202604 due=41,096. Cash=100,000 → thu 202604
     SUBFRI: 202604 due=123,288. Cash=0 → KHÔNG thu
==============================================================================*/
-- món cũ hơn cho SUBX (test FIFO ordering)
INSERT INTO T_SI_FEE_CHARGE (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_PERIOD,C_PERIOD_FROM,C_PERIOD_TO,C_FEE_TOTAL,C_FEE_DUE)
 VALUES ('SUBX','KH03','SDIF','202603','2026-03-01','2026-03-31',50000,50000);
-- cash khả dụng
INSERT INTO T_SI_CURRENT (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_CASH,C_LAST_AUM,C_STATUS,C_LAST_SYNC_DATE) VALUES
 ('SUBX'  ,'KH03','SDIF', 60000,1000000000,'ACTIVE','2026-04-29'),
 ('SUBONE','KH01','SDIF',100000,1000000000,'ACTIVE','2026-04-29'),
 ('SUBFRI','KH02','SDIF',     0,1000000000,'ACTIVE','2026-04-29');

EXEC SP_FEE_COLLECT '2026-05-04','EVT-TEST-001',@p_rows=@rows OUTPUT;
-- lệnh thu gồm SUBX/202603 + SUBONE/202604 = 2 món; MỖI MÓN 1 request id UNIQUE (prefix batch 'EVT-TEST-001-')
INSERT INTO @R SELECT 'COLLECT FIFO 2 món, request id UNIQUE/món',
  CASE WHEN @rows=2
        AND (SELECT C_BO_EVENT_ID FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202603') LIKE 'EVT-TEST-001-%'
        AND (SELECT C_BO_EVENT_ID FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBONE' AND C_PERIOD='202604') LIKE 'EVT-TEST-001-%'
        AND (SELECT C_BO_EVENT_ID FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202603')
          <> (SELECT C_BO_EVENT_ID FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBONE' AND C_PERIOD='202604') THEN 1 ELSE 0 END,
  CONCAT('rows=',@rows);
-- SUBX/202604 KHÔNG vào lệnh (thiếu tiền); SUBFRI KHÔNG (cash 0)
INSERT INTO @R SELECT 'COLLECT bỏ món thiếu tiền (SUBX 202604, SUBFRI)',
  CASE WHEN (SELECT C_BO_EVENT_ID FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202604') IS NULL
        AND (SELECT C_BO_EVENT_ID FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBFRI' AND C_PERIOD='202604') IS NULL THEN 1 ELSE 0 END, NULL;

/*==============================================================================
  4) INGEST BO RESULT — map theo REQUEST ID (id SDI đã gửi) + trạng thái bút toán. KHÔNG period.
     SUBX/202603 collected=1 → PAID; SUBONE/202604 collected=0 → retry.
==============================================================================*/
-- đọc request id BO sẽ echo lại (= C_BO_EVENT_ID đã gán ở COLLECT)
DECLARE @reqX   VARCHAR(64) = (SELECT C_BO_EVENT_ID FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBX'   AND C_PERIOD='202603');
DECLARE @reqOne VARCHAR(64) = (SELECT C_BO_EVENT_ID FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBONE' AND C_PERIOD='202604');
-- BO chỉ gửi request_id + trạng thái (ko period, ko amount → SDI lấy C_FEE_DUE)
DECLARE @json NVARCHAR(MAX) = N'[{"request_id":"'+@reqX+'","collected":1},{"request_id":"'+@reqOne+'","collected":0}]';
EXEC SP_INGEST_FEE_COLLECT_RESULT @p_json=@json,@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;

INSERT INTO @R SELECT 'INGEST by request_id collected=1 → PAID (paid=due)',
  CASE WHEN (SELECT C_STATUS FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202603')='PAID'
        AND (SELECT C_FEE_PAID FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202603')=50000 THEN 1 ELSE 0 END,
  CONCAT('err=',@ec);
INSERT INTO @R SELECT 'INGEST by request_id collected=0 → UNPAID + req id clear',
  CASE WHEN (SELECT C_STATUS FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBONE' AND C_PERIOD='202604')='UNPAID'
        AND (SELECT C_BO_EVENT_ID FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBONE' AND C_PERIOD='202604') IS NULL THEN 1 ELSE 0 END, NULL;

/*==============================================================================
  4b) UDF nợ phí = (A) đã chốt chưa thu + (B) kỳ hiện tại tích lũy chưa chốt.
      SUBX: nợ 202604=82192 (B) + tích lũy 202605=123287.671233 → DEBT=205479.671233.
      SUBLOCK: 202603 PAID + ko dải chưa chốt → 0.  SUBONE: nợ 202604=41096, ko tích lũy → 41096.
==============================================================================*/
INSERT INTO @R SELECT 'UDF_SI_FEE_ACCRUING SUBX = 123287.671233 (kỳ 202605 chưa chốt)',
  CASE WHEN ROUND(dbo.UDF_SI_FEE_ACCRUING('SUBX'),6) = 123287.671233 THEN 1 ELSE 0 END,
  CONCAT('accruing=', dbo.UDF_SI_FEE_ACCRUING('SUBX'));
INSERT INTO @R SELECT 'UDF_SI_FEE_DEBT SUBX = 205479.671233 (nợ+tích lũy)',
  CASE WHEN ROUND(dbo.UDF_SI_FEE_DEBT('SUBX'),6) = 205479.671233 THEN 1 ELSE 0 END,
  CONCAT('debt=', dbo.UDF_SI_FEE_DEBT('SUBX'));
INSERT INTO @R SELECT 'UDF_SI_FEE_DEBT SUBLOCK = 0 (đã thu, ko tích lũy)',
  CASE WHEN dbo.UDF_SI_FEE_DEBT('SUBLOCK') = 0 THEN 1 ELSE 0 END,
  CONCAT('debt=', dbo.UDF_SI_FEE_DEBT('SUBLOCK'));
INSERT INTO @R SELECT 'UDF_SI_FEE_DEBT SUBONE = 41096 (nợ, ko tích lũy)',
  CASE WHEN dbo.UDF_SI_FEE_DEBT('SUBONE') = 41096 THEN 1 ELSE 0 END,
  CONCAT('debt=', dbo.UDF_SI_FEE_DEBT('SUBONE'));

/*==============================================================================
  5) WIRING EOD — phí chốt trong SP_EOD_RUN (J15/J16), KHÔNG eager trong luồng sync.
     (a) SP_EOD_SET_SOURCE_READY 'ASSET_NAV' READY KHÔNG còn tự accrue (hook đã gỡ).
     (b) SP_EOD_RUN có 2 job J15_FEE_ACCRUE + J16_FEE_CLOSE, guard OBJECT_ID pluggable.
==============================================================================*/
INSERT INTO T_SI_PORTFOLIO (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_JOIN_DATE,C_STATUS)
 VALUES ('SUBHOOK','KH09','SDIF','2026-01-01','ACTIVE');
EXEC SP_SET_SI_FEE_RATE @p_si_account='SUBHOOK',@p_effective_from='2026-01-01',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
INSERT INTO T_SI_BALANCE (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_AUM,C_DAILY_RETURN,C_CASH,C_CASH_IN,C_CASH_OUT)
 VALUES ('2026-06-15','SUBHOOK','KH09','SDIF',1000000000,NULL,0,0,0);
-- (a) SET_SOURCE_READY ASSET_NAV READY → PHẢI KHÔNG accrue (đã gỡ hook eager)
EXEC SP_EOD_SET_SOURCE_READY @p_business_date='2026-06-15',@p_source='ASSET_NAV',@p_total_record=1,
     @p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
INSERT INTO @R SELECT '★ Sync ASSET_NAV READY KHÔNG accrue (hook đã gỡ)',
  CASE WHEN NOT EXISTS (SELECT 1 FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBHOOK') THEN 1 ELSE 0 END,
  CONCAT('rows=',(SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBHOOK'));
-- (b) SP_EOD_RUN wiring 2 job phí + guard pluggable
INSERT INTO @R SELECT '★ SP_EOD_RUN có J15_FEE_ACCRUE + J16_FEE_CLOSE (guard)',
  CASE WHEN OBJECT_DEFINITION(OBJECT_ID('SP_EOD_RUN')) LIKE '%J15_FEE_ACCRUE%'
        AND OBJECT_DEFINITION(OBJECT_ID('SP_EOD_RUN')) LIKE '%J16_FEE_CLOSE%'
        AND OBJECT_DEFINITION(OBJECT_ID('SP_EOD_RUN')) LIKE '%SP_EOD_FEE_ACCRUE%'
        AND OBJECT_DEFINITION(OBJECT_ID('SP_EOD_SET_SOURCE_READY')) NOT LIKE '%SP_EOD_FEE_ACCRUE%' THEN 1 ELSE 0 END, NULL;

/*==============================================================================
  KẾT QUẢ
==============================================================================*/
SELECT name AS [Check], CASE WHEN ok=1 THEN 'PASS' ELSE 'FAIL' END AS [Result], detail AS [Detail] FROM @R ORDER BY id;
DECLARE @pass INT=(SELECT COUNT(*) FROM @R WHERE ok=1), @tot INT=(SELECT COUNT(*) FROM @R);
PRINT REPLICATE('=',60);
PRINT CONCAT('FEE SMOKE: ',@pass,'/',@tot,' PASS  → ', CASE WHEN @pass=@tot THEN 'ALL GREEN ✓' ELSE 'HAS FAILURES ✗' END);
IF @pass<>@tot SELECT name AS [FAILED], detail FROM @R WHERE ok=0;
