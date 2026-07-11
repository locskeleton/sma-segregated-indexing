/*==============================================================================
  SDI — SMOKE TEST PHÍ QUẢN LÝ (09_FEE.sql). Chạy SAU 01_TABLES + 02_SP_ENGINE + 09_FEE.
  Tự seed dataset nhỏ, EXEC từng proc, assert kết quả (hand-computed).

  Trọng tâm: hạch toán MỖI NGÀY 1 DÒNG — Thứ 6 → 3 dòng T6/T7/CN; dải nghỉ vắt 2 tháng 30/4–1/5/2026
    → 5 dòng per-day (Apr29,30 kỳ 202604 + May1,2,3 kỳ 202605).
  Lịch 2026 (tính tay): Apr29=Thứ4(GD), Apr30=lễ, May1=lễ, May2/3=cuối tuần, May4=Thứ2(GD).
    → UDF_NEXT_BUSINESS_DATE('2026-04-29')='2026-05-04'; dải [Apr29,May4)=5 ngày dương lịch.

  Rate test: 0.015 (1.5%/năm), day_count 365, AUM 1e9 → 1e9×0.015/365 = 41095.8904... → CEILING 2dp = 41095.90.
  Rounding BRD: phí ngày CEILING 2 số lẻ (làm tròn LÊN); chốt kỳ CEILING đến đồng; AUM≤0 ⇒ phí ngày = 0.
==============================================================================*/
SET NOCOUNT ON; SET QUOTED_IDENTIFIER ON; SET ANSI_NULLS ON;

DECLARE @R TABLE (id INT IDENTITY, name VARCHAR(80), ok BIT, detail NVARCHAR(200));
DECLARE @ec INT, @em NVARCHAR(400), @rows BIGINT;
DECLARE @feeDay DECIMAL(20,2) = 41095.90;   -- phí/ngày AUM 1e9 = CEILING(41095.8904, 2dp)

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
 ('SUBX'  ,'KH03','SDIF','2026-01-01','ACTIVE'),
 ('SUBZERO','KH04','SDIF','2026-01-01','ACTIVE');   -- test AUM=0
-- rate per-SI: NULL → copy từ product (test default onboarding)
EXEC SP_SET_SI_FEE_RATE @p_si_account='SUBONE',@p_effective_from='2026-01-01',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
EXEC SP_SET_SI_FEE_RATE @p_si_account='SUBFRI',@p_effective_from='2026-01-01',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
EXEC SP_SET_SI_FEE_RATE @p_si_account='SUBX'  ,@p_effective_from='2026-01-01',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
EXEC SP_SET_SI_FEE_RATE @p_si_account='SUBZERO',@p_effective_from='2026-01-01',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;

INSERT INTO @R SELECT 'SP_SET_SI_FEE_RATE default copy rate 0.015',
  CASE WHEN (SELECT COUNT(*) FROM T_SI_FEE_RATE WHERE C_RATE=0.015 AND C_DAY_COUNT=365)=4 THEN 1 ELSE 0 END,
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
  1) ACCRUE — LOOK-BACKWARD, AUM CỦA CHÍNH NGÀY ĐÓ (Asset gửi mọi ngày lịch)
     Dải accrue @D = (phiên_GD_trước, D].
     SUBONE  @Apr22 (T4; prev=Apr21) = 1 dòng (Apr22), kỳ 202604
     SUBFRI  ★ NẠP TIỀN T7: AUM T6=1e9 → T7/CN/T2 = 2e9 (KH nạp 1e9 vào T7 25/04)
             @Apr24 (T6) = 1 dòng (Apr24, base 1e9)
             @Apr27 (T2; prev=Apr24) = 3 dòng (Apr25,26,27 — base 2e9 MỖI NGÀY)
             ⇒ phí T7/CN tính trên TIỀN THẬT CÓ ngày đó. Look-forward CŨ sẽ lấy AUM T6 (1e9) ⇒ THU THIẾU.
     SUBX    dải nghỉ vắt tháng (30/4+1/5 lễ, 2-3/5 cuối tuần):
             @Apr29 (T4) = 1 dòng (Apr29, kỳ 202604)
             @May04 (T2; prev=Apr29) = 5 dòng: Apr30 (202604) + May1,2,3,4 (202605)
==============================================================================*/
DECLARE @feeDay2 DECIMAL(20,2) = 82191.79;   -- phí/ngày AUM 2e9 = CEILING(82191.7808, 2dp) — KHÔNG phải 2×41095.90

INSERT INTO T_SI_BALANCE (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_AUM,C_DAILY_RETURN,C_CASH,C_CASH_IN,C_CASH_OUT) VALUES
 ('2026-04-22','SUBONE' ,'KH01','SDIF',1000000000,NULL,0,0,0),
 -- ★ SUBFRI: Asset gửi CẢ T7/CN. Nạp 1e9 vào T7 25/04 → AUM T7/CN/T2 = 2e9
 ('2026-04-24','SUBFRI' ,'KH02','SDIF',1000000000,NULL,0,0,0),          -- T6
 ('2026-04-25','SUBFRI' ,'KH02','SDIF',2000000000,0,1000000000,1000000000,0),  -- T7: NẠP 1e9 (return=0: không có phiên)
 ('2026-04-26','SUBFRI' ,'KH02','SDIF',2000000000,0,1000000000,0,0),    -- CN
 ('2026-04-27','SUBFRI' ,'KH02','SDIF',2000000000,NULL,1000000000,0,0), -- T2
 -- SUBX: AUM phẳng 1e9 suốt dải nghỉ vắt tháng
 ('2026-04-29','SUBX'   ,'KH03','SDIF',1000000000,NULL,0,0,0),
 ('2026-04-30','SUBX'   ,'KH03','SDIF',1000000000,0,0,0,0),
 ('2026-05-01','SUBX'   ,'KH03','SDIF',1000000000,0,0,0,0),
 ('2026-05-02','SUBX'   ,'KH03','SDIF',1000000000,0,0,0,0),
 ('2026-05-03','SUBX'   ,'KH03','SDIF',1000000000,0,0,0,0),
 ('2026-05-04','SUBX'   ,'KH03','SDIF',1000000000,NULL,0,0,0),
 ('2026-04-22','SUBZERO','KH04','SDIF',         0,NULL,0,0,0);   -- AUM=0 → phí ngày = 0

EXEC SP_EOD_FEE_ACCRUE '2026-04-22',@p_rows=@rows OUTPUT;   -- SUBONE + SUBZERO
EXEC SP_EOD_FEE_ACCRUE '2026-04-24',@p_rows=@rows OUTPUT;   -- SUBFRI: Apr24
EXEC SP_EOD_FEE_ACCRUE '2026-04-27',@p_rows=@rows OUTPUT;   -- SUBFRI: Apr25,26,27 (base 2e9)
EXEC SP_EOD_FEE_ACCRUE '2026-04-29',@p_rows=@rows OUTPUT;   -- SUBX: Apr29
EXEC SP_EOD_FEE_ACCRUE '2026-05-04',@p_rows=@rows OUTPUT;   -- SUBX: Apr30 + May1..4

-- ★ AUM=0 → phí ngày = 0 (dòng vẫn tạo, fee 0.00) + KHÔNG sinh charge rác
INSERT INTO @R SELECT '★ AUM=0 → phí ngày 0, KHÔNG sinh charge',
  CASE WHEN (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBZERO')=1
        AND (SELECT C_FEE_AMOUNT FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBZERO')=0
        AND NOT EXISTS (SELECT 1 FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBZERO') THEN 1 ELSE 0 END,
  CONCAT('fee=',(SELECT C_FEE_AMOUNT FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBZERO'));

-- SUBONE: 1 dòng (Apr22), kỳ 202604, fee = feeDay
INSERT INTO @R SELECT 'ACCRUE SUBONE 1 dòng (Apr22) kỳ202604',
  CASE WHEN (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBONE')=1
        AND (SELECT C_FEE_DATE FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBONE')='2026-04-22'
        AND (SELECT C_PERIOD FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBONE')='202604'
        AND (SELECT ROUND(C_FEE_AMOUNT,6) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBONE')=@feeDay THEN 1 ELSE 0 END,
  (SELECT CONCAT('fee=',C_FEE_AMOUNT) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBONE');

-- ★★ CỐT LÕI: NẠP TIỀN T7 → PHÍ T7/CN TÍNH TRÊN AUM MỚI (2e9), KHÔNG PHẢI AUM T6 (1e9).
INSERT INTO @R SELECT '★★ Nạp T7: phí T7/CN base = AUM ngày đó (2e9), KHÔNG phải AUM T6',
  CASE WHEN (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBFRI')=4
        AND (SELECT C_AUM        FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBFRI' AND C_FEE_DATE='2026-04-24')=1000000000
        AND (SELECT C_FEE_AMOUNT FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBFRI' AND C_FEE_DATE='2026-04-24')=@feeDay
        AND (SELECT C_AUM        FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBFRI' AND C_FEE_DATE='2026-04-25')=2000000000
        AND (SELECT C_FEE_AMOUNT FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBFRI' AND C_FEE_DATE='2026-04-25')=@feeDay2   -- T7
        AND (SELECT C_FEE_AMOUNT FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBFRI' AND C_FEE_DATE='2026-04-26')=@feeDay2   -- CN
        AND (SELECT C_FEE_AMOUNT FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBFRI' AND C_FEE_DATE='2026-04-27')=@feeDay2   -- T2
        -- 3 ngày T7/CN/T2 accrue CÙNG 1 ngày GD (T2 27/04) — look-backward
        AND (SELECT COUNT(DISTINCT C_ACCRUED_ON) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBFRI')=2 THEN 1 ELSE 0 END,
  (SELECT CONCAT('feeT7=',C_FEE_AMOUNT,' (look-forward cũ sẽ là ',@feeDay,')')
     FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBFRI' AND C_FEE_DATE='2026-04-25');

-- ★ SUBX: 6 dòng per-day (vắt tháng); 202604 = Apr29,30 (2) ; 202605 = May1,2,3,4 (4)
INSERT INTO @R SELECT '★ ACCRUE SUBX 6 dòng per-day (dải nghỉ vắt tháng)',
  CASE WHEN (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX')=6
        AND (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202604' AND C_FEE_DATE IN ('2026-04-29','2026-04-30'))=2
        AND (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202605' AND C_FEE_DATE IN ('2026-05-01','2026-05-02','2026-05-03','2026-05-04'))=4
        AND (SELECT ROUND(SUM(C_FEE_AMOUNT),6) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202604')=ROUND(2*@feeDay,6)
        AND (SELECT ROUND(SUM(C_FEE_AMOUNT),6) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202605')=ROUND(4*@feeDay,6) THEN 1 ELSE 0 END,
  CONCAT('count=',(SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX'));

/*--- idempotent: re-accrue May04 → vẫn 6 dòng cho SUBX (không nhân đôi) ---*/
EXEC SP_EOD_FEE_ACCRUE '2026-05-04',@p_rows=@rows OUTPUT;
INSERT INTO @R SELECT 'ACCRUE idempotent re-run (SUBX vẫn 6)',
  CASE WHEN (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX')=6 THEN 1 ELSE 0 END,
  CONCAT('count=',(SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX'));

-- ★ CHARGE CỘNG DỒN NGAY (chưa chốt): J15 tạo dòng charge với C_CLOSED_AT NULL
INSERT INTO @R SELECT '★ Charge sinh NGAY khi accrue, C_CLOSED_AT NULL (đang tích lũy)',
  CASE WHEN (SELECT C_FEE_TOTAL  FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBONE' AND C_PERIOD='202604')=@feeDay
        AND (SELECT C_CLOSED_AT  FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBONE' AND C_PERIOD='202604') IS NULL
        AND (SELECT C_CLOSED_AT  FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBX'   AND C_PERIOD='202605') IS NULL THEN 1 ELSE 0 END,
  (SELECT CONCAT('total=',C_FEE_TOTAL) FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBONE' AND C_PERIOD='202604');

/*==============================================================================
  2) CLOSE PERIOD — chốt ở ngày GD ĐẦU tháng SAU (May04), KHÔNG phải ngày GD cuối tháng.
     Lý do: look-backward ⇒ tại EOD ngày GD cuối tháng, T7/CN cuối tháng CHƯA accrue.
==============================================================================*/
-- close giữa tháng (Apr29: prev=Apr28 cùng tháng) → no-op; charge vẫn CHƯA chốt
EXEC SP_FEE_CLOSE_PERIOD '2026-04-29',@p_rows=@rows OUTPUT;
INSERT INTO @R SELECT 'CLOSE giữa tháng (Apr29) = no-op, chưa chốt kỳ nào',
  CASE WHEN @rows=0 AND (SELECT COUNT(*) FROM T_SI_FEE_CHARGE WHERE C_CLOSED_AT IS NOT NULL)=0 THEN 1 ELSE 0 END,
  CONCAT('rows=',@rows);

-- ★ chốt 202604 tại May04 (ngày GD ĐẦU tháng 5; prev=Apr29 khác tháng)
EXEC SP_FEE_CLOSE_PERIOD '2026-05-04',@p_rows=@rows OUTPUT;
INSERT INTO @R SELECT '★ CLOSE 202604 tại ngày GD đầu tháng 5 → 3 charge chốt',
  CASE WHEN @rows=3
        AND (SELECT COUNT(*) FROM T_SI_FEE_CHARGE WHERE C_PERIOD='202604' AND C_CLOSED_AT IS NOT NULL)=3
        AND (SELECT COUNT(*) FROM T_SI_FEE_CHARGE WHERE C_PERIOD='202605' AND C_CLOSED_AT IS NOT NULL)=0 THEN 1 ELSE 0 END,
  CONCAT('rows=',@rows);
-- SUBX charge 202604: total=2×feeDay=82191.80, due=CEILING=82192
INSERT INTO @R SELECT 'CLOSE SUBX 202604 total=82191.80 due=82192 (CEILING)',
  CASE WHEN (SELECT C_FEE_TOTAL FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202604')=2*@feeDay
        AND (SELECT C_FEE_DUE FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202604')=CEILING(2*@feeDay)
        AND (SELECT C_STATUS FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202604')='UNPAID' THEN 1 ELSE 0 END,
  (SELECT CONCAT('total=',C_FEE_TOTAL,' due=',C_FEE_DUE) FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202604');
-- ★ SUBFRI 202604 gồm CẢ 4 ngày (Apr24 base 1e9 + Apr25/26/27 base 2e9) = feeDay + 3×feeDay2
INSERT INTO @R SELECT '★ SUBFRI 202604 = feeDay + 3×feeDay2 (phí T7/CN theo tiền thật)',
  CASE WHEN (SELECT C_FEE_TOTAL FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBFRI' AND C_PERIOD='202604')=@feeDay+3*@feeDay2 THEN 1 ELSE 0 END,
  (SELECT CONCAT('total=',C_FEE_TOTAL,' (look-forward cũ: ',4*@feeDay,')') FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBFRI' AND C_PERIOD='202604');
-- close KHÔNG đổi C_STATUS balance (vẫn =1 hợp lệ); không sinh invalid
INSERT INTO @R SELECT 'CLOSE giữ balance C_STATUS=1 (không sinh invalid)',
  CASE WHEN (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_PERIOD='202604' AND C_STATUS<>1)=0
        AND (SELECT C_STATUS FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202605' AND C_FEE_DATE='2026-05-01')=1 THEN 1 ELSE 0 END, NULL;

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
EXEC SP_EOD_FEE_ACCRUE   '2026-03-31',@p_rows=@rows OUTPUT;   -- 1 ngày kỳ 202603, fee=feeDay
EXEC SP_FEE_CLOSE_PERIOD '2026-04-01',@p_rows=@rows OUTPUT;   -- Apr01 = ngày GD ĐẦU tháng 4 → chốt 202603
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
-- ★ SUBLOCK2 (UNPAID) → recompute AUM=2e9: fee = CEILING(2e9×0.015/365, 2dp) = 82191.79 (≠ 2×41095.90!)
INSERT INTO @R SELECT '★ Chưa thu: SUBLOCK2 re-accrue recompute = 82191.79',
  CASE WHEN (SELECT C_FEE_AMOUNT FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBLOCK2' AND C_PERIOD='202603')=82191.79
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
-- món cũ hơn cho SUBX (test FIFO ordering) — ĐÃ CHỐT (C_CLOSED_AT NOT NULL) mới thu được
INSERT INTO T_SI_FEE_CHARGE (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_PERIOD,C_PERIOD_FROM,C_PERIOD_TO,C_FEE_TOTAL,C_FEE_DUE,C_CLOSED_AT)
 VALUES ('SUBX','KH03','SDIF','202603','2026-03-01','2026-03-31',50000,50000,GETDATE());

-- ★ SUBNC: kỳ 202605 CHƯA CHỐT + tiền dư dả → PHẢI KHÔNG bị thu ("chưa chốt tháng chưa thu")
INSERT INTO T_SI_PORTFOLIO (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_JOIN_DATE,C_STATUS)
 VALUES ('SUBNC','KH10','SDIF','2026-01-01','ACTIVE');
EXEC SP_SET_SI_FEE_RATE @p_si_account='SUBNC',@p_effective_from='2026-01-01',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
INSERT INTO T_SI_BALANCE (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_AUM,C_DAILY_RETURN,C_CASH,C_CASH_IN,C_CASH_OUT)
 VALUES ('2026-05-05','SUBNC','KH10','SDIF',1000000000,NULL,0,0,0);   -- T3 05/05
EXEC SP_EOD_FEE_ACCRUE '2026-05-05',@p_rows=@rows OUTPUT;   -- kỳ 202605, charge sinh ra nhưng CHƯA CHỐT
INSERT INTO T_SI_CURRENT (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_CASH,C_CASH_AVAILABLE,C_LAST_AUM,C_STATUS,C_LAST_SYNC_DATE)
 VALUES ('SUBNC','KH10','SDIF',10000000,10000000,1000000000,'ACTIVE','2026-05-05');   -- tiền THỪA sức trả
-- cash: TỔNG (C_CASH) vs KHẢ DỤNG (C_CASH_AVAILABLE) — SP_FEE_COLLECT PHẢI dùng KHẢ DỤNG.
--   SUBX: tổng 999,999,999 nhưng khả dụng chỉ 60,000 (phần còn lại phong toả/chờ khớp) → nếu proc lỡ đọc
--   C_CASH thì nó sẽ thu CẢ 2 món của SUBX (test @rows=2 vẫn pass) ⇒ thêm assert riêng bên dưới.
INSERT INTO T_SI_CURRENT (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_CASH,C_CASH_AVAILABLE,C_LAST_AUM,C_STATUS,C_LAST_SYNC_DATE) VALUES
 ('SUBX'  ,'KH03','SDIF', 999999999, 60000,1000000000,'ACTIVE','2026-04-29'),
 ('SUBONE','KH01','SDIF',    100000,100000,1000000000,'ACTIVE','2026-04-29'),
 ('SUBFRI','KH02','SDIF',         0,     0,1000000000,'ACTIVE','2026-04-29');

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
-- ★★ "CHƯA CHỐT THÁNG CHƯA THU": SUBNC có tiền THỪA sức trả nhưng kỳ 202605 chưa chốt → KHÔNG thu
INSERT INTO @R SELECT '★★ Chưa chốt tháng CHƯA THU (SUBNC dư tiền vẫn không bị thu)',
  CASE WHEN (SELECT C_CLOSED_AT   FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBNC' AND C_PERIOD='202605') IS NULL
        AND (SELECT C_FEE_DUE     FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBNC' AND C_PERIOD='202605') > 0
        AND (SELECT C_BO_EVENT_ID FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBNC' AND C_PERIOD='202605') IS NULL THEN 1 ELSE 0 END,
  (SELECT CONCAT('due=',C_FEE_DUE,' closed=NULL') FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBNC' AND C_PERIOD='202605');

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
      SUBX: nợ 202604=82192 (B) + tích lũy 202605=3×41095.90=123287.70 → DEBT=205479.70.
      SUBLOCK: 202603 PAID + ko dải chưa chốt → 0.  SUBONE: nợ 202604=41096, ko tích lũy → 41096.
==============================================================================*/
-- SUBX 202605 = May1,2,3,4 = 4×feeDay = 164383.60 (CHƯA CHỐT → là "đang tích lũy", KHÔNG phải nợ)
INSERT INTO @R SELECT 'UDF_SI_FEE_ACCRUING SUBX = 4×feeDay (kỳ 202605 chưa chốt)',
  CASE WHEN dbo.UDF_SI_FEE_ACCRUING('SUBX') = 4*@feeDay THEN 1 ELSE 0 END,
  CONCAT('accruing=', dbo.UDF_SI_FEE_ACCRUING('SUBX'));
-- nợ = 202604 đã chốt chưa thu (82192) + tích lũy 202605 (164383.60). 202603 đã PAID → loại.
INSERT INTO @R SELECT 'UDF_SI_FEE_DEBT SUBX = nợ chốt (82192) + tích lũy (164383.60)',
  CASE WHEN dbo.UDF_SI_FEE_DEBT('SUBX') = CEILING(2*@feeDay) + 4*@feeDay THEN 1 ELSE 0 END,
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
  6) BÁO CÁO — 3 report SP chạy + trả đúng số dòng (INSERT EXEC)
==============================================================================*/
DECLARE @rc INT;
-- (1) SINH PHÍ HÀNG NGÀY: SUBFRI Apr24-26 = 3 dòng per-day
CREATE TABLE #rd (si VARCHAR(20),cust VARCHAR(10),mc VARCHAR(20),fee_date DATE,accrued DATE,period CHAR(6),aum DECIMAL(20,0),rate DECIMAL(10,6),dc INT,fee DECIMAL(20,2));
INSERT #rd EXEC SP_RPT_FEE_DAILY @p_from_date='2026-04-24',@p_to_date='2026-04-26',@p_si_account='SUBFRI';
SET @rc=(SELECT COUNT(*) FROM #rd);
INSERT INTO @R SELECT 'RPT_FEE_DAILY SUBFRI Apr24-26 = 3 dòng', CASE WHEN @rc=3 THEN 1 ELSE 0 END, CONCAT('rows=',@rc);
-- (2) CHỐT PHÍ HÀNG KỲ: kỳ 202604 = 3 charge (SUBONE/SUBFRI/SUBX)
CREATE TABLE #rc2 (si VARCHAR(20),cust VARCHAR(10),mc VARCHAR(20),period CHAR(6),pf DATE,pt DATE,total DECIMAL(20,2),due DECIMAL(20,0),paid DECIMAL(20,0),remain DECIMAL(20,0),status VARCHAR(10),closed DATETIME,collected DATETIME,evt VARCHAR(64));
INSERT #rc2 EXEC SP_RPT_FEE_CHARGE @p_period='202604';
SET @rc=(SELECT COUNT(*) FROM #rc2);
INSERT INTO @R SELECT 'RPT_FEE_CHARGE kỳ 202604 = 3 charge', CASE WHEN @rc=3 THEN 1 ELSE 0 END, CONCAT('rows=',@rc);
-- (3) GIAO DỊCH THU PHÍ: SUBX = 1 (kỳ 202603 PAID có request id); 202604 UNPAID no event → loại
CREATE TABLE #rc3 (si VARCHAR(20),cust VARCHAR(10),mc VARCHAR(20),period CHAR(6),req VARCHAR(64),due DECIMAL(20,0),paid DECIMAL(20,0),status VARCHAR(10),collected DATETIME);
INSERT #rc3 EXEC SP_RPT_FEE_COLLECTION @p_si_account='SUBX';
SET @rc=(SELECT COUNT(*) FROM #rc3);
INSERT INTO @R SELECT 'RPT_FEE_COLLECTION SUBX = 1 (202603 PAID)',
  CASE WHEN @rc=1 AND EXISTS(SELECT 1 FROM #rc3 WHERE period='202603' AND status='PAID') THEN 1 ELSE 0 END, CONCAT('rows=',@rc);
DROP TABLE #rd, #rc2, #rc3;

/*==============================================================================
  KẾT QUẢ
==============================================================================*/
SELECT name AS [Check], CASE WHEN ok=1 THEN 'PASS' ELSE 'FAIL' END AS [Result], detail AS [Detail] FROM @R ORDER BY id;
DECLARE @pass INT=(SELECT COUNT(*) FROM @R WHERE ok=1), @tot INT=(SELECT COUNT(*) FROM @R);
PRINT REPLICATE('=',60);
PRINT CONCAT('FEE SMOKE: ',@pass,'/',@tot,' PASS  → ', CASE WHEN @pass=@tot THEN 'ALL GREEN ✓' ELSE 'HAS FAILURES ✗' END);
IF @pass<>@tot SELECT name AS [FAILED], detail FROM @R WHERE ok=0;
