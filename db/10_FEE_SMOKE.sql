/*==============================================================================
  SDI — SMOKE TEST PHÍ QUẢN LÝ (09_FEE.sql). Chạy SAU 01_TABLES + 02_SP_ENGINE + 09_FEE.
  Tự seed dataset nhỏ, EXEC từng proc, assert kết quả (hand-computed).

  Trọng tâm: case dải nghỉ vắt 2 tháng 30/4–1/5/2026 → TÁCH 2 dòng daily (kỳ 04 vs 05).
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
  1) ACCRUE — 3 trường hợp dải
     SUBONE @Apr22 (Thứ4→Thứ5)  = 1 ngày, kỳ 202604
     SUBFRI @Apr24 (Thứ6→Thứ2)  = 3 ngày (T6+T7+CN), kỳ 202604
     SUBX   @Apr29 (vắt tháng)   = 2 dòng: 202604 (2 ngày) + 202605 (3 ngày)
==============================================================================*/
INSERT INTO T_SI_BALANCE (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_AUM,C_DAILY_RETURN,C_CASH,C_CASH_IN,C_CASH_OUT) VALUES
 ('2026-04-22','SUBONE','KH01','SDIF',1000000000,NULL,0,0,0),
 ('2026-04-24','SUBFRI','KH02','SDIF',1000000000,NULL,0,0,0),
 ('2026-04-29','SUBX'  ,'KH03','SDIF',1000000000,NULL,0,0,0);

EXEC SP_EOD_FEE_ACCRUE '2026-04-22',@p_rows=@rows OUTPUT;
EXEC SP_EOD_FEE_ACCRUE '2026-04-24',@p_rows=@rows OUTPUT;
EXEC SP_EOD_FEE_ACCRUE '2026-04-29',@p_rows=@rows OUTPUT;

-- SUBONE: 1 dòng, days=1, kỳ 202604, fee = 1×feeDay
INSERT INTO @R SELECT 'ACCRUE SUBONE 1 dòng days=1 kỳ202604',
  CASE WHEN (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBONE')=1
        AND (SELECT C_DAYS FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBONE')=1
        AND (SELECT C_PERIOD FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBONE')='202604'
        AND (SELECT ROUND(C_FEE_AMOUNT,6) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBONE')=@feeDay THEN 1 ELSE 0 END,
  (SELECT CONCAT('days=',C_DAYS,' fee=',C_FEE_AMOUNT) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBONE');

-- SUBFRI: 1 dòng, days=3, kỳ 202604, fee = 3×feeDay
INSERT INTO @R SELECT 'ACCRUE SUBFRI 1 dòng days=3 (T6+T7+CN)',
  CASE WHEN (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBFRI')=1
        AND (SELECT C_DAYS FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBFRI')=3
        AND (SELECT ROUND(C_FEE_AMOUNT,6) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBFRI')=ROUND(3*@feeDay,6) THEN 1 ELSE 0 END,
  (SELECT CONCAT('days=',C_DAYS,' fee=',C_FEE_AMOUNT) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBFRI');

-- ★ SUBX: 2 dòng (case 30/4-1/5)
INSERT INTO @R SELECT '★ ACCRUE SUBX TÁCH 2 dòng (vắt tháng)',
  CASE WHEN (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX')=2 THEN 1 ELSE 0 END,
  CONCAT('count=',(SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX'));
-- dòng kỳ 202604: from=Apr29, to=Apr30, days=2
INSERT INTO @R SELECT '★ SUBX kỳ202604 from=Apr29 to=Apr30 days=2',
  CASE WHEN EXISTS (SELECT 1 FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202604'
        AND C_FROM_DATE='2026-04-29' AND C_TO_DATE='2026-04-30' AND C_DAYS=2 AND ROUND(C_FEE_AMOUNT,6)=ROUND(2*@feeDay,6)) THEN 1 ELSE 0 END,
  (SELECT CONCAT('from=',CONVERT(VARCHAR,C_FROM_DATE,23),' to=',CONVERT(VARCHAR,C_TO_DATE,23),' days=',C_DAYS,' fee=',C_FEE_AMOUNT)
   FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202604');
-- dòng kỳ 202605: from=May1, to=May3, days=3
INSERT INTO @R SELECT '★ SUBX kỳ202605 from=May01 to=May03 days=3',
  CASE WHEN EXISTS (SELECT 1 FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202605'
        AND C_FROM_DATE='2026-05-01' AND C_TO_DATE='2026-05-03' AND C_DAYS=3 AND ROUND(C_FEE_AMOUNT,6)=ROUND(3*@feeDay,6)) THEN 1 ELSE 0 END,
  (SELECT CONCAT('from=',CONVERT(VARCHAR,C_FROM_DATE,23),' to=',CONVERT(VARCHAR,C_TO_DATE,23),' days=',C_DAYS,' fee=',C_FEE_AMOUNT)
   FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202605');
-- tổng ngày 2 dòng = 5 (= dải [Apr29,May4))
INSERT INTO @R SELECT '★ SUBX Σdays=5 (khớp dải look-forward)',
  CASE WHEN (SELECT SUM(C_DAYS) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX')=5 THEN 1 ELSE 0 END,
  CONCAT('Σdays=',(SELECT SUM(C_DAYS) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX'));

/*--- idempotent: re-accrue Apr29 → vẫn 2 dòng cho SUBX (không nhân đôi) ---*/
EXEC SP_EOD_FEE_ACCRUE '2026-04-29',@p_rows=@rows OUTPUT;
INSERT INTO @R SELECT 'ACCRUE idempotent re-run (SUBX vẫn 2)',
  CASE WHEN (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX')=2 THEN 1 ELSE 0 END,
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
-- daily 202604 → CLOSED; daily 202605 → vẫn ACCRUED
INSERT INTO @R SELECT 'CLOSE daily 202604=CLOSED, 202605=ACCRUED',
  CASE WHEN (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_PERIOD='202604' AND C_STATUS<>'CLOSED')=0
        AND (SELECT C_STATUS FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202605')='ACCRUED' THEN 1 ELSE 0 END, NULL;

/*==============================================================================
  2b) BẤT BIẾN SAU CHỐT (no-delete) — re-accrue Apr29 SAU khi đã CLOSE 202604:
      daily 202604 (CLOSED) PHẢI giữ nguyên (fee + status), KHÔNG bị xoá/đổi.
      (Bug DELETE cũ: re-accrue sẽ xoá sạch daily đã chốt → mất backing của charge.)
==============================================================================*/
DECLARE @feeBefore DECIMAL(20,6) = (SELECT C_FEE_AMOUNT FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202604');
EXEC SP_EOD_FEE_ACCRUE '2026-04-29',@p_rows=@rows OUTPUT;   -- re-accrue sau close
INSERT INTO @R SELECT '★ Re-accrue sau CLOSE: daily 202604 BẤT BIẾN',
  CASE WHEN (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202604')=1
        AND (SELECT C_STATUS     FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202604')='CLOSED'
        AND (SELECT C_FEE_AMOUNT FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202604')=@feeBefore THEN 1 ELSE 0 END,
  CONCAT('fee giữ=',@feeBefore);
-- 202605 (ACCRUED, chưa chốt) thì VẪN được cập nhật tại chỗ → còn đúng 1 dòng, không nhân đôi
INSERT INTO @R SELECT '★ Re-accrue: 202605 (ACCRUED) update tại chỗ, không nhân đôi',
  CASE WHEN (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202605')=1
        AND (SELECT C_STATUS FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202605')='ACCRUED' THEN 1 ELSE 0 END,
  CONCAT('count=',(SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_SI_ACCOUNT='SUBX'));
-- không có DELETE: tổng daily toàn bộ smoke không giảm bất thường (no VOID trong luồng thường)
INSERT INTO @R SELECT '★ No VOID trong luồng thường (chỉ orphan calendar mới VOID)',
  CASE WHEN (SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_STATUS='VOID')=0 THEN 1 ELSE 0 END,
  CONCAT('void=',(SELECT COUNT(*) FROM T_SI_FEE_BALANCE WHERE C_STATUS='VOID'));

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
-- event gồm SUBX/202603 + SUBONE/202604 = 2 món
INSERT INTO @R SELECT 'COLLECT FIFO chọn 2 món (đủ tiền)',
  CASE WHEN @rows=2
        AND (SELECT C_BO_EVENT_ID FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202603')='EVT-TEST-001'
        AND (SELECT C_BO_EVENT_ID FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBONE' AND C_PERIOD='202604')='EVT-TEST-001' THEN 1 ELSE 0 END,
  CONCAT('rows=',@rows);
-- SUBX/202604 KHÔNG vào event (thiếu tiền); SUBFRI KHÔNG (cash 0)
INSERT INTO @R SELECT 'COLLECT bỏ món thiếu tiền (SUBX 202604, SUBFRI)',
  CASE WHEN (SELECT C_BO_EVENT_ID FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202604') IS NULL
        AND (SELECT C_BO_EVENT_ID FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBFRI' AND C_PERIOD='202604') IS NULL THEN 1 ELSE 0 END, NULL;

/*==============================================================================
  4) INGEST BO RESULT — SUBX/202603 collected=1 → PAID; SUBONE/202604 collected=0 → retry
==============================================================================*/
DECLARE @json NVARCHAR(MAX) = N'[
  {"si_account":"SUBX","period":"202603","collected":1,"amount":50000},
  {"si_account":"SUBONE","period":"202604","collected":0,"amount":0}
]';
EXEC SP_INGEST_FEE_COLLECT_RESULT @p_json=@json,@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;

INSERT INTO @R SELECT 'INGEST collected=1 → PAID (SUBX 202603)',
  CASE WHEN (SELECT C_STATUS FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202603')='PAID'
        AND (SELECT C_FEE_PAID FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBX' AND C_PERIOD='202603')=50000 THEN 1 ELSE 0 END,
  CONCAT('err=',@ec);
INSERT INTO @R SELECT 'INGEST collected=0 → UNPAID + event clear (SUBONE)',
  CASE WHEN (SELECT C_STATUS FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBONE' AND C_PERIOD='202604')='UNPAID'
        AND (SELECT C_BO_EVENT_ID FROM T_SI_FEE_CHARGE WHERE C_SI_ACCOUNT='SUBONE' AND C_PERIOD='202604') IS NULL THEN 1 ELSE 0 END, NULL;

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
