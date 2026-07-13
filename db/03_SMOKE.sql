/*==============================================================================
  SDI SMOKE — thin-layer (chạy sau 01+02+05+06).
  Asset gửi per-SI per-ngày: AUM + daily_return (TWR) → SDI ingest (SP_INGEST_ASSET_NAV) → LƯU (KHÔNG derive)
  → agg master (AUM + AUM-weighted return) → reconcile. FO chỉ gửi holdings. Cashflow SDI vẫn nhập (đối soát).
==============================================================================*/
SET NOCOUNT ON; SET QUOTED_IDENTIFIER ON; SET ANSI_NULLS ON;
DECLARE @ec INT, @em NVARCHAR(400);

-- reset
DELETE FROM T_EOD_WORK; DELETE FROM T_SI_BALANCE;
DELETE FROM T_MASTER_BALANCE; DELETE FROM T_MASTER_INDEX_DAILY; DELETE FROM T_MASTER_HOLDING_BALANCE;
DELETE FROM T_EOD_RUN; DELETE FROM T_MASTER_CURRENT;
DELETE FROM T_SI_PORTFOLIO_HOLDING; DELETE FROM T_SI_CURRENT; DELETE FROM T_SI_HOLDING_HIST;
DELETE FROM T_SI_CASHFLOW_EVENT; DELETE FROM T_EOD_RECON_BREAK; DELETE FROM T_EOD_PIPELINE;
DELETE FROM T_PRICE_DAILY; DELETE FROM T_MASTER_PORTFOLIO_TICKER; DELETE FROM T_SI_PORTFOLIO; DELETE FROM T_MASTER_PORTFOLIO;

INSERT T_MASTER_PORTFOLIO (C_MASTER_CODE,C_MASTER_NAME,C_STATUS,C_INCEPTION_DATE,C_BENCHMARK_CODE) VALUES ('SDI01',N'Demo','ACTIVE','2026-01-02','VNINDEX');
INSERT T_SI_PORTFOLIO (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_JOIN_DATE,C_STATUS) VALUES ('SUB001','KH001','SDI01','2026-01-02','ACTIVE');
INSERT T_MASTER_PORTFOLIO_TICKER (C_MASTER_CODE,C_EFFECTIVE_DATE,C_TICKER,C_TARGET_WEIGHT) VALUES ('SDI01','2026-01-02','AAA',0.6),('SDI01','2026-01-02','BBB',0.4);
INSERT T_PRICE_DAILY (C_TICKER,C_BUSINESS_DATE,C_REF_PRICE,C_CLOSE_PRICE) VALUES
 ('AAA','2026-01-02',100,100),('BBB','2026-01-02',50,50),
 ('AAA','2026-01-05',100,110),('BBB','2026-01-05',50,48),
 ('AAA','2026-01-06',110,110),('BBB','2026-01-06',48,48);

PRINT '======== CUSTOMER EOD: ingest Asset NAV → derive ========';

-- DAY 02: nạp 10tr; Asset gửi aum=10tr, daily_return=NULL (phiên đầu)
INSERT T_SI_CASHFLOW_EVENT (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_BUSINESS_DATE,C_EVENT_TYPE,C_AMOUNT) VALUES ('SUB001','KH001','SDI01','2026-01-02','INITIAL',10000000);
EXEC SP_INGEST_CUSTOMER N'{"cust_code":"KH001","business_date":"2026-01-02","sub_accounts":[{"si_account":"SUB001","holdings":[{"ticker":"AAA","quantity":60000,"avg_cost":100},{"ticker":"BBB","quantity":80000,"avg_cost":50}]}]}';
EXEC SP_INGEST_ASSET_NAV N'[{"si_account":"SUB001","aum":10000000,"daily_return":null,"cash":0,"cash_in":10000000,"cash_out":0}]','2026-01-02',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
EXEC SP_EOD_SET_SOURCE_READY '2026-01-02','MKT_DATA',NULL,NULL,@ec OUTPUT,@em OUTPUT;
EXEC SP_EOD_RUN_INDEX '2026-01-02',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
EXEC SP_EOD_SET_SOURCE_READY '2026-01-02','ASSET_NAV',1,NULL,@ec OUTPUT,@em OUTPUT;   -- total=1 SI (SUB001); batch completeness
EXEC SP_EOD_SET_SOURCE_READY '2026-01-02','FO_INGEST',1,NULL,@ec OUTPUT,@em OUTPUT;
EXEC SP_EOD_RUN '2026-01-02',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
IF @ec<>0 PRINT CONCAT('  !!! EOD 02 ec=',@ec,' ',@em);

-- [BRD reconcile C#] Asset đẩy CẢ acc KHÔNG thuộc SDI → BỎ QUA (không reject). @p_rows = #SI thuộc SDI đã ghi.
DECLARE @ir BIGINT;
EXEC SP_INGEST_ASSET_NAV N'[{"si_account":"SUB001","aum":10000000,"daily_return":null,"cash":0,"cash_in":10000000,"cash_out":0},{"si_account":"SUBZZZ","aum":9,"cash":9}]','2026-01-02',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT,@p_rows=@ir OUTPUT;   -- SUBZZZ lạ (không có registry)
DECLARE @zz INT=(SELECT COUNT(*) FROM T_SI_BALANCE WHERE C_SI_ACCOUNT='SUBZZZ');
IF @ec=0 AND @ir=1 AND @zz=0 PRINT '  OK ingest bỏ qua acc lạ SUBZZZ, ghi 1 SI thuộc SDI (@p_rows=1)'; ELSE PRINT CONCAT('  !!! ingest acc lạ: ec=',@ec,' rows=',@ir,' zz=',@zz);
EXEC SP_INGEST_ASSET_NAV N'[{"si_account":"SUB001","cash":0}]','2026-01-02',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT,@p_rows=@ir OUTPUT;   -- SI thuộc SDI thiếu aum → validate FAIL
IF @ec<>0 AND @ir=0 PRINT '  OK ingest thiếu aum (SI thuộc SDI) → err=21, @p_rows=0'; ELSE PRINT CONCAT('  !!! ingest fail: ec=',@ec,' rows=',@ir);

-- DAY 05: thị trường tăng → Asset gửi aum=10.44tr, daily_return=0.044 (no flow)
EXEC SP_INGEST_CUSTOMER N'{"cust_code":"KH001","business_date":"2026-01-05","sub_accounts":[{"si_account":"SUB001","holdings":[{"ticker":"AAA","quantity":60000,"avg_cost":100},{"ticker":"BBB","quantity":80000,"avg_cost":50}]}]}';
EXEC SP_INGEST_ASSET_NAV N'[{"si_account":"SUB001","aum":10440000,"daily_return":0.044,"cash":0,"cash_in":0,"cash_out":0}]','2026-01-05',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
EXEC SP_EOD_SET_SOURCE_READY '2026-01-05','MKT_DATA',NULL,NULL,@ec OUTPUT,@em OUTPUT;
EXEC SP_EOD_RUN_INDEX '2026-01-05',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
EXEC SP_EOD_SET_SOURCE_READY '2026-01-05','ASSET_NAV',1,NULL,@ec OUTPUT,@em OUTPUT;   -- total=1 SI (SUB001); batch completeness
EXEC SP_EOD_SET_SOURCE_READY '2026-01-05','FO_INGEST',1,NULL,@ec OUTPUT,@em OUTPUT;
EXEC SP_EOD_RUN '2026-01-05',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
IF @ec<>0 PRINT CONCAT('  !!! EOD 05 ec=',@ec,' ',@em);

-- DAY 06: nạp 1.044tr, KHÔNG biến động giá → Asset gửi aum=11.484tr, daily_return=0 (chỉ nạp, không lãi)
INSERT T_SI_CASHFLOW_EVENT (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_BUSINESS_DATE,C_EVENT_TYPE,C_AMOUNT) VALUES ('SUB001','KH001','SDI01','2026-01-06','TOPUP',1044000);
EXEC SP_INGEST_CUSTOMER N'{"cust_code":"KH001","business_date":"2026-01-06","sub_accounts":[{"si_account":"SUB001","holdings":[{"ticker":"AAA","quantity":60000,"avg_cost":100},{"ticker":"BBB","quantity":80000,"avg_cost":50}]}]}';
EXEC SP_INGEST_ASSET_NAV N'[{"si_account":"SUB001","aum":11484000,"daily_return":0,"cash":1044000,"cash_in":1044000,"cash_out":0}]','2026-01-06',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
EXEC SP_EOD_SET_SOURCE_READY '2026-01-06','MKT_DATA',NULL,NULL,@ec OUTPUT,@em OUTPUT;
EXEC SP_EOD_RUN_INDEX '2026-01-06',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
EXEC SP_EOD_SET_SOURCE_READY '2026-01-06','ASSET_NAV',1,NULL,@ec OUTPUT,@em OUTPUT;   -- total=1 SI (SUB001); batch completeness
EXEC SP_EOD_SET_SOURCE_READY '2026-01-06','FO_INGEST',1,NULL,@ec OUTPUT,@em OUTPUT;
EXEC SP_EOD_RUN '2026-01-06',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
IF @ec<>0 PRINT CONCAT('  !!! EOD 06 ec=',@ec,' ',@em);

DECLARE @nav2 DECIMAL(20,0)=(SELECT C_AUM FROM T_SI_BALANCE WHERE C_SI_ACCOUNT='SUB001' AND C_BUSINESS_DATE='2026-01-02');
DECLARE @ret2 DECIMAL(10,6)=(SELECT C_DAILY_RETURN FROM T_SI_BALANCE WHERE C_SI_ACCOUNT='SUB001' AND C_BUSINESS_DATE='2026-01-02');
IF @nav2=10000000 AND @ret2 IS NULL PRINT CONCAT('  OK day02: AUM=',@nav2,' return=NULL (phiên đầu)'); ELSE PRINT CONCAT('  !!! day02: AUM=',@nav2,' return=',ISNULL(CAST(@ret2 AS VARCHAR(20)),'null'));
DECLARE @ret5 DECIMAL(10,6)=(SELECT C_DAILY_RETURN FROM T_SI_BALANCE WHERE C_SI_ACCOUNT='SUB001' AND C_BUSINESS_DATE='2026-01-05');
IF ABS(@ret5-0.044)<0.0001 PRINT CONCAT('  OK day05: return=',@ret5,' (Asset cấp daily_return)'); ELSE PRINT CONCAT('  !!! day05: return=',@ret5);
DECLARE @nav6 DECIMAL(20,0)=(SELECT C_AUM FROM T_SI_BALANCE WHERE C_SI_ACCOUNT='SUB001' AND C_BUSINESS_DATE='2026-01-06');
DECLARE @ret6 DECIMAL(10,6)=(SELECT C_DAILY_RETURN FROM T_SI_BALANCE WHERE C_SI_ACCOUNT='SUB001' AND C_BUSINESS_DATE='2026-01-06');
IF @nav6=11484000 AND @ret6=0
   PRINT CONCAT('  OK day06: AUM=',@nav6,' return=',@ret6,' (nạp 1.044tr, không lãi)');
ELSE PRINT CONCAT('  !!! day06: AUM=',@nav6,' return=',@ret6);

DECLARE @mnav DECIMAL(20,0)=(SELECT C_AUM FROM T_MASTER_BALANCE WHERE C_MASTER_CODE='SDI01' AND C_BUSINESS_DATE='2026-01-06');
IF @mnav=11484000 PRINT CONCAT('  OK master agg @06: AUM=',@mnav); ELSE PRINT CONCAT('  !!! master agg @06: ',@mnav);
DECLARE @nullRows INT=(SELECT COUNT(*) FROM T_EOD_RUN WHERE C_STATUS='DONE' AND C_ROWS IS NULL);
IF @nullRows=0 PRINT '  OK C_ROWS populate đủ (0 job DONE NULL)'; ELSE PRINT CONCAT('  !!! C_ROWS NULL: ',@nullRows);
-- [PHÍ ĐÃ TÁCH KHỎI EOD 2026-07-13] EOD chỉ chạy NGÀY GD (gate lịch), còn phí chạy theo NGÀY DƯƠNG LỊCH
--   (365) → để phí trong EOD sẽ MẤT phí ~115 ngày nghỉ/năm. Điểm vào phí = SP_FEE_RUN_DAILY (app gọi mỗi
--   ngày lịch, kể cả T7/CN). Ở đây assert NGƯỢC LẠI: EOD KHÔNG được sinh job phí nào.
IF OBJECT_ID('SP_EOD_FEE_ACCRUE','P') IS NOT NULL
BEGIN
    DECLARE @feeJobs INT=(SELECT COUNT(*) FROM T_EOD_RUN WHERE C_BUSINESS_DATE='2026-01-06'
        AND C_JOB IN ('J15_FEE_ACCRUE','J16_FEE_CLOSE'));
    IF @feeJobs=0 PRINT '  OK phí KHÔNG nằm trong EOD (đã tách sang SP_FEE_RUN_DAILY — chạy 365 ngày)';
    ELSE PRINT CONCAT('  !!! EOD vẫn còn job phí: ',@feeJobs,' → ngày nghỉ sẽ MẤT phí');
END
ELSE PRINT '  -- (09_FEE chưa cài → bỏ qua check)';

-- [Cách A] INDEX consistency: C_DAILY_RETURN PHẢI = index_2dp_t / index_2dp_(t-1) − 1 (user suy từ 2dp ra KHỚP, hết lệch).
DECLARE @idxLech INT = (
    SELECT COUNT(*) FROM (
        SELECT C_DAILY_RETURN,
               recon = CAST(CAST(C_INDEX_VALUE AS FLOAT) / LAG(C_INDEX_VALUE) OVER (PARTITION BY C_MASTER_CODE ORDER BY C_BUSINESS_DATE) - 1 AS DECIMAL(10,6))
        FROM T_MASTER_INDEX_DAILY
    ) t WHERE t.recon IS NOT NULL AND t.C_DAILY_RETURN <> t.recon);
IF @idxLech=0 PRINT '  OK index daily_return = reconstruct từ index 2dp (Cách A, hết lệch)';
ELSE PRINT CONCAT('  !!! index return lệch reconstruct 2dp: ',@idxLech,' dòng');

PRINT '';
PRINT '======== RECONCILE: đo vênh 2 nguồn ========';
DECLARE @brk6 INT=(SELECT COUNT(*) FROM T_EOD_RECON_BREAK WHERE C_BUSINESS_DATE='2026-01-06');
IF @brk6=0 PRINT '  OK reconcile @06 sạch (2 nguồn khớp)'; ELSE PRINT CONCAT('  !!! reconcile @06 break: ',@brk6);
EXEC SP_INGEST_ASSET_NAV N'[{"si_account":"SUB001","aum":11043999,"daily_return":0,"cash":1044000,"cash_in":2000000,"cash_out":0}]','2026-01-06',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
EXEC SP_EOD_RECONCILE '2026-01-06',@p_rows=@ec OUTPUT;
DECLARE @cfd DECIMAL(20,6)=(SELECT C_DIFF FROM T_EOD_RECON_BREAK WHERE C_BUSINESS_DATE='2026-01-06' AND C_CHECK_NAME='CASHFLOW_MISMATCH');
-- cashflow: SDI 1.044tr − Asset cash_in 2tr = −956000. (HOLDINGS_MISMATCH + NAV_CONSISTENCY đã gỡ thin-layer)
IF @cfd=-956000 PRINT CONCAT('  OK reconcile đo vênh: cashflow diff=',@cfd);
ELSE PRINT CONCAT('  !!! reconcile diff sai: cashflow=',ISNULL(CAST(@cfd AS VARCHAR(30)),'(null)'));
EXEC SP_INGEST_ASSET_NAV N'[{"si_account":"SUB001","aum":11484000,"daily_return":0,"cash":1044000,"cash_in":1044000,"cash_out":0}]','2026-01-06',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
EXEC SP_EOD_RECONCILE '2026-01-06',@p_rows=@ec OUTPUT;

PRINT '';
PRINT '======== PIPELINE RESET: watermark, log GIỮ, re-run ========';
DECLARE @ovr VARCHAR(20),@eodst VARCHAR(10),@ecP INT,@emP NVARCHAR(400);
DECLARE @j07b DATETIME=(SELECT C_ENDED_AT FROM T_EOD_RUN WHERE C_BUSINESS_DATE='2026-01-06' AND C_JOB='J07_COMPUTE');
EXEC SP_EOD_RESET @p_business_date='2026-01-06',@p_err_code=@ecP OUTPUT,@p_err_msg=@emP OUTPUT;
DECLARE @wm DATETIME=(SELECT C_EOD_RESET_AT FROM T_EOD_PIPELINE WHERE C_BUSINESS_DATE='2026-01-06');
DECLARE @logK INT=(SELECT COUNT(*) FROM T_EOD_RUN WHERE C_BUSINESS_DATE='2026-01-06');
SELECT @eodst=C_EOD_STATUS FROM T_EOD_PIPELINE WHERE C_BUSINESS_DATE='2026-01-06';
IF @eodst='PENDING' AND @wm IS NOT NULL AND @logK>0 PRINT CONCAT('  OK reset: EOD=PENDING, watermark set, log GIỮ (',@logK,' dòng)');
ELSE PRINT CONCAT('  !!! reset sai: eod=',@eodst,' wm=',ISNULL(CONVERT(VARCHAR(30),@wm,121),'null'),' log=',@logK);
EXEC SP_EOD_RUN '2026-01-06',@p_err_code=@ecP OUTPUT,@p_err_msg=@emP OUTPUT;
DECLARE @j07a DATETIME=(SELECT C_ENDED_AT FROM T_EOD_RUN WHERE C_BUSINESS_DATE='2026-01-06' AND C_JOB='J07_COMPUTE');
SELECT @ovr=C_OVERALL_STATUS FROM T_EOD_PIPELINE WHERE C_BUSINESS_DATE='2026-01-06';
IF @ovr='EOD_DONE' AND @j07a>@j07b PRINT '  OK re-run sau reset: job chạy LẠI, EOD_DONE'; ELSE PRINT CONCAT('  !!! re-run sai: overall=',@ovr);

PRINT '';
PRINT '======== DATE GUARD ========';
EXEC SP_EOD_RUN '2099-06-15',@p_err_code=@ecP OUTPUT,@p_err_msg=@emP OUTPUT;
IF @ecP=10 PRINT '  OK EOD ngày chưa sẵn sàng → err=10 precondition'; ELSE PRINT CONCAT('  !!! EOD guard: ',@ecP);
DECLARE @ecF INT,@emF NVARCHAR(400);
EXEC SP_GET_ASSET_REPORT 'SUB001','2099-01-01',@p_err_code=@ecF OUTPUT,@p_err_msg=@emF OUTPUT;
IF @ecF=5 PRINT '  OK FR-06 ngày bừa bãi → err=5'; ELSE PRINT CONCAT('  !!! FR-06 guard: ',@ecF);
EXEC SP_GET_ASSET_REPORT 'SUB001','2026-01-06',@p_err_code=@ecF OUTPUT,@p_err_msg=@emF OUTPUT;
IF @ecF=0 PRINT '  OK FR-06 happy (asset report từ thành phần Asset)'; ELSE PRINT CONCAT('  !!! FR-06 happy: ',@ecF,' ',@emF);

PRINT '';
PRINT '======== INDEX completeness HARD-FAIL + weight guard (engine giữ nguyên) ========';
UPDATE T_MASTER_PORTFOLIO SET C_STATUS='CLOSED' WHERE C_MASTER_CODE='SDI01';   -- cô lập (all-or-nothing)
INSERT T_MASTER_PORTFOLIO (C_MASTER_CODE,C_MASTER_NAME,C_STATUS,C_INCEPTION_DATE,C_BENCHMARK_CODE) VALUES ('MMISS',N'MissPx','ACTIVE','2026-03-01','VNINDEX');
INSERT T_MASTER_PORTFOLIO_TICKER (C_MASTER_CODE,C_EFFECTIVE_DATE,C_TICKER,C_TARGET_WEIGHT) VALUES ('MMISS','2026-03-01','PXM1',50),('MMISS','2026-03-01','PXM2',50);
INSERT T_PRICE_DAILY (C_TICKER,C_BUSINESS_DATE,C_REF_PRICE,C_CLOSE_PRICE) VALUES ('PXM1','2026-03-02',100,110);  -- thiếu PXM2
DECLARE @ecMM INT=0;
BEGIN TRY EXEC SP_EOD_SI_INDEX '2026-03-02'; END TRY BEGIN CATCH SET @ecMM=ERROR_NUMBER(); END CATCH
IF @ecMM=51011 AND NOT EXISTS(SELECT 1 FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MMISS') PRINT '  OK completeness HARD-FAIL: THROW 51011, không ghi';
ELSE PRINT CONCAT('  !!! completeness: ec=',@ecMM);
INSERT T_MASTER_PORTFOLIO (C_MASTER_CODE,C_MASTER_NAME,C_STATUS,C_INCEPTION_DATE,C_BENCHMARK_CODE) VALUES ('MZW',N'ZeroW','ACTIVE','2026-03-01','VNINDEX');
INSERT T_MASTER_PORTFOLIO_TICKER (C_MASTER_CODE,C_EFFECTIVE_DATE,C_TICKER,C_TARGET_WEIGHT) VALUES ('MZW','2026-03-01','ZWA',0),('MZW','2026-03-01','ZWB',0);
INSERT T_PRICE_DAILY (C_TICKER,C_BUSINESS_DATE,C_REF_PRICE,C_CLOSE_PRICE) VALUES ('ZWA','2026-03-03',100,100),('ZWB','2026-03-03',50,50),('PXM1','2026-03-03',100,110),('PXM2','2026-03-03',100,100);  -- MMISS đủ giá @03-03 để weight-guard fire (không vướng completeness)
DECLARE @ecZW INT=0;
BEGIN TRY EXEC SP_EOD_SI_INDEX '2026-03-03'; END TRY BEGIN CATCH SET @ecZW=ERROR_NUMBER(); END CATCH
IF @ecZW=51012 PRINT '  OK weight Σ=0 → THROW 51012'; ELSE PRINT CONCAT('  !!! weight guard: ec=',@ecZW);
DELETE FROM T_MASTER_PORTFOLIO_TICKER WHERE C_MASTER_CODE IN ('MMISS','MZW');
DELETE FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE IN ('MMISS','MZW');
DELETE FROM T_PRICE_DAILY WHERE C_TICKER IN ('PXM1','PXM2','ZWA','ZWB');
UPDATE T_MASTER_PORTFOLIO SET C_STATUS='ACTIVE' WHERE C_MASTER_CODE='SDI01';

PRINT '';
PRINT '======== ASSET_NAV completeness: thiếu SI → SP_EOD_RUN err=12 ========';
INSERT T_SI_PORTFOLIO (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_JOIN_DATE,C_STATUS) VALUES ('SUB002','KH002','SDI01','2026-01-06','ACTIVE');
EXEC SP_EOD_RUN '2026-01-06',@p_err_code=@ecP OUTPUT,@p_err_msg=@emP OUTPUT;   -- SUB002 chưa có asset_daily @06
IF @ecP=12 PRINT '  OK thiếu Asset NAV cho SUB002 → err=12 (chặn EOD, không bỏ ngầm)'; ELSE PRINT CONCAT('  !!! asset completeness: ec=',@ecP);
DELETE FROM T_SI_PORTFOLIO WHERE C_SI_ACCOUNT='SUB002';

PRINT '';
PRINT '======== ASSET_NAV batch gate (received/total — Kafka batch ≤100/msg) ========';
DECLARE @ag1 INT, @ag2 INT, @ast1 VARCHAR(10), @ast2 VARCHAR(10);
EXEC SP_EOD_SET_SOURCE_READY '2026-01-06','ASSET_NAV',2,NULL,@ag1 OUTPUT,@em OUTPUT;  -- received=1 (SUB001) / total=2 → CHƯA đủ
SELECT @ast1=C_ASSET_NAV_STATUS FROM T_EOD_PIPELINE WHERE C_BUSINESS_DATE='2026-01-06';
EXEC SP_EOD_SET_SOURCE_READY '2026-01-06','ASSET_NAV',1,NULL,@ag2 OUTPUT,@em OUTPUT;  -- received=1 / total=1 → đủ
SELECT @ast2=C_ASSET_NAV_STATUS FROM T_EOD_PIPELINE WHERE C_BUSINESS_DATE='2026-01-06';
IF @ag1=4 AND @ast1='PENDING' AND @ag2=0 AND @ast2='READY'
   PRINT '  OK batch gate: 1/2 → err=4 PENDING ; 1/1 → READY';
ELSE PRINT CONCAT('  !!! batch gate: ag1=',@ag1,' st1=',@ast1,' ag2=',@ag2,' st2=',@ast2);

PRINT '';
PRINT '======== LỊCH GD: ngày nghỉ KHÔNG được cộng dồn vào index (T_TRADING_HOLIDAY + rule T7/CN) ========';
-- Ngày dùng: 2026-07-01 T4, 02 T5, 03 T6, 04 T7, 05 CN, 06 T2. Lễ: 30/04, 02/09.
DECLARE @ecH INT, @emH NVARCHAR(400);
-- TỰ CHỨA: không dựa vào seed 01_TABLES (10_FEE_SMOKE.sql XOÁ SẠCH T_TRADING_HOLIDAY rồi nạp lịch riêng → chạy
--   fee-smoke trước file này sẽ làm assert lễ vỡ). Upsert lại 2 ngày lễ block này cần (idempotent).
EXEC SP_INGEST_TRADING_HOLIDAY N'[{"holiday_date":"2026-04-30","note":"30/4"},{"holiday_date":"2026-09-02","note":"Quốc khánh"}]',
     'smoke', @ecH OUTPUT, @emH OUTPUT;

-- (A) UDF_IS_BUSINESS_DATE: T7/CN → 0 (rule), lễ 02/09 → 0 (T_TRADING_HOLIDAY), ngày thường → 1
DECLARE @isSat BIT=dbo.UDF_IS_BUSINESS_DATE('2026-07-04'), @isSun BIT=dbo.UDF_IS_BUSINESS_DATE('2026-07-05'),
        @isHol BIT=dbo.UDF_IS_BUSINESS_DATE('2026-09-02'), @isWed BIT=dbo.UDF_IS_BUSINESS_DATE('2026-07-01');
IF @isSat=0 AND @isSun=0 AND @isHol=0 AND @isWed=1
   PRINT '  OK UDF_IS_BUSINESS_DATE: T7=0, CN=0, lễ 02/09=0, ngày thường=1';
ELSE PRINT CONCAT('  !!! UDF_IS_BUSINESS_DATE sai: T7=',@isSat,' CN=',@isSun,' lễ=',@isHol,' thường=',@isWed);

-- (B) INGEST GIÁ ngày nghỉ → err=23, KHÔNG ghi dòng nào
EXEC SP_INGEST_PRICE_DAILY N'[{"ticker":"HX1","ref_price":100,"close_price":110}]','2026-07-04',NULL,NULL,@ecH OUTPUT,@emH OUTPUT;
DECLARE @ecSat INT=@ecH;
EXEC SP_INGEST_PRICE_DAILY N'[{"ticker":"HX1","ref_price":100,"close_price":110}]','2026-04-30',NULL,NULL,@ecH OUTPUT,@emH OUTPUT;
IF @ecSat=23 AND @ecH=23 AND NOT EXISTS (SELECT 1 FROM T_PRICE_DAILY WHERE C_TICKER='HX1')
   PRINT '  OK ingest GIÁ ngày T7 + lễ 30/04 → err=23, KHÔNG ghi dòng nào';
ELSE PRINT CONCAT('  !!! ingest giá ngày nghỉ KHÔNG bị chặn: T7 err=',@ecSat,' lễ err=',@ecH);

-- (C) ⚠️ NGƯỢC LẠI — SP_INGEST_ASSET_NAV ngày T7 PHẢI VẪN NHẬN (Asset gửi aum/tiền MỌI ngày lịch).
--     Nếu guard lịch bị áp nhầm vào đây thì nạp/rút cuối tuần sẽ mất trắng.
EXEC SP_INGEST_ASSET_NAV N'[{"si_account":"SUB001","aum":11000000,"daily_return":0,"cash":1000000,"cash_in":1000000,"cash_out":0}]',
     '2026-07-04', NULL, @ecH OUTPUT, @emH OUTPUT;
IF @ecH=0 AND EXISTS (SELECT 1 FROM T_SI_BALANCE WHERE C_SI_ACCOUNT='SUB001' AND C_BUSINESS_DATE='2026-07-04')
   PRINT '  OK ingest ASSET_NAV ngày T7 VẪN NHẬN (err=0) — nạp/rút cuối tuần không mất';
ELSE PRINT CONCAT('  !!! ASSET_NAV ngày T7 bị chặn nhầm: err=',@ecH,' ',ISNULL(@emH,''));
DELETE FROM T_SI_BALANCE WHERE C_BUSINESS_DATE='2026-07-04';

-- (D) SP_INGEST_TRADING_HOLIDAY: nạp lễ mới (17/02 T3) → giá ngày đó bị chặn; is_delete=1 → gỡ → nạp lại OK
EXEC SP_INGEST_TRADING_HOLIDAY N'[{"holiday_date":"2026-02-17","note":"Tết Bính Ngọ"}]','ops',@ecH OUTPUT,@emH OUTPUT;
EXEC SP_INGEST_PRICE_DAILY N'[{"ticker":"HX2","ref_price":10,"close_price":11}]','2026-02-17',NULL,NULL,@ecH OUTPUT,@emH OUTPUT;
DECLARE @ecTet INT=@ecH;
EXEC SP_INGEST_TRADING_HOLIDAY N'[{"holiday_date":"2026-02-17","is_delete":1}]','ops',@ecH OUTPUT,@emH OUTPUT;
EXEC SP_INGEST_PRICE_DAILY N'[{"ticker":"HX2","ref_price":10,"close_price":11}]','2026-02-17',NULL,NULL,@ecH OUTPUT,@emH OUTPUT;
IF @ecTet=23 AND @ecH=0 AND EXISTS (SELECT 1 FROM T_PRICE_DAILY WHERE C_TICKER='HX2')
   PRINT '  OK SP_INGEST_TRADING_HOLIDAY: nạp lễ → giá bị chặn (23); gỡ lễ → nạp lại OK';
ELSE PRINT CONCAT('  !!! SP_INGEST_TRADING_HOLIDAY sai: có lễ err=',@ecTet,' (cần 23); sau gỡ err=',@ecH,' (cần 0)');
-- validate all-or-nothing: trùng ngày / sai định dạng / add+delete cùng ngày → err=21, không ghi
EXEC SP_INGEST_TRADING_HOLIDAY N'[{"holiday_date":"2026-06-10"},{"holiday_date":"2026-06-10"}]','ops',@ecH OUTPUT,@emH OUTPUT;
DECLARE @ecDup INT=@ecH;
EXEC SP_INGEST_TRADING_HOLIDAY N'[{"holiday_date":"khong-phai-ngay"}]','ops',@ecH OUTPUT,@emH OUTPUT;
IF @ecDup=21 AND @ecH=21 AND NOT EXISTS (SELECT 1 FROM T_TRADING_HOLIDAY WHERE C_HOLIDAY_DATE='2026-06-10')
   PRINT '  OK validate lịch: ngày trùng / sai định dạng → err=21, không ghi dòng nào';
ELSE PRINT CONCAT('  !!! validate lịch sai: dup=',@ecDup,' format=',@ecH);
DELETE FROM T_PRICE_DAILY WHERE C_TICKER='HX2';

-- (E) CỐT LÕI — giá ngày nghỉ LỌT vào DB (INSERT thẳng, bypass guard) thì index VẪN ĐÚNG.
--     HAA: T4 flat → T5 +10% → T6 +10%; T7/CN có dòng giá RÁC (carry-forward y hệt phiên T6); T2 flat.
--     Không lịch: nhân thêm 1.1 hai lần → 1464.10 (sai +21%). Có lịch: dừng ở 1210.
UPDATE T_MASTER_PORTFOLIO SET C_STATUS='CLOSED' WHERE C_MASTER_CODE='SDI01';   -- cô lập (all-or-nothing)
INSERT T_MASTER_PORTFOLIO (C_MASTER_CODE,C_MASTER_NAME,C_STATUS,C_INCEPTION_DATE,C_BENCHMARK_CODE) VALUES ('MHOL',N'HolidayGuard','ACTIVE','2026-07-01','VNINDEX');
INSERT T_MASTER_PORTFOLIO_TICKER (C_MASTER_CODE,C_EFFECTIVE_DATE,C_TICKER,C_TARGET_WEIGHT) VALUES ('MHOL','2026-07-01','HAA',1.0);
INSERT T_PRICE_DAILY (C_TICKER,C_BUSINESS_DATE,C_REF_PRICE,C_CLOSE_PRICE) VALUES
 ('HAA','2026-07-01',100,100),('HAA','2026-07-02',100,110),('HAA','2026-07-03',110,121),
 ('HAA','2026-07-04',110,121),   -- T7 RÁC
 ('HAA','2026-07-05',110,121),   -- CN RÁC
 ('HAA','2026-07-06',121,121);
EXEC SP_EOD_RECOMPUTE_INDEX_RANGE @p_from_date='2026-07-01', @p_to_date='2026-07-06', @p_err_code=@ecH OUTPUT, @p_err_msg=@emH OUTPUT;
DECLARE @hFri DECIMAL(18,2)=(SELECT C_INDEX_VALUE FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MHOL' AND C_BUSINESS_DATE='2026-07-03');
DECLARE @hMon DECIMAL(18,2)=(SELECT C_INDEX_VALUE FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MHOL' AND C_BUSINESS_DATE='2026-07-06');
DECLARE @hWkn INT=(SELECT COUNT(*) FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MHOL' AND C_BUSINESS_DATE IN ('2026-07-04','2026-07-05'));
DECLARE @hAll INT=(SELECT COUNT(*) FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MHOL');
IF @ecH=0 AND @hFri=1210.00 AND @hMon=1210.00 AND @hWkn=0 AND @hAll=4
   PRINT CONCAT('  OK loop DẢI NGÀY có giá rác T7/CN: index T6=',@hFri,' → T2=',@hMon,' (KHÔNG cộng dồn; nếu hỏng sẽ là 1464.10), 0 dòng index cuối tuần');
ELSE PRINT CONCAT('  !!! index CỘNG DỒN NGÀY NGHỈ: err=',@ecH,' T6=',@hFri,' T2=',@hMon,' #row cuối tuần=',@hWkn,' #row=',@hAll);

-- (F) UDF_PREV_BUSINESS_DATE nhảy qua ngày rác: prev(T2 06/07) = T6 03/07 (KHÔNG phải CN 05/07)
DECLARE @prevMon DATE = dbo.UDF_PREV_BUSINESS_DATE('2026-07-06');
DECLARE @lastBiz DATE = dbo.UDF_LAST_BUSINESS_DATE();
IF @prevMon='2026-07-03' AND @lastBiz='2026-07-06'
   PRINT '  OK UDF_PREV/LAST_BUSINESS_DATE bỏ qua T7/CN dù 2 ngày đó CÓ dòng giá';
ELSE PRINT CONCAT('  !!! prev/last business date SAI: prev=',ISNULL(CONVERT(VARCHAR(10),@prevMon,23),'(null)'),
                  ' last=',ISNULL(CONVERT(VARCHAR(10),@lastBiz,23),'(null)'));

-- (G) gọi THẲNG SP_EOD_SI_INDEX ngày T7 → THROW 51013, không ghi index
DECLARE @ecTh INT=0;
BEGIN TRY EXEC SP_EOD_SI_INDEX '2026-07-04'; END TRY BEGIN CATCH SET @ecTh=ERROR_NUMBER(); END CATCH
IF @ecTh=51013 AND NOT EXISTS (SELECT 1 FROM T_MASTER_INDEX_DAILY WHERE C_BUSINESS_DATE='2026-07-04')
   PRINT '  OK gọi thẳng SP_EOD_SI_INDEX ngày T7 → THROW 51013, không ghi index';
ELSE PRINT CONCAT('  !!! direct call ngày nghỉ KHÔNG throw: ec=',@ecTh);

-- (H) EOD pipeline ngày nghỉ: SET_SOURCE_READY / RUN_INDEX / RUN đều err=13, KHÔNG mở pipeline
EXEC SP_EOD_SET_SOURCE_READY '2026-07-04','MKT_DATA',NULL,NULL,@ecH OUTPUT,@emH OUTPUT;
DECLARE @ecRdy INT=@ecH;
EXEC SP_EOD_RUN_INDEX '2026-07-04', @p_err_code=@ecH OUTPUT, @p_err_msg=@emH OUTPUT;
DECLARE @ecRix INT=@ecH;
EXEC SP_EOD_RUN '2026-07-05', @p_err_code=@ecH OUTPUT, @p_err_msg=@emH OUTPUT;
IF @ecRdy=13 AND @ecRix=13 AND @ecH=13 AND NOT EXISTS (SELECT 1 FROM T_EOD_PIPELINE WHERE C_BUSINESS_DATE IN ('2026-07-04','2026-07-05'))
   PRINT '  OK ngày nghỉ: SET_SOURCE_READY/RUN_INDEX/RUN đều err=13, KHÔNG mở pipeline';
ELSE PRINT CONCAT('  !!! EOD ngày nghỉ không chặn: ready=',@ecRdy,' run_index=',@ecRix,' run=',@ecH);

DELETE FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MHOL';
DELETE FROM T_MASTER_PORTFOLIO_TICKER WHERE C_MASTER_CODE='MHOL';
DELETE FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE='MHOL';
DELETE FROM T_PRICE_DAILY WHERE C_TICKER='HAA';
UPDATE T_MASTER_PORTFOLIO SET C_STATUS='ACTIVE' WHERE C_MASTER_CODE='SDI01';

PRINT '';
PRINT '======== RECONCILE dòng tiền CUỐI TUẦN (CASHFLOW_MISMATCH dải, không phải 1 ngày) ========';
-- KH nạp 1e9 vào T7 25/04. EOD chỉ chạy ngày GD ⇒ nếu chỉ so đúng ngày @p_d thì cặp T7 KHÔNG BAO GIỜ
--   được đối soát (vùng mù). Nay so cả dải (phiên GD trước, @p_d] tại phiên T2 27/04.
DECLARE @ecRC INT, @emRC NVARCHAR(400), @rRC BIGINT;
INSERT T_SI_PORTFOLIO (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_JOIN_DATE,C_STATUS) VALUES ('SRC1','KRC1','SDI01','2026-04-01','ACTIVE');
INSERT T_SI_CASHFLOW_EVENT (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_BUSINESS_DATE,C_EVENT_TYPE,C_AMOUNT)
 VALUES ('SRC1','KRC1','SDI01','2026-04-25','TOPUP',1000000000);   -- T7 — value date THẬT
-- (a) Asset BÁO ĐÚNG (cash_in 1e9 ở row T7) → KHÔNG break
INSERT T_SI_BALANCE (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_AUM,C_DAILY_RETURN,C_CASH,C_CASH_AVAILABLE,C_CASH_IN,C_CASH_OUT) VALUES
 ('2026-04-25','SRC1','KRC1','SDI01',2000000000,0,1000000000,1000000000,1000000000,0),   -- T7
 ('2026-04-26','SRC1','KRC1','SDI01',2000000000,0,1000000000,1000000000,0,0),            -- CN
 ('2026-04-27','SRC1','KRC1','SDI01',2000000000,NULL,1000000000,1000000000,0,0);         -- T2
INSERT T_EOD_WORK (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_AUM)
 VALUES ('2026-04-27','SRC1','KRC1','SDI01',2000000000);
EXEC SP_EOD_RECONCILE '2026-04-27', @p_rows=@rRC OUTPUT;
DECLARE @brOK INT = (SELECT COUNT(*) FROM T_EOD_RECON_BREAK WHERE C_BUSINESS_DATE='2026-04-27' AND C_CHECK_NAME='CASHFLOW_MISMATCH');
-- (b) Asset BÁO THIẾU (quên cash_in ngày T7) → PHẢI ra break ở phiên T2
UPDATE T_SI_BALANCE SET C_CASH_IN=0 WHERE C_SI_ACCOUNT='SRC1' AND C_BUSINESS_DATE='2026-04-25';
EXEC SP_EOD_RECONCILE '2026-04-27', @p_rows=@rRC OUTPUT;
DECLARE @brBad INT = (SELECT COUNT(*) FROM T_EOD_RECON_BREAK WHERE C_BUSINESS_DATE='2026-04-27' AND C_CHECK_NAME='CASHFLOW_MISMATCH');
DECLARE @brDiff DECIMAL(20,0) = (SELECT C_DIFF FROM T_EOD_RECON_BREAK WHERE C_BUSINESS_DATE='2026-04-27' AND C_CHECK_NAME='CASHFLOW_MISMATCH');
IF @brOK=0 AND @brBad=1 AND @brDiff=1000000000
   PRINT '  OK CF ngày T7: Asset báo đúng → 0 break; Asset báo thiếu → BREAK ở phiên T2 (lệch 1e9). Hết vùng mù cuối tuần';
ELSE PRINT CONCAT('  !!! reconcile CF cuối tuần sai: break_khi_đúng=',@brOK,' break_khi_lệch=',@brBad,' diff=',ISNULL(@brDiff,-1));
DELETE FROM T_EOD_RECON_BREAK WHERE C_BUSINESS_DATE='2026-04-27';
DELETE FROM T_EOD_WORK WHERE C_SI_ACCOUNT='SRC1';
DELETE FROM T_SI_BALANCE WHERE C_SI_ACCOUNT='SRC1';
DELETE FROM T_SI_CASHFLOW_EVENT WHERE C_SI_ACCOUNT='SRC1';
DELETE FROM T_SI_PORTFOLIO WHERE C_SI_ACCOUNT='SRC1';

PRINT '======== END SMOKE ========';
