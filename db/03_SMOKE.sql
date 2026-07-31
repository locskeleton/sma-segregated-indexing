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
DELETE FROM T_PRICE_DAILY; DELETE FROM T_MASTER_PORTFOLIO_TICKER_HIST; DELETE FROM T_MASTER_PORTFOLIO_TICKER; DELETE FROM T_SI_PORTFOLIO; DELETE FROM T_MASTER_PORTFOLIO;

INSERT T_MASTER_PORTFOLIO (C_MASTER_CODE,C_MASTER_NAME,C_STATUS,C_INCEPTION_DATE,C_BENCHMARK_CODE) VALUES ('SDI01',N'Demo','ACTIVE','2026-01-02','VNINDEX');
INSERT T_SI_PORTFOLIO (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_JOIN_DATE,C_STATUS) VALUES ('SUB001','KH001','SDI01','2026-01-02','ACTIVE');
-- Rổ = HIST (nguồn as-of, J12 đọc từ đây) + bảng rổ hiện tại. Mốc duyệt TRƯỚC ngày tính index đầu tiên.
INSERT T_MASTER_PORTFOLIO_TICKER_HIST (C_MASTER_CODE,C_TICKER,C_TARGET_WEIGHT,C_CONFIRM_TIME) VALUES
 ('SDI01','AAA',0.6,'2026-01-01 09:00:00'),('SDI01','BBB',0.4,'2026-01-01 09:00:00');
INSERT T_MASTER_PORTFOLIO_TICKER (C_MASTER_CODE,C_TICKER,C_TARGET_WEIGHT) VALUES ('SDI01','AAA',0.6),('SDI01','BBB',0.4);
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
INSERT T_MASTER_PORTFOLIO_TICKER_HIST (C_MASTER_CODE,C_TICKER,C_TARGET_WEIGHT,C_CONFIRM_TIME) VALUES
 ('MMISS','PXM1',50,'2026-03-01 09:00:00'),('MMISS','PXM2',50,'2026-03-01 09:00:00');
INSERT T_PRICE_DAILY (C_TICKER,C_BUSINESS_DATE,C_REF_PRICE,C_CLOSE_PRICE) VALUES ('PXM1','2026-03-02',100,110);  -- thiếu PXM2
DECLARE @ecMM INT=0;
BEGIN TRY EXEC SP_EOD_SI_INDEX '2026-03-02'; END TRY BEGIN CATCH SET @ecMM=ERROR_NUMBER(); END CATCH
IF @ecMM=51011 AND NOT EXISTS(SELECT 1 FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MMISS') PRINT '  OK completeness HARD-FAIL: THROW 51011, không ghi';
ELSE PRINT CONCAT('  !!! completeness: ec=',@ecMM);
INSERT T_MASTER_PORTFOLIO (C_MASTER_CODE,C_MASTER_NAME,C_STATUS,C_INCEPTION_DATE,C_BENCHMARK_CODE) VALUES ('MZW',N'ZeroW','ACTIVE','2026-03-01','VNINDEX');
-- MZW: mọi mã đã bị GỠ (weight 0 trong HIST) ⇒ rổ as-of rỗng ⇒ Σ=0 ⇒ phải THROW 51012, không im lặng
INSERT T_MASTER_PORTFOLIO_TICKER_HIST (C_MASTER_CODE,C_TICKER,C_TARGET_WEIGHT,C_CONFIRM_TIME) VALUES
 ('MZW','ZWA',0,'2026-03-01 09:00:00'),('MZW','ZWB',0,'2026-03-01 09:00:00');
INSERT T_PRICE_DAILY (C_TICKER,C_BUSINESS_DATE,C_REF_PRICE,C_CLOSE_PRICE) VALUES ('ZWA','2026-03-03',100,100),('ZWB','2026-03-03',50,50),('PXM1','2026-03-03',100,110),('PXM2','2026-03-03',100,100);  -- MMISS đủ giá @03-03 để weight-guard fire (không vướng completeness)
DECLARE @ecZW INT=0;
BEGIN TRY EXEC SP_EOD_SI_INDEX '2026-03-03'; END TRY BEGIN CATCH SET @ecZW=ERROR_NUMBER(); END CATCH
IF @ecZW=51012 PRINT '  OK weight Σ=0 → THROW 51012'; ELSE PRINT CONCAT('  !!! weight guard: ec=',@ecZW);
DELETE FROM T_MASTER_PORTFOLIO_TICKER_HIST WHERE C_MASTER_CODE IN ('MMISS','MZW');
DELETE FROM T_MASTER_PORTFOLIO_TICKER WHERE C_MASTER_CODE IN ('MMISS','MZW');
DELETE FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE IN ('MMISS','MZW');
DELETE FROM T_PRICE_DAILY WHERE C_TICKER IN ('PXM1','PXM2','ZWA','ZWB');
UPDATE T_MASTER_PORTFOLIO SET C_STATUS='ACTIVE' WHERE C_MASTER_CODE='SDI01';

PRINT '';
PRINT '======== RỔ: cổng nạp SP_INGEST_MASTER_PORTFOLIO_TICKER (delta) + gỡ mã bằng weight 0 ========';
UPDATE T_MASTER_PORTFOLIO SET C_STATUS='CLOSED' WHERE C_MASTER_CODE='SDI01';   -- cô lập (all-or-nothing)
INSERT T_MASTER_PORTFOLIO (C_MASTER_CODE,C_MASTER_NAME,C_STATUS,C_INCEPTION_DATE,C_BENCHMARK_CODE)
VALUES ('MGATE',N'Gate','ACTIVE','2026-04-01','VNINDEX');
DECLARE @ecG INT, @emG NVARCHAR(400);

-- (a) nạp rổ đầu (rổ trống → delta = cả rổ): GA 0.6 + GB 0.4
EXEC SP_INGEST_MASTER_PORTFOLIO_TICKER 'MGATE',
     N'[{"ticker":"GA","weight":0.6},{"ticker":"GB","weight":0.4}]','2026-04-01 09:00:00','ops',@ecG OUTPUT,@emG OUTPUT;
IF @ecG=0 AND (SELECT COUNT(*) FROM T_MASTER_PORTFOLIO_TICKER WHERE C_MASTER_CODE='MGATE')=2
   PRINT '  OK nạp rổ đầu qua cổng (2 mã)'; ELSE PRINT CONCAT('  !!! nạp rổ đầu: ec=',@ecG,' ',ISNULL(@emG,''));

-- (b) ★ ĐỔI TỶ TRỌNG LÀM HỤT Σ (GA 0.6→1.0 mà quên hạ GB) → Σ=1.4 → chặn.
--     Đây là ca sinh ra index sai im lặng ở mô hình cũ.
EXEC SP_INGEST_MASTER_PORTFOLIO_TICKER 'MGATE',
     N'[{"ticker":"GA","weight":1.0}]','2026-04-02 09:00:00','ops',@ecG OUTPUT,@emG OUTPUT;
IF @ecG=23 AND NOT EXISTS (SELECT 1 FROM T_MASTER_PORTFOLIO_TICKER_HIST
                           WHERE C_MASTER_CODE='MGATE' AND C_CONFIRM_TIME='2026-04-02 09:00:00')
   PRINT '  OK delta làm Σ=1.4 → err=23, KHÔNG ghi dòng nào';
ELSE PRINT CONCAT('  !!! Σ sau khi áp phải bị chặn: ec=',@ecG,' ',ISNULL(@emG,''));

-- (c) mã TRÙNG trong batch → chặn
EXEC SP_INGEST_MASTER_PORTFOLIO_TICKER 'MGATE',
     N'[{"ticker":"GA","weight":0.5},{"ticker":"GA","weight":0.5}]','2026-04-02 09:00:00','ops',@ecG OUTPUT,@emG OUTPUT;
IF @ecG=22 PRINT '  OK mã trùng trong batch → err=22'; ELSE PRINT CONCAT('  !!! dup guard: ec=',@ecG);

-- (d) gỡ mã KHÔNG có trong rổ → chặn (gõ nhầm mã / gỡ hai lần)
EXEC SP_INGEST_MASTER_PORTFOLIO_TICKER 'MGATE',
     N'[{"ticker":"GZZ","weight":0}]','2026-04-02 09:00:00','ops',@ecG OUTPUT,@emG OUTPUT;
IF @ecG=24 PRINT '  OK gỡ mã không có trong rổ → err=24'; ELSE PRINT CONCAT('  !!! ghost guard: ec=',@ecG);

-- (e) confirm_time KHÔNG mới hơn thay đổi gần nhất → chặn (ghi lùi làm rổ hiện tại lệch rổ as-of)
EXEC SP_INGEST_MASTER_PORTFOLIO_TICKER 'MGATE',
     N'[{"ticker":"GA","weight":1.0},{"ticker":"GB","weight":0}]','2026-04-01 08:00:00','ops',@ecG OUTPUT,@emG OUTPUT;
IF @ecG=25 PRINT '  OK confirm_time ghi lùi → err=25'; ELSE PRINT CONCAT('  !!! time guard: ec=',@ecG);

-- (f) ★ GỠ GB ĐÚNG CÁCH: weight 0 kèm nâng GA lên 1.0 (Σ vẫn = 1).
--     GB huỷ niêm yết (KHÔNG có giá) mà index vẫn phải tính được.
EXEC SP_INGEST_MASTER_PORTFOLIO_TICKER 'MGATE',
     N'[{"ticker":"GA","weight":1.0},{"ticker":"GB","weight":0}]','2026-04-02 09:00:00','ops',@ecG OUTPUT,@emG OUTPUT;
INSERT T_PRICE_DAILY (C_TICKER,C_BUSINESS_DATE,C_REF_PRICE,C_CLOSE_PRICE) VALUES ('GA','2026-04-03',100,110);
DECLARE @ecGi INT=0, @nGi BIGINT;
BEGIN TRY EXEC SP_EOD_SI_INDEX '2026-04-03', @nGi OUTPUT; END TRY BEGIN CATCH SET @ecGi=ERROR_NUMBER(); END CATCH
DECLARE @vG DECIMAL(18,2)=(SELECT C_INDEX_VALUE FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MGATE' AND C_BUSINESS_DATE='2026-04-03');
DECLARE @nCur INT=(SELECT COUNT(*) FROM T_MASTER_PORTFOLIO_TICKER WHERE C_MASTER_CODE='MGATE');
IF @ecG=0 AND @ecGi=0 AND @vG=1100.00 AND @nCur=1
   PRINT '  OK gỡ mã bằng weight 0: GB rời rổ hiện tại, KHÔNG bị đòi giá, index = 1100.00 (chỉ theo GA)';
ELSE PRINT CONCAT('  !!! gỡ bằng weight 0: ec_ingest=',@ecG,' ec_index=',@ecGi,' index=',ISNULL(CONVERT(VARCHAR(20),@vG),'(null)'),' #ro=',@nCur);

-- (g) ★ RỔ AS-OF TRƯỚC KHI GỠ vẫn phải thấy GB — đây là toàn bộ lý do HIST tồn tại
DECLARE @bTruoc INT=(SELECT COUNT(*) FROM dbo.UDF_INDEX_BASKET_ASOF('2026-04-01') WHERE C_MASTER_CODE='MGATE' AND C_TARGET_WEIGHT<>0);
DECLARE @bSau   INT=(SELECT COUNT(*) FROM dbo.UDF_INDEX_BASKET_ASOF('2026-04-03') WHERE C_MASTER_CODE='MGATE' AND C_TARGET_WEIGHT<>0);
IF @bTruoc=2 AND @bSau=1
   PRINT '  OK rổ as-of: 01/04 = 2 mã (trước khi gỡ), 03/04 = 1 mã — lịch sử KHÔNG bị rổ hiện tại đè lên';
ELSE PRINT CONCAT('  !!! rổ as-of sai: 01/04=',@bTruoc,' 03/04=',@bSau);

-- (h) gỡ SẠCH mã (mọi mã weight 0 trong HIST) → Σ=0 → THROW 51012, không sinh index câm
INSERT T_MASTER_PORTFOLIO_TICKER_HIST (C_MASTER_CODE,C_TICKER,C_TARGET_WEIGHT,C_CONFIRM_TIME)
VALUES ('MGATE','GA',0,'2026-04-07 09:00:00');   -- ghi thẳng: cổng chặn Σ=0 nên phải bypass để test guard J12
INSERT T_PRICE_DAILY (C_TICKER,C_BUSINESS_DATE,C_REF_PRICE,C_CLOSE_PRICE) VALUES ('GA','2026-04-08',110,110);
DECLARE @ecGz INT=0;
BEGIN TRY EXEC SP_EOD_SI_INDEX '2026-04-08', @nGi OUTPUT; END TRY BEGIN CATCH SET @ecGz=ERROR_NUMBER(); END CATCH
IF @ecGz=51012 PRINT '  OK rổ gỡ SẠCH mã (Σ=0) → THROW 51012, KHÔNG im lặng bỏ qua';
ELSE PRINT CONCAT('  !!! rổ gỡ sạch phải THROW 51012: ec=',@ecGz);

-- (i) HIST là log DELTA: chỉ ghi mã THỰC SỰ đổi. Lần (f) chỉ đổi GA và GB ⇒ đúng 2 dòng ở mốc đó.
DECLARE @hF INT=(SELECT COUNT(*) FROM T_MASTER_PORTFOLIO_TICKER_HIST
                 WHERE C_MASTER_CODE='MGATE' AND C_CONFIRM_TIME='2026-04-02 09:00:00');
DECLARE @hGo INT=(SELECT COUNT(*) FROM T_MASTER_PORTFOLIO_TICKER_HIST
                  WHERE C_MASTER_CODE='MGATE' AND C_TICKER='GB' AND C_TARGET_WEIGHT=0);
DECLARE @hTong INT=(SELECT COUNT(*) FROM T_MASTER_PORTFOLIO_TICKER_HIST WHERE C_MASTER_CODE='MGATE');
IF @hF=2 AND @hGo=1 AND @hTong=5
   PRINT '  OK HIST delta: mốc gỡ có đúng 2 dòng (GA đổi + GB gỡ), tổng 5 dòng, có vết gỡ GB';
ELSE PRINT CONCAT('  !!! HIST delta: dong_moc_go=',@hF,' vet_go_GB=',@hGo,' tong=',@hTong);

DELETE FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MGATE';
DELETE FROM T_MASTER_PORTFOLIO_TICKER_HIST WHERE C_MASTER_CODE='MGATE';
DELETE FROM T_MASTER_PORTFOLIO_TICKER WHERE C_MASTER_CODE='MGATE';
DELETE FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE='MGATE';

PRINT '';
PRINT '======== RỔ ĐỔI GIỮA DẢI: THÊM mã + GỠ mã → recompute range phải KHỚP TỪNG PHIÊN ========';
-- Chuỗi index là NHÂN DỒN nên sai một phiên là lệch vĩnh viễn. Ca này ép đủ 3 chuyện xảy ra
--   TRONG cùng một dải recompute: thêm mã mới, gỡ mã cũ, và mã bị gỡ KHÔNG CÒN GIÁ (huỷ niêm yết).
-- Trọng số + tỷ lệ giá đều chọn NHỊ PHÂN CHÍNH XÁC (1/2, 1/4, 5/4, 3/2) để số kỳ vọng tính tay
--   khớp tuyệt đối, không phải "xấp xỉ" — sai 1 đồng là test đỏ.
--
--   Phiên   Rổ hiệu lực              FACTOR                                index (publish)
--   D1 01/6 RA .5  RB .5             .5(1.25)+.5(1.00)          = 1.125    1000    → 1125.00
--   D2 02/6 RA .5  RB .5             .5(1.00)+.5(1.50)          = 1.25     1125    → 1406.25
--   D3 03/6 RA .5  RB .25 RC .25 ★+  .5(1.00)+.25(1.00)+.25(1.5)= 1.125    1406.25 → 1582.03
--   D4 04/6 RA .5  RB .25 RC .25     .5(1.50)+.25(1.00)+.25(1.0)= 1.25     1582.03125 → 1977.54
--   D5 05/6 RA .5  RC .5        ★−   .5(1.00)+.5(1.25)          = 1.125    1977.5390625 → 2224.73
--   D6 08/6 RA .5  RC .5             .5(1.00)+.5(1.00)          = 1.0      → 2224.73
--   ★+ RC vào rổ từ D3 — CỐ Ý không có giá D1/D2 (chưa niêm yết): mã chưa vào rổ KHÔNG được đòi giá.
--   ★− RB gỡ khỏi rổ từ D5 — CỐ Ý không có giá D5/D6 (huỷ niêm yết): mã đã gỡ KHÔNG được đòi giá.
UPDATE T_MASTER_PORTFOLIO SET C_STATUS='CLOSED' WHERE C_MASTER_CODE='SDI01';   -- cô lập (all-or-nothing)
INSERT T_MASTER_PORTFOLIO (C_MASTER_CODE,C_MASTER_NAME,C_STATUS,C_INCEPTION_DATE,C_BENCHMARK_CODE)
VALUES ('MRB',N'Rebalance',  'ACTIVE','2026-06-01','VNINDEX');

-- ⚠️ RA giữ nguyên .5 suốt cả 3 mốc ⇒ CỐ Ý KHÔNG có dòng HIST ở 06-03 và 06-05 (đúng ngữ nghĩa log delta).
--    Đây là điều làm test BIẾT CẮN: nếu ai đó đổi iTVF sang PARTITION chỉ theo master (lỗi kinh điển với
--    log delta) thì RA sẽ BIẾN MẤT khỏi rổ từ D3 trở đi, Σw tụt còn .5 và index lệch ngay.
--    Seed mà mốc nào cũng ghi lại ĐỦ mọi mã thì hai cách cài đặt cho ra kết quả GIỐNG HỆT ⇒ test vô dụng.
INSERT T_MASTER_PORTFOLIO_TICKER_HIST (C_MASTER_CODE,C_TICKER,C_TARGET_WEIGHT,C_CONFIRM_TIME) VALUES
 ('MRB','RA',0.50,'2026-06-01 00:00:00'),('MRB','RB',0.50,'2026-06-01 00:00:00'),
                                         ('MRB','RB',0.25,'2026-06-03 00:00:00'),('MRB','RC',0.25,'2026-06-03 00:00:00'),
                                         ('MRB','RB',0.00,'2026-06-05 00:00:00'),('MRB','RC',0.50,'2026-06-05 00:00:00');
INSERT T_MASTER_PORTFOLIO_TICKER (C_MASTER_CODE,C_TICKER,C_TARGET_WEIGHT) VALUES ('MRB','RA',0.50),('MRB','RC',0.50);

INSERT T_PRICE_DAILY (C_TICKER,C_BUSINESS_DATE,C_REF_PRICE,C_CLOSE_PRICE) VALUES
 ('RA','2026-06-01',100.0000,125.0000),('RA','2026-06-02',125.0000,125.0000),('RA','2026-06-03',125.0000,125.0000),
 ('RA','2026-06-04',125.0000,187.5000),('RA','2026-06-05',187.5000,187.5000),('RA','2026-06-08',187.5000,187.5000),
 ('RB','2026-06-01',100.0000,100.0000),('RB','2026-06-02',100.0000,150.0000),('RB','2026-06-03',150.0000,150.0000),
 ('RB','2026-06-04',150.0000,150.0000),   -- HẾT: RB huỷ niêm yết sau khi bị gỡ
 ('RC','2026-06-03',100.0000,150.0000),   -- RC bắt đầu có giá ĐÚNG phiên nó vào rổ
 ('RC','2026-06-04',150.0000,150.0000),('RC','2026-06-05',150.0000,187.5000),('RC','2026-06-08',187.5000,187.5000);

DECLARE @ecR INT, @emR NVARCHAR(400);
EXEC SP_EOD_RECOMPUTE_INDEX_RANGE @p_from_date='2026-06-01', @p_to_date='2026-06-08',
     @p_err_code=@ecR OUTPUT, @p_err_msg=@emR OUTPUT;

DECLARE @r1 DECIMAL(18,2)=(SELECT C_INDEX_VALUE FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MRB' AND C_BUSINESS_DATE='2026-06-01');
DECLARE @r2 DECIMAL(18,2)=(SELECT C_INDEX_VALUE FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MRB' AND C_BUSINESS_DATE='2026-06-02');
DECLARE @r3 DECIMAL(18,2)=(SELECT C_INDEX_VALUE FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MRB' AND C_BUSINESS_DATE='2026-06-03');
DECLARE @r4 DECIMAL(18,2)=(SELECT C_INDEX_VALUE FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MRB' AND C_BUSINESS_DATE='2026-06-04');
DECLARE @r5 DECIMAL(18,2)=(SELECT C_INDEX_VALUE FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MRB' AND C_BUSINESS_DATE='2026-06-05');
DECLARE @r6 DECIMAL(18,2)=(SELECT C_INDEX_VALUE FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MRB' AND C_BUSINESS_DATE='2026-06-08');
DECLARE @rN INT=(SELECT COUNT(*) FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MRB');
IF @ecR=0 AND @rN=6 AND @r1=1125.00 AND @r2=1406.25 AND @r3=1582.03 AND @r4=1977.54 AND @r5=2224.73 AND @r6=2224.73
   PRINT '  OK thêm/gỡ mã giữa dải: 6 phiên khớp TỪNG SỐ (1125.00 1406.25 1582.03 1977.54 2224.73 2224.73)';
ELSE PRINT CONCAT('  !!! index sai: err=',@ecR,' #dong=',@rN,' | ',@r1,' ',@r2,' ',@r3,' ',@r4,' ',@r5,' ',@r6,' ',ISNULL(@emR,''));

-- (b) mã CHƯA vào rổ / ĐÃ gỡ khỏi rổ đều KHÔNG được có mặt trong rổ as-of
DECLARE @bD2 INT=(SELECT COUNT(*) FROM dbo.UDF_INDEX_BASKET_ASOF('2026-06-02') WHERE C_MASTER_CODE='MRB' AND C_TARGET_WEIGHT<>0);
DECLARE @bD4 INT=(SELECT COUNT(*) FROM dbo.UDF_INDEX_BASKET_ASOF('2026-06-04') WHERE C_MASTER_CODE='MRB' AND C_TARGET_WEIGHT<>0);
DECLARE @bD6 INT=(SELECT COUNT(*) FROM dbo.UDF_INDEX_BASKET_ASOF('2026-06-08') WHERE C_MASTER_CODE='MRB' AND C_TARGET_WEIGHT<>0);
DECLARE @bRB INT=(SELECT COUNT(*) FROM dbo.UDF_INDEX_BASKET_ASOF('2026-06-08') WHERE C_MASTER_CODE='MRB' AND C_TICKER='RB' AND C_TARGET_WEIGHT<>0);
IF @bD2=2 AND @bD4=3 AND @bD6=2 AND @bRB=0
   PRINT '  OK rổ as-of theo phiên: D2=2 mã · D4=3 mã (RC vào) · D6=2 mã (RB ra, không đòi giá)';
ELSE PRINT CONCAT('  !!! rổ as-of sai: D2=',@bD2,' D4=',@bD4,' D6=',@bD6,' RB_con_trong_ro=',@bRB);

-- (c) ★ CHẠY LẠI CẢ DẢI phải ra Y HỆT (idempotent — recompute không được cộng dồn thêm lần nữa)
SELECT C_BUSINESS_DATE, C_INDEX_VALUE_RAW, C_INDEX_VALUE, C_DAILY_RETURN
INTO #rb_lan1 FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MRB';
EXEC SP_EOD_RECOMPUTE_INDEX_RANGE @p_from_date='2026-06-01', @p_to_date='2026-06-08',
     @p_err_code=@ecR OUTPUT, @p_err_msg=@emR OUTPUT;
DECLARE @dif1 INT=(SELECT COUNT(*) FROM (
    SELECT C_BUSINESS_DATE,C_INDEX_VALUE_RAW,C_INDEX_VALUE,C_DAILY_RETURN FROM #rb_lan1
    EXCEPT SELECT C_BUSINESS_DATE,C_INDEX_VALUE_RAW,C_INDEX_VALUE,C_DAILY_RETURN
           FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MRB') x);
IF @ecR=0 AND @dif1=0 PRINT '  OK chạy lại CẢ DẢI lần 2 → giống hệt lần 1 (raw 12dp + publish + daily_return)';
ELSE PRINT CONCAT('  !!! chạy lại dải bị lệch: err=',@ecR,' #dong_khac=',@dif1);

-- (d) ★ CHẠY TỪNG NGÀY phải bằng CHẠY CẢ DẢI — bắt lỗi anchor/thứ tự trong vòng lặp range
DELETE FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MRB';
EXEC SP_EOD_SI_INDEX '2026-06-01'; EXEC SP_EOD_SI_INDEX '2026-06-02'; EXEC SP_EOD_SI_INDEX '2026-06-03';
EXEC SP_EOD_SI_INDEX '2026-06-04'; EXEC SP_EOD_SI_INDEX '2026-06-05'; EXEC SP_EOD_SI_INDEX '2026-06-08';
DECLARE @dif2 INT=(SELECT COUNT(*) FROM (
    SELECT C_BUSINESS_DATE,C_INDEX_VALUE_RAW,C_INDEX_VALUE,C_DAILY_RETURN FROM #rb_lan1
    EXCEPT SELECT C_BUSINESS_DATE,C_INDEX_VALUE_RAW,C_INDEX_VALUE,C_DAILY_RETURN
           FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MRB') x);
IF @dif2=0 PRINT '  OK chạy TỪNG NGÀY = chạy CẢ DẢI (không lệch anchor/thứ tự)';
ELSE PRINT CONCAT('  !!! từng ngày khác cả dải: #dong_khac=',@dif2);

-- (e) ★ CHẠY LẠI MỘT ĐOẠN GIỮA (D4..D6, đúng đoạn có sự kiện GỠ mã) phải không đổi số
EXEC SP_EOD_RECOMPUTE_INDEX_RANGE @p_from_date='2026-06-04', @p_to_date='2026-06-08',
     @p_err_code=@ecR OUTPUT, @p_err_msg=@emR OUTPUT;
DECLARE @dif3 INT=(SELECT COUNT(*) FROM (
    SELECT C_BUSINESS_DATE,C_INDEX_VALUE_RAW,C_INDEX_VALUE,C_DAILY_RETURN FROM #rb_lan1
    EXCEPT SELECT C_BUSINESS_DATE,C_INDEX_VALUE_RAW,C_INDEX_VALUE,C_DAILY_RETURN
           FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MRB') x);
IF @ecR=0 AND @dif3=0 PRINT '  OK chạy lại ĐOẠN GIỮA (D4..D6, có sự kiện gỡ mã) → số không đổi';
ELSE PRINT CONCAT('  !!! chạy lại đoạn giữa bị lệch: err=',@ecR,' #dong_khac=',@dif3);

DROP TABLE #rb_lan1;
DELETE FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MRB';
DELETE FROM T_MASTER_PORTFOLIO_TICKER_HIST WHERE C_MASTER_CODE='MRB';
DELETE FROM T_MASTER_PORTFOLIO_TICKER WHERE C_MASTER_CODE='MRB';
DELETE FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE='MRB';
DELETE FROM T_PRICE_DAILY WHERE C_TICKER IN ('RA','RB','RC');
UPDATE T_MASTER_PORTFOLIO SET C_STATUS='ACTIVE' WHERE C_MASTER_CODE='SDI01';
DELETE FROM T_PRICE_DAILY WHERE C_TICKER IN ('GA','GB');
UPDATE T_MASTER_PORTFOLIO SET C_STATUS='ACTIVE' WHERE C_MASTER_CODE='SDI01';

PRINT '';
PRINT '======== INDEX scope as-of: master PHÁT SINH SAU + rổ BACKDATE không được kéo về quá khứ ========';
-- Ca thật khi CHẠY LẠI LỊCH SỬ: FO lập master mới (inception 06/2026) nhưng khai weight hiệu lực từ 03/2026
--   (backdate — đầu quý/đầu chiến lược), và trong rổ có mã NIÊM YẾT SAU nên không thể có giá tháng 3.
--   Thiếu vị từ C_INCEPTION_DATE<=@d thì master mới lọt scope ngày 03/2026 → completeness THROW 51011 → vì
--   all-or-nothing, master CŨ hợp lệ cũng KHÔNG được tính lại ⇒ recompute-từ-inception bất khả thi.
UPDATE T_MASTER_PORTFOLIO SET C_STATUS='CLOSED' WHERE C_MASTER_CODE='SDI01';   -- cô lập (all-or-nothing)
INSERT T_MASTER_PORTFOLIO (C_MASTER_CODE,C_MASTER_NAME,C_STATUS,C_INCEPTION_DATE,C_BENCHMARK_CODE) VALUES
 ('MOLD',N'OldMaster','ACTIVE','2026-03-01','VNINDEX'),
 ('MNEW',N'NewMaster','ACTIVE','2026-06-01','VNINDEX');   -- ★ ra đời SAU dải recompute
INSERT T_MASTER_PORTFOLIO_TICKER_HIST (C_MASTER_CODE,C_TICKER,C_TARGET_WEIGHT,C_CONFIRM_TIME) VALUES
 ('MOLD','OLDA',1.0,'2026-03-01 09:00:00'),
 ('MNEW','NEWX',1.0,'2026-03-01 09:00:00');               -- ★ duyệt tỷ trọng TRƯỚC ngày master ra đời (backdate)
INSERT T_MASTER_PORTFOLIO_TICKER (C_MASTER_CODE,C_TICKER,C_TARGET_WEIGHT) VALUES
 ('MOLD','OLDA',1.0),('MNEW','NEWX',1.0);
INSERT T_PRICE_DAILY (C_TICKER,C_BUSINESS_DATE,C_REF_PRICE,C_CLOSE_PRICE) VALUES
 ('OLDA','2026-03-02',100,110),('OLDA','2026-03-03',110,110);
 -- NEWX CỐ Ý không có giá tháng 3 (niêm yết sau)
-- Rác từ bản CŨ: index ma của MNEW ở ngày nó chưa tồn tại → recompute phải DỌN (DELETE rộng hơn INSERT)
INSERT T_MASTER_INDEX_DAILY (C_BUSINESS_DATE,C_MASTER_CODE,C_INDEX_VALUE_RAW,C_INDEX_VALUE,C_DAILY_RETURN)
VALUES ('2026-03-02','MNEW',999.000000000000,999.00,NULL);
DECLARE @ecIN INT, @emIN NVARCHAR(400);
EXEC SP_EOD_RECOMPUTE_INDEX_RANGE @p_from_date='2026-03-02', @p_to_date='2026-03-03',
     @p_err_code=@ecIN OUTPUT, @p_err_msg=@emIN OUTPUT;
DECLARE @nOld INT=(SELECT COUNT(*) FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MOLD');
DECLARE @nNew INT=(SELECT COUNT(*) FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MNEW');
DECLARE @vOld DECIMAL(18,2)=(SELECT C_INDEX_VALUE FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MOLD' AND C_BUSINESS_DATE='2026-03-02');
IF @ecIN=0 AND @nOld=2 AND @nNew=0 AND @vOld=1100.00
   PRINT '  OK scope as-of inception: master CŨ tính đủ 2 phiên (1100.00), master MỚI 0 dòng, index MA đã bị dọn';
ELSE PRINT CONCAT('  !!! scope as-of inception SAI: err=',@ecIN,' #MOLD=',@nOld,' #MNEW=',@nNew,
                  ' MOLD@03-02=',ISNULL(CONVERT(VARCHAR(20),@vOld),'(null)'),' ',ISNULL(@emIN,''));
-- Ngày MNEW đã ra đời thì PHẢI vào scope trở lại (đủ giá) — guard không được biến thành "loại vĩnh viễn"
--   (OLDA cũng phải có giá @06-01: MOLD vẫn trong scope, mà completeness là all-or-nothing xuyên master)
INSERT T_PRICE_DAILY (C_TICKER,C_BUSINESS_DATE,C_REF_PRICE,C_CLOSE_PRICE) VALUES
 ('NEWX','2026-06-01',100,105),('OLDA','2026-06-01',110,110);
EXEC SP_EOD_RECOMPUTE_INDEX_RANGE @p_from_date='2026-06-01', @p_to_date='2026-06-01',
     @p_err_code=@ecIN OUTPUT, @p_err_msg=@emIN OUTPUT;
DECLARE @vNew DECIMAL(18,2)=(SELECT C_INDEX_VALUE FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MNEW' AND C_BUSINESS_DATE='2026-06-01');
IF @ecIN=0 AND @vNew=1050.00 PRINT '  OK từ ngày inception trở đi master MỚI vào scope bình thường (1050.00)';
ELSE PRINT CONCAT('  !!! master mới không vào scope sau inception: err=',@ecIN,' MNEW@06-01=',
                  ISNULL(CONVERT(VARCHAR(20),@vNew),'(null)'),' ',ISNULL(@emIN,''));
DELETE FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE IN ('MOLD','MNEW');
DELETE FROM T_MASTER_PORTFOLIO_TICKER_HIST WHERE C_MASTER_CODE IN ('MOLD','MNEW');
DELETE FROM T_MASTER_PORTFOLIO_TICKER WHERE C_MASTER_CODE IN ('MOLD','MNEW');
DELETE FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE IN ('MOLD','MNEW');
DELETE FROM T_PRICE_DAILY WHERE C_TICKER IN ('OLDA','NEWX');
UPDATE T_MASTER_PORTFOLIO SET C_STATUS='ACTIVE' WHERE C_MASTER_CODE='SDI01';

PRINT '';
PRINT '======== ASSET_NAV completeness: thiếu SI → SP_EOD_RUN err=12 ========';
INSERT T_SI_PORTFOLIO (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_JOIN_DATE,C_STATUS) VALUES ('SUB002','KH002','SDI01','2026-01-06','ACTIVE');
EXEC SP_EOD_RUN '2026-01-06',@p_err_code=@ecP OUTPUT,@p_err_msg=@emP OUTPUT;   -- SUB002 chưa có asset_daily @06
IF @ecP=12 PRINT '  OK thiếu Asset NAV cho SUB002 → err=12 (chặn EOD, không bỏ ngầm)'; ELSE PRINT CONCAT('  !!! asset completeness: ec=',@ecP);
DELETE FROM T_SI_PORTFOLIO WHERE C_SI_ACCOUNT='SUB002';

PRINT '';
PRINT '======== ASSET_NAV = CỜ (Kafka/Redis quyết đủ) + POST-CHECK err=12 mới là chốt chặn ========';
-- [KAFKA 2026-07-14] Proc KHÔNG còn tự đếm đủ/thiếu. Consumer (Redis: SADD si_account vs totalRow) quyết,
--   rồi gọi proc này để GHI CỜ. Đủ/thiếu so với REGISTRY SDI do SP_EOD_RUN lo (err=12 — test ngay trên).
DECLARE @ag1 INT, @ast1 VARCHAR(10), @agTot INT, @agRecv INT;
-- ★ Asset khai 999 record (gồm CẢ acc KHÔNG thuộc SDI) nhưng SDI chỉ nhận 1 SI → CŨ: kẹt PENDING vĩnh viễn
--   (received 1 < total 999) ⇒ EOD + chain KHÔNG BAO GIỜ chạy. NAY: READY, và TOTAL/RECEIVED chỉ để audit.
EXEC SP_EOD_SET_SOURCE_READY '2026-01-06','ASSET_NAV',999,NULL,@ag1 OUTPUT,@em OUTPUT;
SELECT @ast1=C_ASSET_NAV_STATUS, @agTot=C_ASSET_NAV_TOTAL, @agRecv=C_ASSET_NAV_RECEIVED
  FROM T_EOD_PIPELINE WHERE C_BUSINESS_DATE='2026-01-06';
IF @ag1=0 AND @ast1='READY' AND @agTot=999 AND @agRecv=1
   PRINT CONCAT('  OK ASSET_NAV = cờ: err=0 READY dù received(',@agRecv,') < total khai(',@agTot,') — acc lạ bị lọc là BÌNH THƯỜNG');
ELSE PRINT CONCAT('  !!! ASSET_NAV flag sai: err=',@ag1,' status=',@ast1,' total=',@agTot,' recv=',@agRecv);
-- ★★ Nhưng chốt chặn THẬT vẫn còn: thêm 1 SI ACTIVE chưa có dòng Asset → SP_EOD_RUN err=12 (post-check)
INSERT T_SI_PORTFOLIO (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_JOIN_DATE,C_STATUS) VALUES ('SUB003','KH003','SDI01','2026-01-06','ACTIVE');
DECLARE @agRun INT;
EXEC SP_EOD_RUN '2026-01-06',@p_err_code=@agRun OUTPUT,@p_err_msg=@em OUTPUT;
IF @agRun=12
   PRINT '  OK POST-CHECK: ASSET_NAV=READY nhưng thiếu SI của SDI → SP_EOD_RUN err=12, VẪN CHẶN EOD';
ELSE PRINT CONCAT('  !!! post-check KHÔNG chặn: err=',@agRun);
DELETE FROM T_SI_PORTFOLIO WHERE C_SI_ACCOUNT='SUB003';

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
INSERT T_MASTER_PORTFOLIO_TICKER_HIST (C_MASTER_CODE,C_TICKER,C_TARGET_WEIGHT,C_CONFIRM_TIME) VALUES ('MHOL','HAA',1.0,'2026-07-01 08:00:00');
INSERT T_MASTER_PORTFOLIO_TICKER (C_MASTER_CODE,C_TICKER,C_TARGET_WEIGHT) VALUES ('MHOL','HAA',1.0);
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
DELETE FROM T_MASTER_PORTFOLIO_TICKER_HIST WHERE C_MASTER_CODE='MHOL';
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
