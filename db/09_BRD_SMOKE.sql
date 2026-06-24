/*==============================================================================
  BRD ASSET-SYNC SMOKE — luồng mới: Asset gửi NAV/tiền/phí số tổng per-SI; SDI ingest →
  derive unit/UP/PnL/return (init 10k, prior-day) → agg master → reconcile (đo vênh).
  Chạy sau 01+02+05+06. (03_SMOKE cũ = mô hình compute/fee cũ, cần rewrite riêng.)
==============================================================================*/
SET NOCOUNT ON; SET QUOTED_IDENTIFIER ON; SET ANSI_NULLS ON;
DECLARE @ec INT, @em NVARCHAR(400);
PRINT '======== BRD ASSET-SYNC SMOKE ========';

DELETE FROM T_EOD_WORK; DELETE FROM T_SI_NAV_BALANCE; DELETE FROM T_SI_NAV_CURRENT; DELETE FROM T_SI_UNIT_LEDGER;
DELETE FROM T_MASTER_NAV_BALANCE; DELETE FROM T_MASTER_NAV_CURRENT; DELETE FROM T_MASTER_INDEX_DAILY; DELETE FROM T_MASTER_HOLDING_BALANCE;
DELETE FROM T_SI_ASSET_DAILY; DELETE FROM T_SI_PORTFOLIO_HOLDING; DELETE FROM T_SI_HOLDING_HIST;
DELETE FROM T_SI_CASHFLOW_EVENT; DELETE FROM T_EOD_RUN; DELETE FROM T_EOD_PIPELINE; DELETE FROM T_EOD_RECON_BREAK;
DELETE FROM T_PRICE_DAILY; DELETE FROM T_MASTER_PORTFOLIO_TICKER; DELETE FROM T_SI_PORTFOLIO; DELETE FROM T_MASTER_PORTFOLIO;

INSERT T_MASTER_PORTFOLIO (C_MASTER_CODE,C_MASTER_NAME,C_STATUS,C_INCEPTION_DATE,C_BENCHMARK_CODE) VALUES ('SDI01',N'Demo','ACTIVE','2026-01-02','VNINDEX');
INSERT T_SI_PORTFOLIO (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_JOIN_DATE,C_STATUS) VALUES ('SUB001','KH001','SDI01','2026-01-02','ACTIVE');
INSERT T_MASTER_PORTFOLIO_TICKER (C_MASTER_CODE,C_EFFECTIVE_DATE,C_TICKER,C_TARGET_WEIGHT) VALUES ('SDI01','2026-01-02','AAA',0.6),('SDI01','2026-01-02','BBB',0.4);
INSERT T_PRICE_DAILY (C_TICKER,C_BUSINESS_DATE,C_REF_PRICE,C_CLOSE_PRICE) VALUES
 ('AAA','2026-01-02',100,100),('BBB','2026-01-02',50,50),
 ('AAA','2026-01-05',100,110),('BBB','2026-01-05',50,48);

/*--- DAY 02: nạp 10tr; holdings AAA60000/BBB80000 → stock 10tr; cash 0; fee 0. NAV=10tr, UP=10000 ---*/
INSERT T_SI_CASHFLOW_EVENT (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_BUSINESS_DATE,C_EVENT_TYPE,C_AMOUNT) VALUES ('SUB001','KH001','SDI01','2026-01-02','INITIAL',10000000);
EXEC SP_INGEST_CUSTOMER N'{"cust_code":"KH001","business_date":"2026-01-02","sub_accounts":[{"si_account":"SUB001","holdings":[{"ticker":"AAA","quantity":60000,"avg_cost":100},{"ticker":"BBB","quantity":80000,"avg_cost":50}]}]}';
EXEC SP_INGEST_ASSET_NAV N'[{"si_account":"SUB001","stock_value":10000000,"cash":0,"pending_cash":0,"div_cash":0,"fee_accum":0,"cash_in":10000000,"cash_out":0}]','2026-01-02',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
IF @ec<>0 PRINT CONCAT('  !!! ingest asset 02 ec=',@ec,' ',@em);
EXEC SP_EOD_SET_SOURCE_READY '2026-01-02','MKT_DATA',NULL,NULL,@ec OUTPUT,@em OUTPUT;
EXEC SP_EOD_RUN_INDEX '2026-01-02',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
EXEC SP_EOD_SET_SOURCE_READY '2026-01-02','ASSET_NAV',NULL,NULL,@ec OUTPUT,@em OUTPUT;
EXEC SP_EOD_SET_SOURCE_READY '2026-01-02','FO_INGEST',1,NULL,@ec OUTPUT,@em OUTPUT;
EXEC SP_EOD_RUN '2026-01-02',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
IF @ec<>0 PRINT CONCAT('  !!! EOD 02 ec=',@ec,' ',@em);
DECLARE @nav2 DECIMAL(20,0)=(SELECT C_NAV FROM T_SI_NAV_BALANCE WHERE C_SI_ACCOUNT='SUB001' AND C_BUSINESS_DATE='2026-01-02');
DECLARE @up2 DECIMAL(18,6)=(SELECT C_UNIT_PRICE FROM T_SI_NAV_BALANCE WHERE C_SI_ACCOUNT='SUB001' AND C_BUSINESS_DATE='2026-01-02');
DECLARE @un2 DECIMAL(18,6)=(SELECT C_UNIT FROM T_SI_NAV_BALANCE WHERE C_SI_ACCOUNT='SUB001' AND C_BUSINESS_DATE='2026-01-02');
IF @nav2=10000000 AND @up2=10000 AND @un2=1000 PRINT CONCAT('  OK day02: NAV=',@nav2,' UP=',@up2,' units=',@un2,' (init 10k)');
ELSE PRINT CONCAT('  !!! day02 sai: NAV=',@nav2,' UP=',@up2,' units=',@un2);

/*--- DAY 05: no flow; giá AAA110/BBB48 → stock=10,440,000; NAV=10,440,000; UP=10440; return=0.044 ---*/
EXEC SP_INGEST_CUSTOMER N'{"cust_code":"KH001","business_date":"2026-01-05","sub_accounts":[{"si_account":"SUB001","holdings":[{"ticker":"AAA","quantity":60000,"avg_cost":100},{"ticker":"BBB","quantity":80000,"avg_cost":50}]}]}';
EXEC SP_INGEST_ASSET_NAV N'[{"si_account":"SUB001","stock_value":10440000,"cash":0,"pending_cash":0,"div_cash":0,"fee_accum":0,"cash_in":0,"cash_out":0}]','2026-01-05',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
EXEC SP_EOD_SET_SOURCE_READY '2026-01-05','MKT_DATA',NULL,NULL,@ec OUTPUT,@em OUTPUT;
EXEC SP_EOD_RUN_INDEX '2026-01-05',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
EXEC SP_EOD_SET_SOURCE_READY '2026-01-05','ASSET_NAV',NULL,NULL,@ec OUTPUT,@em OUTPUT;
EXEC SP_EOD_SET_SOURCE_READY '2026-01-05','FO_INGEST',1,NULL,@ec OUTPUT,@em OUTPUT;
EXEC SP_EOD_RUN '2026-01-05',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
IF @ec<>0 PRINT CONCAT('  !!! EOD 05 ec=',@ec,' ',@em);
DECLARE @up5 DECIMAL(18,6)=(SELECT C_UNIT_PRICE FROM T_SI_NAV_BALANCE WHERE C_SI_ACCOUNT='SUB001' AND C_BUSINESS_DATE='2026-01-05');
DECLARE @ret5 DECIMAL(10,6)=(SELECT C_DAILY_RETURN FROM T_SI_NAV_BALANCE WHERE C_SI_ACCOUNT='SUB001' AND C_BUSINESS_DATE='2026-01-05');
IF @up5=10440 AND ABS(@ret5-0.044)<0.0001 PRINT CONCAT('  OK day05: UP=',@up5,' return=',@ret5,' (units giữ nguyên, NAV từ Asset)');
ELSE PRINT CONCAT('  !!! day05 sai: UP=',@up5,' return=',@ret5);

/*--- MASTER AGG: SUM per-SI → master NAV=10,440,000 @05 ---*/
DECLARE @mnav DECIMAL(20,0)=(SELECT C_NAV FROM T_MASTER_NAV_BALANCE WHERE C_MASTER_CODE='SDI01' AND C_BUSINESS_DATE='2026-01-05');
IF @mnav=10440000 PRINT CONCAT('  OK master agg @05: NAV=',@mnav); ELSE PRINT CONCAT('  !!! master agg sai: ',@mnav);

/*--- RECONCILE clean (cashflow SDI = Asset; holdings = stock_value) → 0 break @05 ---*/
DECLARE @brk INT=(SELECT COUNT(*) FROM T_EOD_RECON_BREAK WHERE C_BUSINESS_DATE='2026-01-05');
IF @brk=0 PRINT '  OK reconcile @05 sạch (2 nguồn khớp)'; ELSE PRINT CONCAT('  !!! reconcile @05 có break: ',@brk);

/*--- RECONCILE đo vênh: bơm Asset cash_in lệch SDI cashflow → CASHFLOW_MISMATCH + diff ---*/
EXEC SP_INGEST_ASSET_NAV N'[{"si_account":"SUB001","stock_value":10440000,"cash":0,"pending_cash":0,"div_cash":0,"fee_accum":0,"cash_in":500000,"cash_out":0}]','2026-01-05',@p_err_code=@ec OUTPUT,@p_err_msg=@em OUTPUT;
EXEC SP_EOD_RECONCILE '2026-01-05',@p_rows=@ec OUTPUT;
DECLARE @cfdiff DECIMAL(20,6)=(SELECT C_DIFF FROM T_EOD_RECON_BREAK WHERE C_BUSINESS_DATE='2026-01-05' AND C_CHECK_NAME='CASHFLOW_MISMATCH' AND C_SI_ACCOUNT='SUB001');
-- SDI cashflow @05 = 0; Asset cash_in−out = 500000 → diff = 0 − 500000 = −500000
IF @cfdiff=-500000 PRINT CONCAT('  OK reconcile đo vênh: CASHFLOW_MISMATCH diff=',@cfdiff,' (SDI 0 vs Asset 500000)');
ELSE PRINT CONCAT('  !!! cashflow reconcile sai: diff=',ISNULL(CAST(@cfdiff AS VARCHAR(30)),'(không có break)'));

PRINT '======== END BRD SMOKE ========';
