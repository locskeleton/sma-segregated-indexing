/*==============================================================================
  PM TOOL SMOKE — dataset 1 master × 3 KH × 3 phiên, số TÍNH TAY ĐƯỢC.
  Seed THẲNG state/daily tables (không qua EOD) để verify công thức serve-layer:
    AUM-weighted return/TE, deviation, cash drag, histogram, top-N, rebalance detail.

  Dữ liệu:
    Dates D1=2026-01-05, D2=01-06, D3=01-07. base(INCEPTION)=D1, end=D3.
    Master M1 index: 1000 → 1050 (r=.05) → 1071 (r=.02)  ⇒ R_master(D1→D3)=.071
    UP per KH:  S1 10000/10500/10800  S2 10000/10300/10600  S3 10000/11000/11200
    PnL(TWR D1→D3): S1=.08  S2=.06  S3=.12
    AUM(end): S1=100M S2=200M S3=100M  (Σ=400M)
    ⇒ AUM-weighted return = (.08·100+.06·200+.12·100)/400 = .08
       Deviation = (.08−.071)·10000 = 90 BPS
       per-KH dev: S1=90 S2=−110(<−100) S3=490(>100) ⇒ #devHi=1 #devLo=1
    TE per KH (STDEV(active) × √2):  S1≈.008571 S2≈.029126 S3≈.051818
       AUM-weighted TE = (…·100+…·200+…·100)/400 ≈ .029661 → badge MED; #TE>.05 = 1 (S3)
    Cash drag per KH (tien/aum): S1=10/100=.10(>.05) S2=4/200=.02 S3=2/100=.02 ⇒ #cashOver=1
    Net flow kỳ (D2,D3]: in=50M out=20M net=30M.  AUM base(D1)=370M → growth=400/370−1≈.081081
==============================================================================*/
SET QUOTED_IDENTIFIER ON;  -- bảng có filtered index → cần QI ON cho DML
SET ANSI_NULLS ON;
GO
SET NOCOUNT ON;

DELETE FROM T_MASTER_PM_CONFIG       WHERE C_MASTER_CODE='M1';
DELETE FROM T_SI_NAV_BALANCE         WHERE C_MASTER_CODE='M1';
DELETE FROM T_SI_NAV_CURRENT         WHERE C_MASTER_CODE='M1';
DELETE FROM T_SI_PORTFOLIO           WHERE C_MASTER_CODE='M1';
DELETE FROM T_MASTER_NAV_BALANCE     WHERE C_MASTER_CODE='M1';
DELETE FROM T_MASTER_NAV_CURRENT     WHERE C_MASTER_CODE='M1';
DELETE FROM T_MASTER_INDEX_DAILY     WHERE C_MASTER_CODE='M1';
DELETE FROM T_MASTER_HOLDING_BALANCE WHERE C_MASTER_CODE='M1';
DELETE FROM T_MASTER_PORTFOLIO_TICKER WHERE C_MASTER_CODE='M1';
DELETE FROM T_BENCHMARK_DAILY        WHERE C_BENCHMARK_CODE='BM';
DELETE FROM T_PRICE_DAILY            WHERE C_TICKER='AAA';
DELETE FROM T_TICKER_INDUSTRY        WHERE C_TICKER IN ('AAA','BBB','CCC');
DELETE FROM T_MASTER_PORTFOLIO       WHERE C_MASTER_CODE='M1';

DECLARE @D1 DATE='2026-01-05', @D2 DATE='2026-01-06', @D3 DATE='2026-01-07';

INSERT T_MASTER_PORTFOLIO (C_MASTER_CODE,C_MASTER_NAME,C_STATUS,C_INCEPTION_DATE,C_BENCHMARK_CODE)
VALUES ('M1',N'PM Smoke Master','ACTIVE',@D1,'BM');   -- KHÔNG khai fee config → không accrue

-- target weights: 2 mốc rebalance (D1, D3)
INSERT T_MASTER_PORTFOLIO_TICKER (C_MASTER_CODE,C_EFFECTIVE_DATE,C_TICKER,C_TARGET_WEIGHT) VALUES
 ('M1',@D1,'AAA',0.60),('M1',@D1,'BBB',0.40),
 ('M1',@D3,'AAA',0.50),('M1',@D3,'BBB',0.30),('M1',@D3,'CCC',0.20);

-- master holdings balance quanh rebalance D3 (phiên trước=D2)
INSERT T_MASTER_HOLDING_BALANCE (C_BUSINESS_DATE,C_MASTER_CODE,C_TICKER,C_QUANTITY,C_MARKET_PRICE,C_MARKET_VALUE,C_WEIGHT) VALUES
 (@D2,'M1','AAA',6000,40,240000,0.60),(@D2,'M1','BBB',8000,20,160000,0.40),
 (@D3,'M1','AAA',5000,40,200000,0.50),(@D3,'M1','BBB',6000,20,120000,0.30),(@D3,'M1','CCC',4000,20,80000,0.20);

INSERT T_MASTER_INDEX_DAILY (C_BUSINESS_DATE,C_MASTER_CODE,C_INDEX_VALUE,C_DAILY_RETURN) VALUES
 (@D1,'M1',1000.000000,NULL),(@D2,'M1',1050.000000,0.050000),(@D3,'M1',1071.000000,0.020000);

INSERT T_BENCHMARK_DAILY (C_BENCHMARK_CODE,C_BUSINESS_DATE,C_INDEX_VALUE) VALUES
 ('BM',@D1,800.0000),('BM',@D2,820.0000),('BM',@D3,835.0000);

-- master daily NAV (cash_in/out, total_account, total_asset)
INSERT T_MASTER_NAV_BALANCE
 (C_BUSINESS_DATE,C_MASTER_CODE,C_CASH,C_AUM,C_NAV,C_UNIT,C_UNIT_PRICE,C_DAILY_PNL,C_DAILY_RETURN,C_CASH_IN,C_CASH_OUT,C_TOTAL_ACCOUNT) VALUES
 (@D1,'M1',16000000,370000000,370000000,37000,10000,0,NULL,0,0,3),
 (@D2,'M1',16000000,390000000,390000000,37000,10541,20000000,0.050000,50000000,0,3),
 (@D3,'M1',16000000,400000000,400000000,37000,10752,10000000,0.020000,0,20000000,3);

-- master current snapshot (cash drag = 16/400 = .04)
INSERT T_MASTER_NAV_CURRENT
 (C_MASTER_CODE,C_CASH,C_AUM,C_LAST_NAV,C_UNIT,C_LAST_UNIT_PRICE,C_TOTAL_ACCOUNT,C_LAST_BUSINESS_DATE)
 VALUES ('M1',16000000,400000000,400000000,37000,10752,3,@D3);

-- sub-accounts
INSERT T_SI_PORTFOLIO (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_JOIN_DATE,C_STATUS) VALUES
 ('S1','C1','M1',@D1,'ACTIVE'),('S2','C2','M1',@D1,'ACTIVE'),('S3','C3','M1',@D1,'ACTIVE');

-- per-KH current (AUM end = LAST_NAV+payable[=0]; tien = cash+pending+div)
INSERT T_SI_NAV_CURRENT
 (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_UNIT,C_CASH,C_PAYABLE_FEE,C_LAST_NAV,C_LAST_UNIT_PRICE,C_STATUS,C_LAST_BUSINESS_DATE) VALUES
 ('S1','C1','M1',10000,10000000,0,100000000,10800,'ACTIVE',@D3),
 ('S2','C2','M1',20000, 4000000,0,200000000,10600,'ACTIVE',@D3),
 ('S3','C3','M1', 9000, 2000000,0,100000000,11200,'ACTIVE',@D3);

-- per-KH daily balance (unit_price + daily_return). D1 return NULL.
INSERT T_SI_NAV_BALANCE (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_NAV,C_PAYABLE_FEE,C_UNIT,C_UNIT_PRICE,C_DAILY_PNL,C_DAILY_RETURN) VALUES
 (@D1,'S1','C1','M1',100000000,0,10000,10000,0,NULL),
 (@D2,'S1','C1','M1',105000000,0,10000,10500,5000000,0.050000),
 (@D3,'S1','C1','M1',108000000,0,10000,10800,3000000,0.028571),
 (@D1,'S2','C2','M1',200000000,0,20000,10000,0,NULL),
 (@D2,'S2','C2','M1',206000000,0,20000,10300,6000000,0.030000),
 (@D3,'S2','C2','M1',212000000,0,20000,10600,6000000,0.029126),
 (@D1,'S3','C3','M1',100000000,0, 9000,10000,0,NULL),
 (@D2,'S3','C3','M1',110000000,0, 9000,11000,10000000,0.100000),
 (@D3,'S3','C3','M1',112000000,0, 9000,11200,2000000,0.018182);

-- T_PRICE_DAILY: cần cho UDF_PREV_BUSINESS_DATE (SP_EOD_TE_ACCUM lấy @prev). 1 mã dummy đủ các phiên.
INSERT T_PRICE_DAILY (C_TICKER,C_BUSINESS_DATE,C_REF_PRICE,C_CLOSE_PRICE) VALUES
 ('AAA',@D1,40,40),('AAA',@D2,40,40),('AAA',@D3,40,40);

-- Lũy kế active return (TE prefix-sum) qua EOD proc SP_EOD_TE_ACCUM — chạy tuần tự D1→D3 (test luôn EOD).
EXEC SP_EOD_TE_ACCUM @D1;
EXEC SP_EOD_TE_ACCUM @D2;
EXEC SP_EOD_TE_ACCUM @D3;
GO

DECLARE @ec INT, @em NVARCHAR(400);
PRINT '======== P1: SP_SET_MASTER_PM_CONFIG (set cash_drag=0.10, giữ còn lại default) ========';
EXEC SP_SET_MASTER_PM_CONFIG @p_master_code='M1', @p_cash_drag_threshold=0.10, @p_updated_by='smoke', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
PRINT '-- reset về toàn default cho các assert dưới (deviation A/B nay là tham số SP, default +100/-100) --';
EXEC SP_SET_MASTER_PM_CONFIG @p_master_code='M1', @p_updated_by='smoke', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;  -- all NULL → default

PRINT '';
PRINT '======== US2: SP_GET_MASTER_OVERVIEW (kỳ vọng: KH_RET=.08 MASTER_RET=.071 DEV=90 TE≈.029661 badge MED #TE>=1 #cash=1 #devHi=1 #devLo=1 growth≈.081081) ========';
EXEC SP_GET_MASTER_OVERVIEW @p_master_code='M1', @p_range='INCEPTION', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;

PRINT '';
PRINT '======== US3: SP_GET_MASTER_PERFORMANCE (3 đường; KH_COMPOSITE base≈1.0; RS2 rebalance D1,D3) ========';
EXEC SP_GET_MASTER_PERFORMANCE @p_master_code='M1', @p_range='INCEPTION', @p_resolution='D', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;

PRINT '';
PRINT '======== US3 click: SP_GET_MASTER_REBALANCE_DETAIL @D3 (weight CCC 0→.2; AAA .6→.5; qty delta) ========';
EXEC SP_GET_MASTER_REBALANCE_DETAIL @p_master_code='M1', @p_date='2026-01-07', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;

PRINT '';
PRINT '======== US4: SP_GET_MASTER_PNL_DIST (gain=3 loss=0 avg=.08 median=.08; hist 0..10%=2 10..20%=1) ========';
EXEC SP_GET_MASTER_PNL_DIST @p_master_code='M1', @p_range='INCEPTION', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;

PRINT '';
PRINT '======== US5: SP_GET_MASTER_TOP_KH DESC (S3 .12, S1 .08, S2 .06) ========';
EXEC SP_GET_MASTER_TOP_KH @p_master_code='M1', @p_range='INCEPTION', @p_topn=10, @p_dir='DESC', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
PRINT '-- ASC (S2 .06, S1 .08, S3 .12) --';
EXEC SP_GET_MASTER_TOP_KH @p_master_code='M1', @p_range='INCEPTION', @p_topn=10, @p_dir='ASC', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;

PRINT '';
PRINT '======== US1: SP_GET_PM_OVERVIEW_ALL (header #master>=1 #KH=3; RS3 list M1) ========';
EXEC SP_GET_PM_OVERVIEW_ALL @p_range='INCEPTION', @p_sort='AUM', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;

PRINT '';
PRINT '======== SP_GET_MASTER_ALERTS (phiên scratch D4: cố ý drift để có breach) ========';
-- D3 khớp target hoàn hảo → seed phiên D4 với holdings lệch để test alert.
-- Ngành: AAA,BBB=BANK ; CCC=TECH. Target hiệu lực @D4 = eff D3 (AAA .50 / BBB .30 / CCC .20).
-- Holdings @D4: AAA .62 / BBB .28 / CCC .10  → drift AAA .12, BBB .02, CCC .10 ; ngành BANK .90 TECH .10.
INSERT T_TICKER_INDUSTRY (C_TICKER,C_INDUSTRY_CODE,C_INDUSTRY_NAME) VALUES
 ('AAA','BANK',N'Ngân hàng'),('BBB','BANK',N'Ngân hàng'),('CCC','TECH',N'Công nghệ');
INSERT T_MASTER_HOLDING_BALANCE (C_BUSINESS_DATE,C_MASTER_CODE,C_TICKER,C_QUANTITY,C_MARKET_PRICE,C_MARKET_VALUE,C_WEIGHT) VALUES
 ('2026-01-08','M1','AAA',6200,40,248000,0.62),
 ('2026-01-08','M1','BBB',5600,20,112000,0.28),
 ('2026-01-08','M1','CCC',2000,20, 40000,0.10);
-- ngưỡng: symbol .45 → AAA(.62) vượt; drift .08 → AAA(.12)+CCC(.10) vượt; industry .70 → BANK(.90) vượt
EXEC SP_SET_MASTER_PM_CONFIG @p_master_code='M1', @p_symbol_weight_alert=0.45,
     @p_drift_threshold=0.08, @p_industry_weight_alert=0.70, @p_updated_by='smoke', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
PRINT '-- KỲ VỌNG RS1: #symbol=1 (AAA) #drift=2 (AAA,CCC) #industry=1 (BANK); err_code=0 --';
EXEC SP_GET_MASTER_ALERTS @p_master_code='M1', @p_date='2026-01-08', @p_user='smoke',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
PRINT '  err_code='+CAST(@ec AS VARCHAR(10))+' (kỳ vọng 0)';
PRINT '-- err path: master không tồn tại → err_code=1, KHÔNG result set --';
EXEC SP_GET_MASTER_ALERTS @p_master_code='NOPE', @p_user='smoke',
     @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
PRINT '  bad-master err_code='+CAST(@ec AS VARCHAR(10))+' (kỳ vọng 1) msg='+ISNULL(@em,'NULL');
-- cleanup scratch + reset config
DELETE FROM T_MASTER_HOLDING_BALANCE WHERE C_MASTER_CODE='M1' AND C_BUSINESS_DATE='2026-01-08';
EXEC SP_SET_MASTER_PM_CONFIG @p_master_code='M1', @p_updated_by='smoke', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
GO

PRINT '';
PRINT '======== BRD §3.8: SP_GET_MASTER_DEVIATION_DIST (dev per-KH: S1=90 S2=-110 S3=490 bps) ========';
PRINT '-- KỲ VỌNG RS1: #KH=3 dev_aumw=90 median=90 #>100(A)=1(S3) #<-100(B)=1(S2); RS2: "<-25"=1(S2) ">=25"=2(S1,S3) ---';
DECLARE @ec2 INT, @em2 NVARCHAR(400);
EXEC SP_GET_MASTER_DEVIATION_DIST @p_master_code='M1', @p_range='INCEPTION',
     @p_err_code=@ec2 OUTPUT, @p_err_msg=@em2 OUTPUT;
PRINT '  err_code='+CAST(@ec2 AS VARCHAR(10))+' (kỳ vọng 0)';
PRINT '-- override A=500/B=-200 → #>A=0 #<B=0 (deviation value khong doi) --';
EXEC SP_GET_MASTER_DEVIATION_DIST @p_master_code='M1', @p_range='INCEPTION',
     @p_dev_threshold_high=500, @p_dev_threshold_low=-200,
     @p_err_code=@ec2 OUTPUT, @p_err_msg=@em2 OUTPUT;
GO
