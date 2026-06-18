/*==============================================================================
  SDI ENGINE — BENCHMARK SEED + RUN (đo execution time mấy job quan trọng)
  Chạy SAU 01_TABLES.sql + 02_SP_ENGINE.sql trên 1 DB sạch (xem db/bench.ps1).
  Seed dataset synthetic theo scale (sqlcmd vars) rồi EXEC SP_EOD_RUN 1 phiên.
  Duration per job đọc từ T_EOD_RUN (C_STARTED_AT/C_ENDED_AT) — bench.ps1 ghi CSV.

  sqlcmd vars (bench.ps1 truyền -v):
    NCUST  số khách hàng           (vd 50000)
    NSI    số strategy/SI mỗi KH   (vd 5)      → subaccount = NCUST × NSI
    NTICK  số mã mỗi SI            (vd 25)     → holdings   = NCUST × NSI × NTICK
    DT     business date           (vd 2026-01-02)
==============================================================================*/
SET NOCOUNT ON; SET QUOTED_IDENTIFIER ON; SET ANSI_NULLS ON;
DECLARE @nCust INT = $(NCUST), @nSi INT = $(NSI), @nTick INT = $(NTICK);
DECLARE @d DATE = '$(DT)';
DECLARE @price DECIMAL(18,4) = 10000, @qty DECIMAL(20,0) = 100;

PRINT CONCAT('BENCH seed: nCust=',@nCust,' nSi=',@nSi,' nTick=',@nTick,
             ' → subaccount=',@nCust*@nSi,' holdings=',@nCust*@nSi*@nTick,' @ ',CONVERT(VARCHAR,@d,23));

/*--- reset (TRUNCATE: schema KHÔNG có FK constraint nên an toàn, nhanh, no-log) ---*/
TRUNCATE TABLE T_EOD_WORK;             TRUNCATE TABLE T_UNIT_LEDGER;
TRUNCATE TABLE T_CUSTOMER_NAV_BALANCE;   TRUNCATE TABLE T_SI_NAV_BALANCE;
TRUNCATE TABLE T_CUSTOMER_FEE_INCOME;  TRUNCATE TABLE T_SI_INDEX_DAILY;
TRUNCATE TABLE T_SI_HOLDING_BALANCE;     TRUNCATE TABLE T_SI_NAV_CURRENT;
TRUNCATE TABLE T_EOD_RUN;              TRUNCATE TABLE T_INDEXING_PORTFOLIO_TICKER;
TRUNCATE TABLE T_CUSTOMER_NAV_CURRENT;       TRUNCATE TABLE T_CASHFLOW_EVENT;
TRUNCATE TABLE T_CUSTOMER_HOLDING_HIST; TRUNCATE TABLE T_FO_CASH_SYNC; TRUNCATE TABLE T_CUSTOMER_CASH_HIST;
TRUNCATE TABLE T_PRICE_DAILY;          TRUNCATE TABLE T_MASTER_PORTFOLIO_TICKER;
TRUNCATE TABLE T_INDEXING_PORTFOLIO;   TRUNCATE TABLE T_MASTER_PORTFOLIO;

/*--- tally số (đủ lớn để TOP @nCust): 10 × 10k × 10 = 1,000,000 ---*/
;WITH L0 AS (SELECT n FROM (VALUES(1),(1),(1),(1),(1),(1),(1),(1),(1),(1)) v(n)),
 NUMS AS (
    SELECT TOP (1000000) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
    FROM L0 a CROSS JOIN L0 b CROSS JOIN L0 c CROSS JOIN L0 d CROSS JOIN L0 e CROSS JOIN L0 f
 )
SELECT n INTO #cust FROM NUMS WHERE n <= @nCust;        -- KH 1..@nCust
CREATE UNIQUE CLUSTERED INDEX IX_cust ON #cust(n);

/*--- strategy (SI) 1..@nSi + mã + giá ---*/
INSERT INTO T_MASTER_PORTFOLIO (C_SI_CODE,C_SI_NAME,C_STATUS,C_INCEPTION_DATE,C_MGMT_FEE_RATE,C_BENCHMARK_CODE)
SELECT CONCAT('SDI',FORMAT(s.n,'00')), CONCAT(N'Bench SI ',s.n), 'ACTIVE', @d, 0.01, 'VNINDEX'
FROM (SELECT TOP (@nSi) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) n
      FROM sys.all_columns) s;

-- mã global TK0001..TK{nTick}, mọi SI dùng chung pool (trọng số đều = 1/nTick)
SELECT TOP (@nTick) CONCAT('TK',FORMAT(ROW_NUMBER() OVER (ORDER BY (SELECT NULL)),'0000')) AS tk
INTO #tk FROM sys.all_columns;

INSERT INTO T_MASTER_PORTFOLIO_TICKER (C_SI_CODE,C_EFFECTIVE_DATE,C_TICKER,C_TARGET_WEIGHT)
SELECT mp.C_SI_CODE, @d, t.tk, 1.0/@nTick
FROM T_MASTER_PORTFOLIO mp CROSS JOIN #tk t;

INSERT INTO T_PRICE_DAILY (C_TICKER,C_BUSINESS_DATE,C_CLOSE_PRICE)
SELECT tk, @d, @price FROM #tk;

/*--- subaccount (KH × SI) ---*/
INSERT INTO T_INDEXING_PORTFOLIO (C_CUST_CODE,C_SI_CODE,C_JOIN_DATE,C_STATUS,C_MGMT_FEE_RATE)
SELECT CONCAT('KH',FORMAT(c.n,'00000000')), mp.C_SI_CODE, @d, 'ACTIVE', 0.01
FROM #cust c CROSS JOIN T_MASTER_PORTFOLIO mp;

/*--- holdings: FO nạp THẲNG vào current (KH × SI × mã) = volume chính. J14b DIFF → interval hist trong EOD. ---*/
INSERT INTO T_INDEXING_PORTFOLIO_TICKER (C_CUST_CODE,C_SI_CODE,C_TICKER,C_QUANTITY,C_AVG_COST)
SELECT ip.C_CUST_CODE, ip.C_SI_CODE, mpt.C_TICKER, @qty, @price
FROM T_INDEXING_PORTFOLIO ip
JOIN T_MASTER_PORTFOLIO_TICKER mpt ON mpt.C_SI_CODE = ip.C_SI_CODE;

/*--- cash FO sync (=0) + cashflow INITIAL (= giá trị cp để init unit) ---*/
INSERT INTO T_FO_CASH_SYNC (C_BUSINESS_DATE,C_CUST_CODE,C_SI_CODE,C_CASH)
SELECT @d, C_CUST_CODE, C_SI_CODE, 0 FROM T_INDEXING_PORTFOLIO;

INSERT INTO T_CASHFLOW_EVENT (C_CUST_CODE,C_SI_CODE,C_BUSINESS_DATE,C_EVENT_TYPE,C_AMOUNT)
SELECT C_CUST_CODE, C_SI_CODE, @d, 'INITIAL', @nTick*@qty*@price FROM T_INDEXING_PORTFOLIO;

DROP TABLE #cust, #tk;

/*--- CHẠY EOD (đây là phần được đo qua T_EOD_RUN) ---*/
EXEC SP_EOD_RUN @d;

/*--- duration per job (bench.ps1 đọc kết quả này) ---*/
SELECT C_JOB AS job, DATEDIFF(MILLISECOND, C_STARTED_AT, C_ENDED_AT) AS ms, C_STATUS AS status, C_ROWS AS rows
FROM T_EOD_RUN WHERE C_BUSINESS_DATE=@d ORDER BY C_JOB;
