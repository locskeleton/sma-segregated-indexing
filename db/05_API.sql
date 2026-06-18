SET QUOTED_IDENTIFIER ON;  -- procs đọc bảng có filtered index (hist) → cần QI ON lúc CREATE PROC
SET ANSI_NULLS ON;
GO
/*==============================================================================
  SDI MODULE — READ API (SQL Server)  | ALL-IN-DB: mỗi API = app EXEC 1 proc
  6 proc đọc cho FR-01..FR-06 (SDI-spec §10). App chỉ serialize JSON, KHÔNG tính.
  Định danh public:
    - KH:  C_CUST_CODE VARCHAR(10).
    - SUB-ACCOUNT: C_SI_ACCOUNT VARCHAR(20) (mã sub-account, customer-level, = đơn vị API).
      Master suy từ sub-account (T_INDEXING_PORTFOLIO). Mọi bảng customer-level khóa theo C_SI_ACCOUNT.
  Read-only. T0 unit price = 10.000.
==============================================================================*/

/*---------------------------------------------- UDF: range filter → ngày cutoff */
CREATE OR ALTER FUNCTION UDF_RANGE_CUTOFF (@end DATE, @range VARCHAR(20))
RETURNS DATE
AS
BEGIN
    RETURN CASE UPPER(ISNULL(@range,'INCEPTION'))
        WHEN '1M'  THEN DATEADD(MONTH, -1, @end)
        WHEN '3M'  THEN DATEADD(MONTH, -3, @end)
        WHEN '6M'  THEN DATEADD(MONTH, -6, @end)
        WHEN '1Y'  THEN DATEADD(YEAR,  -1, @end)
        WHEN '3Y'  THEN DATEADD(YEAR,  -3, @end)
        WHEN 'YTD' THEN DATEFROMPARTS(YEAR(@end) - 1, 12, 31)
        ELSE NULL
    END;
END
GO

/*===========================================================================
  FR-01 — GET /customer/{id}/si-overview : tổng quan các sub-account của 1 KH
    RS1: breakdown từng sub-account (current NAV/unit_price + %return inception)
    RS2: tổng hợp toàn KH (Σ NAV, Σ cash, số sub-account)
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_SI_OVERVIEW
    @C_CUST_CODE VARCHAR(10)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT  ip.C_SI_ACCOUNT,         -- mã sub-account (đơn vị customer-level)
            ip.C_MASTER_CODE,        -- master KH đầu tư
            mp.C_SI_NAME,
            ip.C_STATUS,
            ip.C_JOIN_DATE,
            nc.C_UNIT,
            nc.C_CASH,
            nc.C_LAST_NAV,
            nc.C_LAST_UNIT_PRICE,
            nc.C_LAST_BUSINESS_DATE,
            CAST(nc.C_LAST_UNIT_PRICE / 10000.0 - 1 AS DECIMAL(10,6)) AS C_RETURN_INCEPTION
    FROM        T_INDEXING_PORTFOLIO   ip
    JOIN        T_MASTER_PORTFOLIO     mp ON mp.C_MASTER_CODE = ip.C_MASTER_CODE
    LEFT JOIN   T_CUSTOMER_NAV_CURRENT nc ON nc.C_SI_ACCOUNT = ip.C_SI_ACCOUNT
    WHERE  ip.C_CUST_CODE = @C_CUST_CODE
    ORDER BY nc.C_LAST_NAV DESC;

    SELECT  @C_CUST_CODE          AS C_CUST_CODE,
            COUNT(*)              AS C_SI_COUNT,
            SUM(nc.C_LAST_NAV)    AS C_TOTAL_NAV,
            SUM(nc.C_CASH)        AS C_TOTAL_CASH
    FROM        T_INDEXING_PORTFOLIO   ip
    LEFT JOIN   T_CUSTOMER_NAV_CURRENT nc ON nc.C_SI_ACCOUNT = ip.C_SI_ACCOUNT
    WHERE  ip.C_CUST_CODE = @C_CUST_CODE;
END
GO

/*===========================================================================
  FR-02 — GET /customer/{id}/si/{si_account} : chi tiết 1 sub-account
    Current NAV/unit + TWR (unit_price) + MWR (Modified Dietz) theo range.
    RS1: current + range metrics. RS2: dòng master-level mới nhất (tham chiếu).
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_SI_DETAIL
    @C_CUST_CODE  VARCHAR(10),
    @C_SI_ACCOUNT VARCHAR(20),
    @RANGE        VARCHAR(20) = 'INCEPTION'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @master VARCHAR(20) = (SELECT C_MASTER_CODE FROM T_INDEXING_PORTFOLIO
                                   WHERE C_SI_ACCOUNT=@C_SI_ACCOUNT AND C_CUST_CODE=@C_CUST_CODE);
    IF @master IS NULL BEGIN RAISERROR('Sub-account not found for cust/si_account',16,1); RETURN; END

    DECLARE @end DATE, @cutoff DATE, @base DATE;
    DECLARE @base_nav DECIMAL(20,0), @base_up DECIMAL(18,6),
            @end_nav  DECIMAL(20,0), @end_up  DECIMAL(18,6);

    SELECT @end = MAX(C_BUSINESS_DATE) FROM T_CUSTOMER_NAV_BALANCE WHERE C_SI_ACCOUNT=@C_SI_ACCOUNT;
    SET @cutoff = dbo.UDF_RANGE_CUTOFF(@end, @RANGE);
    SELECT @base = MAX(C_BUSINESS_DATE) FROM T_CUSTOMER_NAV_BALANCE
     WHERE C_SI_ACCOUNT=@C_SI_ACCOUNT AND C_BUSINESS_DATE <= @cutoff;
    IF @base IS NULL
        SELECT @base = MIN(C_BUSINESS_DATE) FROM T_CUSTOMER_NAV_BALANCE WHERE C_SI_ACCOUNT=@C_SI_ACCOUNT;

    SELECT @base_nav = C_NAV, @base_up = C_UNIT_PRICE FROM T_CUSTOMER_NAV_BALANCE
     WHERE C_SI_ACCOUNT=@C_SI_ACCOUNT AND C_BUSINESS_DATE = @base;
    SELECT @end_nav = C_NAV, @end_up = C_UNIT_PRICE FROM T_CUSTOMER_NAV_BALANCE
     WHERE C_SI_ACCOUNT=@C_SI_ACCOUNT AND C_BUSINESS_DATE = @end;

    -- MWR Modified Dietz: cần lịch phiên (T_PRICE_DAILY distinct date) cho trọng số w_i
    DECLARE @T INT, @cf_net DECIMAL(20,0) = 0, @weighted DECIMAL(18,6) = 0;
    SELECT @T = COUNT(*) FROM (SELECT DISTINCT C_BUSINESS_DATE FROM T_PRICE_DAILY
                               WHERE C_BUSINESS_DATE > @base AND C_BUSINESS_DATE <= @end) c;

    ;WITH cal AS (
        SELECT DISTINCT C_BUSINESS_DATE d FROM T_PRICE_DAILY
         WHERE C_BUSINESS_DATE > @base AND C_BUSINESS_DATE <= @end
    ), flows AS (
        SELECT C_BUSINESS_DATE bd,
               CASE WHEN C_EVENT_TYPE = 'WITHDRAW' THEN -C_AMOUNT ELSE C_AMOUNT END cf
        FROM T_CASHFLOW_EVENT
        WHERE C_SI_ACCOUNT=@C_SI_ACCOUNT AND C_BUSINESS_DATE > @base AND C_BUSINESS_DATE <= @end
    ), flow_w AS (
        SELECT f.cf, (SELECT COUNT(*) FROM cal WHERE cal.d <= f.bd) AS ti FROM flows f
    )
    SELECT @cf_net   = ISNULL(SUM(cf), 0),
           @weighted = ISNULL(SUM(cf * (@T - ti) * 1.0 / NULLIF(@T,0)), 0)
    FROM flow_w;

    DECLARE @denom DECIMAL(18,6) = @base_nav + @weighted;

    SELECT  @C_SI_ACCOUNT          AS C_SI_ACCOUNT,
            @master                AS C_MASTER_CODE,
            @RANGE                 AS C_RANGE,
            @base                  AS C_BASE_DATE,
            @end                   AS C_END_DATE,
            @base_nav              AS C_BASE_NAV,
            @base_up               AS C_BASE_UNIT_PRICE,
            @end_nav               AS C_END_NAV,
            @end_up                AS C_END_UNIT_PRICE,
            nc.C_LAST_NAV          AS C_CURRENT_NAV,
            nc.C_LAST_UNIT_PRICE   AS C_CURRENT_UNIT_PRICE,
            nc.C_UNIT              AS C_CURRENT_UNIT,
            nc.C_LAST_BUSINESS_DATE AS C_CURRENT_DATE,
            CASE WHEN @base_up IS NULL OR @base_up = 0 THEN NULL
                 ELSE CAST(@end_up / @base_up - 1 AS DECIMAL(10,6)) END AS C_TWR_PCT,
            (@end_nav - @base_nav - @cf_net) AS C_PNL_MONEY,
            @cf_net                AS C_CF_NET,
            CASE WHEN @T = 0 OR ABS(@denom) < 0.0001 THEN NULL
                 ELSE CAST((@end_nav - @base_nav - @cf_net) / @denom AS DECIMAL(10,6)) END AS C_MWR_PCT
    FROM T_CUSTOMER_NAV_CURRENT nc WHERE nc.C_SI_ACCOUNT=@C_SI_ACCOUNT;

    -- RS2: master-level mới nhất (đường "Hiệu suất master" tham chiếu)
    SELECT TOP 1 C_BUSINESS_DATE, C_NAV, C_UNIT_PRICE, C_DAILY_RETURN,
                 C_TOTAL_ASSET, C_CASH, C_STOCK_VALUE
    FROM T_MASTER_NAV_BALANCE WHERE C_MASTER_CODE = @master ORDER BY C_BUSINESS_DATE DESC;
END
GO

/*===========================================================================
  FR-03 — GET /customer/{id}/si/{si_account}/performance?range= : chart so sánh
    Chuỗi ngày [mốc..cuối]: unit_price sub-account (TWR) + master unit_price (TR)
    + master index (PR) + benchmark (PR). App rebase về dòng đầu.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_SI_PERFORMANCE
    @C_CUST_CODE  VARCHAR(10),
    @C_SI_ACCOUNT VARCHAR(20),
    @RANGE        VARCHAR(20) = '1Y'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @master VARCHAR(20) = (SELECT C_MASTER_CODE FROM T_INDEXING_PORTFOLIO
                                   WHERE C_SI_ACCOUNT=@C_SI_ACCOUNT AND C_CUST_CODE=@C_CUST_CODE);
    IF @master IS NULL BEGIN RAISERROR('Sub-account not found for cust/si_account',16,1); RETURN; END

    DECLARE @bench VARCHAR(20) = (SELECT C_BENCHMARK_CODE FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE = @master);
    DECLARE @end DATE, @cutoff DATE, @base DATE;

    SELECT @end = MAX(C_BUSINESS_DATE) FROM T_CUSTOMER_NAV_BALANCE WHERE C_SI_ACCOUNT=@C_SI_ACCOUNT;
    SET @cutoff = dbo.UDF_RANGE_CUTOFF(@end, @RANGE);
    SELECT @base = MAX(C_BUSINESS_DATE) FROM T_CUSTOMER_NAV_BALANCE
     WHERE C_SI_ACCOUNT=@C_SI_ACCOUNT AND C_BUSINESS_DATE <= @cutoff;
    IF @base IS NULL
        SELECT @base = MIN(C_BUSINESS_DATE) FROM T_CUSTOMER_NAV_BALANCE WHERE C_SI_ACCOUNT=@C_SI_ACCOUNT;

    SELECT  cd.C_BUSINESS_DATE,
            cd.C_UNIT_PRICE          AS C_CUST_UNIT_PRICE,   -- TWR sub-account
            sd.C_UNIT_PRICE          AS C_MASTER_UNIT_PRICE, -- TR master
            si.C_INDEX_VALUE         AS C_MASTER_INDEX,      -- PR danh mục mẫu
            bm.C_INDEX_VALUE         AS C_BENCHMARK          -- PR benchmark ngoài
    FROM        T_CUSTOMER_NAV_BALANCE cd
    LEFT JOIN   T_MASTER_NAV_BALANCE   sd ON sd.C_MASTER_CODE = @master AND sd.C_BUSINESS_DATE = cd.C_BUSINESS_DATE
    LEFT JOIN   T_MASTER_INDEX_DAILY   si ON si.C_MASTER_CODE = @master AND si.C_BUSINESS_DATE = cd.C_BUSINESS_DATE
    LEFT JOIN   T_BENCHMARK_DAILY      bm ON bm.C_BENCHMARK_CODE = @bench AND bm.C_BUSINESS_DATE = cd.C_BUSINESS_DATE
    WHERE  cd.C_SI_ACCOUNT=@C_SI_ACCOUNT AND cd.C_BUSINESS_DATE >= @base AND cd.C_BUSINESS_DATE <= @end
    ORDER BY cd.C_BUSINESS_DATE;
END
GO

/*===========================================================================
  FR-04 — GET /customer/{id}/si/{si_account}/info : thông tin đầu tư sub-account
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_SI_INFO
    @C_CUST_CODE  VARCHAR(10),
    @C_SI_ACCOUNT VARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT  ip.PK_INDEXING_PORTFOLIO AS C_SUBACCOUNT_PK_ID,
            ip.C_SI_ACCOUNT,
            mp.C_MASTER_CODE, mp.C_SI_NAME, mp.C_INCEPTION_DATE, mp.C_BENCHMARK_CODE,
            ip.C_CUST_CODE,
            ip.C_SUB_ACCOUNT_NO,
            ip.C_JOIN_DATE,
            ip.C_CLOSE_DATE,
            ip.C_STATUS,
            ip.C_INITIAL_AMOUNT,
            ip.C_SIP_AMOUNT,
            ip.C_SIP_SCHEDULE,
            ip.C_MIN_INVEST,
            COALESCE(ip.C_MGMT_FEE_RATE, mp.C_MGMT_FEE_RATE) AS C_MGMT_FEE_RATE_EFFECTIVE
    FROM       T_INDEXING_PORTFOLIO ip
    JOIN       T_MASTER_PORTFOLIO   mp ON mp.C_MASTER_CODE = ip.C_MASTER_CODE
    WHERE ip.C_CUST_CODE = @C_CUST_CODE AND ip.C_SI_ACCOUNT = @C_SI_ACCOUNT;
END
GO

/*===========================================================================
  FR-05 — GET /customer/{id}/si/{si_account}/holdings : holdings hiện tại top-N + "OTHER"
    Holdings CURRENT của sub-account (T_INDEXING_PORTFOLIO_TICKER) × giá mới nhất.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_SI_HOLDINGS
    @C_CUST_CODE  VARCHAR(10),
    @C_SI_ACCOUNT VARCHAR(20),
    @TOP          INT = 20
AS
BEGIN
    SET NOCOUNT ON;

    IF NOT EXISTS (SELECT 1 FROM T_INDEXING_PORTFOLIO WHERE C_SI_ACCOUNT=@C_SI_ACCOUNT AND C_CUST_CODE=@C_CUST_CODE)
        BEGIN RAISERROR('Sub-account not found for cust/si_account',16,1); RETURN; END

    DECLARE @pd DATE = (SELECT MAX(C_BUSINESS_DATE) FROM T_PRICE_DAILY);

    ;WITH h AS (
        SELECT t.C_TICKER, t.C_QUANTITY, p.C_CLOSE_PRICE,
               t.C_QUANTITY * p.C_CLOSE_PRICE AS mv
        FROM       T_INDEXING_PORTFOLIO_TICKER t
        JOIN       T_PRICE_DAILY p ON p.C_TICKER = t.C_TICKER AND p.C_BUSINESS_DATE = @pd
        WHERE  t.C_SI_ACCOUNT = @C_SI_ACCOUNT
    ), tot AS (SELECT SUM(mv) smv FROM h),
       ranked AS (SELECT h.*, ROW_NUMBER() OVER (ORDER BY mv DESC) rn FROM h)
    SELECT C_TICKER, C_QUANTITY, C_CLOSE_PRICE AS C_MARKET_PRICE, mv AS C_MARKET_VALUE,
           CAST(mv / NULLIF(smv,0) AS DECIMAL(12,8)) AS C_WEIGHT, 0 AS C_SORT
    FROM ranked CROSS JOIN tot WHERE rn <= @TOP
    UNION ALL
    SELECT N'OTHER', NULL, NULL, SUM(mv),
           CAST(SUM(mv) / NULLIF(MIN(smv),0) AS DECIMAL(12,8)), 1 AS C_SORT
    FROM ranked CROSS JOIN tot WHERE rn > @TOP
    HAVING COUNT(*) > 0
    ORDER BY C_SORT, C_MARKET_VALUE DESC;
END
GO

/*===========================================================================
  FR-06 — GET /customer/{id}/si/{si_account}/asset-report : báo cáo tài sản @ASOF
    Reconstruct INTERVAL: cash (cash_hist) + stock (holding_hist × giá ≤ asOf).
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_ASSET_REPORT
    @C_CUST_CODE  VARCHAR(10),
    @C_SI_ACCOUNT VARCHAR(20),
    @ASOF         DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @master VARCHAR(20) = (SELECT C_MASTER_CODE FROM T_INDEXING_PORTFOLIO
                                   WHERE C_SI_ACCOUNT=@C_SI_ACCOUNT AND C_CUST_CODE=@C_CUST_CODE);
    IF @master IS NULL BEGIN RAISERROR('Sub-account not found for cust/si_account',16,1); RETURN; END

    IF @ASOF IS NULL
        SELECT @ASOF = MAX(C_BUSINESS_DATE) FROM T_CUSTOMER_NAV_BALANCE WHERE C_SI_ACCOUNT=@C_SI_ACCOUNT;

    DECLARE @cash DECIMAL(20,0) = (
        SELECT C_CASH FROM T_CUSTOMER_CASH_HIST
        WHERE C_SI_ACCOUNT=@C_SI_ACCOUNT
          AND C_VALID_FROM <= @ASOF AND (C_VALID_TO > @ASOF OR C_VALID_TO IS NULL));

    DECLARE @stock DECIMAL(20,0) = (
        SELECT SUM(h.C_QUANTITY * px.C_CLOSE_PRICE)
        FROM T_CUSTOMER_HOLDING_HIST h
        OUTER APPLY (SELECT TOP 1 C_CLOSE_PRICE FROM T_PRICE_DAILY
                     WHERE C_TICKER = h.C_TICKER AND C_BUSINESS_DATE <= @ASOF
                     ORDER BY C_BUSINESS_DATE DESC) px
        WHERE h.C_SI_ACCOUNT=@C_SI_ACCOUNT
          AND h.C_VALID_FROM <= @ASOF AND (h.C_VALID_TO > @ASOF OR h.C_VALID_TO IS NULL));

    -- RS1: summary
    SELECT  @ASOF                  AS C_ASOF,
            @C_SI_ACCOUNT          AS C_SI_ACCOUNT,
            @master                AS C_MASTER_CODE,
            nd.C_NAV,
            nd.C_UNIT,
            nd.C_UNIT_PRICE,
            ISNULL(@cash, 0)       AS C_CASH,
            ISNULL(@stock, 0)      AS C_STOCK_VALUE,
            ISNULL(@cash,0) + ISNULL(@stock,0) AS C_TOTAL_ASSET,
            fi.C_CUM_DIVIDEND,
            fi.C_CUM_CUSTODY_FEE,
            fi.C_CUM_MGMT_FEE
    FROM (SELECT 1 x) z
    LEFT JOIN   T_CUSTOMER_NAV_BALANCE nd ON nd.C_SI_ACCOUNT=@C_SI_ACCOUNT AND nd.C_BUSINESS_DATE = @ASOF
    OUTER APPLY (
        SELECT  SUM(CASE WHEN C_TYPE = 'DIVIDEND'    THEN C_AMOUNT END) AS C_CUM_DIVIDEND,
                SUM(CASE WHEN C_TYPE = 'CUSTODY_FEE' THEN C_AMOUNT END) AS C_CUM_CUSTODY_FEE,
                SUM(CASE WHEN C_TYPE = 'MGMT_FEE'    THEN C_AMOUNT END) AS C_CUM_MGMT_FEE
        FROM T_CUSTOMER_FEE_INCOME
        WHERE C_SI_ACCOUNT=@C_SI_ACCOUNT AND C_BUSINESS_DATE <= @ASOF
    ) fi;

    -- RS2: holdings reconstruct @asOf
    SELECT  h.C_TICKER, h.C_QUANTITY,
            px.C_CLOSE_PRICE AS C_MARKET_PRICE,
            h.C_QUANTITY * px.C_CLOSE_PRICE AS C_MARKET_VALUE,
            CAST(h.C_QUANTITY * px.C_CLOSE_PRICE / NULLIF(@stock,0) AS DECIMAL(12,8)) AS C_WEIGHT,
            h.C_AVG_COST
    FROM T_CUSTOMER_HOLDING_HIST h
    OUTER APPLY (SELECT TOP 1 C_CLOSE_PRICE FROM T_PRICE_DAILY
                 WHERE C_TICKER = h.C_TICKER AND C_BUSINESS_DATE <= @ASOF
                 ORDER BY C_BUSINESS_DATE DESC) px
    WHERE h.C_SI_ACCOUNT=@C_SI_ACCOUNT
      AND h.C_VALID_FROM <= @ASOF AND (h.C_VALID_TO > @ASOF OR h.C_VALID_TO IS NULL)
    ORDER BY C_MARKET_VALUE DESC;

    -- RS3: chi tiết cổ tức/phí ≤ asOf (sparse)
    SELECT C_BUSINESS_DATE, C_TYPE, C_TICKER, C_AMOUNT, C_SOURCE
    FROM T_CUSTOMER_FEE_INCOME
    WHERE C_SI_ACCOUNT=@C_SI_ACCOUNT AND C_BUSINESS_DATE <= @ASOF
    ORDER BY C_BUSINESS_DATE DESC, C_TYPE;
END
GO
