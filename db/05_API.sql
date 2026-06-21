SET QUOTED_IDENTIFIER ON;  -- procs đọc bảng có filtered index (hist) → cần QI ON lúc CREATE PROC
SET ANSI_NULLS ON;
GO
/*==============================================================================
  SDI MODULE — READ API (SQL Server)  | ALL-IN-DB: mỗi API = app EXEC 1 proc
  6 proc đọc cho FR-01..FR-06 (SDI-spec §10). App chỉ serialize JSON, KHÔNG tính.
  Định danh public:
    - KH:  C_CUST_CODE VARCHAR(10) — CHỈ FR-01 (list các sub-account của 1 KH).
    - SUB-ACCOUNT: C_SI_ACCOUNT VARCHAR(20) (mã sub-account, customer-level, = đơn vị API).
      UNIQUE toàn cục ⇒ các proc per-si (FR-02..06) CHỈ nhận @p_si_account (KHÔNG cần cust).
      master suy từ DỮ LIỆU: FR-02/03/05/06 lấy từ T_SI_NAV_CURRENT (có sau FO ingest) — tiểu khoản
      chưa có data ⇒ not-found (không thuộc đường serve). FR-01 (list) + FR-04 (info) đọc T_SI_PORTFOLIO
      (endpoint registry: join_date/sub_account_no/initial_amount/sip...). Ownership/auth do tầng API gác.
  Read-only. T0 unit price = 10.000.
==============================================================================*/

/*---------------------------------------------- UDF: range filter → ngày cutoff */
CREATE OR ALTER FUNCTION UDF_RANGE_CUTOFF (@end DATE, @range VARCHAR(20))
RETURNS DATE
AS
BEGIN
    RETURN CASE UPPER(ISNULL(@range,'INCEPTION'))
        WHEN '1D'  THEN DATEADD(DAY,   -1, @end)
        WHEN '1W'  THEN DATEADD(DAY,   -7, @end)
        WHEN '1M'  THEN DATEADD(MONTH, -1, @end)
        WHEN '3M'  THEN DATEADD(MONTH, -3, @end)
        WHEN '3T'  THEN DATEADD(MONTH, -3, @end)   -- alias 3 tháng
        WHEN '6M'  THEN DATEADD(MONTH, -6, @end)
        WHEN '6T'  THEN DATEADD(MONTH, -6, @end)   -- alias 6 tháng
        WHEN '1Y'  THEN DATEADD(YEAR,  -1, @end)
        WHEN '3Y'  THEN DATEADD(YEAR,  -3, @end)
        WHEN 'MTD' THEN DATEADD(DAY, -1, DATEFROMPARTS(YEAR(@end), MONTH(@end), 1))         -- mốc = cuối tháng trước
        WHEN 'QTD' THEN DATEADD(DAY, -1, DATEFROMPARTS(YEAR(@end), (DATEPART(QUARTER,@end)-1)*3 + 1, 1)) -- cuối quý trước
        WHEN 'YTD' THEN DATEFROMPARTS(YEAR(@end) - 1, 12, 31)
        ELSE NULL  -- INCEPTION / INCEP / unknown → toàn kỳ
    END;
END
GO

/*===========================================================================
  FR-01 — GET /customer/{id}/si-overview : tổng quan các sub-account của 1 KH
    RS1: breakdown từng sub-account (current NAV/unit_price + %return inception)
    RS2: tổng hợp toàn KH (Σ NAV, Σ cash, số sub-account)
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_SI_OVERVIEW
    @p_cust_code VARCHAR(10),
    @p_user        VARCHAR(64)   = NULL,
    @p_err_code    INT           OUTPUT,
    @p_err_msg     NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @p_err_code = 0; SET @p_err_msg = NULL;
    BEGIN TRY

    SELECT  ip.C_SI_ACCOUNT,         -- mã sub-account (đơn vị customer-level)
            ip.C_MASTER_CODE,        -- master KH đầu tư
            mp.C_MASTER_NAME,
            ip.C_STATUS,
            ip.C_JOIN_DATE,
            nc.C_UNIT,
            nc.C_CASH,
            nc.C_LAST_NAV,
            nc.C_LAST_UNIT_PRICE,
            nc.C_LAST_BUSINESS_DATE,
            CAST(nc.C_LAST_UNIT_PRICE / 10000.0 - 1 AS DECIMAL(10,6)) AS C_RETURN_INCEPTION
    FROM        T_SI_PORTFOLIO   ip
    INNER JOIN        T_MASTER_PORTFOLIO     mp ON mp.C_MASTER_CODE = ip.C_MASTER_CODE
    LEFT JOIN   T_SI_NAV_CURRENT nc ON nc.C_SI_ACCOUNT = ip.C_SI_ACCOUNT
    WHERE  ip.C_CUST_CODE = @p_cust_code
    ORDER BY nc.C_LAST_NAV DESC;

    SELECT  @p_cust_code          AS C_CUST_CODE,
            COUNT(*)              AS C_SI_COUNT,
            SUM(nc.C_LAST_NAV)    AS C_TOTAL_NAV,
            SUM(nc.C_CASH)        AS C_TOTAL_CASH
    FROM        T_SI_PORTFOLIO   ip
    LEFT JOIN   T_SI_NAV_CURRENT nc ON nc.C_SI_ACCOUNT = ip.C_SI_ACCOUNT
    WHERE  ip.C_CUST_CODE = @p_cust_code;
    END TRY
    BEGIN CATCH
        IF @p_err_code = 0 BEGIN SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE(); END  -- lỗi runtime → OUT, KHÔNG THROW
    END CATCH
END
GO

/*===========================================================================
  FR-02 — GET /customer/{id}/si/{si_account} : chi tiết 1 sub-account
    Current NAV/unit + TWR (unit_price) + MWR (Modified Dietz) theo range.
    RS1: current + range metrics. RS2: dòng master-level mới nhất (tham chiếu).
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_SI_DETAIL
    @p_si_account VARCHAR(20),               -- si_account UNIQUE toàn cục → đủ định danh (master suy từ đây)
    @p_range        VARCHAR(20) = 'INCEPTION',
    @p_user        VARCHAR(64)   = NULL,
    @p_err_code    INT           OUTPUT,
    @p_err_msg     NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @p_err_code = 0; SET @p_err_msg = NULL;
    BEGIN TRY

    -- master + trạng thái current: 1 seek clustered PK T_SI_NAV_CURRENT (có sau FO ingest, đọc luôn cho RS1).
    -- KHÔNG lấy master từ T_SI_PORTFOLIO — tiểu khoản chưa có dữ liệu (chưa ingest/EOD) không thuộc đường
    -- serve thật (SMO đọc Asset post-EOD) ⇒ không cần handle case đó, NULL → not-found.
    DECLARE @master VARCHAR(20),
            @cur_nav DECIMAL(20,0), @cur_up DECIMAL(18,6), @cur_unit DECIMAL(18,6), @cur_date DATE;
    SELECT @master   = C_MASTER_CODE, @cur_nav  = C_LAST_NAV, @cur_up = C_LAST_UNIT_PRICE,
           @cur_unit = C_UNIT,        @cur_date = C_LAST_BUSINESS_DATE
    FROM T_SI_NAV_CURRENT WHERE C_SI_ACCOUNT=@p_si_account;
    IF @master IS NULL BEGIN SET @p_err_code = 1; SET @p_err_msg = N'Sub-account not found'; RAISERROR(@p_err_msg, 16, 1); END

    DECLARE @end DATE, @cutoff DATE, @base DATE;
    DECLARE @base_nav DECIMAL(20,0), @base_up DECIMAL(18,6),
            @end_nav  DECIMAL(20,0), @end_up  DECIMAL(18,6);

    -- mốc CUỐI kỳ + giá trị: 1 read (TOP 1 đuôi index IX_SI_NAV_BALANCE_ACCT (si,date) DESC) gộp date+nav+up.
    SELECT TOP 1 @end = C_BUSINESS_DATE, @end_nav = C_NAV, @end_up = C_UNIT_PRICE
    FROM T_SI_NAV_BALANCE WHERE C_SI_ACCOUNT=@p_si_account ORDER BY C_BUSINESS_DATE DESC;

    SET @cutoff = dbo.UDF_RANGE_CUTOFF(@end, @p_range);

    -- mốc ĐẦU kỳ + giá trị: 1 read (mốc ≤ cutoff gần nhất); fallback = mốc sớm nhất nếu range trùm cả lịch sử.
    SELECT TOP 1 @base = C_BUSINESS_DATE, @base_nav = C_NAV, @base_up = C_UNIT_PRICE
    FROM T_SI_NAV_BALANCE WHERE C_SI_ACCOUNT=@p_si_account AND C_BUSINESS_DATE <= @cutoff
    ORDER BY C_BUSINESS_DATE DESC;
    IF @base IS NULL
        SELECT TOP 1 @base = C_BUSINESS_DATE, @base_nav = C_NAV, @base_up = C_UNIT_PRICE
        FROM T_SI_NAV_BALANCE WHERE C_SI_ACCOUNT=@p_si_account ORDER BY C_BUSINESS_DATE ASC;

    -- MWR Modified Dietz: cần lịch phiên cho trọng số w_i. Lấy từ T_MASTER_INDEX_DAILY (1 dòng/master/phiên)
    -- thay vì T_PRICE_DAILY (date×TẤT CẢ mã) — cùng tập ngày GD nhưng ~250 dòng thay vì hàng triệu.
    DECLARE @T INT, @cf_net DECIMAL(20,0) = 0, @weighted DECIMAL(18,6) = 0;
    SELECT @T = COUNT(*) FROM T_MASTER_INDEX_DAILY
     WHERE C_MASTER_CODE=@master AND C_BUSINESS_DATE > @base AND C_BUSINESS_DATE <= @end;

    ;WITH cal AS (
        SELECT C_BUSINESS_DATE d FROM T_MASTER_INDEX_DAILY
         WHERE C_MASTER_CODE=@master AND C_BUSINESS_DATE > @base AND C_BUSINESS_DATE <= @end
    ), flows AS (
        SELECT C_BUSINESS_DATE bd,
               CASE WHEN C_EVENT_TYPE = 'WITHDRAW' THEN -C_AMOUNT ELSE C_AMOUNT END cf
        FROM T_SI_CASHFLOW_EVENT
        WHERE C_SI_ACCOUNT=@p_si_account AND C_BUSINESS_DATE > @base AND C_BUSINESS_DATE <= @end
    ), flow_w AS (
        SELECT f.cf, (SELECT COUNT(*) FROM cal WHERE cal.d <= f.bd) AS ti FROM flows f
    )
    SELECT @cf_net   = ISNULL(SUM(cf), 0),
           @weighted = ISNULL(SUM(cf * (@T - ti) * 1.0 / NULLIF(@T,0)), 0)
    FROM flow_w;

    DECLARE @denom DECIMAL(18,6) = @base_nav + @weighted;

    SELECT  @p_si_account          AS C_SI_ACCOUNT,
            @master                AS C_MASTER_CODE,
            @p_range                 AS C_RANGE,
            @base                  AS C_BASE_DATE,
            @end                   AS C_END_DATE,
            @base_nav              AS C_BASE_NAV,
            @base_up               AS C_BASE_UNIT_PRICE,
            @end_nav               AS C_END_NAV,
            @end_up                AS C_END_UNIT_PRICE,
            @cur_nav               AS C_CURRENT_NAV,
            @cur_up                AS C_CURRENT_UNIT_PRICE,
            @cur_unit              AS C_CURRENT_UNIT,
            @cur_date              AS C_CURRENT_DATE,
            CASE WHEN @base_up IS NULL OR @base_up = 0 THEN NULL
                 ELSE CAST(@end_up / @base_up - 1 AS DECIMAL(10,6)) END AS C_TWR_PCT,
            (@end_nav - @base_nav - @cf_net) AS C_PNL_MONEY,
            @cf_net                AS C_CF_NET,
            CASE WHEN @T = 0 OR ABS(@denom) < 0.0001 THEN NULL
                 ELSE CAST((@end_nav - @base_nav - @cf_net) / @denom AS DECIMAL(10,6)) END AS C_MWR_PCT;

    -- RS2: master-level mới nhất (đường "Hiệu suất master" tham chiếu)
    SELECT TOP 1 C_BUSINESS_DATE, C_NAV, C_UNIT_PRICE, C_DAILY_RETURN,
                 C_TOTAL_ASSET, C_CASH, C_STOCK_VALUE
    FROM T_MASTER_NAV_BALANCE WHERE C_MASTER_CODE = @master ORDER BY C_BUSINESS_DATE DESC;
    END TRY
    BEGIN CATCH
        IF @p_err_code = 0 BEGIN SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE(); END  -- lỗi runtime → OUT, KHÔNG THROW
    END CATCH
END
GO

/*===========================================================================
  FR-03 — GET /customer/{id}/si/{si_account}/performance?range= : chart so sánh
    Chuỗi ngày [mốc..cuối]: unit_price sub-account (TWR) + master unit_price (TR)
    + master index (PR) + benchmark (PR). App rebase về dòng đầu.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_SI_PERFORMANCE
    @p_si_account VARCHAR(20),
    @p_range        VARCHAR(20) = '1Y',
    @p_user        VARCHAR(64)   = NULL,
    @p_err_code    INT           OUTPUT,
    @p_err_msg     NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @p_err_code = 0; SET @p_err_msg = NULL;
    BEGIN TRY

    -- master suy từ T_SI_NAV_CURRENT (có sau FO ingest) — KHÔNG từ T_SI_PORTFOLIO; chưa có data → not-found.
    DECLARE @master VARCHAR(20) = (SELECT C_MASTER_CODE FROM T_SI_NAV_CURRENT WHERE C_SI_ACCOUNT=@p_si_account);
    IF @master IS NULL BEGIN SET @p_err_code = 1; SET @p_err_msg = N'Sub-account not found'; RAISERROR(@p_err_msg, 16, 1); END

    DECLARE @bench VARCHAR(20) = (SELECT C_BENCHMARK_CODE FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE = @master);
    DECLARE @end DATE, @cutoff DATE, @base DATE, @first DATE;

    -- khung ngày: 1 read gộp MAX(cuối)+MIN(đầu) → fallback dùng @first (bỏ read MIN lần 3).
    SELECT @end = MAX(C_BUSINESS_DATE), @first = MIN(C_BUSINESS_DATE)
    FROM T_SI_NAV_BALANCE WHERE C_SI_ACCOUNT=@p_si_account;
    SET @cutoff = dbo.UDF_RANGE_CUTOFF(@end, @p_range);
    SELECT @base = MAX(C_BUSINESS_DATE) FROM T_SI_NAV_BALANCE
     WHERE C_SI_ACCOUNT=@p_si_account AND C_BUSINESS_DATE <= @cutoff;
    IF @base IS NULL SET @base = @first;

    SELECT  cd.C_BUSINESS_DATE,
            cd.C_UNIT_PRICE          AS C_CUST_UNIT_PRICE,   -- TWR sub-account
            sd.C_UNIT_PRICE          AS C_MASTER_UNIT_PRICE, -- TR master
            si.C_INDEX_VALUE         AS C_MASTER_INDEX,      -- PR danh mục mẫu
            bm.C_INDEX_VALUE         AS C_BENCHMARK          -- PR benchmark ngoài
    FROM        T_SI_NAV_BALANCE cd
    LEFT JOIN   T_MASTER_NAV_BALANCE   sd ON sd.C_MASTER_CODE = @master AND sd.C_BUSINESS_DATE = cd.C_BUSINESS_DATE
    LEFT JOIN   T_MASTER_INDEX_DAILY   si ON si.C_MASTER_CODE = @master AND si.C_BUSINESS_DATE = cd.C_BUSINESS_DATE
    LEFT JOIN   T_BENCHMARK_DAILY      bm ON bm.C_BENCHMARK_CODE = @bench AND bm.C_BUSINESS_DATE = cd.C_BUSINESS_DATE
    WHERE  cd.C_SI_ACCOUNT=@p_si_account AND cd.C_BUSINESS_DATE >= @base AND cd.C_BUSINESS_DATE <= @end
    ORDER BY cd.C_BUSINESS_DATE;
    END TRY
    BEGIN CATCH
        IF @p_err_code = 0 BEGIN SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE(); END  -- lỗi runtime → OUT, KHÔNG THROW
    END CATCH
END
GO

/*===========================================================================
  FR-04 — GET /customer/{id}/si/{si_account}/info : thông tin đầu tư sub-account
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_SI_INFO
    @p_si_account VARCHAR(20),
    @p_user        VARCHAR(64)   = NULL,
    @p_err_code    INT           OUTPUT,
    @p_err_msg     NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @p_err_code = 0; SET @p_err_msg = NULL;
    BEGIN TRY

    IF NOT EXISTS (SELECT 1 FROM T_SI_PORTFOLIO WHERE C_SI_ACCOUNT=@p_si_account)
        BEGIN SET @p_err_code = 1; SET @p_err_msg = N'Sub-account not found'; RAISERROR(@p_err_msg, 16, 1); END

    SELECT  ip.PK_SI_PORTFOLIO AS C_SUBACCOUNT_PK_ID,
            ip.C_SI_ACCOUNT,
            mp.C_MASTER_CODE, mp.C_MASTER_NAME, mp.C_INCEPTION_DATE, mp.C_BENCHMARK_CODE,
            ip.C_CUST_CODE,
            ip.C_SUB_ACCOUNT_NO,
            ip.C_JOIN_DATE,
            ip.C_CLOSE_DATE,
            ip.C_STATUS,
            ip.C_INITIAL_AMOUNT,
            ip.C_SIP_AMOUNT,
            ip.C_SIP_SCHEDULE,
            ip.C_MIN_INVEST,
            fc.C_RATE AS C_MGMT_FEE_RATE_EFFECTIVE   -- rate phí QL hiệu lực từ T_FEE_CONFIG (master, type MGMT_FEE)
    FROM       T_SI_PORTFOLIO ip
    INNER JOIN       T_MASTER_PORTFOLIO   mp ON mp.C_MASTER_CODE = ip.C_MASTER_CODE
    LEFT  JOIN       T_FEE_CONFIG fc ON fc.C_MASTER_CODE = ip.C_MASTER_CODE AND fc.C_FEE_TYPE = 'MGMT_FEE'
    WHERE ip.C_SI_ACCOUNT = @p_si_account;
    END TRY
    BEGIN CATCH
        IF @p_err_code = 0 BEGIN SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE(); END  -- lỗi runtime → OUT, KHÔNG THROW
    END CATCH
END
GO

/*===========================================================================
  FR-05 — GET /customer/{id}/si/{si_account}/holdings : holdings hiện tại top-N + "OTHER"
    Holdings CURRENT của sub-account (T_SI_PORTFOLIO_HOLDING) × giá mới nhất ≤ @pd.
    Sản phẩm SEGREGATED: KH sở hữu cổ phiếu THẬT ⇒ báo cáo TUYỆT ĐỐI không được giấu
    vị thế nào. Định giá theo per-mã latest-price (giống FR-06), KHÔNG khớp đúng 1 ngày.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_SI_HOLDINGS
    @p_si_account VARCHAR(20),
    @p_top          INT = 20,
    @p_user        VARCHAR(64)   = NULL,
    @p_err_code    INT           OUTPUT,
    @p_err_msg     NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @p_err_code = 0; SET @p_err_msg = NULL;
    BEGIN TRY

    -- tồn tại theo DỮ LIỆU (T_SI_NAV_CURRENT, có sau ingest) — KHÔNG từ T_SI_PORTFOLIO; chưa có data → not-found.
    IF NOT EXISTS (SELECT 1 FROM T_SI_NAV_CURRENT WHERE C_SI_ACCOUNT=@p_si_account)
        BEGIN SET @p_err_code = 1; SET @p_err_msg = N'Sub-account not found'; RAISERROR(@p_err_msg, 16, 1); END

    -- @pd = ngày giá mới nhất TOÀN THỊ TRƯỜNG (mốc định giá "hiện tại"). Lấy MAX trên clustered
    --   (C_BUSINESS_DATE, C_TICKER) leading-date ⇒ seek dòng cuối, rẻ.
    DECLARE @pd DATE = (SELECT MAX(C_BUSINESS_DATE) FROM T_PRICE_DAILY);

    ;WITH h AS (
        -- Định giá MỖI holding bằng giá ĐÓNG CỬA MỚI NHẤT ≤ @pd CỦA CHÍNH MÃ ĐÓ (per-ticker), KHÔNG
        -- ép khớp đúng ngày @pd. Vì sao KHÔNG dùng INNER JOIN ... date=@pd:
        --   mã bị halt/đình chỉ GD/hủy niêm yết/feed thiếu giá ngày @pd ⇒ không có dòng giá @pd
        --   ⇒ INNER JOIN sẽ LOẠI mã đó ⇒ vị thế biến mất khỏi danh sách + khỏi mẫu số weight (tot.smv)
        --   ⇒ GIẤU cổ phiếu KH đang sở hữu (sai với sản phẩm segregated).
        -- OUTER APPLY giữ MỌI holding (vì là APPLY trái, holding không match vẫn ra 1 dòng px NULL):
        --   • mã có giá @pd            → px = giá @pd (giống hành vi cũ khi feed dày đặc).
        --   • mã halt/giá trễ hơn @pd  → px = giá last-known (phiên gần nhất ≤ @pd của mã).
        --   • mã CHƯA TỪNG có giá       → px.C_CLOSE_PRICE = NULL ⇒ mv = NULL (vẫn HIỆN, không giấu).
        -- Tối ưu cho per-customer ít mã: top-1/mã (KHÔNG dùng window-CTE quét toàn universe giá).
        SELECT t.C_TICKER, t.C_QUANTITY, px.C_CLOSE_PRICE,
               t.C_QUANTITY * px.C_CLOSE_PRICE AS mv
        FROM       T_SI_PORTFOLIO_HOLDING t
        OUTER APPLY (SELECT TOP 1 C_CLOSE_PRICE FROM T_PRICE_DAILY
                     WHERE C_TICKER = t.C_TICKER AND C_BUSINESS_DATE <= @pd
                     ORDER BY C_BUSINESS_DATE DESC) px
        WHERE  t.C_SI_ACCOUNT = @p_si_account
    ),
    -- tot.smv = Σ market value (SUM bỏ qua mv NULL) → mẫu số tính weight. Mã không định giá được
    --   (mv NULL) KHÔNG vào mẫu số nhưng VẪN được liệt kê (weight NULL) ở RS.
    tot AS (SELECT SUM(mv) smv FROM h),
    -- xếp hạng theo market value giảm dần để cắt top-N. mv NULL = thấp nhất ⇒ rơi xuống cuối/“OTHER”.
       ranked AS (SELECT h.*, ROW_NUMBER() OVER (ORDER BY mv DESC) rn FROM h)
    -- RS: top-N mã lớn nhất + 1 dòng "OTHER" gộp phần còn lại (nếu có). weight = mv / Σmv.
    SELECT C_TICKER, C_QUANTITY, C_CLOSE_PRICE AS C_MARKET_PRICE, mv AS C_MARKET_VALUE,
           CAST(mv / NULLIF(smv,0) AS DECIMAL(12,8)) AS C_WEIGHT, 0 AS C_SORT
    FROM ranked CROSS JOIN tot WHERE rn <= @p_top
    UNION ALL
    SELECT N'OTHER', NULL, NULL, SUM(mv),               -- gộp các mã ngoài top-N thành 1 dòng
           CAST(SUM(mv) / NULLIF(MIN(smv),0) AS DECIMAL(12,8)), 1 AS C_SORT
    FROM ranked CROSS JOIN tot WHERE rn > @p_top
    HAVING COUNT(*) > 0                                  -- không có mã ngoài top-N ⇒ bỏ dòng OTHER
    ORDER BY C_SORT, C_MARKET_VALUE DESC;
    END TRY
    BEGIN CATCH
        IF @p_err_code = 0 BEGIN SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE(); END  -- lỗi runtime → OUT, KHÔNG THROW
    END CATCH
END
GO

/*===========================================================================
  FR-06 — GET /customer/{id}/si/{si_account}/asset-report : báo cáo tài sản @p_asof
    Reconstruct INTERVAL: cash (cash_hist) + stock (holding_hist × giá ≤ asOf).
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_ASSET_REPORT
    @p_si_account VARCHAR(20),
    @p_asof         DATE = NULL,
    @p_user        VARCHAR(64)   = NULL,
    @p_err_code    INT           OUTPUT,
    @p_err_msg     NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @p_err_code = 0; SET @p_err_msg = NULL;
    BEGIN TRY

    -- master suy từ T_SI_NAV_CURRENT (có sau FO ingest) — KHÔNG từ T_SI_PORTFOLIO; chưa có data → not-found.
    DECLARE @master VARCHAR(20) = (SELECT C_MASTER_CODE FROM T_SI_NAV_CURRENT WHERE C_SI_ACCOUNT=@p_si_account);
    IF @master IS NULL BEGIN SET @p_err_code = 1; SET @p_err_msg = N'Sub-account not found'; RAISERROR(@p_err_msg, 16, 1); END

    IF @p_asof IS NULL
        SELECT @p_asof = MAX(C_BUSINESS_DATE) FROM T_SI_NAV_BALANCE WHERE C_SI_ACCOUNT=@p_si_account;

    DECLARE @cash DECIMAL(20,0) = (
        SELECT C_CASH FROM T_SI_CASH_HIST
        WHERE C_SI_ACCOUNT=@p_si_account
          AND C_VALID_FROM <= @p_asof AND (C_VALID_TO > @p_asof OR C_VALID_TO IS NULL));

    -- Holdings reconstruct @asof + giá ≤ asOf — MATERIALIZE 1 LẦN, dùng chung @stock (RS1) + RS2.
    -- OUTER APPLY top-1/mã KH giữ: tối ưu cho per-customer ít mã (KHÔNG window-CTE vì sẽ quét latest-price
    -- toàn universe). Trước đây reconstruct chạy 2 lần (@stock + RS2) → gộp còn 1.
    SELECT h.C_TICKER, h.C_QUANTITY, h.C_AVG_COST,
           px.C_CLOSE_PRICE AS C_MARKET_PRICE,
           h.C_QUANTITY * px.C_CLOSE_PRICE AS C_MARKET_VALUE
    INTO #hold
    FROM T_SI_HOLDING_HIST h
    OUTER APPLY (SELECT TOP 1 C_CLOSE_PRICE FROM T_PRICE_DAILY
                 WHERE C_TICKER = h.C_TICKER AND C_BUSINESS_DATE <= @p_asof
                 ORDER BY C_BUSINESS_DATE DESC) px
    WHERE h.C_SI_ACCOUNT=@p_si_account
      AND h.C_VALID_FROM <= @p_asof AND (h.C_VALID_TO > @p_asof OR h.C_VALID_TO IS NULL);
    DECLARE @stock DECIMAL(20,0) = (SELECT SUM(C_MARKET_VALUE) FROM #hold);

    -- RS1: summary
    --   Rollup theo NHÓM (fee_group): C_ACCUM_INCOME = Σ thu nhập (INCOME); C_ACCUM_FEE_PAYABLE = Σ phí ĐÃ phát sinh (PAYABLE, ledger).
    --   Chi tiết ĐÃ CẮT theo type: dividend/custody/mgmt_paid. C_FEE_ACCRUED_TOTAL = TỔNG phí phải trả ACCRUED chưa cắt (mọi loại, từ payable).
    SELECT  @p_asof                  AS C_ASOF,
            @p_si_account          AS C_SI_ACCOUNT,
            @master                AS C_MASTER_CODE,
            nd.C_NAV,
            nd.C_UNIT,
            nd.C_UNIT_PRICE,
            ISNULL(@cash, 0)       AS C_CASH,
            ISNULL(@stock, 0)      AS C_STOCK_VALUE,
            ISNULL(@cash,0) + ISNULL(@stock,0) AS C_TOTAL_ASSET,
            ISNULL(fi.C_ACCUM_INCOME, 0)       AS C_ACCUM_INCOME,        -- Σ theo group INCOME (tổng thu nhập)
            ISNULL(fi.C_ACCUM_FEE_PAYABLE, 0)  AS C_ACCUM_FEE_PAYABLE,   -- Σ theo group PAYABLE (tổng phí phải trả)
            fi.C_ACCUM_DIVIDEND,
            fi.C_ACCUM_CUSTODY_FEE,
            ISNULL(fi.C_MGMT_FEE_PAID, 0)  AS C_ACCUM_MGMT_FEE_PAID,
            ISNULL(nd.C_PAYABLE_FEE, 0)    AS C_FEE_ACCRUED_TOTAL   -- TỔNG phí phải trả accrued chưa cắt @asOf (mọi loại: mgmt+tax+...)
    FROM (SELECT 1 x) z
    LEFT JOIN   T_SI_NAV_BALANCE nd ON nd.C_SI_ACCOUNT=@p_si_account AND nd.C_BUSINESS_DATE = @p_asof
    OUTER APPLY (
        SELECT  SUM(CASE WHEN C_FEE_GROUP = 'INCOME'  THEN C_AMOUNT END) AS C_ACCUM_INCOME,       -- rollup nhóm
                SUM(CASE WHEN C_FEE_GROUP = 'PAYABLE' THEN C_AMOUNT END) AS C_ACCUM_FEE_PAYABLE,  -- rollup nhóm
                SUM(CASE WHEN C_FEE_TYPE  = 'DIVIDEND'    THEN C_AMOUNT END) AS C_ACCUM_DIVIDEND,
                SUM(CASE WHEN C_FEE_TYPE  = 'CUSTODY_FEE' THEN C_AMOUNT END) AS C_ACCUM_CUSTODY_FEE,
                SUM(CASE WHEN C_FEE_TYPE  = 'MGMT_FEE'    THEN C_AMOUNT END) AS C_MGMT_FEE_PAID  -- phí QL BO đã cắt thực
        FROM T_SI_FEE_LEDGER
        WHERE C_SI_ACCOUNT=@p_si_account AND C_BUSINESS_DATE <= @p_asof
    ) fi;

    -- RS2: holdings reconstruct @p_asof — đọc lại #hold (KHÔNG reconstruct lần 2)
    SELECT  C_TICKER, C_QUANTITY, C_MARKET_PRICE, C_MARKET_VALUE,
            CAST(C_MARKET_VALUE / NULLIF(@stock,0) AS DECIMAL(12,8)) AS C_WEIGHT,
            C_AVG_COST
    FROM #hold
    ORDER BY C_MARKET_VALUE DESC;

    -- RS3: chi tiết THU NHẬP ≤ asOf — group INCOME (DIVIDEND + loại thu nhập thêm sau). GROUP-based → KHÔNG sót loại mới.
    SELECT C_BUSINESS_DATE, C_FEE_TYPE, C_TICKER, C_AMOUNT, C_SOURCE
    FROM T_SI_FEE_LEDGER
    WHERE C_SI_ACCOUNT=@p_si_account AND C_BUSINESS_DATE <= @p_asof AND C_FEE_GROUP='INCOME'
    ORDER BY C_BUSINESS_DATE DESC, C_FEE_TYPE;

    -- RS4: chi tiết PHÍ PHẢI TRẢ ≤ asOf — group PAYABLE (CUSTODY_FEE + MGMT_FEE + loại phí thêm sau). GROUP-based.
    SELECT C_BUSINESS_DATE, C_FEE_TYPE, C_AMOUNT, C_SOURCE, C_SOURCE_EVENT_ID
    FROM T_SI_FEE_LEDGER
    WHERE C_SI_ACCOUNT=@p_si_account AND C_BUSINESS_DATE <= @p_asof AND C_FEE_GROUP='PAYABLE'
    ORDER BY C_BUSINESS_DATE DESC, C_FEE_TYPE;

    -- RS5: KÊ TỪNG KHOẢN PHẢI TRẢ accrued chưa cắt @asOf (cho sao kê NAV). Per loại ACCRUE:
    --   pending = Σ accrue(loại,≤asOf, T_SI_FEE_ACCRUAL) − Σ cắt(loại,≤asOf, T_SI_FEE_LEDGER). Σ pending = C_FEE_ACCRUED_TOTAL.
    SELECT a.C_FEE_TYPE,
           CAST(a.C_ACCRUED AS DECIMAL(20,4))                     AS C_FEE_ACCRUED,
           CAST(ISNULL(c.C_PAID,0) AS DECIMAL(20,4))              AS C_FEE_PAID,
           CAST(a.C_ACCRUED - ISNULL(c.C_PAID,0) AS DECIMAL(20,4)) AS C_FEE_PENDING
    FROM (SELECT C_FEE_TYPE, SUM(C_ACCRUAL_AMOUNT) AS C_ACCRUED
          FROM T_SI_FEE_ACCRUAL WHERE C_SI_ACCOUNT=@p_si_account AND C_BUSINESS_DATE <= @p_asof
          GROUP BY C_FEE_TYPE) a
    LEFT JOIN (SELECT C_FEE_TYPE, CAST(SUM(C_AMOUNT) AS DECIMAL(20,6)) AS C_PAID  -- CAST tránh SUM→(38,0) cắt scale khi trừ (bẫy DECIMAL-38)
               FROM T_SI_FEE_LEDGER WHERE C_SI_ACCOUNT=@p_si_account AND C_FEE_GROUP='PAYABLE' AND C_BUSINESS_DATE <= @p_asof
               GROUP BY C_FEE_TYPE) c ON c.C_FEE_TYPE = a.C_FEE_TYPE
    ORDER BY a.C_FEE_TYPE;

    DROP TABLE #hold;
    END TRY
    BEGIN CATCH
        IF @p_err_code = 0 BEGIN SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE(); END  -- lỗi runtime → OUT, KHÔNG THROW
        IF OBJECT_ID('tempdb..#hold') IS NOT NULL DROP TABLE #hold;
    END CATCH
END
GO

/*===========================================================================
  SDI → ASSET SYNC PRODUCER (Kafka, "SP build JSON, app publish")
  SP_GET_ASSET_SNAPSHOT : build payload tài sản per sub-account cho 1 NGÀY → app đọc
    result set, publish từng C_PAYLOAD_JSON lên Kafka (key = C_SI_ACCOUNT) cho Asset.
    SMO đọc từ Asset (KHÔNG gọi SDI). Xem memory sdi-asset-sync-architecture.

  MODE: 'EOD' (snapshot ngày hôm nay) | 'HISTORY' (đẩy LẠI ngày quá khứ). 2 mode DÙNG CHUNG
    code & payload — chỉ khác @p_business_date. RECONSTRUCT-ONLY từ bảng DATED
    (T_SI_NAV_BALANCE + T_SI_CASH_HIST + T_SI_HOLDING_HIST×giá + T_SI_FEE_*) ⇒ EOD và replay
    cùng ngày cho RA PAYLOAD Y HỆT. TUYỆT ĐỐI KHÔNG đọc *_CURRENT (đổi mỗi ngày → replay sai).

  Driver = các sub-account có dòng NAV_BALANCE @ngày (đã chốt EOD ngày đó).
  total_asset = NAV + payable (authoritative). cash = TIỀN MẶT (cash_hist; pending/div KHÔNG
    có lịch sử per-ngày → KHÔNG đưa vào payload, tránh emit field không reproduce được khi replay).
  ⚠️ PAYLOAD DRAFT (tên field tự đặt) — map lại theo schema Asset thật khi có.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_ASSET_SNAPSHOT
    @p_business_date DATE,
    @p_mode          VARCHAR(10)   = 'EOD',
    @p_user          VARCHAR(64)   = NULL,
    @p_err_code      INT           OUTPUT,
    @p_err_msg       NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @p_err_code = 0; SET @p_err_msg = NULL;
    BEGIN TRY
    IF @p_mode NOT IN ('EOD','HISTORY')
        BEGIN SET @p_err_code=2; SET @p_err_msg=N'@p_mode phải EOD hoặc HISTORY'; RAISERROR(@p_err_msg, 16, 1); END
    IF NOT EXISTS (SELECT 1 FROM T_SI_NAV_BALANCE WHERE C_BUSINESS_DATE=@p_business_date)
        BEGIN SET @p_err_code=3; SET @p_err_msg=N'Không có dữ liệu EOD cho ngày '+CONVERT(VARCHAR(10),@p_business_date,23); RAISERROR(@p_err_msg, 16, 1); END

    -- Holdings reconstruct @ngày (per-mã giá mới nhất ≤ ngày — resilient halt, giống FR-05/06)
    SELECT b.C_SI_ACCOUNT, h.C_TICKER, h.C_QUANTITY,
           px.C_CLOSE_PRICE AS C_PRICE,
           CAST(h.C_QUANTITY * px.C_CLOSE_PRICE AS DECIMAL(38,4)) AS C_MV
    INTO #hold
    FROM       (SELECT DISTINCT C_SI_ACCOUNT FROM T_SI_NAV_BALANCE WHERE C_BUSINESS_DATE=@p_business_date) b
    INNER JOIN T_SI_HOLDING_HIST h ON h.C_SI_ACCOUNT=b.C_SI_ACCOUNT
           AND h.C_VALID_FROM <= @p_business_date AND (h.C_VALID_TO > @p_business_date OR h.C_VALID_TO IS NULL)
    OUTER APPLY (SELECT TOP 1 C_CLOSE_PRICE FROM T_PRICE_DAILY
                 WHERE C_TICKER=h.C_TICKER AND C_BUSINESS_DATE<=@p_business_date
                 ORDER BY C_BUSINESS_DATE DESC) px;
    CREATE CLUSTERED INDEX IX_hold ON #hold (C_SI_ACCOUNT);

    -- BREAKDOWN phí phải trả per-type @ngày (cho sao kê kê từng khoản): accrued − đã cắt = pending.
    -- RECONSTRUCT-ONLY từ bảng DATED (T_SI_FEE_ACCRUAL + T_SI_FEE_LEDGER ≤ ngày) → replay y hệt. Driver = loại
    -- ACCRUE (custody point-event không accrue → không vào breakdown). Σ pending = payable_fee (tổng).
    SELECT a.C_SI_ACCOUNT, a.C_FEE_TYPE, a.accrued,
           ISNULL(c.paid,0) AS paid, a.accrued - ISNULL(c.paid,0) AS pending
    INTO #feebreak
    FROM (SELECT C_SI_ACCOUNT, C_FEE_TYPE, SUM(C_ACCRUAL_AMOUNT) AS accrued
          FROM T_SI_FEE_ACCRUAL
          WHERE C_BUSINESS_DATE <= @p_business_date
            AND C_SI_ACCOUNT IN (SELECT C_SI_ACCOUNT FROM T_SI_NAV_BALANCE WHERE C_BUSINESS_DATE=@p_business_date)
          GROUP BY C_SI_ACCOUNT, C_FEE_TYPE) a
    LEFT JOIN (SELECT C_SI_ACCOUNT, C_FEE_TYPE, CAST(SUM(C_AMOUNT) AS DECIMAL(20,6)) AS paid  -- CAST tránh bẫy DECIMAL-38
               FROM T_SI_FEE_LEDGER WHERE C_FEE_GROUP='PAYABLE' AND C_BUSINESS_DATE <= @p_business_date
               GROUP BY C_SI_ACCOUNT, C_FEE_TYPE) c ON c.C_SI_ACCOUNT=a.C_SI_ACCOUNT AND c.C_FEE_TYPE=a.C_FEE_TYPE;
    CREATE CLUSTERED INDEX IX_fb ON #feebreak (C_SI_ACCOUNT);

    -- 1 dòng payload JSON / sub-account
    SELECT b.C_SI_ACCOUNT, b.C_BUSINESS_DATE,
        (SELECT
            b.C_CUST_CODE                                   AS cust_code,
            b.C_SI_ACCOUNT                                  AS si_account,
            b.C_MASTER_CODE                                 AS master_code,
            CONVERT(VARCHAR(10), b.C_BUSINESS_DATE, 23)     AS business_date,
            @p_mode                                         AS mode,
            b.C_NAV                                          AS nav,
            b.C_PAYABLE_FEE                                  AS payable_fee,
            (b.C_NAV + b.C_PAYABLE_FEE)                      AS total_asset,   -- authoritative (gồm receivables)
            b.C_UNIT                                         AS unit,
            b.C_UNIT_PRICE                                   AS unit_price,
            b.C_DAILY_PNL                                    AS daily_pnl,
            b.C_DAILY_RETURN                                 AS daily_return,
            ISNULL(ch.C_CASH, 0)                            AS cash,          -- tiền mặt (pending/div: không có hist → bỏ)
            ISNULL(st.stock_value, 0)                       AS stock_value,
            ISNULL(fi.div, 0)                               AS accum_dividend,
            ISNULL(fi.cust, 0)                              AS accum_custody_fee,
            ISNULL(fi.paid, 0)                             AS mgmt_fee_paid,
            -- kê TỪNG khoản phải trả accrued chưa cắt per-type (Σ pending = payable_fee tổng ở trên)
            JSON_QUERY(ISNULL((SELECT fb.C_FEE_TYPE AS fee_type, fb.accrued, fb.paid, fb.pending
                               FROM #feebreak fb WHERE fb.C_SI_ACCOUNT=b.C_SI_ACCOUNT
                               FOR JSON PATH), N'[]'))      AS payable_breakdown,
            JSON_QUERY(ISNULL((SELECT hh.C_TICKER AS ticker, hh.C_QUANTITY AS qty,
                                      hh.C_PRICE AS price, hh.C_MV AS market_value
                               FROM #hold hh WHERE hh.C_SI_ACCOUNT=b.C_SI_ACCOUNT
                               FOR JSON PATH), N'[]'))      AS holdings
         FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)              AS C_PAYLOAD_JSON
    FROM T_SI_NAV_BALANCE b
    LEFT JOIN T_SI_CASH_HIST ch ON ch.C_SI_ACCOUNT=b.C_SI_ACCOUNT
           AND ch.C_VALID_FROM <= @p_business_date AND (ch.C_VALID_TO > @p_business_date OR ch.C_VALID_TO IS NULL)
    OUTER APPLY (SELECT SUM(C_MV) AS stock_value FROM #hold WHERE C_SI_ACCOUNT=b.C_SI_ACCOUNT) st
    OUTER APPLY (SELECT SUM(CASE WHEN C_FEE_TYPE='DIVIDEND'    THEN C_AMOUNT END) AS div,
                        SUM(CASE WHEN C_FEE_TYPE='CUSTODY_FEE' THEN C_AMOUNT END) AS cust,
                        SUM(CASE WHEN C_FEE_TYPE='MGMT_FEE'    THEN C_AMOUNT END) AS paid
                 FROM T_SI_FEE_LEDGER WHERE C_SI_ACCOUNT=b.C_SI_ACCOUNT AND C_BUSINESS_DATE<=@p_business_date) fi
    WHERE b.C_BUSINESS_DATE=@p_business_date
    ORDER BY b.C_SI_ACCOUNT;

    DROP TABLE #hold; DROP TABLE #feebreak;
    END TRY
    BEGIN CATCH
        IF @p_err_code = 0 BEGIN SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE(); END  -- lỗi runtime → OUT, KHÔNG THROW
        IF OBJECT_ID('tempdb..#hold') IS NOT NULL DROP TABLE #hold;
        IF OBJECT_ID('tempdb..#feebreak') IS NOT NULL DROP TABLE #feebreak;
    END CATCH
END
GO

/*===========================================================================
  SP_GET_ASSET_MASTER_SNAPSHOT (SDI→Asset sync, luồng 8b/8c) — payload NAV MASTER-level/ngày.
    1 dòng/master → app publish Kafka (key = C_MASTER_CODE). Cho Asset dựng overview/AUM cấp
    quỹ (8b current = ngày mới nhất) + đường master TR (unit_price) cho chart FR-03.
    MODE EOD | HISTORY (=replay) — RECONSTRUCT-ONLY từ T_MASTER_NAV_BALANCE (DATED) ⇒ replay y hệt.
    Master-level lưu ĐỦ breakdown (cash/pending/div/stock/total_asset) trong NAV_BALANCE (khác per-KH).
    *** master index + benchmark TÁCH sang SP_GET_ASSET_INDEX_SNAPSHOT (event riêng) — benchmark
        dùng chung nhiều master nên KHÔNG nhét vào đây (tránh dup mỗi master gửi lại). ***
    ⚠️ PAYLOAD DRAFT — map theo schema Asset thật khi có.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_ASSET_MASTER_SNAPSHOT
    @p_business_date DATE,
    @p_mode          VARCHAR(10)   = 'EOD',
    @p_user          VARCHAR(64)   = NULL,
    @p_err_code      INT           OUTPUT,
    @p_err_msg       NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @p_err_code = 0; SET @p_err_msg = NULL;
    BEGIN TRY
    IF @p_mode NOT IN ('EOD','HISTORY')
        BEGIN SET @p_err_code=2; SET @p_err_msg=N'@p_mode phải EOD hoặc HISTORY'; RAISERROR(@p_err_msg, 16, 1); END
    IF NOT EXISTS (SELECT 1 FROM T_MASTER_NAV_BALANCE WHERE C_BUSINESS_DATE=@p_business_date)
        BEGIN SET @p_err_code=3; SET @p_err_msg=N'Không có dữ liệu master EOD ngày '+CONVERT(VARCHAR(10),@p_business_date,23); RAISERROR(@p_err_msg, 16, 1); END

    SELECT b.C_MASTER_CODE, b.C_BUSINESS_DATE,
        (SELECT
            b.C_MASTER_CODE                              AS master_code,
            mp.C_MASTER_NAME                             AS master_name,
            CONVERT(VARCHAR(10), b.C_BUSINESS_DATE, 23)  AS business_date,
            @p_mode                                      AS mode,
            b.C_NAV          AS master_nav,
            b.C_UNIT         AS master_unit,
            b.C_UNIT_PRICE   AS master_unit_price,       -- master TR (đường "hiệu suất master" chart FR-03)
            b.C_DAILY_PNL    AS master_daily_pnl,
            b.C_DAILY_RETURN AS master_daily_return,
            b.C_TOTAL_ASSET  AS total_asset,
            b.C_CASH         AS cash,
            b.C_PENDING_CASH AS pending_cash,            -- master-level CÓ lịch sử (khác per-KH)
            b.C_DIV_CASH     AS div_cash,
            b.C_STOCK_VALUE  AS stock_value,
            b.C_PAYABLE_FEE  AS payable_fee,
            b.C_TOTAL_ACCOUNT AS total_account,
            b.C_CASH_IN      AS cash_in,
            b.C_CASH_OUT     AS cash_out,
            mp.C_BENCHMARK_CODE AS benchmark_code         -- chỉ tham chiếu code (giá trị index/benchmark ở SP riêng)
         FOR JSON PATH, WITHOUT_ARRAY_WRAPPER)          AS C_PAYLOAD_JSON
    FROM       T_MASTER_NAV_BALANCE b
    INNER JOIN T_MASTER_PORTFOLIO   mp  ON mp.C_MASTER_CODE = b.C_MASTER_CODE
    WHERE b.C_BUSINESS_DATE = @p_business_date
    ORDER BY b.C_MASTER_CODE;
    END TRY
    BEGIN CATCH
        IF @p_err_code = 0 BEGIN SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE(); END  -- lỗi runtime → OUT, KHÔNG THROW
    END CATCH
END
GO

/*===========================================================================
  SP_GET_ASSET_INDEX_SNAPSHOT (SDI→Asset sync, EVENT RIÊNG cho index/benchmark)
    Trả 1 BẢN GHI DUY NHẤT = 1 JSON ARRAY; mỗi phần tử = 1 master GỘP index DM + benchmark
    của master đó: {master_code, business_date, index_value (DM), benchmark_code, benchmark_value}.
    App publish 1 message/ngày (cả mảng). MODE EOD|HISTORY, RECONSTRUCT-ONLY (DATED) → replay y hệt
    (mode KHÔNG nằm trong payload → EOD & HISTORY ra mảng GIỐNG HỆT).
    benchmark_value join theo benchmark_code của master (LEFT — master chưa có benchmark/giá → NULL).
    ⚠️ PAYLOAD DRAFT — map theo schema Asset thật khi có.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_ASSET_INDEX_SNAPSHOT
    @p_business_date DATE,
    @p_mode          VARCHAR(10)   = 'EOD',
    @p_user          VARCHAR(64)   = NULL,
    @p_err_code      INT           OUTPUT,
    @p_err_msg       NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @p_err_code = 0; SET @p_err_msg = NULL;
    BEGIN TRY
    IF @p_mode NOT IN ('EOD','HISTORY')
        BEGIN SET @p_err_code=2; SET @p_err_msg=N'@p_mode phải EOD hoặc HISTORY'; RAISERROR(@p_err_msg, 16, 1); END

    -- 1 bản ghi: JSON ARRAY, mỗi phần tử = 1 master (index DM + benchmark của master đó)
    SELECT ISNULL((
        SELECT idx.C_MASTER_CODE                          AS master_code,
               CONVERT(VARCHAR(10), idx.C_BUSINESS_DATE, 23) AS business_date,
               idx.C_INDEX_VALUE                          AS index_value,      -- index danh mục master (PR)
               mp.C_BENCHMARK_CODE                        AS benchmark_code,
               bm.C_INDEX_VALUE                           AS benchmark_value   -- giá benchmark (PR) của master
        FROM       T_MASTER_INDEX_DAILY idx
        INNER JOIN T_MASTER_PORTFOLIO   mp ON mp.C_MASTER_CODE   = idx.C_MASTER_CODE
        LEFT JOIN  T_BENCHMARK_DAILY    bm ON bm.C_BENCHMARK_CODE = mp.C_BENCHMARK_CODE
                                          AND bm.C_BUSINESS_DATE  = idx.C_BUSINESS_DATE
        WHERE idx.C_BUSINESS_DATE = @p_business_date
        ORDER BY idx.C_MASTER_CODE
        FOR JSON PATH
    ), N'[]') AS C_PAYLOAD_JSON;
    END TRY
    BEGIN CATCH
        IF @p_err_code = 0 BEGIN SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE(); END  -- lỗi runtime → OUT, KHÔNG THROW
    END CATCH
END
GO
