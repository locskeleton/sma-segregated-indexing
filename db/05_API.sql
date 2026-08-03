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
      master suy từ DỮ LIỆU: FR-02/03/05/06 lấy từ T_SI_CURRENT (có sau FO ingest) — tiểu khoản
      chưa có data ⇒ not-found (không thuộc đường serve). FR-01 (list) + FR-04 (info) đọc T_SI_PORTFOLIO
      (endpoint registry: join_date/sub_account_no/initial_amount/sip...). Ownership/auth do tầng API gác.
  Read-only. T0 unit price = 10.000.

  *** ERR-CODE CONVENTION (date-guard) ***
    0=OK · 1=not found (entity) · 2=alerts no-holdings (06_PM) / @p_mode sai · 3=không có data index @NGÀY
    (SP_GET_ASSET_INDEX_SNAPSHOT) · 4=fee multi-accrue guard (Option B) · 5=NGÀY-bừa-bãi cho entity
    (FR-06 asof / rebalance @date: ngày nghỉ·tương lai·trước-mở·sau-đóng — chặn NAV=NULL+asset>0 im lặng).
    [EOD pipeline: 10=precondition / NGÀY không phải ngày GD.]
    (BRD 2026-06-22: gỡ producer ASSET + MASTER snapshot; GIỮ INDEX snapshot (đẩy riêng khi BO price-ready).
     BO trả phí QL lũy kế/ngày thẳng Asset. Xem docs/SDI-asset-gap.md.)
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
    RS1: breakdown từng sub-account (current NAV/AUM + cash + %return inception)
    RS2: tổng hợp toàn KH (Σ NAV, Σ cash, số sub-account)
  [thin-layer] AUM = C_LAST_AUM (Asset gửi). %return inception = compound TOÀN BỘ
    daily_return của si từ T_SI_BALANCE: EXP(Σ LN(1+r))−1 (TWR Asset-supplied).
    Bỏ cột C_UNIT / C_LAST_UNIT_PRICE (Asset KHÔNG cấp unit/UP nữa).
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
            nc.C_CASH,
            nc.C_LAST_AUM,                          -- AUM gần nhất (Asset gửi)
            nc.C_LAST_BUSINESS_DATE,
            -- [thin-layer] %return inception = compound TOÀN BỘ daily_return của si (TWR).
            --   NULL nếu chưa có ngày nào có return (EXP/Σ trên tập rỗng → NULL).
            ret.C_RETURN_INCEPTION
    FROM        T_SI_PORTFOLIO   ip
    INNER JOIN        T_MASTER_PORTFOLIO     mp ON mp.C_MASTER_CODE = ip.C_MASTER_CODE
    LEFT JOIN   T_SI_CURRENT nc ON nc.C_SI_ACCOUNT = ip.C_SI_ACCOUNT
    OUTER APPLY (
        -- compound = EXP(Σ LN(1+r))−1. GUARD wipeout: nếu CÓ ngày 1+r ≤ 0 (return ≤ −100%, mất sạch)
        --   → LOG(≤0) lỗi "invalid floating point" ⇒ clamp arg LOG về 1 và ép kết quả = −1.0 (−100%, mất hết).
        SELECT CASE WHEN MAX(CASE WHEN 1.0+b.C_DAILY_RETURN <= 0 THEN 1 ELSE 0 END) = 1 THEN CAST(-1 AS DECIMAL(10,6))
                    ELSE CAST(EXP(SUM(LOG(CASE WHEN 1.0+b.C_DAILY_RETURN > 0 THEN 1.0+b.C_DAILY_RETURN ELSE 1 END))) - 1 AS DECIMAL(10,6))
               END AS C_RETURN_INCEPTION
        FROM T_SI_BALANCE b
        WHERE b.C_SI_ACCOUNT = ip.C_SI_ACCOUNT AND b.C_DAILY_RETURN IS NOT NULL
    ) ret
    WHERE  ip.C_CUST_CODE = @p_cust_code
    ORDER BY nc.C_LAST_AUM DESC;

    SELECT  @p_cust_code          AS C_CUST_CODE,
            COUNT(*)              AS C_SI_COUNT,
            SUM(nc.C_LAST_AUM)    AS C_TOTAL_AUM,
            SUM(nc.C_CASH)        AS C_TOTAL_CASH
    FROM        T_SI_PORTFOLIO   ip
    LEFT JOIN   T_SI_CURRENT nc ON nc.C_SI_ACCOUNT = ip.C_SI_ACCOUNT
    WHERE  ip.C_CUST_CODE = @p_cust_code;
    END TRY
    BEGIN CATCH
        IF @p_err_code = 0 BEGIN SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE(); END  -- lỗi runtime → OUT, KHÔNG THROW
    END CATCH
END
GO

/*===========================================================================
  FR-02 — GET /customer/{id}/si/{si_account} : chi tiết 1 sub-account
    Current NAV(AUM) + TWR (compound daily_return) + MWR (Modified Dietz) theo range.
    RS1: current + range metrics. RS2: dòng master-level mới nhất (tham chiếu).
  [thin-layer] NAV = AUM (C_AUM/C_LAST_AUM, Asset gửi). Bỏ unit/UP.
    TWR kỳ (@base,@end] = EXP(Σ LN(1+r))−1 từ daily_return (Asset ĐÃ khử dòng tiền).
    MWR (Modified Dietz) GIỮ logic cashflow; base/end NAV → AUM.
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

    -- master + trạng thái current: 1 seek clustered PK T_SI_CURRENT (có sau FO ingest, đọc luôn cho RS1).
    -- KHÔNG lấy master từ T_SI_PORTFOLIO — tiểu khoản chưa có dữ liệu (chưa ingest/EOD) không thuộc đường
    -- serve thật (SMO đọc Asset post-EOD) ⇒ không cần handle case đó, NULL → not-found.
    DECLARE @master VARCHAR(20),
            @cur_nav DECIMAL(20,0), @cur_date DATE;   -- [thin-layer] AUM hiện tại; bỏ unit/UP
    SELECT @master   = C_MASTER_CODE, @cur_nav  = C_LAST_AUM,
           @cur_date = C_LAST_BUSINESS_DATE
    FROM T_SI_CURRENT WHERE C_SI_ACCOUNT=@p_si_account;
    IF @master IS NULL BEGIN SET @p_err_code = 1; SET @p_err_msg = N'Sub-account not found'; RAISERROR(@p_err_msg, 16, 1); END

    DECLARE @end DATE, @cutoff DATE, @base DATE;
    DECLARE @base_nav DECIMAL(20,0), @end_nav DECIMAL(20,0);   -- AUM mốc đầu/cuối kỳ (bỏ UP)

    -- mốc CUỐI kỳ + AUM: 1 read (TOP 1 đuôi index IX_SI_NAV_BALANCE_ACCT (si,date) DESC).
    SELECT TOP 1 @end = C_BUSINESS_DATE, @end_nav = C_AUM
    FROM T_SI_BALANCE WHERE C_SI_ACCOUNT=@p_si_account ORDER BY C_BUSINESS_DATE DESC;

    SET @cutoff = dbo.UDF_RANGE_CUTOFF(@end, @p_range);

    -- mốc ĐẦU kỳ + AUM: 1 read (mốc ≤ cutoff gần nhất); fallback = mốc sớm nhất nếu range trùm cả lịch sử.
    --   @base là mốc THAM CHIẾU (chưa tính return của chính ngày @base); TWR compound từ ngày > @base.
    SELECT TOP 1 @base = C_BUSINESS_DATE, @base_nav = C_AUM
    FROM T_SI_BALANCE WHERE C_SI_ACCOUNT=@p_si_account AND C_BUSINESS_DATE <= @cutoff
    ORDER BY C_BUSINESS_DATE DESC;
    IF @base IS NULL
        SELECT TOP 1 @base = C_BUSINESS_DATE, @base_nav = C_AUM
        FROM T_SI_BALANCE WHERE C_SI_ACCOUNT=@p_si_account ORDER BY C_BUSINESS_DATE ASC;

    -- [thin-layer] TWR kỳ = compound daily_return của si trên (@base,@end]: EXP(Σ LN(1+r))−1.
    --   Tập rỗng (không ngày nào có return) → NULL. Mỗi r ∈ T_SI_BALANCE (Asset gửi, đã khử CF).
    --   GUARD wipeout: ngày 1+r ≤ 0 (return ≤ −100%) → LOG lỗi ⇒ clamp + ép TWR = −1.0.
    DECLARE @twr DECIMAL(10,6);
    SELECT @twr = CASE WHEN MAX(CASE WHEN 1.0+C_DAILY_RETURN <= 0 THEN 1 ELSE 0 END) = 1 THEN CAST(-1 AS DECIMAL(10,6))
                       ELSE CAST(EXP(SUM(LOG(CASE WHEN 1.0+C_DAILY_RETURN > 0 THEN 1.0+C_DAILY_RETURN ELSE 1 END))) - 1 AS DECIMAL(10,6))
                  END
    FROM T_SI_BALANCE
    WHERE C_SI_ACCOUNT=@p_si_account AND C_BUSINESS_DATE > @base AND C_BUSINESS_DATE <= @end
      AND C_DAILY_RETURN IS NOT NULL;

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
            @base_nav              AS C_BASE_NAV,      -- AUM mốc đầu kỳ
            @end_nav               AS C_END_NAV,       -- AUM mốc cuối kỳ
            @cur_nav               AS C_CURRENT_NAV,   -- AUM hiện tại (T_SI_CURRENT)
            @cur_date              AS C_CURRENT_DATE,
            @twr                   AS C_TWR_PCT,       -- [thin-layer] compound daily_return
            (@end_nav - @base_nav - @cf_net) AS C_PNL_MONEY,
            @cf_net                AS C_CF_NET,
            CASE WHEN @T = 0 OR ABS(@denom) < 0.0001 THEN NULL
                 ELSE CAST((@end_nav - @base_nav - @cf_net) / @denom AS DECIMAL(10,6)) END AS C_MWR_PCT;

    -- RS2: master-level mới nhất (đường "Hiệu suất master" tham chiếu).
    --   [thin-layer] bỏ C_UNIT_PRICE master (đã drop); C_DAILY_RETURN = master AUM-weighted daily return.
    SELECT TOP 1 C_BUSINESS_DATE, C_AUM, C_DAILY_RETURN, C_CASH
    FROM T_MASTER_BALANCE WHERE C_MASTER_CODE = @master ORDER BY C_BUSINESS_DATE DESC;
    END TRY
    BEGIN CATCH
        IF @p_err_code = 0 BEGIN SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE(); END  -- lỗi runtime → OUT, KHÔNG THROW
    END CATCH
END
GO

/*===========================================================================
  FR-03 — GET /customer/{id}/si/{si_account}/performance?range= : chart so sánh
    Chuỗi ngày [mốc..cuối], 4 đường:
      (a) KH       = compound daily_return sub-account (TWR)         → C_CUST_RETURN
      (b) master TR= compound master AUM-weighted daily_return       → C_MASTER_RETURN
      (c) index PR = T_MASTER_INDEX_DAILY.C_INDEX_VALUE (danh mục mẫu) → C_MASTER_INDEX
      (d) benchmark= T_BENCHMARK_DAILY.C_INDEX_VALUE (PR ngoài)        → C_BENCHMARK
  [thin-layer] Bỏ unit_price (Asset không cấp). (a)(b) = lợi suất tích lũy CUMULATIVE
    EXP(Σ LN(1+r) OVER ORDER BY date)−1 trên window [@base..@end] (mốc đầu ≈ 0% trừ ngày @base
    có return). App rebase 2 đường index/benchmark (PR, giá trị tuyệt đối) về dòng đầu như cũ.
    NHẤT QUÁN: (a)(b) trả CUMULATIVE RETURN (đã compound), (c)(d) trả MỨC index thô (app rebase).
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

    -- master suy từ T_SI_CURRENT (có sau FO ingest) — KHÔNG từ T_SI_PORTFOLIO; chưa có data → not-found.
    DECLARE @master VARCHAR(20) = (SELECT C_MASTER_CODE FROM T_SI_CURRENT WHERE C_SI_ACCOUNT=@p_si_account);
    IF @master IS NULL BEGIN SET @p_err_code = 1; SET @p_err_msg = N'Sub-account not found'; RAISERROR(@p_err_msg, 16, 1); END

    DECLARE @bench VARCHAR(20) = (SELECT C_BENCHMARK_CODE FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE = @master);
    DECLARE @end DATE, @cutoff DATE, @base DATE, @first DATE;

    -- khung ngày: 1 read gộp MAX(cuối)+MIN(đầu) → fallback dùng @first (bỏ read MIN lần 3).
    SELECT @end = MAX(C_BUSINESS_DATE), @first = MIN(C_BUSINESS_DATE)
    FROM T_SI_BALANCE WHERE C_SI_ACCOUNT=@p_si_account;
    SET @cutoff = dbo.UDF_RANGE_CUTOFF(@end, @p_range);
    SELECT @base = MAX(C_BUSINESS_DATE) FROM T_SI_BALANCE
     WHERE C_SI_ACCOUNT=@p_si_account AND C_BUSINESS_DATE <= @cutoff;
    IF @base IS NULL SET @base = @first;

    -- [thin-layer] CUMULATIVE compound return trong window:
    --   running EXP(Σ LN(1+r) OVER (ORDER BY date))−1. ISNULL(r,0): ngày thiếu return KHÔNG ngắt chuỗi.
    --   (a) KH: r = cd.C_DAILY_RETURN (T_SI_BALANCE per-si).
    --   (b) master TR: r = sd.C_DAILY_RETURN (T_MASTER_BALANCE, = master AUM-weighted daily return).
    --   Window dùng range của KH (cd) làm trục thời gian; master/index/benchmark LEFT JOIN theo ngày.
    --   GUARD wipeout (1+r ≤ 0): clamp arg LOG về 1 để KHÔNG lỗi; từ ngày wipeout trở đi ép cumulative = −1.0
    --   (running MAX của cờ wipeout). Pre-wipeout không bao giờ có 1+r ≤ 0 nên clamp vô hại.
    SELECT  cd.C_BUSINESS_DATE,
            CASE WHEN MAX(CASE WHEN 1.0+ISNULL(cd.C_DAILY_RETURN,0) <= 0 THEN 1 ELSE 0 END)
                     OVER (ORDER BY cd.C_BUSINESS_DATE ROWS UNBOUNDED PRECEDING) = 1 THEN CAST(-1 AS DECIMAL(18,8))
                 ELSE CAST(EXP(SUM(LOG(CASE WHEN 1.0+ISNULL(cd.C_DAILY_RETURN,0) > 0 THEN 1.0+ISNULL(cd.C_DAILY_RETURN,0) ELSE 1 END))
                          OVER (ORDER BY cd.C_BUSINESS_DATE ROWS UNBOUNDED PRECEDING)) - 1 AS DECIMAL(18,8))
            END AS C_CUST_RETURN,    -- (a) lợi suất tích lũy KH
            CASE WHEN MAX(CASE WHEN 1.0+ISNULL(sd.C_DAILY_RETURN,0) <= 0 THEN 1 ELSE 0 END)
                     OVER (ORDER BY cd.C_BUSINESS_DATE ROWS UNBOUNDED PRECEDING) = 1 THEN CAST(-1 AS DECIMAL(18,8))
                 ELSE CAST(EXP(SUM(LOG(CASE WHEN 1.0+ISNULL(sd.C_DAILY_RETURN,0) > 0 THEN 1.0+ISNULL(sd.C_DAILY_RETURN,0) ELSE 1 END))
                          OVER (ORDER BY cd.C_BUSINESS_DATE ROWS UNBOUNDED PRECEDING)) - 1 AS DECIMAL(18,8))
            END AS C_MASTER_RETURN,  -- (b) lợi suất tích lũy master (AUM-weighted)
            si.C_INDEX_VALUE         AS C_MASTER_INDEX,      -- (c) PR danh mục mẫu (mức thô, app rebase)
            bm.C_INDEX_VALUE         AS C_BENCHMARK          -- (d) PR benchmark ngoài (mức thô, app rebase)
    FROM        T_SI_BALANCE cd
    LEFT JOIN   T_MASTER_BALANCE   sd ON sd.C_MASTER_CODE = @master AND sd.C_BUSINESS_DATE = cd.C_BUSINESS_DATE
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
            CAST(NULL AS DECIMAL(10,6)) AS C_MGMT_FEE_RATE_EFFECTIVE   -- [BRD] rate phí do Asset quản; SDI không giữ T_FEE_CONFIG
    FROM       T_SI_PORTFOLIO ip
    INNER JOIN       T_MASTER_PORTFOLIO   mp ON mp.C_MASTER_CODE = ip.C_MASTER_CODE
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

    -- tồn tại theo DỮ LIỆU (T_SI_CURRENT, có sau ingest) — KHÔNG từ T_SI_PORTFOLIO; chưa có data → not-found.
    IF NOT EXISTS (SELECT 1 FROM T_SI_CURRENT WHERE C_SI_ACCOUNT=@p_si_account)
        BEGIN SET @p_err_code = 1; SET @p_err_msg = N'Sub-account not found'; RAISERROR(@p_err_msg, 16, 1); END

    -- @pd = PHIÊN GD mới nhất có giá (mốc định giá "hiện tại"). Lọc theo LỊCH: MAX(C_BUSINESS_DATE) trần trụi
    --   sẽ trỏ vào T7/CN nếu 1 dòng giá ngày nghỉ lọt vào bảng ⇒ định giá holdings theo phiên không tồn tại.
    DECLARE @pd DATE = dbo.UDF_LAST_BUSINESS_DATE();

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

    -- master suy từ T_SI_CURRENT (có sau FO ingest) — KHÔNG từ T_SI_PORTFOLIO; chưa có data → not-found.
    DECLARE @master VARCHAR(20) = (SELECT C_MASTER_CODE FROM T_SI_CURRENT WHERE C_SI_ACCOUNT=@p_si_account);
    IF @master IS NULL BEGIN SET @p_err_code = 1; SET @p_err_msg = N'Sub-account not found'; RAISERROR(@p_err_msg, 16, 1); END

    -- [BRD asset-sync] Phí QL đã trừ sẵn trong NAV ròng Asset gửi (Asset KHÔNG gửi số phí lũy kế riêng) ⇒ AUM = NAV.

    IF @p_asof IS NULL
        SELECT @p_asof = MAX(C_BUSINESS_DATE) FROM T_SI_BALANCE WHERE C_SI_ACCOUNT=@p_si_account;

    -- DATE GUARD (err=5): @p_asof phải có dòng EOD (T_SI_BALANCE) cho ĐÚNG sub-account này (chặn ngày
    --   nghỉ/tương lai/trước-mở/sau-đóng). [thin-layer] T_SI_BALANCE là nguồn trực tiếp (Asset ghi thẳng).
    IF NOT EXISTS (SELECT 1 FROM T_SI_BALANCE WHERE C_SI_ACCOUNT=@p_si_account AND C_BUSINESS_DATE=@p_asof)
    BEGIN SET @p_err_code = 5;
        SET @p_err_msg = CONCAT(N'Không có dữ liệu EOD cho sub-account ', @p_si_account, N' @ ',
            CONVERT(VARCHAR(10),@p_asof,23), N' (ngày nghỉ/tương lai/trước khi mở/sau khi đóng TK).');
        RAISERROR(@p_err_msg, 16, 1); END

    -- [thin-layer] Asset gửi số TỔNG: AUM (C_AUM) + cash (1 số). KHÔNG còn C_STOCK_VALUE (Asset không cấp).
    --   Stock value = AUM − cash (suy ra, vì AUM = stock + tổng tiền). cash = TỔNG tiền (gộp tiền mặt + bán chờ + cổ tức tiền).
    DECLARE @aum DECIMAL(20,0), @cash DECIMAL(20,0), @stock DECIMAL(20,0);
    SELECT @aum=C_AUM, @cash=C_CASH
    FROM T_SI_BALANCE WHERE C_SI_ACCOUNT=@p_si_account AND C_BUSINESS_DATE=@p_asof;
    SET @stock = ISNULL(@aum,0) - ISNULL(@cash,0);   -- stock suy ra (Asset không gửi tách riêng)

    -- [thin-layer] return tích lũy INCEPTION→@asof = EXP(Σ LN(1+r))−1 (compound daily_return ≤ @asof).
    --   GUARD wipeout (1+r ≤ 0): clamp + ép −1.0 (tránh LOG lỗi).
    DECLARE @ret_incep DECIMAL(10,6);
    SELECT @ret_incep = CASE WHEN MAX(CASE WHEN 1.0+C_DAILY_RETURN <= 0 THEN 1 ELSE 0 END) = 1 THEN CAST(-1 AS DECIMAL(10,6))
                             ELSE CAST(EXP(SUM(LOG(CASE WHEN 1.0+C_DAILY_RETURN > 0 THEN 1.0+C_DAILY_RETURN ELSE 1 END))) - 1 AS DECIMAL(10,6))
                        END
    FROM T_SI_BALANCE
    WHERE C_SI_ACCOUNT=@p_si_account AND C_BUSINESS_DATE <= @p_asof AND C_DAILY_RETURN IS NOT NULL;

    -- Holdings chi tiết per-mã (FO holdings reconstruct @asof × giá ≤ asof) cho RS2. ⚠️ Σ(FO×giá) CÓ THỂ lệch
    --   stock value Asset-derive (đo ở reconcile HOLDINGS_MISMATCH); RS1 total dùng số Asset (authoritative).
    SELECT h.C_TICKER, h.C_QUANTITY, h.C_AVG_COST, px.C_CLOSE_PRICE AS C_MARKET_PRICE,
           h.C_QUANTITY * px.C_CLOSE_PRICE AS C_MARKET_VALUE
    INTO #hold
    FROM T_SI_HOLDING_HIST h
    OUTER APPLY (SELECT TOP 1 C_CLOSE_PRICE FROM T_PRICE_DAILY
                 WHERE C_TICKER = h.C_TICKER AND C_BUSINESS_DATE <= @p_asof
                 ORDER BY C_BUSINESS_DATE DESC) px
    WHERE h.C_SI_ACCOUNT=@p_si_account
      AND h.C_VALID_FROM <= @p_asof AND (h.C_VALID_TO > @p_asof OR h.C_VALID_TO IS NULL);

    -- RS1: summary @asof. [thin-layer] AUM = C_AUM (Asset) = stock + tổng tiền. Bỏ unit/UP (Asset không cấp).
    --   C_RETURN_INCEPTION = compound daily_return inception→@asof (TWR).
    SELECT  @p_asof                AS C_ASOF,
            @p_si_account          AS C_SI_ACCOUNT,
            @master                AS C_MASTER_CODE,
            ISNULL(@aum,0)         AS C_AUM,          -- AUM (= NAV ròng Asset gửi)
            ISNULL(@aum,0)         AS C_AUM,          -- alias rõ nghĩa AUM = NAV
            ISNULL(@cash,0)        AS C_CASH,         -- TỔNG tiền (1 số)
            @stock                 AS C_STOCK_VALUE,  -- = AUM − cash (suy ra; Asset không gửi tách)
            @ret_incep             AS C_RETURN_INCEPTION;

    -- RS2: holdings chi tiết per-mã (FO)
    SELECT  C_TICKER, C_QUANTITY, C_MARKET_PRICE, C_MARKET_VALUE,
            CAST(C_MARKET_VALUE / NULLIF((SELECT SUM(C_MARKET_VALUE) FROM #hold),0) AS DECIMAL(12,8)) AS C_WEIGHT,
            C_AVG_COST
    FROM #hold
    ORDER BY C_MARKET_VALUE DESC;

    -- [BRD asset-sync] BỎ RS3/RS4/RS5 (chi tiết income/fee): SDI không quản chi tiết giao dịch phí nữa.
    --   Phí QL đã trừ sẵn trong NAV ròng Asset gửi (không tách riêng). Cổ tức đã gộp trong tiền/NAV Asset gửi.

    DROP TABLE #hold;
    END TRY
    BEGIN CATCH
        IF @p_err_code = 0 BEGIN SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE(); END  -- lỗi runtime → OUT, KHÔNG THROW
        IF OBJECT_ID('tempdb..#hold') IS NOT NULL DROP TABLE #hold;
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
    -- DATE GUARD (err=3, nhất quán snapshot family): không có index @ngày → ngày nghỉ/tương lai/chưa tính.
    IF NOT EXISTS (SELECT 1 FROM T_MASTER_INDEX_DAILY WHERE C_BUSINESS_DATE=@p_business_date)
        BEGIN SET @p_err_code=3; SET @p_err_msg=N'Không có dữ liệu index cho ngày '+CONVERT(VARCHAR(10),@p_business_date,23); RAISERROR(@p_err_msg, 16, 1); END

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

/*===========================================================================
  BÁO CÁO TỔNG TÀI SẢN AUM — SP_GET_REPORT_AUM_TOTAL
    Ảnh chụp per-tiểu-khoản tại MỘT ngày chốt, kèm 11 filter đúng như màn hình lọc.

  ── NGỮ NGHĨA NGÀY (GIẢ ĐỊNH — cần nghiệp vụ xác nhận) ──────────────────────────────────────
    @p_to_date   = NGÀY CHỐT SỐ. Mọi số tiền là ảnh chụp CUỐI ngày này. NULL = phiên có dữ liệu
                   gần nhất. Tiêu đề báo cáo "Ngày: dd/mm/yyyy" chính là ngày này.
    @p_from_date = CHỈ lọc TẬP tiểu khoản (đã mở trước/trong kỳ, chưa đóng trước kỳ), KHÔNG đổi
                   số tiền. Căn cứ: lưới KHÔNG có cột ngày ⇒ báo cáo là MỘT ngày, không phải dải.

  ── CỘT SUY RA, KHÔNG PHẢI SỐ ASSET GỬI ─────────────────────────────────────────────────────
    Giá trị chứng khoán = AUM − Tổng tiền. [thin-layer] Asset KHÔNG gửi stock_value; AUM = NAV =
    chứng khoán + tổng tiền. CÙNG cách suy với SP_GET_ASSET_REPORT — đừng đổi một chỗ mà quên chỗ kia.

  ── BỐN CỘT TIỀN LUÔN CỘNG KHỚP ─────────────────────────────────────────────────────────────
    Tổng tiền = Tiền mặt + Cổ tức chờ về + Tiền bán CK chờ về.
    Asset gửi TỔNG (`cash`) và hai khoản chờ về; "Tiền mặt" là phần CÒN LẠI, KHÔNG phải một số
    Asset gửi riêng ⇒ bốn cột luôn khớp theo định nghĩa, không bao giờ lệch nhau vài đồng.
    ⚠️ ĐỪNG dùng C_CASH_AVAILABLE làm "Tiền mặt": khả dụng còn loại thêm tiền PHONG TOẢ/CHỜ KHỚP
       nên nó NHỎ HƠN, và bốn cột sẽ không cộng khớp. Ingest đã chặn div+sell > cash (err=22) nên
       "Tiền mặt" không thể ra âm.

  ── DỮ LIỆU TỔ CHỨC ─────────────────────────────────────────────────────────────────────────
    Họ tên KH / TKCK / MKT_ID / phòng ban / đơn vị KD / khối lấy từ T_CUSTOMER_INFO (bản sao đẩy
    từ hệ tài khoản + CRM). LEFT JOIN: chưa đấu nguồn thì cột NULL và filter tổ chức không khớp
    dòng nào — ĐÚNG hành vi, không phải lỗi.

  RS1: lưới báo cáo.  RS2: dòng tổng (số tiểu khoản, Σ AUM, Σ chứng khoán, Σ tiền).
  err: 0 OK · 3 ngày chốt chưa có dữ liệu · 20 tham số không hợp lệ · -1 runtime. KHÔNG THROW.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_REPORT_AUM_TOTAL
    @p_from_date     DATE          = NULL,   -- Từ ngày   (lọc tập tiểu khoản)
    @p_to_date       DATE          = NULL,   -- Đến ngày  (NGÀY CHỐT SỐ; NULL = phiên gần nhất)
    @p_division      VARCHAR(32)   = NULL,   -- Khối nghiệp vụ
    @p_business_unit VARCHAR(32)   = NULL,   -- Đơn vị kinh doanh
    @p_department    VARCHAR(32)   = NULL,   -- Phòng ban
    @p_referrer_id   VARCHAR(32)   = NULL,   -- ID người giới thiệu
    @p_manager_id    VARCHAR(32)   = NULL,   -- ID sale quản lý
    @p_master_code   VARCHAR(20)   = NULL,   -- DM Master
    @p_tkck          VARCHAR(32)   = NULL,   -- TKCK
    @p_alias         VARCHAR(50)   = NULL,   -- Alias DM Index
    @p_si_account    VARCHAR(20)   = NULL,   -- Sub SDI
    @p_include_total BIT           = 1,      -- 0 = CHỈ trả lưới (RS1), bỏ dòng tổng
    @p_user          VARCHAR(64)   = NULL,
    @p_err_code      INT           OUTPUT,
    @p_err_msg       NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @p_err_code = 0; SET @p_err_msg = NULL;
    BEGIN TRY

    IF @p_to_date IS NULL
        SELECT @p_to_date = MAX(C_BUSINESS_DATE) FROM T_SI_BALANCE;

    IF @p_to_date IS NULL
    BEGIN SET @p_err_code=3; SET @p_err_msg=N'Chưa có dữ liệu số dư nào trong hệ thống.';
          RAISERROR(@p_err_msg,16,1); END

    IF @p_from_date IS NOT NULL AND @p_from_date > @p_to_date
    BEGIN SET @p_err_code=20; SET @p_err_msg=N'"Từ ngày" lớn hơn "Đến ngày".';
          RAISERROR(@p_err_msg,16,1); END

    -- DATE GUARD (err=3): ngày chốt PHẢI có dòng số dư. Chặn ngày tương lai / ngày chưa ingest —
    --   nếu không, báo cáo trả 0 dòng và người đọc tưởng "hôm đó không ai có tài sản".
    -- ⚠️ KHÔNG gate bằng lịch giao dịch: Asset gửi số dư MỌI ngày dương lịch (kể cả T7/CN) nên chốt
    --    AUM cuối tuần là hợp lệ. Lịch chỉ gate phần TÍNH TOÁN (index/EOD), không gate báo cáo.
    IF NOT EXISTS (SELECT 1 FROM T_SI_BALANCE WHERE C_BUSINESS_DATE=@p_to_date)
    BEGIN SET @p_err_code=3;
        SET @p_err_msg = CONCAT(N'Chưa có dữ liệu số dư @ ', CONVERT(VARCHAR(10),@p_to_date,23),
            N' (ngày tương lai hoặc Asset chưa đồng bộ).');
        RAISERROR(@p_err_msg,16,1); END

    ;WITH R AS (
        SELECT
            b.C_SI_ACCOUNT, b.C_CUST_CODE, b.C_MASTER_CODE,
            b.C_AUM, b.C_CASH, b.C_CASH_AVAILABLE,
            b.C_DIVIDEND_PENDING, b.C_SELL_PENDING,
            stock  = b.C_AUM - b.C_CASH,                        -- suy ra: Asset không gửi stock_value
            onhand = b.C_CASH - b.C_DIVIDEND_PENDING - b.C_SELL_PENDING,   -- Tiền mặt = phần CÒN LẠI của tổng tiền
            tkck   = COALESCE(ci.C_TKCK, p.C_SUB_ACCOUNT_NO),   -- ưu tiên hệ tài khoản; fallback số TK FO
            ci.C_FULL_NAME, ci.C_MKT_ID_REFERRER, ci.C_MKT_ID_MANAGER,
            ci.C_DEPARTMENT_NAME, ci.C_BUSINESS_UNIT_NAME, ci.C_DIVISION_NAME,
            mp.C_MASTER_NAME, mp.C_ALIAS
        FROM       T_SI_BALANCE       b
        INNER JOIN T_SI_PORTFOLIO     p  ON p.C_SI_ACCOUNT    = b.C_SI_ACCOUNT
        LEFT  JOIN T_MASTER_PORTFOLIO mp ON mp.C_MASTER_CODE  = b.C_MASTER_CODE
        LEFT  JOIN V_CUSTOMER_INFO    ci ON ci.C_CUST_CODE    = b.C_CUST_CODE   -- ★ VIEW, không phải bảng: đổi nguồn sửa 1 chỗ
        WHERE b.C_BUSINESS_DATE = @p_to_date
          -- "Từ ngày": tiểu khoản phải đã mở trước/trong kỳ và chưa đóng TRƯỚC kỳ
          AND (@p_from_date IS NULL
               OR (p.C_JOIN_DATE <= @p_to_date
                   AND (p.C_CLOSE_DATE IS NULL OR p.C_CLOSE_DATE >= @p_from_date)))
          AND (@p_master_code   IS NULL OR b.C_MASTER_CODE         = @p_master_code)
          AND (@p_si_account    IS NULL OR b.C_SI_ACCOUNT          = @p_si_account)
          AND (@p_alias         IS NULL OR mp.C_ALIAS              = @p_alias)
          AND (@p_tkck          IS NULL OR COALESCE(ci.C_TKCK, p.C_SUB_ACCOUNT_NO) = @p_tkck)
          AND (@p_division      IS NULL OR ci.C_DIVISION_CODE      = @p_division)
          AND (@p_business_unit IS NULL OR ci.C_BUSINESS_UNIT_CODE = @p_business_unit)
          AND (@p_department    IS NULL OR ci.C_DEPARTMENT_CODE    = @p_department)
          AND (@p_referrer_id   IS NULL OR ci.C_MKT_ID_REFERRER    = @p_referrer_id)
          AND (@p_manager_id    IS NULL OR ci.C_MKT_ID_MANAGER     = @p_manager_id)
    )
    SELECT
        C_STT                = ROW_NUMBER() OVER (ORDER BY R.C_DIVISION_NAME, R.C_BUSINESS_UNIT_NAME,
                                                           R.C_DEPARTMENT_NAME, R.tkck, R.C_SI_ACCOUNT),
        C_BUSINESS_DATE      = @p_to_date,
        C_TKCK               = R.tkck,
        C_CUST_CODE          = R.C_CUST_CODE,
        C_FULL_NAME          = R.C_FULL_NAME,        -- NULL nếu chưa đấu T_CUSTOMER_INFO
        C_SI_ACCOUNT         = R.C_SI_ACCOUNT,       -- Sub SDI
        C_ALIAS              = R.C_ALIAS,            -- Alias DM Index
        C_AUM                = R.C_AUM,
        C_PCT_STOCK_AUM      = CAST(R.stock  * 1.0 / NULLIF(R.C_AUM,0) AS DECIMAL(9,6)),
        C_STOCK_VALUE        = R.stock,              -- = AUM − tổng tiền (SUY RA)
        C_PCT_CASH_AUM       = CAST(R.C_CASH * 1.0 / NULLIF(R.C_AUM,0) AS DECIMAL(9,6)),
        C_CASH_TOTAL         = R.C_CASH,             -- Tổng tiền (Asset gửi, ĐÃ GỘP 2 khoản chờ về)
        C_CASH_ON_HAND       = R.onhand,             -- Tiền mặt = tổng tiền − cổ tức chờ − bán chờ
        C_DIVIDEND_PENDING   = R.C_DIVIDEND_PENDING, -- Cổ tức chờ về
        C_SELL_PENDING       = R.C_SELL_PENDING,     -- Tiền bán CK chờ về (T+)
        C_CASH_AVAILABLE     = R.C_CASH_AVAILABLE,   -- (thêm) tiền KHẢ DỤNG — khác "tiền mặt": loại cả phong toả/chờ khớp
        C_MASTER_CODE        = R.C_MASTER_CODE,      -- DM master
        C_MASTER_NAME        = R.C_MASTER_NAME,
        C_MKT_ID_REFERRER    = R.C_MKT_ID_REFERRER,
        C_MKT_ID_MANAGER     = R.C_MKT_ID_MANAGER,
        C_DEPARTMENT_NAME    = R.C_DEPARTMENT_NAME,
        C_BUSINESS_UNIT_NAME = R.C_BUSINESS_UNIT_NAME,
        C_DIVISION_NAME      = R.C_DIVISION_NAME
    FROM R
    ORDER BY R.C_DIVISION_NAME, R.C_BUSINESS_UNIT_NAME, R.C_DEPARTMENT_NAME, R.tkck, R.C_SI_ACCOUNT
    OPTION (RECOMPILE);   -- 11 filter optional: KHÔNG được dùng chung plan giữa các tổ hợp lọc

    -- RS2: dòng tổng. Phải lặp lại y hệt bộ lọc của RS1 (CTE không dùng lại được ở statement sau).
    --   ⚠️ Sửa điều kiện lọc ở RS1 thì PHẢI sửa y hệt ở đây, nếu không TỔNG KHÔNG KHỚP LƯỚI.
    -- @p_include_total=0 để CHỈ còn RS1: caller nào hứng kết quả bằng INSERT ... EXEC (test harness,
    --   job nạp bảng tạm) sẽ VỠ nếu proc trả 2 result set khác schema — SQL Server đòi mọi result set
    --   phải khớp bảng đích. App gọi bình thường thì để mặc định 1 và đọc cả hai.
    IF @p_include_total = 1
    SELECT
        C_BUSINESS_DATE = @p_to_date,
        C_CNT_SI        = COUNT(*),
        C_TOTAL_AUM     = ISNULL(SUM(b.C_AUM),0),
        C_TOTAL_STOCK   = ISNULL(SUM(b.C_AUM - b.C_CASH),0),
        C_TOTAL_CASH    = ISNULL(SUM(b.C_CASH),0)
    FROM       T_SI_BALANCE       b
    INNER JOIN T_SI_PORTFOLIO     p  ON p.C_SI_ACCOUNT    = b.C_SI_ACCOUNT
    LEFT  JOIN T_MASTER_PORTFOLIO mp ON mp.C_MASTER_CODE  = b.C_MASTER_CODE
    LEFT  JOIN V_CUSTOMER_INFO    ci ON ci.C_CUST_CODE    = b.C_CUST_CODE   -- ★ VIEW, không phải bảng: đổi nguồn sửa 1 chỗ
    WHERE b.C_BUSINESS_DATE = @p_to_date
      AND (@p_from_date IS NULL
           OR (p.C_JOIN_DATE <= @p_to_date
               AND (p.C_CLOSE_DATE IS NULL OR p.C_CLOSE_DATE >= @p_from_date)))
      AND (@p_master_code   IS NULL OR b.C_MASTER_CODE         = @p_master_code)
      AND (@p_si_account    IS NULL OR b.C_SI_ACCOUNT          = @p_si_account)
      AND (@p_alias         IS NULL OR mp.C_ALIAS              = @p_alias)
      AND (@p_tkck          IS NULL OR COALESCE(ci.C_TKCK, p.C_SUB_ACCOUNT_NO) = @p_tkck)
      AND (@p_division      IS NULL OR ci.C_DIVISION_CODE      = @p_division)
      AND (@p_business_unit IS NULL OR ci.C_BUSINESS_UNIT_CODE = @p_business_unit)
      AND (@p_department    IS NULL OR ci.C_DEPARTMENT_CODE    = @p_department)
      AND (@p_referrer_id   IS NULL OR ci.C_MKT_ID_REFERRER    = @p_referrer_id)
      AND (@p_manager_id    IS NULL OR ci.C_MKT_ID_MANAGER     = @p_manager_id)
    OPTION (RECOMPILE);

    END TRY
    BEGIN CATCH
        IF @p_err_code = 0 BEGIN SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE(); END  -- lỗi runtime → OUT, KHÔNG THROW
    END CATCH
END
GO
