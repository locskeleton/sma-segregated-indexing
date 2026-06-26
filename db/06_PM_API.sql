SET QUOTED_IDENTIFIER ON;  -- đọc bảng có filtered index → QI ON lúc CREATE PROC
SET ANSI_NULLS ON;
GO
/*==============================================================================
  SDI MODULE — PM TOOL READ API (SQL Server)  | ALL-IN-DB, serve-layer on-read
  Dashboard PM quản lý cấp MASTER (10 master, ~50k KH). Spec: docs/SDI-pm-tool-spec.md
  master-keyed: nhận @p_master_code (+ range); KHÔNG trả định danh KH ngoài top-N (US5).
  2 bản chất: Snapshot (current, T_MASTER/SI_NAV_CURRENT) | Hiệu suất (T-1, *_NAV_BALANCE).
  Công thức (spec §2) — [thin-layer] SDI KHÔNG tự tính hiệu suất: Asset gửi per-KH/ngày AUM + daily_return (TWR):
    AUM = C_LAST_AUM (per KH) — [BRD] phí QL đã trừ trong NAV Asset gửi ⇒ AUM = NAV (gross = net)
    Rᵢ (TWR kỳ KH i) = COMPOUND daily_return = EXP(SUM(LOG(1+C_DAILY_RETURN)))−1 qua (base,end] (T_SI_BALANCE)
    DM tổng KH = AUM-weighted (end-weight): Σ Wᵢ·Rᵢ, Wᵢ=AUMᵢ/ΣAUM (TWR compound, KHÔNG còn unit_price)
    Deviation = (Return − Master Index Return) × 10000 (BPS)
    TE per-KH = STDEV(Rᵢ,t − R_master,t) × √X ; Master = Σ(TEᵢ·AUMᵢ)/ΣAUM (X = #ngày GD, cap 252)
==============================================================================*/

/*---------------------------------------------------------------------------
  UDF_PM_CONFIG — ngưỡng PM hiệu lực cho 1 master (per-master COALESCE default hệ thống).
    Default hệ thống: TE badge low=0.02/high=0.05; TE alert=0.05; cash drag Y=0.05;
                      deviation A=+100 BPS / B=−100 BPS.
---------------------------------------------------------------------------*/
CREATE OR ALTER FUNCTION UDF_PM_CONFIG (@m VARCHAR(20))
RETURNS TABLE AS RETURN
    SELECT  CAST(COALESCE(c.C_TE_BADGE_LOW,        0.020000) AS DECIMAL(10,6)) AS C_TE_BADGE_LOW,
            CAST(COALESCE(c.C_TE_BADGE_HIGH,       0.050000) AS DECIMAL(10,6)) AS C_TE_BADGE_HIGH,
            CAST(COALESCE(c.C_TE_ALERT_THRESHOLD,  0.050000) AS DECIMAL(10,6)) AS C_TE_ALERT_THRESHOLD,
            CAST(COALESCE(c.C_CASH_DRAG_THRESHOLD, 0.050000) AS DECIMAL(9,6))  AS C_CASH_DRAG_THRESHOLD,
            -- (deviation A/B bỏ khỏi config — SP nhận qua tham số, default tại SP +100/-100 BPS)
            -- ngưỡng cảnh báo cấu hình: KHÔNG default (NULL = chưa cấu hình → consumer tương lai tự xử)
            CAST(c.C_DRIFT_THRESHOLD       AS DECIMAL(9,6)) AS C_DRIFT_THRESHOLD,
            CAST(c.C_SYMBOL_WEIGHT_ALERT   AS DECIMAL(9,6)) AS C_SYMBOL_WEIGHT_ALERT,
            CAST(c.C_INDUSTRY_WEIGHT_ALERT AS DECIMAL(9,6)) AS C_INDUSTRY_WEIGHT_ALERT
    FROM        (SELECT @m AS m) z
    LEFT JOIN   T_MASTER_PM_CONFIG c ON c.C_MASTER_CODE = z.m;
GO

/*===========================================================================
  CẤU HÌNH — SP_SET_MASTER_PM_CONFIG : upsert ngưỡng PM per-master.
    NULL ở tham số ⇒ lưu NULL ⇒ SP serve fallback default hệ thống (UDF_PM_CONFIG).
    RS: cấu hình HIỆU LỰC sau khi set.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_SET_MASTER_PM_CONFIG
    @p_master_code          VARCHAR(20),
    @p_te_badge_low         DECIMAL(10,6) = NULL,
    @p_te_badge_high        DECIMAL(10,6) = NULL,
    @p_te_alert_threshold   DECIMAL(10,6) = NULL,
    @p_cash_drag_threshold  DECIMAL(9,6)  = NULL,
    @p_drift_threshold       DECIMAL(9,6) = NULL,
    @p_symbol_weight_alert   DECIMAL(9,6) = NULL,
    @p_industry_weight_alert DECIMAL(9,6) = NULL,
    @p_updated_by           VARCHAR(64)   = NULL,
    @p_user        VARCHAR(64)   = NULL,
    @p_err_code    INT           OUTPUT,
    @p_err_msg     NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @p_err_code = 0; SET @p_err_msg = NULL;
    BEGIN TRY
    IF NOT EXISTS (SELECT 1 FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE = @p_master_code)
        BEGIN SET @p_err_code = 1; SET @p_err_msg = N'Master not found'; RAISERROR(@p_err_msg, 16, 1); END

    MERGE T_MASTER_PM_CONFIG AS t
    USING (SELECT @p_master_code AS m) AS s ON t.C_MASTER_CODE = s.m
    WHEN MATCHED THEN UPDATE SET
        C_TE_BADGE_LOW        = @p_te_badge_low,
        C_TE_BADGE_HIGH       = @p_te_badge_high,
        C_TE_ALERT_THRESHOLD  = @p_te_alert_threshold,
        C_CASH_DRAG_THRESHOLD = @p_cash_drag_threshold,
        C_DRIFT_THRESHOLD       = @p_drift_threshold,
        C_SYMBOL_WEIGHT_ALERT   = @p_symbol_weight_alert,
        C_INDUSTRY_WEIGHT_ALERT = @p_industry_weight_alert,
        C_UPDATED_BY          = @p_updated_by,
        C_UPDATED_TIME        = GETDATE()
    WHEN NOT MATCHED THEN INSERT
        (C_MASTER_CODE, C_TE_BADGE_LOW, C_TE_BADGE_HIGH, C_TE_ALERT_THRESHOLD,
         C_CASH_DRAG_THRESHOLD,
         C_DRIFT_THRESHOLD, C_SYMBOL_WEIGHT_ALERT, C_INDUSTRY_WEIGHT_ALERT, C_UPDATED_BY)
        VALUES
        (@p_master_code, @p_te_badge_low, @p_te_badge_high, @p_te_alert_threshold,
         @p_cash_drag_threshold,
         @p_drift_threshold, @p_symbol_weight_alert, @p_industry_weight_alert, @p_updated_by);

    SELECT @p_master_code AS C_MASTER_CODE, * FROM dbo.UDF_PM_CONFIG(@p_master_code);
    END TRY
    BEGIN CATCH
        IF @p_err_code = 0 BEGIN SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE(); END  -- lỗi runtime → OUT, KHÔNG THROW
    END CATCH
END
GO

/*===========================================================================
  US2 — SP_GET_MASTER_OVERVIEW : tổng quan 1 master (snapshot + hiệu suất kỳ)
    RS1 (1 dòng): info + AUM/growth + net in/out + AUM-weighted TE+badge+#vượt
                  + cash drag+#vượt + deviation+#vượt A/B.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_MASTER_OVERVIEW
    @p_master_code VARCHAR(20),
    @p_range         VARCHAR(20) = 'INCEPTION',
    @p_dev_threshold_high DECIMAL(10,2) = 100.00,   -- A (>A = vượt trội); default +100 BPS, caller truyền override
    @p_dev_threshold_low  DECIMAL(10,2) = -100.00,  -- B (<B = tụt); default -100 BPS
    @p_user        VARCHAR(64)   = NULL,
    @p_err_code    INT           OUTPUT,
    @p_err_msg     NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @p_err_code = 0; SET @p_err_msg = NULL;
    BEGIN TRY
    IF NOT EXISTS (SELECT 1 FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE = @p_master_code)
        BEGIN SET @p_err_code = 1; SET @p_err_msg = N'Master not found'; RAISERROR(@p_err_msg, 16, 1); END

    -- cấu hình ngưỡng hiệu lực (TE/cash-drag từ config). Deviation A/B nhận thẳng từ tham số (default tại SP).
    DECLARE @teLow DECIMAL(10,6), @teHigh DECIMAL(10,6), @teAlert DECIMAL(10,6), @cdThr DECIMAL(9,6);
    SELECT @teLow=C_TE_BADGE_LOW, @teHigh=C_TE_BADGE_HIGH, @teAlert=C_TE_ALERT_THRESHOLD,
           @cdThr=C_CASH_DRAG_THRESHOLD
    FROM dbo.UDF_PM_CONFIG(@p_master_code);
    DECLARE @devHi DECIMAL(10,2) = @p_dev_threshold_high, @devLo DECIMAL(10,2) = @p_dev_threshold_low;

    -- khung ngày (hiệu suất T-1)
    DECLARE @end DATE, @cutoff DATE, @base DATE, @X INT;
    DECLARE @first DATE;
    -- khung ngày: 1 read gộp MAX(cuối)+MIN(đầu) → fallback dùng @first (bỏ read MIN lần 3).
    SELECT @end = MAX(C_BUSINESS_DATE), @first = MIN(C_BUSINESS_DATE)
    FROM T_MASTER_BALANCE WHERE C_MASTER_CODE=@p_master_code;
    SET @cutoff = dbo.UDF_RANGE_CUTOFF(@end, @p_range);
    SELECT @base = MAX(C_BUSINESS_DATE) FROM T_MASTER_BALANCE
     WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE <= @cutoff;
    IF @base IS NULL SET @base = @first;

    -- master index return kỳ (PR) + #ngày GD (cap 252)
    DECLARE @idxBase DECIMAL(18,6), @idxEnd DECIMAL(18,6), @rMaster DECIMAL(18,10);
    -- index @base + @end trong 1 read (IN(base,end) + pivot CASE) thay vì 2 point-read.
    SELECT @idxBase = MAX(CASE WHEN C_BUSINESS_DATE=@base THEN C_INDEX_VALUE END),
           @idxEnd  = MAX(CASE WHEN C_BUSINESS_DATE=@end  THEN C_INDEX_VALUE END)
    FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE IN (@base,@end);
    SET @rMaster = CASE WHEN @idxBase IS NULL OR @idxBase=0 THEN NULL ELSE @idxEnd/@idxBase - 1 END;
    SELECT @X = COUNT(DISTINCT C_BUSINESS_DATE) FROM T_MASTER_INDEX_DAILY
     WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE > @base AND C_BUSINESS_DATE <= @end;
    IF @X > 252 SET @X = 252;

    -- AUM hiện tại + tiền (tầng master — KHÔNG SUM SI)
    DECLARE @aumNow DECIMAL(20,0), @tienNow DECIMAL(20,0), @nKH INT;
    SELECT @aumNow = C_AUM, @nKH = C_TOTAL_ACCOUNT,
           @tienNow = C_CASH
    FROM T_MASTER_CURRENT WHERE C_MASTER_CODE=@p_master_code;

    -- net in/out (base,end] + AUM @base: 1 read T_MASTER_BALANCE [base,end] (gộp 2 read cũ).
    DECLARE @cashIn DECIMAL(20,0), @cashOut DECIMAL(20,0), @aumBase DECIMAL(20,0);
    SELECT @cashIn  = ISNULL(SUM(CASE WHEN C_BUSINESS_DATE > @base THEN C_CASH_IN  END),0),
           @cashOut = ISNULL(SUM(CASE WHEN C_BUSINESS_DATE > @base THEN C_CASH_OUT END),0),
           @aumBase = MAX(CASE WHEN C_BUSINESS_DATE = @base THEN C_AUM END)
    FROM T_MASTER_BALANCE
    WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE >= @base AND C_BUSINESS_DATE <= @end;

    -- per-KH (end-weight): AUM/tien từ CURRENT; return Rᵢ = COMPOUND daily_return qua (base,end] (QUÉT NAV_BALANCE);
    --   TE prefix-sum từ 2 LÁT NAV_BALANCE (@base,@end) — hiệu accum 2 mốc (KHÔNG quét ngày giữa). KH join sau base → accum_base=0.
    CREATE TABLE #kh (si VARCHAR(20), aum DECIMAL(20,6), r DECIMAL(18,10), tien DECIMAL(20,0),
                      car_b FLOAT, car2_b FLOAT, n_b INT, car_e FLOAT, car2_e FLOAT, n_e INT, te FLOAT);

    INSERT #kh (si, aum, r, tien, car_b,car2_b,n_b, car_e,car2_e,n_e)
    SELECT nc.C_SI_ACCOUNT, nc.C_LAST_AUM, 0,
           nc.C_CASH, 0,0,0, 0,0,0
    FROM T_SI_CURRENT nc
    WHERE nc.C_MASTER_CODE=@p_master_code AND nc.C_STATUS='ACTIVE';

    -- Rᵢ = ∏(1+daily_return) qua (base,end] = EXP(Σ ln(1+r))−1 ; bỏ ngày return NULL ; KH không có ngày nào ⇒ r=0
    UPDATE k SET r = c.R
    FROM #kh k INNER JOIN (
        SELECT b.C_SI_ACCOUNT AS si, EXP(SUM(LOG(1.0 + b.C_DAILY_RETURN))) - 1 AS R
        FROM T_SI_BALANCE b
        WHERE b.C_MASTER_CODE=@p_master_code AND b.C_BUSINESS_DATE>@base AND b.C_BUSINESS_DATE<=@end
          AND b.C_DAILY_RETURN IS NOT NULL
        GROUP BY b.C_SI_ACCOUNT
    ) c ON c.si=k.si;

    -- lát @end: accum active đến cuối kỳ
    UPDATE k SET car_e=e.C_ACCUM_ACTIVE_RET, car2_e=e.C_ACCUM_ACTIVE_RET_SQ, n_e=e.C_RET_DAY_COUNT
    FROM #kh k INNER JOIN T_SI_BALANCE e
      ON e.C_MASTER_CODE=@p_master_code AND e.C_BUSINESS_DATE=@end AND e.C_SI_ACCOUNT=k.si;

    -- lát @base: accum active đến base (thiếu lát ⇒ KH join sau base ⇒ accum_base=0)
    UPDATE k SET car_b=b.C_ACCUM_ACTIVE_RET, car2_b=b.C_ACCUM_ACTIVE_RET_SQ, n_b=b.C_RET_DAY_COUNT
    FROM #kh k INNER JOIN T_SI_BALANCE b
      ON b.C_MASTER_CODE=@p_master_code AND b.C_BUSINESS_DATE=@base AND b.C_SI_ACCOUNT=k.si;

    -- TE per KH = STDEV(active) prefix-sum (hiệu base→end) × √min(n,252)
    UPDATE #kh SET te = CASE WHEN (n_e-n_b) >= 2 THEN
        SQRT(CASE WHEN ((car2_e-car2_b) - (car_e-car_b)*(car_e-car_b)/(n_e-n_b))/((n_e-n_b)-1) < 0 THEN 0
                  ELSE ((car2_e-car2_b) - (car_e-car_b)*(car_e-car_b)/(n_e-n_b))/((n_e-n_b)-1) END)
        * SQRT(CASE WHEN (n_e-n_b) > 252 THEN 252 ELSE (n_e-n_b) END)
      END;

    -- tổng hợp per-KH (AUM-weighted): KH return (compound) + TE. (master AUM = @aumNow tầng master, KHÔNG SUM SI.)
    DECLARE @wRet DECIMAL(18,10), @wTE FLOAT;
    SELECT @wRet = SUM(r * aum) / NULLIF(SUM(aum),0)
    FROM #kh;
    SELECT @wTE = SUM(CASE WHEN te IS NOT NULL THEN te * aum END) / NULLIF(SUM(CASE WHEN te IS NOT NULL THEN aum END),0)
    FROM #kh;

    DECLARE @nTEover INT, @nCashOver INT, @nDevHi INT, @nDevLo INT;
    SELECT @nTEover   = COUNT(CASE WHEN te > @teAlert THEN 1 END),
           @nCashOver = COUNT(CASE WHEN aum > 0 AND tien*1.0/aum > @cdThr THEN 1 END),
           @nDevHi    = COUNT(CASE WHEN (r - @rMaster)*10000 > @devHi THEN 1 END),
           @nDevLo    = COUNT(CASE WHEN (r - @rMaster)*10000 < @devLo THEN 1 END)
    FROM #kh;

    SELECT  mp.C_MASTER_CODE, mp.C_MASTER_NAME, mp.C_STATUS, mp.C_INCEPTION_DATE, mp.C_BENCHMARK_CODE,
            @p_range AS C_RANGE, @base AS C_BASE_DATE, @end AS C_END_DATE, @X AS C_TRADING_DAYS,
            @nKH AS C_TOTAL_ACCOUNT,
            @aumNow AS C_AUM, @aumBase AS C_AUM_BASE,
            CASE WHEN @aumBase IS NULL OR @aumBase=0 THEN NULL
                 ELSE CAST(@aumNow*1.0/@aumBase - 1 AS DECIMAL(18,6)) END AS C_AUM_GROWTH_PCT,
            @cashIn AS C_NET_IN, @cashOut AS C_NET_OUT, (@cashIn-@cashOut) AS C_NET_FLOW,
            CAST(CASE WHEN @aumNow=0 THEN NULL ELSE @tienNow*1.0/@aumNow END AS DECIMAL(9,6)) AS C_CASH_DRAG,
            @nCashOver AS C_CNT_CASH_DRAG_OVER,
            CAST(@wRet AS DECIMAL(18,6))    AS C_KH_RETURN_AUMW,
            CAST(@rMaster AS DECIMAL(18,6)) AS C_MASTER_RETURN,
            CAST((@wRet - @rMaster)*10000 AS DECIMAL(12,2)) AS C_DEVIATION_BPS,
            @nDevHi AS C_CNT_DEV_OVER_HIGH, @nDevLo AS C_CNT_DEV_UNDER_LOW,
            CAST(@wTE AS DECIMAL(12,6)) AS C_TE_AUMW,
            CASE WHEN @wTE IS NULL THEN NULL
                 WHEN @wTE < @teLow THEN 'LOW' WHEN @wTE < @teHigh THEN 'MED' ELSE 'HIGH' END AS C_TE_BADGE,
            @nTEover AS C_CNT_TE_OVER
    FROM T_MASTER_PORTFOLIO mp WHERE mp.C_MASTER_CODE=@p_master_code;

    DROP TABLE #kh;
    END TRY
    BEGIN CATCH
        IF @p_err_code = 0 BEGIN SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE(); END  -- lỗi runtime → OUT, KHÔNG THROW
    END CATCH
END
GO

/*===========================================================================
  US3 — SP_GET_MASTER_PERFORMANCE : chart 3 đường + mốc rebalance
    RS1 chuỗi theo @p_resolution (D/W/M, NULL=auto theo độ dài kỳ):
        C_MASTER_INDEX (PR) | C_KH_COMPOSITE (COMPOUND master AUM-weighted daily return, base=1.0) | C_BENCHMARK (PR)
        → App rebase cả 3 về 0% tại điểm đầu.
    RS2: mốc rebalance (effective_date của T_MASTER_PORTFOLIO_TICKER trong kỳ).
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_MASTER_PERFORMANCE
    @p_master_code VARCHAR(20),
    @p_range         VARCHAR(20) = '1Y',
    @p_resolution    VARCHAR(2)  = NULL,   -- 'D'|'W'|'M' ; NULL = auto
    @p_user        VARCHAR(64)   = NULL,
    @p_err_code    INT           OUTPUT,
    @p_err_msg     NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @p_err_code = 0; SET @p_err_msg = NULL;
    BEGIN TRY
    IF NOT EXISTS (SELECT 1 FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE = @p_master_code)
        BEGIN SET @p_err_code = 1; SET @p_err_msg = N'Master not found'; RAISERROR(@p_err_msg, 16, 1); END

    DECLARE @bench VARCHAR(20) = (SELECT C_BENCHMARK_CODE FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE=@p_master_code);
    DECLARE @end DATE, @cutoff DATE, @base DATE;
    DECLARE @first DATE;
    -- khung ngày: 1 read gộp MAX(cuối)+MIN(đầu) → fallback dùng @first (bỏ read MIN lần 3).
    SELECT @end = MAX(C_BUSINESS_DATE), @first = MIN(C_BUSINESS_DATE)
    FROM T_MASTER_BALANCE WHERE C_MASTER_CODE=@p_master_code;
    SET @cutoff = dbo.UDF_RANGE_CUTOFF(@end, @p_range);
    SELECT @base = MAX(C_BUSINESS_DATE) FROM T_MASTER_BALANCE
     WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE <= @cutoff;
    IF @base IS NULL SET @base = @first;

    -- auto resolution theo độ dài kỳ
    IF @p_resolution IS NULL
        SET @p_resolution = CASE WHEN DATEDIFF(DAY,@base,@end) > 90 THEN 'M'
                               WHEN DATEDIFF(DAY,@base,@end) > 21 THEN 'W' ELSE 'D' END;

    -- [thin-layer] Composite KH = chuỗi COMPOUND master AUM-weighted daily return (T_MASTER_BALANCE.C_DAILY_RETURN).
    --   C_KH_COMPOSITE @d = ∏(1+master_daily_return) qua (base,d] ; base=1.0 (App rebase về 0% tại điểm đầu).
    --   (engine đã ghi C_DAILY_RETURN master = AUM-weighted Σ(AUMᵢ·rᵢ)/ΣAUMᵢ ⇒ KHÔNG cần per-KW unit_price nữa.)

    -- sample dates theo resolution (luôn gồm base & end)
    DECLARE @samp TABLE (d DATE PRIMARY KEY);
    ;WITH dd AS (
        SELECT DISTINCT C_BUSINESS_DATE bd FROM T_MASTER_BALANCE
        WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE BETWEEN @base AND @end
    ), bucketed AS (
        SELECT bd,
               CASE @p_resolution
                    WHEN 'M' THEN ROW_NUMBER() OVER (PARTITION BY YEAR(bd),MONTH(bd) ORDER BY bd DESC)
                    WHEN 'W' THEN ROW_NUMBER() OVER (PARTITION BY DATEPART(YEAR,bd),DATEPART(ISO_WEEK,bd) ORDER BY bd DESC)
                    ELSE 1 END AS rn
        FROM dd
    )
    INSERT @samp(d)
    SELECT bd FROM bucketed WHERE @p_resolution='D' OR rn=1
    UNION SELECT @base UNION SELECT @end;

    -- RS1 chuỗi. Composite KH = COMPOUND master daily return từ base: tại mỗi sample-date d,
    --   kc(d) = ∏(1+master_daily_return) qua (base,d] (base=1.0 vì khoảng rỗng). Bỏ ngày return NULL.
    --   1 read T_MASTER_BALANCE [base,end] → mỗi sample-date d gộp các daily return ≤ d (running-compound set-based).
    ;WITH mret AS (
        SELECT C_BUSINESS_DATE AS d, C_DAILY_RETURN AS r
        FROM T_MASTER_BALANCE
        WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE>@base AND C_BUSINESS_DATE<=@end
          AND C_DAILY_RETURN IS NOT NULL
    ), comp AS (
        SELECT s.d AS d,
               CAST(EXP(ISNULL(SUM(LOG(1.0 + mret.r)),0)) AS DECIMAL(18,8)) AS kc
        FROM @samp s
        LEFT JOIN mret ON mret.d > @base AND mret.d <= s.d
        GROUP BY s.d
    )
    SELECT  s.d AS C_BUSINESS_DATE,
            idx.C_INDEX_VALUE AS C_MASTER_INDEX,
            bm.C_INDEX_VALUE  AS C_BENCHMARK,
            comp.kc           AS C_KH_COMPOSITE
    FROM @samp s
    LEFT JOIN comp ON comp.d = s.d
    LEFT JOIN T_MASTER_INDEX_DAILY idx ON idx.C_MASTER_CODE=@p_master_code AND idx.C_BUSINESS_DATE=s.d
    LEFT JOIN T_BENCHMARK_DAILY    bm  ON bm.C_BENCHMARK_CODE=@bench AND bm.C_BUSINESS_DATE=s.d
    ORDER BY s.d;

    -- RS2 mốc rebalance trong kỳ
    SELECT DISTINCT C_EFFECTIVE_DATE
    FROM T_MASTER_PORTFOLIO_TICKER
    WHERE C_MASTER_CODE=@p_master_code AND C_EFFECTIVE_DATE BETWEEN @base AND @end
    ORDER BY C_EFFECTIVE_DATE;
    END TRY
    BEGIN CATCH
        IF @p_err_code = 0 BEGIN SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE(); END  -- lỗi runtime → OUT, KHÔNG THROW
    END CATCH
END
GO

/*===========================================================================
  US3 click — SP_GET_MASTER_REBALANCE_DETAIL : chi tiết 1 mốc rebalance
    RS1: target weight cũ→mới per mã (T_MASTER_PORTFOLIO_TICKER).
    RS2: net delta holdings thực tế per mã (T_MASTER_HOLDING_BALANCE @date vs phiên trước).
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_MASTER_REBALANCE_DETAIL
    @p_master_code VARCHAR(20),
    @p_date          DATE,
    @p_user        VARCHAR(64)   = NULL,
    @p_err_code    INT           OUTPUT,
    @p_err_msg     NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @p_err_code = 0; SET @p_err_msg = NULL;
    BEGIN TRY

    -- DATE GUARD (err=5): master phải có rebalance/holdings ≤ @p_date (chặn ngày trước inception / master chưa có data).
    IF NOT EXISTS (SELECT 1 FROM T_MASTER_PORTFOLIO_TICKER WHERE C_MASTER_CODE=@p_master_code AND C_EFFECTIVE_DATE<=@p_date)
       AND NOT EXISTS (SELECT 1 FROM T_MASTER_HOLDING_BALANCE WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE<=@p_date)
    BEGIN SET @p_err_code=5;
        SET @p_err_msg = CONCAT(N'Không có dữ liệu rebalance/holdings cho master ', @p_master_code, N' ≤ ',
            CONVERT(VARCHAR(10),@p_date,23), N' (ngày trước inception / master không tồn tại).');
        RAISERROR(@p_err_msg, 16, 1); END

    -- effective_date hiệu lực = lớn nhất ≤ @p_date ; kỳ trước = lớn nhất < eff
    DECLARE @eff DATE, @prevEff DATE;
    SELECT @eff = MAX(C_EFFECTIVE_DATE) FROM T_MASTER_PORTFOLIO_TICKER
     WHERE C_MASTER_CODE=@p_master_code AND C_EFFECTIVE_DATE <= @p_date;
    SELECT @prevEff = MAX(C_EFFECTIVE_DATE) FROM T_MASTER_PORTFOLIO_TICKER
     WHERE C_MASTER_CODE=@p_master_code AND C_EFFECTIVE_DATE < @eff;

    -- RS1: weight cũ → mới (full outer: mã ra/vào danh mục)
    SELECT  COALESCE(n.C_TICKER, o.C_TICKER) AS C_TICKER,
            o.C_TARGET_WEIGHT AS C_WEIGHT_OLD,
            n.C_TARGET_WEIGHT AS C_WEIGHT_NEW,
            COALESCE(n.C_TARGET_WEIGHT,0) - COALESCE(o.C_TARGET_WEIGHT,0) AS C_WEIGHT_DELTA
    FROM        (SELECT C_TICKER,C_TARGET_WEIGHT FROM T_MASTER_PORTFOLIO_TICKER
                 WHERE C_MASTER_CODE=@p_master_code AND C_EFFECTIVE_DATE=@eff) n
    FULL OUTER JOIN (SELECT C_TICKER,C_TARGET_WEIGHT FROM T_MASTER_PORTFOLIO_TICKER
                 WHERE C_MASTER_CODE=@p_master_code AND C_EFFECTIVE_DATE=@prevEff) o
      ON o.C_TICKER=n.C_TICKER
    ORDER BY ABS(COALESCE(n.C_TARGET_WEIGHT,0)-COALESCE(o.C_TARGET_WEIGHT,0)) DESC;

    -- RS2: net delta holdings thực tế quanh @eff (phiên có holdings ≤ eff vs phiên ngay trước)
    DECLARE @hd DATE, @hdPrev DATE;
    SELECT @hd = MAX(C_BUSINESS_DATE) FROM T_MASTER_HOLDING_BALANCE
     WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE <= @eff;
    SELECT @hdPrev = MAX(C_BUSINESS_DATE) FROM T_MASTER_HOLDING_BALANCE
     WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE < @hd;

    SELECT  COALESCE(c.C_TICKER, p.C_TICKER) AS C_TICKER,
            ISNULL(p.C_QUANTITY,0) AS C_QTY_PREV,
            ISNULL(c.C_QUANTITY,0) AS C_QTY_NEW,
            ISNULL(c.C_QUANTITY,0) - ISNULL(p.C_QUANTITY,0) AS C_QTY_DELTA,
            c.C_WEIGHT AS C_WEIGHT_ACTUAL
    FROM        (SELECT C_TICKER,C_QUANTITY,C_WEIGHT FROM T_MASTER_HOLDING_BALANCE
                 WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE=@hd) c
    FULL OUTER JOIN (SELECT C_TICKER,C_QUANTITY FROM T_MASTER_HOLDING_BALANCE
                 WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE=@hdPrev) p
      ON p.C_TICKER=c.C_TICKER
    ORDER BY ABS(ISNULL(c.C_QUANTITY,0)-ISNULL(p.C_QUANTITY,0)) DESC;
    END TRY
    BEGIN CATCH
        IF @p_err_code = 0 BEGIN SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE(); END  -- lỗi runtime → OUT, KHÔNG THROW
    END CATCH
END
GO

/*===========================================================================
  US4 — SP_GET_MASTER_PNL_DIST : phân phối lãi/lỗ DM KH (TWR per-KH)
    RS1 (1 dòng): #lãi/#lỗ/#flat + tỷ lệ + avg AUM-weighted + trung vị.
    RS2: histogram buckets %PnL.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_MASTER_PNL_DIST
    @p_master_code VARCHAR(20),
    @p_range         VARCHAR(20) = 'INCEPTION',
    @p_user        VARCHAR(64)   = NULL,
    @p_err_code    INT           OUTPUT,
    @p_err_msg     NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @p_err_code = 0; SET @p_err_msg = NULL;
    BEGIN TRY
    IF NOT EXISTS (SELECT 1 FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE = @p_master_code)
        BEGIN SET @p_err_code = 1; SET @p_err_msg = N'Master not found'; RAISERROR(@p_err_msg, 16, 1); END

    DECLARE @end DATE, @cutoff DATE, @base DATE;
    DECLARE @first DATE;
    -- khung ngày: 1 read gộp MAX(cuối)+MIN(đầu) → fallback dùng @first (bỏ read MIN lần 3).
    SELECT @end = MAX(C_BUSINESS_DATE), @first = MIN(C_BUSINESS_DATE)
    FROM T_MASTER_BALANCE WHERE C_MASTER_CODE=@p_master_code;
    SET @cutoff = dbo.UDF_RANGE_CUTOFF(@end, @p_range);
    SELECT @base = MAX(C_BUSINESS_DATE) FROM T_MASTER_BALANCE
     WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE <= @cutoff;
    IF @base IS NULL SET @base = @first;

    -- per-KH %PnL = COMPOUND daily_return qua (base,end] = EXP(Σ ln(1+r))−1 ; KH không có ngày return ⇒ 0
    CREATE TABLE #p (si VARCHAR(20), aum DECIMAL(20,6), pnl DECIMAL(18,10));
    INSERT #p (si, aum, pnl)
    SELECT nc.C_SI_ACCOUNT, nc.C_LAST_AUM, ISNULL(c.R, 0)
    FROM T_SI_CURRENT nc
    LEFT JOIN (
        SELECT b.C_SI_ACCOUNT AS si, EXP(SUM(LOG(1.0 + b.C_DAILY_RETURN))) - 1 AS R
        FROM T_SI_BALANCE b
        WHERE b.C_MASTER_CODE=@p_master_code AND b.C_BUSINESS_DATE>@base AND b.C_BUSINESS_DATE<=@end
          AND b.C_DAILY_RETURN IS NOT NULL
        GROUP BY b.C_SI_ACCOUNT
    ) c ON c.si=nc.C_SI_ACCOUNT
    WHERE nc.C_MASTER_CODE=@p_master_code AND nc.C_STATUS='ACTIVE';

    -- RS1 summary
    SELECT  @p_master_code AS C_MASTER_CODE, @p_range AS C_RANGE, @base AS C_BASE_DATE, @end AS C_END_DATE,
            COUNT(*)                                   AS C_TOTAL_KH,
            COUNT(CASE WHEN pnl > 0 THEN 1 END)        AS C_CNT_GAIN,
            COUNT(CASE WHEN pnl < 0 THEN 1 END)        AS C_CNT_LOSS,
            COUNT(CASE WHEN pnl = 0 THEN 1 END)        AS C_CNT_FLAT,
            CAST(COUNT(CASE WHEN pnl > 0 THEN 1 END)*1.0/NULLIF(COUNT(*),0) AS DECIMAL(9,6)) AS C_PCT_GAIN,
            CAST(COUNT(CASE WHEN pnl < 0 THEN 1 END)*1.0/NULLIF(COUNT(*),0) AS DECIMAL(9,6)) AS C_PCT_LOSS,
            CAST(SUM(pnl*aum)/NULLIF(SUM(aum),0) AS DECIMAL(18,6)) AS C_AVG_PNL_AUMW,
            CAST((SELECT DISTINCT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pnl) OVER () FROM #p) AS DECIMAL(18,6)) AS C_MEDIAN_PNL
    FROM #p;

    -- RS2 histogram (bucket %PnL)
    SELECT bk.C_BUCKET, bk.C_SORT, COUNT(p.si) AS C_CNT
    FROM (VALUES ('<-20%',1,CAST(-9.99 AS DECIMAL(9,4)),CAST(-0.20 AS DECIMAL(9,4))),
                 ('-20..-10%',2,-0.20,-0.10),
                 ('-10..0%',3,-0.10,0.0),
                 ('0..10%',4,0.0,0.10),
                 ('10..20%',5,0.10,0.20),
                 ('20..30%',6,0.20,0.30),
                 ('>=30%',7,0.30,9.99)) bk(C_BUCKET,C_SORT,lo,hi)
    LEFT JOIN #p p ON (p.pnl >= bk.lo AND p.pnl < bk.hi)
                   OR (bk.C_SORT=7 AND p.pnl >= bk.lo)
    GROUP BY bk.C_BUCKET, bk.C_SORT
    ORDER BY bk.C_SORT;

    DROP TABLE #p;
    END TRY
    BEGIN CATCH
        IF @p_err_code = 0 BEGIN SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE(); END  -- lỗi runtime → OUT, KHÔNG THROW
    END CATCH
END
GO

/*===========================================================================
  US5 — SP_GET_MASTER_TOP_KH : top-N KH theo %PnL (TWR)
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_MASTER_TOP_KH
    @p_master_code VARCHAR(20),
    @p_range         VARCHAR(20) = 'INCEPTION',
    @p_topn          INT = 20,
    @p_dir           VARCHAR(4) = 'DESC',   -- DESC = top lãi ; ASC = top lỗ
    @p_user        VARCHAR(64)   = NULL,
    @p_err_code    INT           OUTPUT,
    @p_err_msg     NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @p_err_code = 0; SET @p_err_msg = NULL;
    BEGIN TRY
    IF NOT EXISTS (SELECT 1 FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE = @p_master_code)
        BEGIN SET @p_err_code = 1; SET @p_err_msg = N'Master not found'; RAISERROR(@p_err_msg, 16, 1); END

    DECLARE @end DATE, @cutoff DATE, @base DATE;
    DECLARE @first DATE;
    -- khung ngày: 1 read gộp MAX(cuối)+MIN(đầu) → fallback dùng @first (bỏ read MIN lần 3).
    SELECT @end = MAX(C_BUSINESS_DATE), @first = MIN(C_BUSINESS_DATE)
    FROM T_MASTER_BALANCE WHERE C_MASTER_CODE=@p_master_code;
    SET @cutoff = dbo.UDF_RANGE_CUTOFF(@end, @p_range);
    SELECT @base = MAX(C_BUSINESS_DATE) FROM T_MASTER_BALANCE
     WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE <= @cutoff;
    IF @base IS NULL SET @base = @first;

    -- per-KH %PnL = COMPOUND daily_return qua (base,end] = EXP(Σ ln(1+r))−1 ; KH không có ngày return ⇒ 0.
    -- [thin-layer] BỎ cột C_UNIT_PRICE_BASE/END (không còn unit price). C_PNL_PCT = compound TWR.
    ;WITH k AS (
        SELECT nc.C_CUST_CODE, nc.C_SI_ACCOUNT,
               nc.C_LAST_AUM AS C_AUM,
               CAST(ISNULL(c.R,0) AS DECIMAL(18,6)) AS C_PNL_PCT
        FROM T_SI_CURRENT nc
        LEFT JOIN (
            SELECT b.C_SI_ACCOUNT AS si, EXP(SUM(LOG(1.0 + b.C_DAILY_RETURN))) - 1 AS R
            FROM T_SI_BALANCE b
            WHERE b.C_MASTER_CODE=@p_master_code AND b.C_BUSINESS_DATE>@base AND b.C_BUSINESS_DATE<=@end
              AND b.C_DAILY_RETURN IS NOT NULL
            GROUP BY b.C_SI_ACCOUNT
        ) c ON c.si=nc.C_SI_ACCOUNT
        WHERE nc.C_MASTER_CODE=@p_master_code AND nc.C_STATUS='ACTIVE'
    )
    SELECT TOP (@p_topn) C_CUST_CODE, C_SI_ACCOUNT, C_AUM, C_PNL_PCT
    FROM k
    ORDER BY CASE WHEN @p_dir='ASC' THEN C_PNL_PCT END ASC,
             CASE WHEN @p_dir<>'ASC' THEN C_PNL_PCT END DESC;
    END TRY
    BEGIN CATCH
        IF @p_err_code = 0 BEGIN SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE(); END  -- lỗi runtime → OUT, KHÔNG THROW
    END CATCH
END
GO

/*===========================================================================
  US1 — SP_GET_PM_OVERVIEW_ALL : tổng quan TẤT CẢ master (toàn hệ)
    RS1 header: #master ACTIVE, Σ#KH.
    RS2 tổng: ΣAUM + growth (vs base mỗi master), Σ net in/out, cash drag toàn hệ, #master cash>ngưỡng.
    RS3 list master: AUM/#KH/master return/KH AUM-weighted return/deviation/AUM-weighted TE/cash drag.
        @p_sort: AUM|RET|DEV|TE|CASH (mặc định AUM desc).
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_PM_OVERVIEW_ALL
    @p_range VARCHAR(20) = 'INCEPTION',
    @p_sort  VARCHAR(8)  = 'AUM',
    @p_user        VARCHAR(64)   = NULL,
    @p_err_code    INT           OUTPUT,
    @p_err_msg     NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @p_err_code = 0; SET @p_err_msg = NULL;
    BEGIN TRY

    -- khung ngày per-master (ACTIVE)
    CREATE TABLE #md (m VARCHAR(20), dend DATE, dfirst DATE, dcut DATE, dbase DATE,
                      idxBase DECIMAL(18,6), idxEnd DECIMAL(18,6), X INT);
    -- dend(MAX)+dfirst(MIN) trong 1 pass → dbase fallback dùng dfirst (bỏ subquery MIN).
    INSERT #md (m, dend, dfirst)
    SELECT mp.C_MASTER_CODE, MAX(b.C_BUSINESS_DATE), MIN(b.C_BUSINESS_DATE)
    FROM T_MASTER_PORTFOLIO mp
    INNER JOIN T_MASTER_BALANCE b ON b.C_MASTER_CODE=mp.C_MASTER_CODE
    WHERE mp.C_STATUS='ACTIVE'
    GROUP BY mp.C_MASTER_CODE;

    UPDATE #md SET dcut = dbo.UDF_RANGE_CUTOFF(dend, @p_range);
    UPDATE m SET dbase = COALESCE(
        (SELECT MAX(C_BUSINESS_DATE) FROM T_MASTER_BALANCE
          WHERE C_MASTER_CODE=m.m AND C_BUSINESS_DATE<=m.dcut),
        m.dfirst)
    FROM #md m;
    -- index @base/@end + #ngày GD: 1 read/master (1 pass [dbase,dend]) thay vì 3 subquery.
    -- flag (isBase/isEnd/aft) tính ở lớp con z để aggregate KHÔNG chứa outer-ref (tránh lỗi 8124).
    UPDATE m SET idxBase = x.idxBase, idxEnd = x.idxEnd,
                 X = CASE WHEN x.nd > 252 THEN 252 ELSE x.nd END
    FROM #md m
    CROSS APPLY (
        SELECT MAX(CASE WHEN isBase=1 THEN v END) AS idxBase,
               MAX(CASE WHEN isEnd=1  THEN v END) AS idxEnd,
               COUNT(DISTINCT CASE WHEN aft=1 THEN d END) AS nd
        FROM (SELECT C_INDEX_VALUE AS v, C_BUSINESS_DATE AS d,
                     CASE WHEN C_BUSINESS_DATE=m.dbase THEN 1 ELSE 0 END AS isBase,
                     CASE WHEN C_BUSINESS_DATE=m.dend  THEN 1 ELSE 0 END AS isEnd,
                     CASE WHEN C_BUSINESS_DATE>m.dbase THEN 1 ELSE 0 END AS aft
              FROM T_MASTER_INDEX_DAILY
              WHERE C_MASTER_CODE=m.m AND C_BUSINESS_DATE>=m.dbase AND C_BUSINESS_DATE<=m.dend) z
    ) x;

    -- per-KH metrics (toàn hệ): AUM/tien từ CURRENT; return Rᵢ = COMPOUND daily_return qua (dbase,dend] theo master của KH;
    --   TE prefix-sum đọc 2 LÁT (@base/@end theo master) — KHÔNG quét toàn lịch sử (TE), nhưng return QUÉT (dbase,dend].
    CREATE TABLE #kh (m VARCHAR(20), si VARCHAR(20), aum DECIMAL(20,6),
                      r DECIMAL(18,10), tien DECIMAL(20,0),
                      car_b FLOAT, car2_b FLOAT, n_b INT, car_e FLOAT, car2_e FLOAT, n_e INT, te FLOAT);
    INSERT #kh (m, si, aum, r, tien, car_b,car2_b,n_b, car_e,car2_e,n_e)
    SELECT nc.C_MASTER_CODE, nc.C_SI_ACCOUNT, nc.C_LAST_AUM,
           0, nc.C_CASH, 0,0,0, 0,0,0
    FROM T_SI_CURRENT nc
    INNER JOIN #md d ON d.m=nc.C_MASTER_CODE
    WHERE nc.C_STATUS='ACTIVE';

    -- return Rᵢ = ∏(1+daily_return) qua (dbase,dend] theo master của KH ; bỏ ngày NULL ; KH không có ngày ⇒ r=0
    -- (set-based compound per master×si: quét lát (dbase,dend] mỗi master)
    UPDATE k SET r = c.R
    FROM #kh k
    INNER JOIN (
        SELECT b.C_MASTER_CODE AS m, b.C_SI_ACCOUNT AS si,
               EXP(SUM(LOG(1.0 + b.C_DAILY_RETURN))) - 1 AS R
        FROM T_SI_BALANCE b
        INNER JOIN #md d ON d.m=b.C_MASTER_CODE
        WHERE b.C_BUSINESS_DATE > d.dbase AND b.C_BUSINESS_DATE <= d.dend
          AND b.C_DAILY_RETURN IS NOT NULL
        GROUP BY b.C_MASTER_CODE, b.C_SI_ACCOUNT
    ) c ON c.m=k.m AND c.si=k.si;

    -- lát @end per master (accum active đến cuối kỳ)
    UPDATE k SET car_e=e.C_ACCUM_ACTIVE_RET, car2_e=e.C_ACCUM_ACTIVE_RET_SQ, n_e=e.C_RET_DAY_COUNT
    FROM #kh k INNER JOIN #md d ON d.m=k.m
    INNER JOIN T_SI_BALANCE e ON e.C_MASTER_CODE=k.m AND e.C_BUSINESS_DATE=d.dend AND e.C_SI_ACCOUNT=k.si;

    -- lát @base per master (accum active đến base; thiếu lát ⇒ accum_base=0)
    UPDATE k SET car_b=b.C_ACCUM_ACTIVE_RET, car2_b=b.C_ACCUM_ACTIVE_RET_SQ, n_b=b.C_RET_DAY_COUNT
    FROM #kh k INNER JOIN #md d ON d.m=k.m
    INNER JOIN T_SI_BALANCE b ON b.C_MASTER_CODE=k.m AND b.C_BUSINESS_DATE=d.dbase AND b.C_SI_ACCOUNT=k.si;

    -- TE per KH = STDEV(active) prefix-sum (hiệu base→end) × √min(n,252)
    UPDATE #kh SET te = CASE WHEN (n_e-n_b) >= 2 THEN
        SQRT(CASE WHEN ((car2_e-car2_b) - (car_e-car_b)*(car_e-car_b)/(n_e-n_b))/((n_e-n_b)-1) < 0 THEN 0
                  ELSE ((car2_e-car2_b) - (car_e-car_b)*(car_e-car_b)/(n_e-n_b))/((n_e-n_b)-1) END)
        * SQRT(CASE WHEN (n_e-n_b) > 252 THEN 252 ELSE (n_e-n_b) END)
      END;

    -- per-master rollup
    DECLARE @cdThrDefault DECIMAL(9,6) = 0.05;
    -- #mr KHÔNG giữ master AUM (lấy mc.C_AUM tầng master ở RS2/RS3, không SUM SI).
    CREATE TABLE #mr (m VARCHAR(20), wret FLOAT, rmaster FLOAT,
                      wte FLOAT, cashdrag FLOAT, cdThr DECIMAL(9,6));
    INSERT #mr (m, wret, wte)
    SELECT k.m,
           SUM(k.r*k.aum)/NULLIF(SUM(k.aum),0),
           SUM(CASE WHEN k.te IS NOT NULL THEN k.te*k.aum END)/NULLIF(SUM(CASE WHEN k.te IS NOT NULL THEN k.aum END),0)
    FROM #kh k GROUP BY k.m;
    UPDATE r SET rmaster = CASE WHEN d.idxBase IS NULL OR d.idxBase=0 THEN NULL ELSE d.idxEnd/d.idxBase-1 END,
                 cdThr = ISNULL(cfg.C_CASH_DRAG_THRESHOLD, @cdThrDefault),
                 cashdrag = CASE WHEN mc.C_AUM=0 THEN NULL
                                 ELSE (mc.C_CASH)*1.0/mc.C_AUM END
    FROM #mr r
    INNER JOIN #md d ON d.m=r.m
    INNER JOIN T_MASTER_CURRENT mc ON mc.C_MASTER_CODE=r.m
    OUTER APPLY dbo.UDF_PM_CONFIG(r.m) cfg;

    -- RS1 header
    SELECT COUNT(*) AS C_TOTAL_MASTER,
           (SELECT ISNULL(SUM(C_TOTAL_ACCOUNT),0) FROM T_MASTER_CURRENT mc
            INNER JOIN #md d ON d.m=mc.C_MASTER_CODE) AS C_TOTAL_KH
    FROM #md;

    -- RS2 tổng toàn hệ
    SELECT  CAST(SUM(mc.C_AUM) AS DECIMAL(38,0)) AS C_TOTAL_AUM,
            CAST(SUM(x.aumBase) AS DECIMAL(38,0))        AS C_TOTAL_AUM_BASE,
            CASE WHEN SUM(x.aumBase)=0 THEN NULL
                 ELSE CAST(SUM(mc.C_AUM)*1.0/NULLIF(SUM(x.aumBase),0) - 1 AS DECIMAL(18,6)) END AS C_AUM_GROWTH_PCT,
            CAST(SUM(x.cin) AS DECIMAL(38,0))  AS C_NET_IN,
            CAST(SUM(x.cout) AS DECIMAL(38,0)) AS C_NET_OUT,
            CAST(SUM(x.cin)-SUM(x.cout) AS DECIMAL(38,0)) AS C_NET_FLOW,
            CAST(SUM(mc.C_CASH)*1.0/NULLIF(SUM(mc.C_AUM),0) AS DECIMAL(9,6)) AS C_CASH_DRAG,
            SUM(CASE WHEN r.cashdrag > r.cdThr THEN 1 ELSE 0 END) AS C_CNT_MASTER_CASH_OVER
    FROM #md d
    INNER JOIN T_MASTER_CURRENT mc ON mc.C_MASTER_CODE=d.m
    INNER JOIN #mr r ON r.m=d.m
    -- aumBase(point) + net in/out(range) 1 read T_MASTER_BALANCE [dbase,dend] (gộp 2 OUTER APPLY cũ).
    OUTER APPLY (SELECT MAX(CASE WHEN isBase=1 THEN ta END) aumBase,
                        ISNULL(SUM(CASE WHEN aft=1 THEN cin END),0) cin,
                        ISNULL(SUM(CASE WHEN aft=1 THEN cout END),0) cout
                 FROM (SELECT C_AUM AS ta, C_CASH_IN AS cin, C_CASH_OUT AS cout,
                              CASE WHEN C_BUSINESS_DATE=d.dbase THEN 1 ELSE 0 END AS isBase,
                              CASE WHEN C_BUSINESS_DATE>d.dbase THEN 1 ELSE 0 END AS aft
                       FROM T_MASTER_BALANCE
                       WHERE C_MASTER_CODE=d.m AND C_BUSINESS_DATE>=d.dbase AND C_BUSINESS_DATE<=d.dend) z) x;

    -- RS3 list master
    SELECT  mp.C_MASTER_CODE, mp.C_MASTER_NAME, mc.C_TOTAL_ACCOUNT AS C_TOTAL_ACCOUNT,
            mc.C_AUM AS C_AUM,
            CAST(r.rmaster AS DECIMAL(18,6)) AS C_MASTER_RETURN,
            CAST(r.wret AS DECIMAL(18,6))    AS C_KH_RETURN_AUMW,
            CAST((r.wret - r.rmaster)*10000 AS DECIMAL(12,2)) AS C_DEVIATION_BPS,
            CAST(r.wte AS DECIMAL(12,6))     AS C_TE_AUMW,
            CAST(r.cashdrag AS DECIMAL(9,6)) AS C_CASH_DRAG
    FROM #mr r
    INNER JOIN T_MASTER_PORTFOLIO mp ON mp.C_MASTER_CODE=r.m
    INNER JOIN T_MASTER_CURRENT mc ON mc.C_MASTER_CODE=r.m
    ORDER BY CASE @p_sort WHEN 'AUM'  THEN mc.C_AUM END DESC,
             CASE @p_sort WHEN 'RET'  THEN r.wret END DESC,
             CASE @p_sort WHEN 'DEV'  THEN (r.wret-r.rmaster) END DESC,
             CASE @p_sort WHEN 'TE'   THEN r.wte END DESC,
             CASE @p_sort WHEN 'CASH' THEN r.cashdrag END DESC,
             mc.C_AUM DESC;

    DROP TABLE #kh; DROP TABLE #mr; DROP TABLE #md;
    END TRY
    BEGIN CATCH
        IF @p_err_code = 0 BEGIN SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE(); END  -- lỗi runtime → OUT, KHÔNG THROW
    END CATCH
END
GO

/*===========================================================================
  SP_GET_MASTER_ALERTS : cảnh báo composition cấp master (PM dashboard alert panel).
    So tỷ trọng THỰC (T_MASTER_HOLDING_BALANCE @date) vs MỤC TIÊU (T_MASTER_PORTFOLIO_TICKER
    eff mới nhất ≤ date) + Σ theo ngành (T_TICKER_INDUSTRY) → đối chiếu ngưỡng PM config.
    Ngưỡng NULL ⇒ alert type TẮT (không cờ, đếm 0).
    RS1 summary (đếm #vượt + ngưỡng); RS2 per-mã (actual/target/drift + cờ); RS3 per-ngành.
    API convention: @p_user audit + @p_err_code/@p_err_msg OUT (0=OK, ≠0=lỗi, KHÔNG THROW).
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_MASTER_ALERTS
    @p_master_code VARCHAR(20),
    @p_date        DATE          = NULL,
    @p_user        VARCHAR(64)   = NULL,
    @p_err_code    INT           OUTPUT,
    @p_err_msg     NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @p_err_code = 0; SET @p_err_msg = NULL;
    BEGIN TRY

    IF NOT EXISTS (SELECT 1 FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE=@p_master_code)
    BEGIN
        SET @p_err_code = 1; SET @p_err_msg = N'Master not found: ' + ISNULL(@p_master_code,N'(null)');
        RAISERROR(@p_err_msg, 16, 1);
    END

    -- ngày mặc định = phiên holdings gần nhất của master
    IF @p_date IS NULL
        SELECT @p_date = MAX(C_BUSINESS_DATE) FROM T_MASTER_HOLDING_BALANCE WHERE C_MASTER_CODE=@p_master_code;
    IF @p_date IS NULL
    BEGIN
        SET @p_err_code = 2; SET @p_err_msg = N'Chưa có holdings balance cho master.';
        RAISERROR(@p_err_msg, 16, 1);
    END

    -- ngưỡng cấu hình (NULL = alert type tắt)
    DECLARE @drift DECIMAL(9,6), @symW DECIMAL(9,6), @indW DECIMAL(9,6);
    SELECT @drift=C_DRIFT_THRESHOLD, @symW=C_SYMBOL_WEIGHT_ALERT, @indW=C_INDUSTRY_WEIGHT_ALERT
    FROM dbo.UDF_PM_CONFIG(@p_master_code);

    -- target weight hiệu lực (eff mới nhất ≤ @p_date)
    DECLARE @eff DATE;
    SELECT @eff = MAX(C_EFFECTIVE_DATE) FROM T_MASTER_PORTFOLIO_TICKER
     WHERE C_MASTER_CODE=@p_master_code AND C_EFFECTIVE_DATE <= @p_date;

    -- per-mã: actual vs target + drift + cờ (FULL OUTER: mã chỉ có ở 1 phía vẫn ra)
    SELECT COALESCE(a.C_TICKER, t.C_TICKER) AS C_TICKER,
           CAST(ISNULL(a.C_WEIGHT,0)        AS DECIMAL(12,8)) AS C_WEIGHT_ACTUAL,
           CAST(ISNULL(t.C_TARGET_WEIGHT,0) AS DECIMAL(12,8)) AS C_WEIGHT_TARGET,
           CAST(ABS(ISNULL(a.C_WEIGHT,0) - ISNULL(t.C_TARGET_WEIGHT,0)) AS DECIMAL(12,8)) AS C_DRIFT,
           CASE WHEN @symW  IS NOT NULL AND ISNULL(a.C_WEIGHT,0) > @symW THEN 1 ELSE 0 END AS C_IS_SYMBOL_ALERT,
           CASE WHEN @drift IS NOT NULL AND ABS(ISNULL(a.C_WEIGHT,0) - ISNULL(t.C_TARGET_WEIGHT,0)) > @drift THEN 1 ELSE 0 END AS C_IS_DRIFT_ALERT
    INTO #pt
    FROM       (SELECT C_TICKER, C_WEIGHT FROM T_MASTER_HOLDING_BALANCE
                WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE=@p_date) a
    FULL OUTER JOIN (SELECT C_TICKER, C_TARGET_WEIGHT FROM T_MASTER_PORTFOLIO_TICKER
                WHERE C_MASTER_CODE=@p_master_code AND C_EFFECTIVE_DATE=@eff) t
      ON t.C_TICKER = a.C_TICKER;

    -- per-ngành: Σ tỷ trọng thực tế theo ngành (mã chưa map → 'UNKNOWN')
    SELECT COALESCE(ti.C_INDUSTRY_CODE,'UNKNOWN') AS C_INDUSTRY_CODE,
           MAX(ti.C_INDUSTRY_NAME)               AS C_INDUSTRY_NAME,
           CAST(SUM(a.C_WEIGHT) AS DECIMAL(12,8)) AS C_WEIGHT_INDUSTRY,
           CASE WHEN @indW IS NOT NULL AND SUM(a.C_WEIGHT) > @indW THEN 1 ELSE 0 END AS C_IS_INDUSTRY_ALERT
    INTO #pi
    FROM       (SELECT C_TICKER, C_WEIGHT FROM T_MASTER_HOLDING_BALANCE
                WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE=@p_date) a
    LEFT JOIN  T_TICKER_INDUSTRY ti ON ti.C_TICKER = a.C_TICKER
    GROUP BY COALESCE(ti.C_INDUSTRY_CODE,'UNKNOWN');

    -- RS1: summary (ngưỡng dùng + #vượt từng loại)
    SELECT @p_master_code AS C_MASTER_CODE, @p_date AS C_BUSINESS_DATE, @eff AS C_EFFECTIVE_DATE,
           @symW AS C_SYMBOL_WEIGHT_ALERT, @drift AS C_DRIFT_THRESHOLD, @indW AS C_INDUSTRY_WEIGHT_ALERT,
           (SELECT COUNT(*) FROM #pt WHERE C_IS_SYMBOL_ALERT=1)   AS C_CNT_SYMBOL_ALERT,
           (SELECT COUNT(*) FROM #pt WHERE C_IS_DRIFT_ALERT=1)    AS C_CNT_DRIFT_ALERT,
           (SELECT COUNT(*) FROM #pi WHERE C_IS_INDUSTRY_ALERT=1) AS C_CNT_INDUSTRY_ALERT;

    -- RS2: per-mã (toàn bộ + cờ, vượt lên đầu)
    SELECT C_TICKER, C_WEIGHT_ACTUAL, C_WEIGHT_TARGET, C_DRIFT, C_IS_SYMBOL_ALERT, C_IS_DRIFT_ALERT
    FROM #pt ORDER BY (C_IS_SYMBOL_ALERT + C_IS_DRIFT_ALERT) DESC, C_DRIFT DESC, C_WEIGHT_ACTUAL DESC;

    -- RS3: per-ngành (toàn bộ + cờ)
    SELECT C_INDUSTRY_CODE, C_INDUSTRY_NAME, C_WEIGHT_INDUSTRY, C_IS_INDUSTRY_ALERT
    FROM #pi ORDER BY C_IS_INDUSTRY_ALERT DESC, C_WEIGHT_INDUSTRY DESC;

    DROP TABLE #pt; DROP TABLE #pi;

    END TRY
    BEGIN CATCH
        IF @p_err_code = 0 BEGIN SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE(); END  -- validation đã set (THROW vào đây); chỉ -1 cho runtime. KHÔNG THROW ra ngoài.
        IF OBJECT_ID('tempdb..#pt') IS NOT NULL DROP TABLE #pt;
        IF OBJECT_ID('tempdb..#pi') IS NOT NULL DROP TABLE #pi;
    END CATCH
END
GO

/*===========================================================================
  SP_GET_MASTER_DEVIATION_DIST (BRD §3.8) — phân phối Performance Deviation per-KH.
    dev_bps = (Return KH TWR − Return Master Index) × 10000, kỳ [base..end].
    RS1: #DM + dev AUM-weighted (BRD field 1) + trung vị + σ + #>A/#<B.
    RS2: histogram theo bucket BPS (width @p_bucket_bps, biên ±@p_cap_bps + 2 overflow).
    Ngưỡng A/B override 3 tầng (giống US2): param tra cứu → config per-master → default.
    API convention: @p_user + @p_err_code/@p_err_msg OUT (0=OK, KHÔNG THROW).
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_MASTER_DEVIATION_DIST
    @p_master_code VARCHAR(20),
    @p_range         VARCHAR(20) = 'INCEPTION',
    @p_dev_threshold_high DECIMAL(10,2) = 100.00,   -- A (>A = vượt trội); default +100 BPS, caller truyền override
    @p_dev_threshold_low  DECIMAL(10,2) = -100.00,  -- B (<B = tụt); default -100 BPS
    @p_bucket_bps    INT = 5,                      -- bề rộng bucket (BPS)
    @p_cap_bps       INT = 25,                     -- biên histogram (±); ngoài biên → overflow
    @p_user        VARCHAR(64)   = NULL,
    @p_err_code    INT           OUTPUT,
    @p_err_msg     NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @p_err_code = 0; SET @p_err_msg = NULL;
    BEGIN TRY
    IF NOT EXISTS (SELECT 1 FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE = @p_master_code)
        BEGIN SET @p_err_code = 1; SET @p_err_msg = N'Master not found'; RAISERROR(@p_err_msg, 16, 1); END
    IF @p_bucket_bps <= 0 OR @p_cap_bps <= 0
        BEGIN SET @p_err_code = 3; SET @p_err_msg = N'bucket_bps/cap_bps phải > 0'; RAISERROR(@p_err_msg, 16, 1); END

    -- ngưỡng A/B nhận thẳng từ tham số (default tại SP +100/-100 BPS, KHÔNG đọc config)
    DECLARE @devHi DECIMAL(10,2) = @p_dev_threshold_high, @devLo DECIMAL(10,2) = @p_dev_threshold_low;

    -- khung ngày + master index return kỳ (PR)
    DECLARE @end DATE, @cutoff DATE, @base DATE;
    DECLARE @first DATE;
    -- khung ngày: 1 read gộp MAX(cuối)+MIN(đầu) → fallback dùng @first (bỏ read MIN lần 3).
    SELECT @end = MAX(C_BUSINESS_DATE), @first = MIN(C_BUSINESS_DATE)
    FROM T_MASTER_BALANCE WHERE C_MASTER_CODE=@p_master_code;
    SET @cutoff = dbo.UDF_RANGE_CUTOFF(@end, @p_range);
    SELECT @base = MAX(C_BUSINESS_DATE) FROM T_MASTER_BALANCE
     WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE <= @cutoff;
    IF @base IS NULL SET @base = @first;

    DECLARE @idxBase DECIMAL(18,6), @idxEnd DECIMAL(18,6), @rMaster DECIMAL(18,10);
    -- index @base + @end trong 1 read (IN(base,end) + pivot CASE) thay vì 2 point-read.
    SELECT @idxBase = MAX(CASE WHEN C_BUSINESS_DATE=@base THEN C_INDEX_VALUE END),
           @idxEnd  = MAX(CASE WHEN C_BUSINESS_DATE=@end  THEN C_INDEX_VALUE END)
    FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE IN (@base,@end);
    SET @rMaster = CASE WHEN @idxBase IS NULL OR @idxBase=0 THEN NULL ELSE @idxEnd/@idxBase - 1 END;

    -- per-KH deviation (BPS) = (TWR KH compound − return master index) × 10000
    --   TWR KH = ∏(1+daily_return) qua (base,end] = EXP(Σ ln(1+r))−1 ; KH không có ngày return ⇒ 0
    CREATE TABLE #d (si VARCHAR(20), aum DECIMAL(20,6), dev_bps DECIMAL(18,6));
    INSERT #d (si, aum, dev_bps)
    SELECT nc.C_SI_ACCOUNT, nc.C_LAST_AUM,
           (ISNULL(c.R,0) - @rMaster) * 10000
    FROM T_SI_CURRENT nc
    LEFT JOIN (
        SELECT b.C_SI_ACCOUNT AS si, EXP(SUM(LOG(1.0 + b.C_DAILY_RETURN))) - 1 AS R
        FROM T_SI_BALANCE b
        WHERE b.C_MASTER_CODE=@p_master_code AND b.C_BUSINESS_DATE>@base AND b.C_BUSINESS_DATE<=@end
          AND b.C_DAILY_RETURN IS NOT NULL
        GROUP BY b.C_SI_ACCOUNT
    ) c ON c.si=nc.C_SI_ACCOUNT
    WHERE nc.C_MASTER_CODE=@p_master_code AND nc.C_STATUS='ACTIVE';

    -- RS1: summary
    SELECT @p_master_code AS C_MASTER_CODE, @p_range AS C_RANGE, @base AS C_BASE_DATE, @end AS C_END_DATE,
           @devHi AS C_DEV_THRESHOLD_HIGH, @devLo AS C_DEV_THRESHOLD_LOW,
           COUNT(*) AS C_TOTAL_KH,
           CAST(SUM(dev_bps*aum)/NULLIF(SUM(aum),0) AS DECIMAL(18,4)) AS C_DEV_AUMW_BPS,
           CAST((SELECT DISTINCT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY dev_bps) OVER () FROM #d) AS DECIMAL(18,4)) AS C_MEDIAN_BPS,
           CAST(STDEV(dev_bps) AS DECIMAL(18,4)) AS C_SIGMA_BPS,
           COUNT(CASE WHEN dev_bps > @devHi THEN 1 END) AS C_CNT_OVER_HIGH,
           COUNT(CASE WHEN dev_bps < @devLo THEN 1 END) AS C_CNT_UNDER_LOW
    FROM #d;

    -- RS2: histogram (axis liên tục gồm bucket rỗng + 2 overflow)
    DECLARE @w DECIMAL(18,4)=@p_bucket_bps, @cap DECIMAL(18,4)=@p_cap_bps;
    ;WITH tally AS (
        SELECT TOP (CAST(2*@cap/@w AS INT)) (ROW_NUMBER() OVER (ORDER BY (SELECT NULL))-1) AS i
        FROM sys.all_objects
    ),
    axis AS (
        SELECT i AS bidx, (-@cap+i*@w) AS lo, (-@cap+(i+1)*@w) AS hi, i+1 AS srt FROM tally
        UNION ALL SELECT -1, NULL, -@cap, 0
        UNION ALL SELECT 9999, @cap, NULL, 1000000
    ),
    cnt AS (
        SELECT CASE WHEN dev_bps < -@cap THEN -1 WHEN dev_bps >= @cap THEN 9999
                    ELSE CAST(FLOOR((dev_bps+@cap)/@w) AS INT) END AS bidx, COUNT(*) AS c
        FROM #d WHERE dev_bps IS NOT NULL
        GROUP BY CASE WHEN dev_bps < -@cap THEN -1 WHEN dev_bps >= @cap THEN 9999
                      ELSE CAST(FLOOR((dev_bps+@cap)/@w) AS INT) END
    )
    SELECT a.srt AS C_SORT,
           CASE WHEN a.bidx=-1   THEN CONCAT('<',CAST(-@cap AS INT),' bps')
                WHEN a.bidx=9999 THEN CONCAT('>=',CAST(@cap AS INT),' bps')
                ELSE CONCAT(CAST(a.lo AS INT),'..',CAST(a.hi AS INT),' bps') END AS C_BUCKET,
           a.lo AS C_LO_BPS, a.hi AS C_HI_BPS, ISNULL(c.c,0) AS C_CNT
    FROM axis a LEFT JOIN cnt c ON c.bidx=a.bidx
    ORDER BY a.srt;

    DROP TABLE #d;
    END TRY
    BEGIN CATCH
        IF @p_err_code = 0 BEGIN SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE(); END  -- lỗi runtime → OUT, KHÔNG THROW
        IF OBJECT_ID('tempdb..#d') IS NOT NULL DROP TABLE #d;
    END CATCH
END
GO

/*===========================================================================
  [BRD đối chiếu] AUM-weighted return KH của 1 master — COMPOUND daily_return (thin-layer).
    wᵢ = AUMᵢ/ΣAUM (AUMᵢ = C_LAST_AUM, current ACTIVE; AUM = NAV vì phí QL đã trừ); return = Σ wᵢ·Rᵢ. Khung base/end giống US2.
    RS: C_MASTER_CODE, C_BASE_DATE, C_END_DATE, C_METHOD, C_KH_RETURN_AUMW.
    Rᵢ = ∏(1+C_DAILY_RETURN) qua (base,end] = EXP(Σ ln(1+r))−1 (QUÉT ngày, Asset gửi daily_return TWR).
    ([thin-layer] ĐÃ GỠ SP_GET_MASTER_RETURN_RETINDEX: không còn unit_price ⇒ chỉ còn 1 API COMPOUND.)
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_MASTER_RETURN_COMPOUND
    @p_master_code VARCHAR(20), @p_range VARCHAR(20)='INCEPTION',
    @p_user VARCHAR(64)=NULL, @p_err_code INT OUTPUT, @p_err_msg NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON; SET @p_err_code=0; SET @p_err_msg=NULL;
    BEGIN TRY
    IF NOT EXISTS (SELECT 1 FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE=@p_master_code)
        BEGIN SET @p_err_code=1; SET @p_err_msg=N'Master not found'; RAISERROR(@p_err_msg,16,1); END
    DECLARE @end DATE, @cutoff DATE, @base DATE, @first DATE;
    SELECT @end=MAX(C_BUSINESS_DATE), @first=MIN(C_BUSINESS_DATE) FROM T_MASTER_BALANCE WHERE C_MASTER_CODE=@p_master_code;
    SET @cutoff=dbo.UDF_RANGE_CUTOFF(@end,@p_range);
    SELECT @base=MAX(C_BUSINESS_DATE) FROM T_MASTER_BALANCE WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE<=@cutoff;
    IF @base IS NULL SET @base=@first;

    ;WITH kh AS (
        SELECT nc.C_SI_ACCOUNT AS si, nc.C_LAST_AUM AS aum
        FROM T_SI_CURRENT nc WHERE nc.C_MASTER_CODE=@p_master_code AND nc.C_STATUS='ACTIVE'
    ), comp AS (   -- ∏(1+r) qua (base,end] = EXP(Σ ln(1+r))−1 ; QUÉT ngày; bỏ ngày return NULL (vd ngày đầu)
        SELECT b.C_SI_ACCOUNT AS si, EXP(SUM(LOG(1.0 + b.C_DAILY_RETURN))) - 1 AS R
        FROM T_SI_BALANCE b
        WHERE b.C_MASTER_CODE=@p_master_code AND b.C_BUSINESS_DATE>@base AND b.C_BUSINESS_DATE<=@end
          AND b.C_DAILY_RETURN IS NOT NULL
        GROUP BY b.C_SI_ACCOUNT
    )
    SELECT @p_master_code AS C_MASTER_CODE, @base AS C_BASE_DATE, @end AS C_END_DATE, 'COMPOUND' AS C_METHOD,
           CAST(SUM(kh.aum * ISNULL(comp.R,0))/NULLIF(SUM(kh.aum),0) AS DECIMAL(18,6)) AS C_KH_RETURN_AUMW
    FROM kh LEFT JOIN comp ON comp.si=kh.si;
    END TRY BEGIN CATCH IF @p_err_code=0 BEGIN SET @p_err_code=-1; SET @p_err_msg=ERROR_MESSAGE(); END END CATCH
END
GO
