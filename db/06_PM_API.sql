SET QUOTED_IDENTIFIER ON;  -- đọc bảng có filtered index → QI ON lúc CREATE PROC
SET ANSI_NULLS ON;
GO
/*==============================================================================
  SDI MODULE — PM TOOL READ API (SQL Server)  | ALL-IN-DB, serve-layer on-read
  Dashboard PM quản lý cấp MASTER (10 master, ~50k KH). Spec: docs/SDI-pm-tool-spec.md
  master-keyed: nhận @p_master_code (+ range); KHÔNG trả định danh KH ngoài top-N (US5).
  2 bản chất: Snapshot (current, T_MASTER/SI_NAV_CURRENT) | Hiệu suất (T-1, *_NAV_BALANCE).
  Công thức (spec §2):
    AUM = total_asset (gross) = stock+cash+pending+div = C_LAST_NAV + C_PAYABLE_FEE (per KH)
    DM tổng KH = AUM-weighted (end-weight): Σ Wᵢ·PnLᵢ, Wᵢ=AUMᵢ/ΣAUM, PnLᵢ=UPᵢ(end)/UPᵢ(base)−1 (TWR)
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
            CAST(COALESCE(c.C_DEV_THRESHOLD_HIGH,  100.00)   AS DECIMAL(10,2)) AS C_DEV_THRESHOLD_HIGH,
            CAST(COALESCE(c.C_DEV_THRESHOLD_LOW,  -100.00)   AS DECIMAL(10,2)) AS C_DEV_THRESHOLD_LOW
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
    @p_dev_threshold_high   DECIMAL(10,2) = NULL,
    @p_dev_threshold_low    DECIMAL(10,2) = NULL,
    @p_updated_by           VARCHAR(64)   = NULL
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT EXISTS (SELECT 1 FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE = @p_master_code)
        BEGIN RAISERROR('Master not found',16,1); RETURN; END

    MERGE T_MASTER_PM_CONFIG AS t
    USING (SELECT @p_master_code AS m) AS s ON t.C_MASTER_CODE = s.m
    WHEN MATCHED THEN UPDATE SET
        C_TE_BADGE_LOW        = @p_te_badge_low,
        C_TE_BADGE_HIGH       = @p_te_badge_high,
        C_TE_ALERT_THRESHOLD  = @p_te_alert_threshold,
        C_CASH_DRAG_THRESHOLD = @p_cash_drag_threshold,
        C_DEV_THRESHOLD_HIGH  = @p_dev_threshold_high,
        C_DEV_THRESHOLD_LOW   = @p_dev_threshold_low,
        C_UPDATED_BY          = @p_updated_by,
        C_UPDATED_TIME        = GETDATE()
    WHEN NOT MATCHED THEN INSERT
        (C_MASTER_CODE, C_TE_BADGE_LOW, C_TE_BADGE_HIGH, C_TE_ALERT_THRESHOLD,
         C_CASH_DRAG_THRESHOLD, C_DEV_THRESHOLD_HIGH, C_DEV_THRESHOLD_LOW, C_UPDATED_BY)
        VALUES
        (@p_master_code, @p_te_badge_low, @p_te_badge_high, @p_te_alert_threshold,
         @p_cash_drag_threshold, @p_dev_threshold_high, @p_dev_threshold_low, @p_updated_by);

    SELECT @p_master_code AS C_MASTER_CODE, * FROM dbo.UDF_PM_CONFIG(@p_master_code);
END
GO

/*===========================================================================
  US2 — SP_GET_MASTER_OVERVIEW : tổng quan 1 master (snapshot + hiệu suất kỳ)
    RS1 (1 dòng): info + AUM/growth + net in/out + AUM-weighted TE+badge+#vượt
                  + cash drag+#vượt + deviation+#vượt A/B.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_MASTER_OVERVIEW
    @p_master_code VARCHAR(20),
    @p_range         VARCHAR(20) = 'INCEPTION'
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT EXISTS (SELECT 1 FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE = @p_master_code)
        BEGIN RAISERROR('Master not found',16,1); RETURN; END

    -- cấu hình ngưỡng hiệu lực
    DECLARE @teLow DECIMAL(10,6), @teHigh DECIMAL(10,6), @teAlert DECIMAL(10,6),
            @cdThr DECIMAL(9,6),  @devHi  DECIMAL(10,2), @devLo  DECIMAL(10,2);
    SELECT @teLow=C_TE_BADGE_LOW, @teHigh=C_TE_BADGE_HIGH, @teAlert=C_TE_ALERT_THRESHOLD,
           @cdThr=C_CASH_DRAG_THRESHOLD, @devHi=C_DEV_THRESHOLD_HIGH, @devLo=C_DEV_THRESHOLD_LOW
    FROM dbo.UDF_PM_CONFIG(@p_master_code);

    -- khung ngày (hiệu suất T-1)
    DECLARE @end DATE, @cutoff DATE, @base DATE, @X INT;
    SELECT @end = MAX(C_BUSINESS_DATE) FROM T_MASTER_NAV_BALANCE WHERE C_MASTER_CODE=@p_master_code;
    SET @cutoff = dbo.UDF_RANGE_CUTOFF(@end, @p_range);
    SELECT @base = MAX(C_BUSINESS_DATE) FROM T_MASTER_NAV_BALANCE
     WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE <= @cutoff;
    IF @base IS NULL
        SELECT @base = MIN(C_BUSINESS_DATE) FROM T_MASTER_NAV_BALANCE WHERE C_MASTER_CODE=@p_master_code;

    -- master index return kỳ (PR) + #ngày GD (cap 252)
    DECLARE @idxBase DECIMAL(18,6), @idxEnd DECIMAL(18,6), @rMaster DECIMAL(18,10);
    SELECT @idxBase = C_INDEX_VALUE FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE=@base;
    SELECT @idxEnd  = C_INDEX_VALUE FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE=@end;
    SET @rMaster = CASE WHEN @idxBase IS NULL OR @idxBase=0 THEN NULL ELSE @idxEnd/@idxBase - 1 END;
    SELECT @X = COUNT(DISTINCT C_BUSINESS_DATE) FROM T_MASTER_INDEX_DAILY
     WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE > @base AND C_BUSINESS_DATE <= @end;
    IF @X > 252 SET @X = 252;

    -- net in/out kỳ (base, end]
    DECLARE @cashIn DECIMAL(20,0), @cashOut DECIMAL(20,0);
    SELECT @cashIn = ISNULL(SUM(C_CASH_IN),0), @cashOut = ISNULL(SUM(C_CASH_OUT),0)
    FROM T_MASTER_NAV_BALANCE
    WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE > @base AND C_BUSINESS_DATE <= @end;

    -- AUM hiện tại + base + cash drag (master current/daily)
    DECLARE @aumNow DECIMAL(20,0), @tienNow DECIMAL(20,0), @nKH INT, @aumBase DECIMAL(20,0);
    SELECT @aumNow = C_TOTAL_ASSET, @nKH = C_TOTAL_ACCOUNT,
           @tienNow = C_CASH + C_PENDING_CASH + C_DIV_CASH
    FROM T_MASTER_NAV_CURRENT WHERE C_MASTER_CODE=@p_master_code;
    SELECT @aumBase = C_TOTAL_ASSET FROM T_MASTER_NAV_BALANCE
     WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE=@base;

    -- per-KH (end-weight): AUM/up_end/tien từ CURRENT; up_base + TE prefix-sum từ 2 LÁT NAV_BALANCE (@base,@end).
    --   TE = STDEV(active) qua (base,end] = hiệu accum 2 mốc (KHÔNG quét ngày giữa). KH join sau base → up_base=10000, accum_base=0.
    CREATE TABLE #kh (si VARCHAR(20), aum DECIMAL(20,6), up_base DECIMAL(18,6),
                      up_end DECIMAL(18,6), tien DECIMAL(20,0),
                      car_b FLOAT, car2_b FLOAT, n_b INT, car_e FLOAT, car2_e FLOAT, n_e INT, te FLOAT);

    INSERT #kh (si, aum, up_end, tien, car_b,car2_b,n_b, car_e,car2_e,n_e)
    SELECT nc.C_SI_ACCOUNT, nc.C_LAST_NAV + nc.C_PAYABLE_FEE, nc.C_LAST_UNIT_PRICE,
           nc.C_CASH + nc.C_PENDING_CASH + nc.C_DIV_CASH, 0,0,0, 0,0,0
    FROM T_SI_NAV_CURRENT nc
    WHERE nc.C_MASTER_CODE=@p_master_code AND nc.C_STATUS='ACTIVE';

    -- lát @end: accum active đến cuối kỳ
    UPDATE k SET car_e=e.C_ACCUM_ACTIVE_RET, car2_e=e.C_ACCUM_ACTIVE_RET_SQ, n_e=e.C_RET_DAY_COUNT
    FROM #kh k JOIN T_SI_NAV_BALANCE e
      ON e.C_MASTER_CODE=@p_master_code AND e.C_BUSINESS_DATE=@end AND e.C_SI_ACCOUNT=k.si;

    -- lát @base: up_base + accum active đến base (thiếu lát ⇒ KH join sau base ⇒ up_base=10000, accum_base=0)
    UPDATE k SET up_base=b.C_UNIT_PRICE, car_b=b.C_ACCUM_ACTIVE_RET, car2_b=b.C_ACCUM_ACTIVE_RET_SQ, n_b=b.C_RET_DAY_COUNT
    FROM #kh k JOIN T_SI_NAV_BALANCE b
      ON b.C_MASTER_CODE=@p_master_code AND b.C_BUSINESS_DATE=@base AND b.C_SI_ACCOUNT=k.si;
    UPDATE #kh SET up_base=10000 WHERE up_base IS NULL;

    -- TE per KH = STDEV(active) prefix-sum (hiệu base→end) × √min(n,252)
    UPDATE #kh SET te = CASE WHEN (n_e-n_b) >= 2 THEN
        SQRT(CASE WHEN ((car2_e-car2_b) - (car_e-car_b)*(car_e-car_b)/(n_e-n_b))/((n_e-n_b)-1) < 0 THEN 0
                  ELSE ((car2_e-car2_b) - (car_e-car_b)*(car_e-car_b)/(n_e-n_b))/((n_e-n_b)-1) END)
        * SQRT(CASE WHEN (n_e-n_b) > 252 THEN 252 ELSE (n_e-n_b) END)
      END;

    -- tổng hợp per-KH
    DECLARE @sumAum DECIMAL(38,6), @wRet DECIMAL(18,10), @wTE FLOAT, @sumAumTE DECIMAL(38,6);
    SELECT @sumAum = SUM(aum),
           @wRet   = SUM((up_end/NULLIF(up_base,0) - 1) * aum) / NULLIF(SUM(aum),0)
    FROM #kh WHERE up_base IS NOT NULL AND up_base <> 0;
    SELECT @sumAumTE = SUM(CASE WHEN te IS NOT NULL THEN aum END),
           @wTE = SUM(CASE WHEN te IS NOT NULL THEN te * aum END) / NULLIF(SUM(CASE WHEN te IS NOT NULL THEN aum END),0)
    FROM #kh;

    DECLARE @nTEover INT, @nCashOver INT, @nDevHi INT, @nDevLo INT;
    SELECT @nTEover   = COUNT(CASE WHEN te > @teAlert THEN 1 END),
           @nCashOver = COUNT(CASE WHEN aum > 0 AND tien*1.0/aum > @cdThr THEN 1 END),
           @nDevHi    = COUNT(CASE WHEN up_base>0 AND ((up_end/up_base-1) - @rMaster)*10000 > @devHi THEN 1 END),
           @nDevLo    = COUNT(CASE WHEN up_base>0 AND ((up_end/up_base-1) - @rMaster)*10000 < @devLo THEN 1 END)
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
END
GO

/*===========================================================================
  US3 — SP_GET_MASTER_PERFORMANCE : chart 3 đường + mốc rebalance
    RS1 chuỗi theo @p_resolution (D/W/M, NULL=auto theo độ dài kỳ):
        C_MASTER_INDEX (PR) | C_KH_COMPOSITE (AUM-weighted end-weight, base=1.0) | C_BENCHMARK (PR)
        → App rebase cả 3 về 0% tại điểm đầu.
    RS2: mốc rebalance (effective_date của T_MASTER_PORTFOLIO_TICKER trong kỳ).
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_MASTER_PERFORMANCE
    @p_master_code VARCHAR(20),
    @p_range         VARCHAR(20) = '1Y',
    @p_resolution    VARCHAR(2)  = NULL   -- 'D'|'W'|'M' ; NULL = auto
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT EXISTS (SELECT 1 FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE = @p_master_code)
        BEGIN RAISERROR('Master not found',16,1); RETURN; END

    DECLARE @bench VARCHAR(20) = (SELECT C_BENCHMARK_CODE FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE=@p_master_code);
    DECLARE @end DATE, @cutoff DATE, @base DATE;
    SELECT @end = MAX(C_BUSINESS_DATE) FROM T_MASTER_NAV_BALANCE WHERE C_MASTER_CODE=@p_master_code;
    SET @cutoff = dbo.UDF_RANGE_CUTOFF(@end, @p_range);
    SELECT @base = MAX(C_BUSINESS_DATE) FROM T_MASTER_NAV_BALANCE
     WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE <= @cutoff;
    IF @base IS NULL
        SELECT @base = MIN(C_BUSINESS_DATE) FROM T_MASTER_NAV_BALANCE WHERE C_MASTER_CODE=@p_master_code;

    -- auto resolution theo độ dài kỳ
    IF @p_resolution IS NULL
        SET @p_resolution = CASE WHEN DATEDIFF(DAY,@base,@end) > 90 THEN 'M'
                               WHEN DATEDIFF(DAY,@base,@end) > 21 THEN 'W' ELSE 'D' END;

    -- end-weight per KH: W_i = aum_i/Σaum ; up_base = lát @base (join sau base → 10000)
    CREATE TABLE #kw (si VARCHAR(20), w DECIMAL(18,12), up_base DECIMAL(18,6));
    ;WITH kh AS (
        SELECT nc.C_SI_ACCOUNT AS si, nc.C_LAST_NAV + nc.C_PAYABLE_FEE AS aum
        FROM T_SI_NAV_CURRENT nc WHERE nc.C_MASTER_CODE=@p_master_code AND nc.C_STATUS='ACTIVE'
    ), tot AS (SELECT SUM(aum) s FROM kh)
    INSERT #kw (si, w, up_base)
    SELECT kh.si, CAST(kh.aum / NULLIF(tot.s,0) AS DECIMAL(18,12)), COALESCE(b.C_UNIT_PRICE,10000)
    FROM kh CROSS JOIN tot
    LEFT JOIN T_SI_NAV_BALANCE b
      ON b.C_MASTER_CODE=@p_master_code AND b.C_BUSINESS_DATE=@base AND b.C_SI_ACCOUNT=kh.si;

    -- sample dates theo resolution (luôn gồm base & end)
    DECLARE @samp TABLE (d DATE PRIMARY KEY);
    ;WITH dd AS (
        SELECT DISTINCT C_BUSINESS_DATE bd FROM T_MASTER_NAV_BALANCE
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

    -- RS1 chuỗi. Composite KH set-based: sample-date là business-date thật ⇒ KH active có đúng 1
    -- dòng NAV_BALANCE @ngày đó → equi-join (master,date∈sample) dùng IX_MASTER (KHÔNG OUTER APPLY
    -- per-KH = tránh 60k scan). Renormalize Σweight present → KH join/đóng giữa kỳ không méo, base=1.0.
    ;WITH comp AS (
        SELECT b.C_BUSINESS_DATE AS d,
               CAST( SUM(CASE WHEN kw.up_base>0 AND b.C_UNIT_PRICE IS NOT NULL
                              THEN kw.w * b.C_UNIT_PRICE / kw.up_base END)
                   / NULLIF(SUM(CASE WHEN kw.up_base>0 AND b.C_UNIT_PRICE IS NOT NULL
                                     THEN kw.w END),0) AS DECIMAL(18,8)) AS kc
        FROM #kw kw
        JOIN T_SI_NAV_BALANCE b ON b.C_SI_ACCOUNT=kw.si AND b.C_MASTER_CODE=@p_master_code
        JOIN @samp s2 ON s2.d=b.C_BUSINESS_DATE
        GROUP BY b.C_BUSINESS_DATE
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

    DROP TABLE #kw;
END
GO

/*===========================================================================
  US3 click — SP_GET_MASTER_REBALANCE_DETAIL : chi tiết 1 mốc rebalance
    RS1: target weight cũ→mới per mã (T_MASTER_PORTFOLIO_TICKER).
    RS2: net delta holdings thực tế per mã (T_MASTER_HOLDING_BALANCE @date vs phiên trước).
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_MASTER_REBALANCE_DETAIL
    @p_master_code VARCHAR(20),
    @p_date          DATE
AS
BEGIN
    SET NOCOUNT ON;

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
END
GO

/*===========================================================================
  US4 — SP_GET_MASTER_PNL_DIST : phân phối lãi/lỗ DM KH (TWR per-KH)
    RS1 (1 dòng): #lãi/#lỗ/#flat + tỷ lệ + avg AUM-weighted + trung vị.
    RS2: histogram buckets %PnL.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_MASTER_PNL_DIST
    @p_master_code VARCHAR(20),
    @p_range         VARCHAR(20) = 'INCEPTION'
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT EXISTS (SELECT 1 FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE = @p_master_code)
        BEGIN RAISERROR('Master not found',16,1); RETURN; END

    DECLARE @end DATE, @cutoff DATE, @base DATE;
    SELECT @end = MAX(C_BUSINESS_DATE) FROM T_MASTER_NAV_BALANCE WHERE C_MASTER_CODE=@p_master_code;
    SET @cutoff = dbo.UDF_RANGE_CUTOFF(@end, @p_range);
    SELECT @base = MAX(C_BUSINESS_DATE) FROM T_MASTER_NAV_BALANCE
     WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE <= @cutoff;
    IF @base IS NULL
        SELECT @base = MIN(C_BUSINESS_DATE) FROM T_MASTER_NAV_BALANCE WHERE C_MASTER_CODE=@p_master_code;

    CREATE TABLE #p (si VARCHAR(20), aum DECIMAL(20,6), pnl DECIMAL(18,10));
    INSERT #p (si, aum, pnl)
    SELECT nc.C_SI_ACCOUNT, nc.C_LAST_NAV + nc.C_PAYABLE_FEE,
           nc.C_LAST_UNIT_PRICE / NULLIF(COALESCE(b.C_UNIT_PRICE,10000),0) - 1
    FROM T_SI_NAV_CURRENT nc
    LEFT JOIN T_SI_NAV_BALANCE b
      ON b.C_MASTER_CODE=@p_master_code AND b.C_BUSINESS_DATE=@base AND b.C_SI_ACCOUNT=nc.C_SI_ACCOUNT
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
END
GO

/*===========================================================================
  US5 — SP_GET_MASTER_TOP_KH : top-N KH theo %PnL (TWR)
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_MASTER_TOP_KH
    @p_master_code VARCHAR(20),
    @p_range         VARCHAR(20) = 'INCEPTION',
    @p_topn          INT = 20,
    @p_dir           VARCHAR(4) = 'DESC'   -- DESC = top lãi ; ASC = top lỗ
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT EXISTS (SELECT 1 FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE = @p_master_code)
        BEGIN RAISERROR('Master not found',16,1); RETURN; END

    DECLARE @end DATE, @cutoff DATE, @base DATE;
    SELECT @end = MAX(C_BUSINESS_DATE) FROM T_MASTER_NAV_BALANCE WHERE C_MASTER_CODE=@p_master_code;
    SET @cutoff = dbo.UDF_RANGE_CUTOFF(@end, @p_range);
    SELECT @base = MAX(C_BUSINESS_DATE) FROM T_MASTER_NAV_BALANCE
     WHERE C_MASTER_CODE=@p_master_code AND C_BUSINESS_DATE <= @cutoff;
    IF @base IS NULL
        SELECT @base = MIN(C_BUSINESS_DATE) FROM T_MASTER_NAV_BALANCE WHERE C_MASTER_CODE=@p_master_code;

    ;WITH k AS (
        SELECT nc.C_CUST_CODE, nc.C_SI_ACCOUNT,
               nc.C_LAST_NAV + nc.C_PAYABLE_FEE AS C_AUM,
               nc.C_LAST_UNIT_PRICE AS C_UNIT_PRICE_END,
               COALESCE(b.C_UNIT_PRICE,10000) AS C_UNIT_PRICE_BASE,
               CAST(nc.C_LAST_UNIT_PRICE/NULLIF(COALESCE(b.C_UNIT_PRICE,10000),0) - 1 AS DECIMAL(18,6)) AS C_PNL_PCT
        FROM T_SI_NAV_CURRENT nc
        LEFT JOIN T_SI_NAV_BALANCE b
          ON b.C_MASTER_CODE=@p_master_code AND b.C_BUSINESS_DATE=@base AND b.C_SI_ACCOUNT=nc.C_SI_ACCOUNT
        WHERE nc.C_MASTER_CODE=@p_master_code AND nc.C_STATUS='ACTIVE'
    )
    SELECT TOP (@p_topn) C_CUST_CODE, C_SI_ACCOUNT, C_AUM, C_UNIT_PRICE_BASE, C_UNIT_PRICE_END, C_PNL_PCT
    FROM k
    ORDER BY CASE WHEN @p_dir='ASC' THEN C_PNL_PCT END ASC,
             CASE WHEN @p_dir<>'ASC' THEN C_PNL_PCT END DESC;
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
    @p_sort  VARCHAR(8)  = 'AUM'
AS
BEGIN
    SET NOCOUNT ON;

    -- khung ngày per-master (ACTIVE)
    CREATE TABLE #md (m VARCHAR(20), dend DATE, dcut DATE, dbase DATE,
                      idxBase DECIMAL(18,6), idxEnd DECIMAL(18,6), X INT);
    INSERT #md (m, dend)
    SELECT mp.C_MASTER_CODE, MAX(b.C_BUSINESS_DATE)
    FROM T_MASTER_PORTFOLIO mp
    JOIN T_MASTER_NAV_BALANCE b ON b.C_MASTER_CODE=mp.C_MASTER_CODE
    WHERE mp.C_STATUS='ACTIVE'
    GROUP BY mp.C_MASTER_CODE;

    UPDATE #md SET dcut = dbo.UDF_RANGE_CUTOFF(dend, @p_range);
    UPDATE m SET dbase = COALESCE(
        (SELECT MAX(C_BUSINESS_DATE) FROM T_MASTER_NAV_BALANCE
          WHERE C_MASTER_CODE=m.m AND C_BUSINESS_DATE<=m.dcut),
        (SELECT MIN(C_BUSINESS_DATE) FROM T_MASTER_NAV_BALANCE WHERE C_MASTER_CODE=m.m))
    FROM #md m;
    UPDATE m SET idxBase = (SELECT C_INDEX_VALUE FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE=m.m AND C_BUSINESS_DATE=m.dbase),
                 idxEnd  = (SELECT C_INDEX_VALUE FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE=m.m AND C_BUSINESS_DATE=m.dend),
                 X = (SELECT CASE WHEN COUNT(DISTINCT C_BUSINESS_DATE)>252 THEN 252 ELSE COUNT(DISTINCT C_BUSINESS_DATE) END
                      FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE=m.m AND C_BUSINESS_DATE>m.dbase AND C_BUSINESS_DATE<=m.dend)
    FROM #md m;

    -- per-KH metrics (toàn hệ): AUM/up_end/tien từ CURRENT; up_base + TE prefix-sum đọc 2 LÁT
    --   (@base/@end theo master của KH) — KHÔNG quét toàn lịch sử.
    CREATE TABLE #kh (m VARCHAR(20), si VARCHAR(20), aum DECIMAL(20,6),
                      up_base DECIMAL(18,6), up_end DECIMAL(18,6), tien DECIMAL(20,0),
                      car_b FLOAT, car2_b FLOAT, n_b INT, car_e FLOAT, car2_e FLOAT, n_e INT, te FLOAT);
    INSERT #kh (m, si, aum, up_end, tien, car_b,car2_b,n_b, car_e,car2_e,n_e)
    SELECT nc.C_MASTER_CODE, nc.C_SI_ACCOUNT, nc.C_LAST_NAV + nc.C_PAYABLE_FEE,
           nc.C_LAST_UNIT_PRICE, nc.C_CASH + nc.C_PENDING_CASH + nc.C_DIV_CASH, 0,0,0, 0,0,0
    FROM T_SI_NAV_CURRENT nc
    JOIN #md d ON d.m=nc.C_MASTER_CODE
    WHERE nc.C_STATUS='ACTIVE';

    -- lát @end per master (accum active đến cuối kỳ)
    UPDATE k SET car_e=e.C_ACCUM_ACTIVE_RET, car2_e=e.C_ACCUM_ACTIVE_RET_SQ, n_e=e.C_RET_DAY_COUNT
    FROM #kh k JOIN #md d ON d.m=k.m
    JOIN T_SI_NAV_BALANCE e ON e.C_MASTER_CODE=k.m AND e.C_BUSINESS_DATE=d.dend AND e.C_SI_ACCOUNT=k.si;

    -- lát @base per master (up_base + accum active đến base; thiếu lát ⇒ up_base=10000, accum_base=0)
    UPDATE k SET up_base=b.C_UNIT_PRICE, car_b=b.C_ACCUM_ACTIVE_RET, car2_b=b.C_ACCUM_ACTIVE_RET_SQ, n_b=b.C_RET_DAY_COUNT
    FROM #kh k JOIN #md d ON d.m=k.m
    JOIN T_SI_NAV_BALANCE b ON b.C_MASTER_CODE=k.m AND b.C_BUSINESS_DATE=d.dbase AND b.C_SI_ACCOUNT=k.si;
    UPDATE #kh SET up_base=10000 WHERE up_base IS NULL;

    -- TE per KH = STDEV(active) prefix-sum (hiệu base→end) × √min(n,252)
    UPDATE #kh SET te = CASE WHEN (n_e-n_b) >= 2 THEN
        SQRT(CASE WHEN ((car2_e-car2_b) - (car_e-car_b)*(car_e-car_b)/(n_e-n_b))/((n_e-n_b)-1) < 0 THEN 0
                  ELSE ((car2_e-car2_b) - (car_e-car_b)*(car_e-car_b)/(n_e-n_b))/((n_e-n_b)-1) END)
        * SQRT(CASE WHEN (n_e-n_b) > 252 THEN 252 ELSE (n_e-n_b) END)
      END;

    -- per-master rollup
    DECLARE @cdThrDefault DECIMAL(9,6) = 0.05;
    CREATE TABLE #mr (m VARCHAR(20), aum DECIMAL(38,6), wret FLOAT, rmaster FLOAT,
                      wte FLOAT, cashdrag FLOAT, cdThr DECIMAL(9,6));
    INSERT #mr (m, aum, wret, wte)
    SELECT k.m, SUM(k.aum),
           SUM(CASE WHEN k.up_base>0 THEN (k.up_end/k.up_base-1)*k.aum END)/NULLIF(SUM(CASE WHEN k.up_base>0 THEN k.aum END),0),
           SUM(CASE WHEN k.te IS NOT NULL THEN k.te*k.aum END)/NULLIF(SUM(CASE WHEN k.te IS NOT NULL THEN k.aum END),0)
    FROM #kh k GROUP BY k.m;
    UPDATE r SET rmaster = CASE WHEN d.idxBase IS NULL OR d.idxBase=0 THEN NULL ELSE d.idxEnd/d.idxBase-1 END,
                 cdThr = ISNULL(cfg.C_CASH_DRAG_THRESHOLD, @cdThrDefault),
                 cashdrag = CASE WHEN mc.C_TOTAL_ASSET=0 THEN NULL
                                 ELSE (mc.C_CASH+mc.C_PENDING_CASH+mc.C_DIV_CASH)*1.0/mc.C_TOTAL_ASSET END
    FROM #mr r
    JOIN #md d ON d.m=r.m
    JOIN T_MASTER_NAV_CURRENT mc ON mc.C_MASTER_CODE=r.m
    OUTER APPLY dbo.UDF_PM_CONFIG(r.m) cfg;

    -- RS1 header
    SELECT COUNT(*) AS C_TOTAL_MASTER,
           (SELECT ISNULL(SUM(C_TOTAL_ACCOUNT),0) FROM T_MASTER_NAV_CURRENT mc
            JOIN #md d ON d.m=mc.C_MASTER_CODE) AS C_TOTAL_KH
    FROM #md;

    -- RS2 tổng toàn hệ
    SELECT  CAST(SUM(mc.C_TOTAL_ASSET) AS DECIMAL(38,0)) AS C_TOTAL_AUM,
            CAST(SUM(bb.aumBase) AS DECIMAL(38,0))       AS C_TOTAL_AUM_BASE,
            CASE WHEN SUM(bb.aumBase)=0 THEN NULL
                 ELSE CAST(SUM(mc.C_TOTAL_ASSET)*1.0/NULLIF(SUM(bb.aumBase),0) - 1 AS DECIMAL(18,6)) END AS C_AUM_GROWTH_PCT,
            CAST(SUM(fl.cin) AS DECIMAL(38,0))  AS C_NET_IN,
            CAST(SUM(fl.cout) AS DECIMAL(38,0)) AS C_NET_OUT,
            CAST(SUM(fl.cin)-SUM(fl.cout) AS DECIMAL(38,0)) AS C_NET_FLOW,
            CAST(SUM(mc.C_CASH+mc.C_PENDING_CASH+mc.C_DIV_CASH)*1.0/NULLIF(SUM(mc.C_TOTAL_ASSET),0) AS DECIMAL(9,6)) AS C_CASH_DRAG,
            SUM(CASE WHEN r.cashdrag > r.cdThr THEN 1 ELSE 0 END) AS C_CNT_MASTER_CASH_OVER
    FROM #md d
    JOIN T_MASTER_NAV_CURRENT mc ON mc.C_MASTER_CODE=d.m
    JOIN #mr r ON r.m=d.m
    OUTER APPLY (SELECT C_TOTAL_ASSET aumBase FROM T_MASTER_NAV_BALANCE
                 WHERE C_MASTER_CODE=d.m AND C_BUSINESS_DATE=d.dbase) bb
    OUTER APPLY (SELECT ISNULL(SUM(C_CASH_IN),0) cin, ISNULL(SUM(C_CASH_OUT),0) cout
                 FROM T_MASTER_NAV_BALANCE
                 WHERE C_MASTER_CODE=d.m AND C_BUSINESS_DATE>d.dbase AND C_BUSINESS_DATE<=d.dend) fl;

    -- RS3 list master
    SELECT  mp.C_MASTER_CODE, mp.C_MASTER_NAME, mc.C_TOTAL_ACCOUNT AS C_TOTAL_ACCOUNT,
            mc.C_TOTAL_ASSET AS C_AUM,
            CAST(r.rmaster AS DECIMAL(18,6)) AS C_MASTER_RETURN,
            CAST(r.wret AS DECIMAL(18,6))    AS C_KH_RETURN_AUMW,
            CAST((r.wret - r.rmaster)*10000 AS DECIMAL(12,2)) AS C_DEVIATION_BPS,
            CAST(r.wte AS DECIMAL(12,6))     AS C_TE_AUMW,
            CAST(r.cashdrag AS DECIMAL(9,6)) AS C_CASH_DRAG
    FROM #mr r
    JOIN T_MASTER_PORTFOLIO mp ON mp.C_MASTER_CODE=r.m
    JOIN T_MASTER_NAV_CURRENT mc ON mc.C_MASTER_CODE=r.m
    ORDER BY CASE @p_sort WHEN 'AUM'  THEN mc.C_TOTAL_ASSET END DESC,
             CASE @p_sort WHEN 'RET'  THEN r.wret END DESC,
             CASE @p_sort WHEN 'DEV'  THEN (r.wret-r.rmaster) END DESC,
             CASE @p_sort WHEN 'TE'   THEN r.wte END DESC,
             CASE @p_sort WHEN 'CASH' THEN r.cashdrag END DESC,
             mc.C_TOTAL_ASSET DESC;

    DROP TABLE #kh; DROP TABLE #mr; DROP TABLE #md;
END
GO
