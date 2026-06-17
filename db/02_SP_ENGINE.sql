/*==============================================================================
  SDI MODULE — ENGINE CORE (SQL Server)  | ALL-IN-DB, set-based, no RBAR
  Naming: SP_ procs, UDF_ functions, T_/C_ tables/cols.
  Mô hình roll-forward: T_POSITION_STATE (current) + áp delta ngày @d → tính lại.
  Thứ tự (master SP_EOD_RUN): J01_SYNC_FO → J07 → J11 → J12 → J13 → J14
  (J06 fee đã bỏ — FO cash đã NET phí; NAV = stock + FO cash.)
==============================================================================*/
SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON;
GO

/*---------------------------------------------------- UDF: prev business date */
CREATE OR ALTER FUNCTION UDF_PREV_BUSINESS_DATE (@d DATE)
RETURNS DATE
AS
BEGIN
    RETURN (SELECT MAX(C_BUSINESS_DATE) FROM T_PRICE_DAILY WHERE C_BUSINESS_DATE < @d);
END
GO

/*---------------------------------------------------- helper: log T_EOD_RUN  */
CREATE OR ALTER PROCEDURE SP_EOD_LOG
    @d DATE, @job VARCHAR(40), @status VARCHAR(10),
    @rows BIGINT = NULL, @msg NVARCHAR(2000) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    MERGE T_EOD_RUN AS t
    USING (SELECT @d AS d, @job AS j) s ON t.C_BUSINESS_DATE = s.d AND t.C_JOB = s.j
    WHEN MATCHED THEN UPDATE SET
        C_STATUS     = @status,
        C_ROWS       = COALESCE(@rows, t.C_ROWS),
        C_STARTED_AT = CASE WHEN @status='RUNNING' THEN SYSUTCDATETIME() ELSE t.C_STARTED_AT END,
        C_ENDED_AT   = CASE WHEN @status IN ('DONE','FAILED') THEN SYSUTCDATETIME() ELSE t.C_ENDED_AT END,
        C_MESSAGE    = @msg
    WHEN NOT MATCHED THEN INSERT (C_BUSINESS_DATE,C_JOB,C_STATUS,C_ROWS,C_STARTED_AT,C_ENDED_AT,C_MESSAGE)
        VALUES (@d,@job,@status,@rows,
                CASE WHEN @status='RUNNING' THEN SYSUTCDATETIME() END,
                CASE WHEN @status IN ('DONE','FAILED') THEN SYSUTCDATETIME() END, @msg);
END
GO

/*===========================================================================
  J01b — SYNC FO: mirror holdings + cash từ snapshot FO (OVERWRITE trạng thái tài sản)
         SDI KHÔNG event-source execution; FO đã phản ánh trade/cổ tức/split/settlement.
         Cashflow (nạp/rút) vẫn lấy từ T_CASHFLOW_EVENT — chỉ dùng cho CF_t (PnL/unit), KHÔNG cộng lại cash.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_SYNC_FO @d DATE
AS
BEGIN
    SET NOCOUNT ON;

    -- 0. DIFF per-KH: snapshot hôm nay (n) vs holdings hiện tại = hôm trước (o) → biến động NET → audit/lịch sử
    DELETE FROM T_CUSTOMER_HOLDING_EVENT WHERE C_BUSINESS_DATE=@d;
    INSERT INTO T_CUSTOMER_HOLDING_EVENT (C_BUSINESS_DATE,C_CUSTOMER_ID,C_SI_ID,C_TICKER,C_QTY_DELTA,C_SOURCE)
    SELECT @d,
           COALESCE(n.C_CUSTOMER_ID,o.C_CUSTOMER_ID),
           COALESCE(n.C_SI_ID,o.C_SI_ID),
           COALESCE(n.C_TICKER,o.C_TICKER),
           COALESCE(n.C_QUANTITY,0) - COALESCE(o.C_QUANTITY,0),
           'SYNC_DIFF'
    FROM (SELECT * FROM T_FO_HOLDING_SYNC WHERE C_BUSINESS_DATE=@d) n
    FULL OUTER JOIN T_INDEXING_PORTFOLIO_TICKER o
      ON o.C_CUSTOMER_ID=n.C_CUSTOMER_ID AND o.C_SI_ID=n.C_SI_ID AND o.C_TICKER=n.C_TICKER
    WHERE COALESCE(n.C_QUANTITY,0) <> COALESCE(o.C_QUANTITY,0);   -- chỉ ghi mã có thay đổi

    -- 1. holdings hiện tại = mirror full snapshot FO (per-KH)
    TRUNCATE TABLE T_INDEXING_PORTFOLIO_TICKER;
    INSERT INTO T_INDEXING_PORTFOLIO_TICKER (C_CUSTOMER_ID,C_SI_ID,C_TICKER,C_QUANTITY,C_AVG_COST)
    SELECT C_CUSTOMER_ID,C_SI_ID,C_TICKER,C_QUANTITY,C_AVG_COST
    FROM T_FO_HOLDING_SYNC WHERE C_BUSINESS_DATE=@d;

    -- cash = overwrite vào state; tạo state cho tiểu khoản mới
    MERGE T_POSITION_STATE AS s
    USING (SELECT C_CUSTOMER_ID,C_SI_ID,C_CASH FROM T_FO_CASH_SYNC WHERE C_BUSINESS_DATE=@d) f
    ON s.C_CUSTOMER_ID=f.C_CUSTOMER_ID AND s.C_SI_ID=f.C_SI_ID
    WHEN MATCHED THEN UPDATE SET C_CASH = f.C_CASH
    WHEN NOT MATCHED THEN INSERT (C_CUSTOMER_ID,C_SI_ID,C_UNIT,C_CASH,C_PAYABLE_FEE,C_LAST_NAV,C_LAST_UNIT_PRICE,C_STATUS)
        VALUES (f.C_CUSTOMER_ID,f.C_SI_ID,0,f.C_CASH,0,0,NULL,'ACTIVE');
END
GO

-- (ĐÃ BỎ J03 APPLY_CA & J04 APPLY_EXEC) — FO sync đã phản ánh cổ tức/split/trade vào cash+holdings.
--   T_CORPORATE_ACTION chỉ còn dùng cho SI INDEX (điều chỉnh P_ref khi có quyền — J12).

-- (ĐÃ BỎ J06 ACCRUE_FEE) — phương án (A): phí quản lý + thuế GD do FO trừ vào cash khi book.
--   FO cash đã NET → SDI KHÔNG accrue/trừ lại (tránh double-count). NAV = stock_value + FO_cash.
GO

/*===========================================================================
  J07–J10 — COMPUTE: MTM → NAV → PnL → Unit/UnitPrice → roll-forward state
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_COMPUTE @d DATE
AS
BEGIN
    SET NOCOUNT ON;

    DELETE FROM T_EOD_WORK WHERE C_BUSINESS_DATE=@d;

    -- seed từ state (đã áp delta cash/payable; last_nav/last_up = hôm qua)
    INSERT INTO T_EOD_WORK (C_BUSINESS_DATE,C_CUSTOMER_ID,C_SI_ID,C_CASH,C_PAYABLE_FEE,C_LAST_NAV,C_LAST_UNIT_PRICE,C_UNIT_PREV)
    SELECT @d,C_CUSTOMER_ID,C_SI_ID,C_CASH,C_PAYABLE_FEE,C_LAST_NAV,C_LAST_UNIT_PRICE,C_UNIT
    FROM T_POSITION_STATE WHERE C_STATUS='ACTIVE';

    -- CF của ngày (cho PnL & unit)
    UPDATE w SET w.C_CF_IN = cf.CF_IN, w.C_CF_OUT = cf.CF_OUT
    FROM T_EOD_WORK w
    JOIN (
        SELECT C_CUSTOMER_ID,C_SI_ID,
               SUM(CASE WHEN C_EVENT_TYPE='WITHDRAW' THEN 0 ELSE C_AMOUNT END) AS CF_IN,
               SUM(CASE WHEN C_EVENT_TYPE='WITHDRAW' THEN C_AMOUNT ELSE 0 END) AS CF_OUT
        FROM T_CASHFLOW_EVENT WHERE C_BUSINESS_DATE=@d GROUP BY C_CUSTOMER_ID,C_SI_ID
    ) cf ON cf.C_CUSTOMER_ID=w.C_CUSTOMER_ID AND cf.C_SI_ID=w.C_SI_ID
    WHERE w.C_BUSINESS_DATE=@d;

    -- J07 MTM (câu nặng nhất — set-based; prod chạy batch-mode trên columnstore)
    UPDATE w SET w.C_STOCK_VALUE = m.SV
    FROM T_EOD_WORK w
    JOIN (
        SELECT h.C_CUSTOMER_ID, h.C_SI_ID, SUM(h.C_QUANTITY * p.C_CLOSE_PRICE) AS SV
        FROM T_INDEXING_PORTFOLIO_TICKER h
        JOIN T_PRICE_DAILY p ON p.C_TICKER=h.C_TICKER AND p.C_BUSINESS_DATE=@d
        GROUP BY h.C_CUSTOMER_ID, h.C_SI_ID
    ) m ON m.C_CUSTOMER_ID=w.C_CUSTOMER_ID AND m.C_SI_ID=w.C_SI_ID
    WHERE w.C_BUSINESS_DATE=@d;

    -- J08 NAV = stock + cash (FO cash đã NET phí QL + thuế GD → SDI KHÔNG trừ lại, tránh double-count)
    UPDATE T_EOD_WORK SET C_NAV = C_STOCK_VALUE + C_CASH WHERE C_BUSINESS_DATE=@d;

    -- J09 PnL = NAV − NAV_prev + ra − vào
    UPDATE T_EOD_WORK SET C_DAILY_PNL = C_NAV - C_LAST_NAV + C_CF_OUT - C_CF_IN WHERE C_BUSINESS_DATE=@d;

    -- J10 Unit: ΔUnit = CF/UnitPrice_(t-1) (giả định cashflow đầu ngày + tham gia đầu tư → giá quy đổi = NAV/unit đầu ngày = UP cuối ngày trước; TWR sạch, không bias)
    --           init khi unit_prev=0 → unit=NAV/10000, UP=10000
    UPDATE T_EOD_WORK SET
        C_DELTA_UNIT = CASE WHEN C_LAST_UNIT_PRICE IS NULL OR C_LAST_UNIT_PRICE=0 OR C_UNIT_PREV=0
                            THEN (C_NAV/10000.0) - C_UNIT_PREV
                            ELSE (C_CF_IN - C_CF_OUT) / C_LAST_UNIT_PRICE END,
        C_UNIT       = CASE WHEN C_LAST_UNIT_PRICE IS NULL OR C_LAST_UNIT_PRICE=0 OR C_UNIT_PREV=0
                            THEN C_NAV/10000.0
                            ELSE C_UNIT_PREV + (C_CF_IN - C_CF_OUT) / C_LAST_UNIT_PRICE END
    WHERE C_BUSINESS_DATE=@d;

    UPDATE T_EOD_WORK SET C_UNIT_PRICE = CASE WHEN C_UNIT>0 THEN C_NAV/C_UNIT ELSE 10000 END
    WHERE C_BUSINESS_DATE=@d;

    -- roll-forward state (cash/payable đã current; cập nhật unit + last_nav + last_up)
    UPDATE s SET
        s.C_UNIT              = w.C_UNIT,
        s.C_LAST_NAV          = w.C_NAV,
        s.C_LAST_UNIT_PRICE   = w.C_UNIT_PRICE,
        s.C_LAST_BUSINESS_DATE= @d
    FROM T_POSITION_STATE s
    JOIN T_EOD_WORK w ON w.C_CUSTOMER_ID=s.C_CUSTOMER_ID AND w.C_SI_ID=s.C_SI_ID AND w.C_BUSINESS_DATE=@d;

    -- unit ledger (chỉ ngày có cashflow)
    DELETE FROM T_UNIT_LEDGER WHERE C_BUSINESS_DATE=@d;
    INSERT INTO T_UNIT_LEDGER (C_CUSTOMER_ID,C_SI_ID,C_BUSINESS_DATE,C_CF_NET,C_DELTA_UNIT,C_UNIT)
    SELECT C_CUSTOMER_ID,C_SI_ID,@d,(C_CF_IN-C_CF_OUT),C_DELTA_UNIT,C_UNIT
    FROM T_EOD_WORK WHERE C_BUSINESS_DATE=@d AND (C_CF_IN-C_CF_OUT)<>0;

    -- LỊCH SỬ per-KH (vì holdings không còn event-source → phải materialize để vẽ chart FR-03)
    DELETE FROM T_INDEXING_PERFORMANCE_DAILY WHERE C_BUSINESS_DATE=@d;
    INSERT INTO T_INDEXING_PERFORMANCE_DAILY (C_BUSINESS_DATE,C_CUSTOMER_ID,C_SI_ID,C_NAV,C_UNIT,C_UNIT_PRICE,C_DAILY_PNL,C_DAILY_RETURN)
    SELECT @d, C_CUSTOMER_ID, C_SI_ID, C_NAV, C_UNIT, C_UNIT_PRICE, C_DAILY_PNL,
           CASE WHEN C_LAST_UNIT_PRICE>0 THEN C_UNIT_PRICE/C_LAST_UNIT_PRICE - 1 END
    FROM T_EOD_WORK WHERE C_BUSINESS_DATE=@d;
END
GO

/*===========================================================================
  J11 — SI AGGREGATE → T_SI_PERFORMANCE_DAILY
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_SI_AGG @d DATE
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @prev DATE = dbo.UDF_PREV_BUSINESS_DATE(@d);
    DELETE FROM T_SI_PERFORMANCE_DAILY WHERE C_BUSINESS_DATE=@d;

    ;WITH agg AS (
        SELECT C_SI_ID, SUM(C_NAV) AS NAV, SUM(C_UNIT) AS UNT, SUM(C_DAILY_PNL) AS PNL
        FROM T_EOD_WORK WHERE C_BUSINESS_DATE=@d GROUP BY C_SI_ID
    )
    INSERT INTO T_SI_PERFORMANCE_DAILY (C_BUSINESS_DATE,C_SI_ID,C_NAV,C_UNIT,C_UNIT_PRICE,C_DAILY_PNL,C_DAILY_RETURN)
    SELECT @d, a.C_SI_ID, a.NAV, a.UNT,
           CASE WHEN a.UNT>0 THEN a.NAV/a.UNT END,
           a.PNL,
           CASE WHEN a.UNT>0 AND prev.C_UNIT_PRICE>0 THEN (a.NAV/a.UNT)/prev.C_UNIT_PRICE - 1 END
    FROM agg a
    LEFT JOIN T_SI_PERFORMANCE_DAILY prev ON prev.C_SI_ID=a.C_SI_ID AND prev.C_BUSINESS_DATE=@prev;
END
GO

/*===========================================================================
  J12 — SI INDEX (danh mục mẫu, 100% cổ phiếu) → T_SI_INDEX_DAILY
        Index_t = Index_(t-1) × Σ w^(t) × P_t / P_ref ;  w^(t)=eff_date≤@d mới nhất
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_SI_INDEX @d DATE
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @prev DATE = dbo.UDF_PREV_BUSINESS_DATE(@d);
    DELETE FROM T_SI_INDEX_DAILY WHERE C_BUSINESS_DATE=@d;

    ;WITH LD AS (
        SELECT C_SI_ID, MAX(C_EFFECTIVE_DATE) AS ED
        FROM T_MASTER_PORTFOLIO_TICKER WHERE C_EFFECTIVE_DATE<=@d GROUP BY C_SI_ID
    ),
    W AS (
        SELECT mw.C_SI_ID, mw.C_TICKER, mw.C_TARGET_WEIGHT
        FROM T_MASTER_PORTFOLIO_TICKER mw JOIN LD ON LD.C_SI_ID=mw.C_SI_ID AND LD.ED=mw.C_EFFECTIVE_DATE
    ),
    FACT AS (
        SELECT W.C_SI_ID,
               SUM( W.C_TARGET_WEIGHT * p.C_CLOSE_PRICE
                    / COALESCE(ca.C_ADJUSTED_REF_PRICE, pref.C_CLOSE_PRICE, p.C_CLOSE_PRICE) ) AS FACTOR
        FROM W
        JOIN T_PRICE_DAILY p         ON p.C_TICKER=W.C_TICKER AND p.C_BUSINESS_DATE=@d
        LEFT JOIN T_PRICE_DAILY pref ON pref.C_TICKER=W.C_TICKER AND pref.C_BUSINESS_DATE=@prev
        LEFT JOIN T_CORPORATE_ACTION ca ON ca.C_TICKER=W.C_TICKER AND ca.C_EX_DATE=@d
        GROUP BY W.C_SI_ID
    )
    INSERT INTO T_SI_INDEX_DAILY (C_BUSINESS_DATE,C_SI_ID,C_INDEX_VALUE,C_DAILY_RETURN)
    SELECT @d, f.C_SI_ID,
           COALESCE(pi.C_INDEX_VALUE, 1000) * f.FACTOR,
           f.FACTOR - 1
    FROM FACT f
    LEFT JOIN T_SI_INDEX_DAILY pi ON pi.C_SI_ID=f.C_SI_ID AND pi.C_BUSINESS_DATE=@prev;
END
GO

/*===========================================================================
  J13 — RECONCILE (cổng publish): sanity checks; lỗi → THROW chặn publish
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_RECONCILE @d DATE
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @bad INT;
    -- NAV âm hoặc unit<=0 nhưng NAV>0 (bất thường)
    SELECT @bad = COUNT(*) FROM T_EOD_WORK
    WHERE C_BUSINESS_DATE=@d AND (C_NAV < 0 OR (C_UNIT<=0 AND C_NAV>0));
    IF @bad > 0
        THROW 50013, 'RECONCILE: phát hiện vị thế NAV âm hoặc unit<=0 với NAV>0.', 1;
    -- Σ customer NAV per SI khớp T_SI_PERFORMANCE (derive cùng nguồn → phải khớp)
    SELECT @bad = COUNT(*)
    FROM T_SI_PERFORMANCE_DAILY p
    JOIN (SELECT C_SI_ID, SUM(C_NAV) NAV FROM T_EOD_WORK WHERE C_BUSINESS_DATE=@d GROUP BY C_SI_ID) a
      ON a.C_SI_ID=p.C_SI_ID AND p.C_BUSINESS_DATE=@d
    WHERE ABS(p.C_NAV - a.NAV) > 1;   -- ngưỡng làm tròn 1 VND
    IF @bad > 0
        THROW 50014, 'RECONCILE: SI NAV != Σ customer NAV.', 1;
END
GO

/*===========================================================================
  J14 — SNAPSHOT: T_HOLDING_DAILY (SI aggregate) + T_ASSET_SNAPSHOT_DAILY
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_SNAPSHOT @d DATE
AS
BEGIN
    SET NOCOUNT ON;
    -- holdings cấp SI + tỷ trọng
    DELETE FROM T_HOLDING_DAILY WHERE C_BUSINESS_DATE=@d;
    ;WITH sih AS (
        SELECT h.C_SI_ID, h.C_TICKER, SUM(h.C_QUANTITY) AS QTY
        FROM T_INDEXING_PORTFOLIO_TICKER h GROUP BY h.C_SI_ID, h.C_TICKER
    ),
    val AS (
        SELECT sih.C_SI_ID, sih.C_TICKER, sih.QTY, p.C_CLOSE_PRICE,
               sih.QTY*p.C_CLOSE_PRICE AS MV
        FROM sih JOIN T_PRICE_DAILY p ON p.C_TICKER=sih.C_TICKER AND p.C_BUSINESS_DATE=@d
    )
    INSERT INTO T_HOLDING_DAILY (C_BUSINESS_DATE,C_SI_ID,C_TICKER,C_QUANTITY,C_MARKET_PRICE,C_MARKET_VALUE,C_WEIGHT)
    SELECT @d, v.C_SI_ID, v.C_TICKER, v.QTY, v.C_CLOSE_PRICE, v.MV,
           CASE WHEN SUM(v.MV) OVER (PARTITION BY v.C_SI_ID) > 0
                THEN v.MV / SUM(v.MV) OVER (PARTITION BY v.C_SI_ID) END
    FROM val v;

    -- snapshot tài sản cấp SI
    DELETE FROM T_ASSET_SNAPSHOT_DAILY WHERE C_BUSINESS_DATE=@d;
    INSERT INTO T_ASSET_SNAPSHOT_DAILY (C_BUSINESS_DATE,C_SI_ID,C_CASH,C_STOCK_VALUE,C_PAYABLE_FEE,C_TOTAL_ASSET,C_NAV)
    SELECT @d, C_SI_ID, SUM(C_CASH), SUM(C_STOCK_VALUE), SUM(C_PAYABLE_FEE),
           SUM(C_CASH+C_STOCK_VALUE), SUM(C_NAV)
    FROM T_EOD_WORK WHERE C_BUSINESS_DATE=@d GROUP BY C_SI_ID;
END
GO

/*===========================================================================
  DISPATCHER: chạy 1 job idempotent + transaction + log (resume-safe)
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_STEP @d DATE, @job VARCHAR(40), @proc SYSNAME
AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT 1 FROM T_EOD_RUN WHERE C_BUSINESS_DATE=@d AND C_JOB=@job AND C_STATUS='DONE')
        RETURN;
    EXEC SP_EOD_LOG @d,@job,'RUNNING';
    BEGIN TRY
        BEGIN TRAN;
        DECLARE @sql NVARCHAR(300) = N'EXEC ' + QUOTENAME(@proc) + N' @d=@d';
        EXEC sp_executesql @sql, N'@d DATE', @d=@d;
        COMMIT;
        EXEC SP_EOD_LOG @d,@job,'DONE';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        DECLARE @err NVARCHAR(2000) = ERROR_MESSAGE();
        EXEC SP_EOD_LOG @d,@job,'FAILED', NULL, @err;
        THROW;
    END CATCH
END
GO

/*===========================================================================
  MASTER ORCHESTRATOR — app chỉ EXEC proc này
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_RUN @C_BUSINESS_DATE DATE
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    DECLARE @d DATE = @C_BUSINESS_DATE;

    EXEC SP_EOD_STEP @d, 'J01_SYNC_FO',  'SP_EOD_SYNC_FO';         -- mirror holdings+cash từ FO (overwrite)
    -- (ĐÃ BỎ J06_FEE) — phí QL + thuế GD do FO trừ vào cash khi book; SDI KHÔNG accrue lại (tránh double-count)
    EXEC SP_EOD_STEP @d, 'J07_COMPUTE',  'SP_EOD_COMPUTE';         -- MTM→NAV→PnL→Unit + roll-forward + perf per-KH
    EXEC SP_EOD_STEP @d, 'J11_SI_AGG',   'SP_EOD_SI_AGG';
    EXEC SP_EOD_STEP @d, 'J12_SI_INDEX', 'SP_EOD_SI_INDEX';
    EXEC SP_EOD_STEP @d, 'J13_RECONCILE','SP_EOD_RECONCILE';       -- cổng
    EXEC SP_EOD_STEP @d, 'J14_SNAPSHOT', 'SP_EOD_SNAPSHOT';
    -- J15 PUBLISH: push sang Asset (current snapshot + SI series) — adapter riêng
END
GO
