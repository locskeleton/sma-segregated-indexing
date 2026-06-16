/*==============================================================================
  SDI MODULE — ENGINE CORE (SQL Server)  | ALL-IN-DB, set-based, no RBAR
  Naming: SP_ procs, UDF_ functions, T_/C_ tables/cols.
  Mô hình roll-forward: T_POSITION_STATE (current) + áp delta ngày @d → tính lại.
  Thứ tự (master SP_EOD_RUN): J05 → J03 → J06 → J04 → J07 → J11 → J12 → J13 → J14
  (J05 cashflow chạy trước để tạo state cho KH mới; J03 CA dùng holdings trước trade.)
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
  J05 — APPLY CASHFLOW  (tạo state cho KH mới + cộng net cashflow vào cash)
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_APPLY_CASHFLOW @d DATE
AS
BEGIN
    SET NOCOUNT ON;
    ;WITH cf AS (
        SELECT C_CUSTOMER_ID, C_SI_ID,
               SUM(CASE WHEN C_EVENT_TYPE='WITHDRAW' THEN -C_AMOUNT ELSE C_AMOUNT END) AS CF_NET
        FROM T_CASHFLOW_EVENT WHERE C_BUSINESS_DATE=@d
        GROUP BY C_CUSTOMER_ID, C_SI_ID
    )
    MERGE T_POSITION_STATE AS s
    USING cf ON s.C_CUSTOMER_ID=cf.C_CUSTOMER_ID AND s.C_SI_ID=cf.C_SI_ID
    WHEN MATCHED THEN UPDATE SET C_CASH = s.C_CASH + cf.CF_NET
    WHEN NOT MATCHED THEN INSERT (C_CUSTOMER_ID,C_SI_ID,C_UNIT,C_CASH,C_PAYABLE_FEE,C_LAST_NAV,C_LAST_UNIT_PRICE,C_STATUS)
        VALUES (cf.C_CUSTOMER_ID,cf.C_SI_ID,0,cf.CF_NET,0,0,NULL,'ACTIVE');
END
GO

/*===========================================================================
  J03 — APPLY CORPORATE ACTION (cổ tức tiền → cash income; split/stock-div → qty)
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_APPLY_CA @d DATE
AS
BEGIN
    SET NOCOUNT ON;
    -- cổ tức tiền mặt: income → cộng vào cash (KHÔNG phải cashflow)
    UPDATE s SET C_CASH = s.C_CASH + d.DIV
    FROM T_POSITION_STATE s
    JOIN (
        SELECT h.C_CUSTOMER_ID, h.C_SI_ID, SUM(h.C_QUANTITY * ca.C_CASH_DIV_PER_SHARE) AS DIV
        FROM T_POSITION_HOLDING h
        JOIN T_CORPORATE_ACTION ca ON ca.C_TICKER=h.C_TICKER AND ca.C_EX_DATE=@d AND ca.C_CA_TYPE='CASH_DIV'
        GROUP BY h.C_CUSTOMER_ID, h.C_SI_ID
    ) d ON d.C_CUSTOMER_ID=s.C_CUSTOMER_ID AND d.C_SI_ID=s.C_SI_ID;

    -- split / cổ tức cổ phiếu: điều chỉnh khối lượng (ratio = new/old)
    INSERT INTO T_CUSTOMER_HOLDING_EVENT (C_CUSTOMER_ID,C_SI_ID,C_TICKER,C_BUSINESS_DATE,C_QTY_DELTA,C_SOURCE)
    SELECT h.C_CUSTOMER_ID,h.C_SI_ID,h.C_TICKER,@d, h.C_QUANTITY*(ca.C_RATIO-1), 'CA'
    FROM T_POSITION_HOLDING h
    JOIN T_CORPORATE_ACTION ca ON ca.C_TICKER=h.C_TICKER AND ca.C_EX_DATE=@d AND ca.C_CA_TYPE IN ('SPLIT','STOCK_DIV');

    UPDATE h SET C_QUANTITY = h.C_QUANTITY * ca.C_RATIO
    FROM T_POSITION_HOLDING h
    JOIN T_CORPORATE_ACTION ca ON ca.C_TICKER=h.C_TICKER AND ca.C_EX_DATE=@d AND ca.C_CA_TYPE IN ('SPLIT','STOCK_DIV');
END
GO

/*===========================================================================
  J04 — APPLY EXECUTION (trade-date): holdings qty + cash mua/bán
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_APPLY_EXEC @d DATE
AS
BEGIN
    SET NOCOUNT ON;
    -- holdings
    ;WITH e AS (
        SELECT C_CUSTOMER_ID, C_SI_ID, C_TICKER,
               SUM(CASE WHEN C_SIDE='BUY' THEN C_QTY ELSE -C_QTY END) AS QTY_DELTA
        FROM T_EXECUTION_FEED WHERE C_BUSINESS_DATE=@d
        GROUP BY C_CUSTOMER_ID, C_SI_ID, C_TICKER
    )
    MERGE T_POSITION_HOLDING AS h
    USING e ON h.C_CUSTOMER_ID=e.C_CUSTOMER_ID AND h.C_SI_ID=e.C_SI_ID AND h.C_TICKER=e.C_TICKER
    WHEN MATCHED THEN UPDATE SET C_QUANTITY = h.C_QUANTITY + e.QTY_DELTA
    WHEN NOT MATCHED THEN INSERT (C_CUSTOMER_ID,C_SI_ID,C_TICKER,C_QUANTITY)
        VALUES (e.C_CUSTOMER_ID,e.C_SI_ID,e.C_TICKER,e.QTY_DELTA);

    -- holding events
    INSERT INTO T_CUSTOMER_HOLDING_EVENT (C_CUSTOMER_ID,C_SI_ID,C_TICKER,C_BUSINESS_DATE,C_QTY_DELTA,C_SOURCE)
    SELECT C_CUSTOMER_ID,C_SI_ID,C_TICKER,@d,
           SUM(CASE WHEN C_SIDE='BUY' THEN C_QTY ELSE -C_QTY END),'EXEC'
    FROM T_EXECUTION_FEED WHERE C_BUSINESS_DATE=@d
    GROUP BY C_CUSTOMER_ID,C_SI_ID,C_TICKER;

    -- cash: BUY giảm tiền (qty*price), SELL tăng tiền
    UPDATE s SET C_CASH = s.C_CASH - x.NET_BUY
    FROM T_POSITION_STATE s
    JOIN (
        SELECT C_CUSTOMER_ID, C_SI_ID,
               SUM(CASE WHEN C_SIDE='BUY' THEN C_QTY*C_EXEC_PRICE ELSE -C_QTY*C_EXEC_PRICE END) AS NET_BUY
        FROM T_EXECUTION_FEED WHERE C_BUSINESS_DATE=@d GROUP BY C_CUSTOMER_ID,C_SI_ID
    ) x ON x.C_CUSTOMER_ID=s.C_CUSTOMER_ID AND x.C_SI_ID=s.C_SI_ID;

    DELETE FROM T_POSITION_HOLDING WHERE C_QUANTITY = 0;
END
GO

/*===========================================================================
  J06 — ACCRUE MGMT FEE: payable += NAV_prev × rate/365  (hạch toán theo ngày)
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_ACCRUE_FEE @d DATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE s SET C_PAYABLE_FEE = s.C_PAYABLE_FEE
                 + s.C_LAST_NAV * (COALESCE(cs.C_MGMT_FEE_RATE, st.C_MGMT_FEE_RATE) / 365.0)
    FROM T_POSITION_STATE s
    JOIN T_STRATEGY st ON st.C_SI_ID = s.C_SI_ID
    LEFT JOIN T_CUSTOMER_SI cs ON cs.C_CUSTOMER_ID=s.C_CUSTOMER_ID AND cs.C_SI_ID=s.C_SI_ID
    WHERE s.C_STATUS='ACTIVE';
    -- THU phí: ngày thu tháng cố định → cash -= payable_mgmt; payable=0 (TODO: tách custody khỏi mgmt nếu cần)
END
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
        FROM T_POSITION_HOLDING h
        JOIN T_PRICE_DAILY p ON p.C_TICKER=h.C_TICKER AND p.C_BUSINESS_DATE=@d
        GROUP BY h.C_CUSTOMER_ID, h.C_SI_ID
    ) m ON m.C_CUSTOMER_ID=w.C_CUSTOMER_ID AND m.C_SI_ID=w.C_SI_ID
    WHERE w.C_BUSINESS_DATE=@d;

    -- J08 NAV = stock + cash − phí phải trả
    UPDATE T_EOD_WORK SET C_NAV = C_STOCK_VALUE + C_CASH - C_PAYABLE_FEE WHERE C_BUSINESS_DATE=@d;

    -- J09 PnL = NAV − NAV_prev + ra − vào
    UPDATE T_EOD_WORK SET C_DAILY_PNL = C_NAV - C_LAST_NAV + C_CF_OUT - C_CF_IN WHERE C_BUSINESS_DATE=@d;

    -- J10 Unit (historic: ΔUnit = CF/UnitPrice_(t-1)); init khi unit_prev=0 → unit=NAV/10000, UP=10000
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
        FROM T_MODEL_WEIGHT WHERE C_EFFECTIVE_DATE<=@d GROUP BY C_SI_ID
    ),
    W AS (
        SELECT mw.C_SI_ID, mw.C_TICKER, mw.C_TARGET_WEIGHT
        FROM T_MODEL_WEIGHT mw JOIN LD ON LD.C_SI_ID=mw.C_SI_ID AND LD.ED=mw.C_EFFECTIVE_DATE
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
        FROM T_POSITION_HOLDING h GROUP BY h.C_SI_ID, h.C_TICKER
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

    EXEC SP_EOD_STEP @d, 'J05_CASHFLOW', 'SP_EOD_APPLY_CASHFLOW';  -- tạo state KH mới
    EXEC SP_EOD_STEP @d, 'J03_CA',       'SP_EOD_APPLY_CA';        -- cổ tức/split (holdings trước trade)
    EXEC SP_EOD_STEP @d, 'J04_EXEC',     'SP_EOD_APPLY_EXEC';
    EXEC SP_EOD_STEP @d, 'J06_FEE',      'SP_EOD_ACCRUE_FEE';
    EXEC SP_EOD_STEP @d, 'J07_COMPUTE',  'SP_EOD_COMPUTE';         -- MTM→NAV→PnL→Unit + roll-forward
    EXEC SP_EOD_STEP @d, 'J11_SI_AGG',   'SP_EOD_SI_AGG';
    EXEC SP_EOD_STEP @d, 'J12_SI_INDEX', 'SP_EOD_SI_INDEX';
    EXEC SP_EOD_STEP @d, 'J13_RECONCILE','SP_EOD_RECONCILE';       -- cổng
    EXEC SP_EOD_STEP @d, 'J14_SNAPSHOT', 'SP_EOD_SNAPSHOT';
    -- J15 PUBLISH: push sang Asset (current snapshot + SI series) — adapter riêng
END
GO
