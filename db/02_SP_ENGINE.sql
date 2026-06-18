SET QUOTED_IDENTIFIER ON;  -- procs ghi/đọc bảng có filtered index → cần QI ON lúc CREATE PROC
SET ANSI_NULLS ON;
GO
/*==============================================================================
  SDI MODULE — ENGINE CORE (SQL Server)  | ALL-IN-DB, set-based, no RBAR
  Naming: SP_ procs, UDF_ functions, T_/C_ tables/cols.
  Mô hình roll-forward: T_SI_NAV_CURRENT (current) + áp delta ngày @d → tính lại.
  INGEST (Kafka per-KH realtime): SP_INGEST_CUSTOMER → cash→state + holdings→current + interval
    history (holding/cash) + cổ tức/phí. (Thay J01_SYNC_FO + J14B_HISTORY cũ — giờ làm tại ingest.)
  Thứ tự (master SP_EOD_RUN): J0_GATE → J07 → J11 → J12 → J13 → J14
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
        C_STARTED_AT = CASE WHEN @status='RUNNING' THEN GETDATE() ELSE t.C_STARTED_AT END,
        C_ENDED_AT   = CASE WHEN @status IN ('DONE','FAILED') THEN GETDATE() ELSE t.C_ENDED_AT END,
        C_MESSAGE    = @msg
    WHEN NOT MATCHED THEN INSERT (C_BUSINESS_DATE,C_JOB,C_STATUS,C_ROWS,C_STARTED_AT,C_ENDED_AT,C_MESSAGE)
        VALUES (@d,@job,@status,@rows,
                CASE WHEN @status='RUNNING' THEN GETDATE() END,
                CASE WHEN @status IN ('DONE','FAILED') THEN GETDATE() END, @msg);
END
GO

/*===========================================================================
  INGEST — Kafka per-KH (FORWARD). App đọc event 1 KH → EXEC proc này (JSON).
    Xử lý NGAY khi nhận: cash (state + interval CASH_HIST), holdings (current + interval
    HOLDING_HIST), cổ tức/phí (append dedup). Thay J01_SYNC_FO + J14B_HISTORY cũ.
  Idempotent: cash/holdings so-trạng-thái (redelivery=no-op); fee dedup theo C_SOURCE_EVENT_ID.
  FORWARD-ONLY: event quá khứ (business_date < watermark) → THROW (history CHƯA hỗ trợ — sẽ làm sau).
  Cashflow nạp/rút KHÔNG qua đây (SDI là nguồn → ghi thẳng T_SI_CASHFLOW_EVENT).
  JSON (seam FO — chốt spec chỉ sửa lớp parse này): event định danh theo SUB-ACCOUNT (si_account).
    {"cust_code","business_date","sub_accounts":[
        {"si_account","cash","holdings":[{"ticker","quantity","avg_cost"}],
         "fees":[{"event_id","type","ticker","amount"}]}]}
  Master suy từ T_SI_PORTFOLIO (sub-account phải đăng ký trước khi ingest).
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_INGEST_CUSTOMER @json NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    DECLARE @cust VARCHAR(10) = JSON_VALUE(@json,'$.cust_code');
    DECLARE @d    DATE        = TRY_CONVERT(DATE, JSON_VALUE(@json,'$.business_date'));
    IF @cust IS NULL OR @d IS NULL THROW 50020, 'INGEST: thiếu cust_code/business_date.', 1;

    BEGIN TRY
        BEGIN TRAN;

        -- sub-account của event; master suy từ config (T_SI_PORTFOLIO)
        DECLARE @sub TABLE (C_SI_ACCOUNT VARCHAR(20) PRIMARY KEY, C_MASTER_CODE VARCHAR(20), C_CASH DECIMAL(20,0));
        INSERT INTO @sub (C_SI_ACCOUNT, C_MASTER_CODE, C_CASH)
        SELECT j.C_SI_ACCOUNT, ip.C_MASTER_CODE, j.C_CASH
        FROM OPENJSON(@json,'$.sub_accounts') WITH (C_SI_ACCOUNT VARCHAR(20) '$.si_account', C_CASH DECIMAL(20,0) '$.cash') j
        LEFT JOIN T_SI_PORTFOLIO ip ON ip.C_SI_ACCOUNT = j.C_SI_ACCOUNT AND ip.C_CUST_CODE = @cust;

        IF EXISTS (SELECT 1 FROM @sub WHERE C_MASTER_CODE IS NULL)
            THROW 50022, 'INGEST: sub-account chưa đăng ký (thiếu T_SI_PORTFOLIO cho cust/si_account).', 1;

        -- FORWARD guard: sub-account đã sync ngày MỚI HƠN @d ⇒ event quá khứ
        IF EXISTS (SELECT 1 FROM @sub n JOIN T_SI_NAV_CURRENT s
                   ON s.C_SI_ACCOUNT=n.C_SI_ACCOUNT WHERE s.C_LAST_SYNC_DATE > @d)
            THROW 50021, 'INGEST: event quá khứ (< watermark) — history CHƯA hỗ trợ (forward-only).', 1;

        /* HOLDINGS: overwrite current (theo sub-account) + diff interval (key C_SI_ACCOUNT) */
        DECLARE @hold TABLE (C_SI_ACCOUNT VARCHAR(20), C_TICKER VARCHAR(20), C_QUANTITY DECIMAL(20,0), C_AVG_COST DECIMAL(18,4));
        INSERT INTO @hold
        SELECT sa.C_SI_ACCOUNT, h.C_TICKER, h.C_QUANTITY, h.C_AVG_COST
        FROM OPENJSON(@json,'$.sub_accounts') WITH (C_SI_ACCOUNT VARCHAR(20) '$.si_account', holdings NVARCHAR(MAX) '$.holdings' AS JSON) sa
        OUTER APPLY OPENJSON(sa.holdings) WITH (C_TICKER VARCHAR(20) '$.ticker', C_QUANTITY DECIMAL(20,0) '$.quantity', C_AVG_COST DECIMAL(18,4) '$.avg_cost') h
        WHERE h.C_TICKER IS NOT NULL;

        DELETE t FROM T_SI_PORTFOLIO_HOLDING t WHERE t.C_SI_ACCOUNT IN (SELECT C_SI_ACCOUNT FROM @sub);
        INSERT INTO T_SI_PORTFOLIO_HOLDING (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_TICKER,C_QUANTITY,C_AVG_COST)
        SELECT hd.C_SI_ACCOUNT, @cust, s.C_MASTER_CODE, hd.C_TICKER, hd.C_QUANTITY, hd.C_AVG_COST
        FROM @hold hd JOIN @sub s ON s.C_SI_ACCOUNT=hd.C_SI_ACCOUNT;

        UPDATE h SET C_VALID_TO=@d
        FROM T_SI_HOLDING_HIST h
        LEFT JOIN T_SI_PORTFOLIO_HOLDING c
          ON c.C_SI_ACCOUNT=h.C_SI_ACCOUNT AND c.C_TICKER=h.C_TICKER
        WHERE h.C_SI_ACCOUNT IN (SELECT C_SI_ACCOUNT FROM @sub) AND h.C_VALID_TO IS NULL
          AND ( c.C_SI_ACCOUNT IS NULL OR c.C_QUANTITY<>h.C_QUANTITY OR ISNULL(c.C_AVG_COST,-1)<>ISNULL(h.C_AVG_COST,-1) );
        INSERT INTO T_SI_HOLDING_HIST (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_TICKER,C_VALID_FROM,C_VALID_TO,C_QUANTITY,C_AVG_COST)
        SELECT c.C_SI_ACCOUNT,c.C_CUST_CODE,c.C_MASTER_CODE,c.C_TICKER,@d,NULL,c.C_QUANTITY,c.C_AVG_COST
        FROM T_SI_PORTFOLIO_HOLDING c
        WHERE c.C_SI_ACCOUNT IN (SELECT C_SI_ACCOUNT FROM @sub)
          AND NOT EXISTS (SELECT 1 FROM T_SI_HOLDING_HIST h
              WHERE h.C_VALID_TO IS NULL AND h.C_SI_ACCOUNT=c.C_SI_ACCOUNT AND h.C_TICKER=c.C_TICKER
                AND h.C_QUANTITY=c.C_QUANTITY AND ISNULL(h.C_AVG_COST,-1)=ISNULL(c.C_AVG_COST,-1));

        /* CASH: diff interval + cập nhật state + watermark (tạo state sub-account mới) */
        UPDATE h SET C_VALID_TO=@d
        FROM T_SI_CASH_HIST h JOIN @sub n ON n.C_SI_ACCOUNT=h.C_SI_ACCOUNT
        WHERE h.C_VALID_TO IS NULL AND h.C_CASH<>n.C_CASH;
        INSERT INTO T_SI_CASH_HIST (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_VALID_FROM,C_VALID_TO,C_CASH)
        SELECT n.C_SI_ACCOUNT, @cust, n.C_MASTER_CODE, @d, NULL, n.C_CASH FROM @sub n
        WHERE NOT EXISTS (SELECT 1 FROM T_SI_CASH_HIST h
            WHERE h.C_VALID_TO IS NULL AND h.C_SI_ACCOUNT=n.C_SI_ACCOUNT AND h.C_CASH=n.C_CASH);

        MERGE T_SI_NAV_CURRENT s
        USING @sub n ON s.C_SI_ACCOUNT=n.C_SI_ACCOUNT
        WHEN MATCHED THEN UPDATE SET s.C_CASH=n.C_CASH, s.C_LAST_SYNC_DATE=@d
        WHEN NOT MATCHED THEN INSERT (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_UNIT,C_CASH,C_PAYABLE_FEE,C_LAST_NAV,C_LAST_UNIT_PRICE,C_STATUS,C_LAST_SYNC_DATE)
            VALUES (n.C_SI_ACCOUNT,@cust,n.C_MASTER_CODE,0,n.C_CASH,0,0,NULL,'ACTIVE',@d);

        /* CỔ TỨC/PHÍ: append, dedup theo C_SOURCE_EVENT_ID (idempotent redelivery) */
        INSERT INTO T_SI_FEE_INCOME (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_TYPE,C_TICKER,C_AMOUNT,C_SOURCE,C_SOURCE_EVENT_ID)
        SELECT @d, f.C_SI_ACCOUNT, @cust, s.C_MASTER_CODE, f.C_TYPE, f.C_TICKER, f.C_AMOUNT, 'FO', f.event_id
        FROM (
            SELECT sa.C_SI_ACCOUNT, x.event_id, x.C_TYPE, x.C_TICKER, x.C_AMOUNT
            FROM OPENJSON(@json,'$.sub_accounts') WITH (C_SI_ACCOUNT VARCHAR(20) '$.si_account', fees NVARCHAR(MAX) '$.fees' AS JSON) sa
            OUTER APPLY OPENJSON(sa.fees) WITH (event_id VARCHAR(64) '$.event_id', C_TYPE VARCHAR(20) '$.type', C_TICKER VARCHAR(20) '$.ticker', C_AMOUNT DECIMAL(20,0) '$.amount') x
            WHERE x.C_TYPE IS NOT NULL
        ) f
        JOIN @sub s ON s.C_SI_ACCOUNT=f.C_SI_ACCOUNT
        WHERE f.event_id IS NULL OR NOT EXISTS (SELECT 1 FROM T_SI_FEE_INCOME e WHERE e.C_SOURCE_EVENT_ID=f.event_id);

        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        THROW;
    END CATCH
END
GO

-- (ĐÃ BỎ J03 APPLY_CA & J04 APPLY_EXEC) — FO sync đã phản ánh cổ tức/split/trade vào cash+holdings.
--   T_CORPORATE_ACTION chỉ còn dùng cho SI INDEX (điều chỉnh P_ref khi có quyền — J12).

-- (ĐÃ BỎ J06 ACCRUE_FEE) — phương án (A): phí quản lý + thuế GD do FO trừ vào cash khi book.
--   FO cash đã NET → SDI KHÔNG accrue/trừ lại (tránh double-count). NAV = stock_value + FO_cash.
GO

/*===========================================================================
  SP_EOD_HISTORY — UTILITY (KHÔNG còn trong EOD pipeline). History forward giờ do
        SP_INGEST_CUSTOMER maintain per-event. Proc này = BULK BACKFILL/sửa lỗi: DIFF
        TOÀN BỘ current vs open-row 1 phát (vd init lần đầu, hoặc dựng lại history).
        Idempotent (ngày không biến động → 0 ghi).
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_HISTORY @d DATE
AS
BEGIN
    SET NOCOUNT ON;

    /* ---- HOLDINGS: DIFF current (T_SI_PORTFOLIO_HOLDING) vs open-row ---- */
    -- ĐÓNG open-row có qty/avg_cost ĐỔI hoặc mã BIẾN MẤT khỏi current
    UPDATE h SET C_VALID_TO=@d
    FROM T_SI_HOLDING_HIST h
    LEFT JOIN T_SI_PORTFOLIO_HOLDING c
      ON c.C_SI_ACCOUNT=h.C_SI_ACCOUNT AND c.C_TICKER=h.C_TICKER
    WHERE h.C_VALID_TO IS NULL
      AND ( c.C_SI_ACCOUNT IS NULL
         OR c.C_QUANTITY <> h.C_QUANTITY
         OR ISNULL(c.C_AVG_COST,-1) <> ISNULL(h.C_AVG_COST,-1) );
    -- MỞ dòng mới cho mã trong current chưa có open-row khớp y hệt (mã mới / vừa đổi)
    INSERT INTO T_SI_HOLDING_HIST (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_TICKER,C_VALID_FROM,C_VALID_TO,C_QUANTITY,C_AVG_COST)
    SELECT c.C_SI_ACCOUNT,c.C_CUST_CODE,c.C_MASTER_CODE,c.C_TICKER,@d,NULL,c.C_QUANTITY,c.C_AVG_COST
    FROM T_SI_PORTFOLIO_HOLDING c
    WHERE NOT EXISTS (SELECT 1 FROM T_SI_HOLDING_HIST h
        WHERE h.C_VALID_TO IS NULL AND h.C_SI_ACCOUNT=c.C_SI_ACCOUNT AND h.C_TICKER=c.C_TICKER
          AND h.C_QUANTITY=c.C_QUANTITY AND ISNULL(h.C_AVG_COST,-1)=ISNULL(c.C_AVG_COST,-1));

    /* ---- CASH: DIFF state.cash (T_SI_NAV_CURRENT) vs open-row ---- */
    UPDATE h SET C_VALID_TO=@d
    FROM T_SI_CASH_HIST h
    JOIN T_SI_NAV_CURRENT s ON s.C_SI_ACCOUNT=h.C_SI_ACCOUNT
    WHERE h.C_VALID_TO IS NULL AND s.C_CASH <> h.C_CASH;
    INSERT INTO T_SI_CASH_HIST (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_VALID_FROM,C_VALID_TO,C_CASH)
    SELECT s.C_SI_ACCOUNT,s.C_CUST_CODE,s.C_MASTER_CODE,@d,NULL,s.C_CASH
    FROM T_SI_NAV_CURRENT s
    WHERE NOT EXISTS (SELECT 1 FROM T_SI_CASH_HIST h
        WHERE h.C_VALID_TO IS NULL AND h.C_SI_ACCOUNT=s.C_SI_ACCOUNT AND h.C_CASH=s.C_CASH);
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

    -- seed từ state (đã áp delta cash/payable; last_nav/last_up = hôm qua). KEY = C_SI_ACCOUNT.
    INSERT INTO T_EOD_WORK (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_CASH,C_PAYABLE_FEE,C_LAST_NAV,C_LAST_UNIT_PRICE,C_UNIT_PREV)
    SELECT @d,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_CASH,C_PAYABLE_FEE,C_LAST_NAV,C_LAST_UNIT_PRICE,C_UNIT
    FROM T_SI_NAV_CURRENT WHERE C_STATUS='ACTIVE';

    -- CF của ngày (cho PnL & unit) — group theo sub-account
    UPDATE w SET w.C_CF_IN = cf.CF_IN, w.C_CF_OUT = cf.CF_OUT
    FROM T_EOD_WORK w
    JOIN (
        SELECT C_SI_ACCOUNT,
               SUM(CASE WHEN C_EVENT_TYPE='WITHDRAW' THEN 0 ELSE C_AMOUNT END) AS CF_IN,
               SUM(CASE WHEN C_EVENT_TYPE='WITHDRAW' THEN C_AMOUNT ELSE 0 END) AS CF_OUT
        FROM T_SI_CASHFLOW_EVENT WHERE C_BUSINESS_DATE=@d GROUP BY C_SI_ACCOUNT
    ) cf ON cf.C_SI_ACCOUNT=w.C_SI_ACCOUNT
    WHERE w.C_BUSINESS_DATE=@d;

    -- J07 MTM (câu nặng nhất — set-based; prod chạy batch-mode trên columnstore) — group theo sub-account
    UPDATE w SET w.C_STOCK_VALUE = m.SV
    FROM T_EOD_WORK w
    JOIN (
        SELECT h.C_SI_ACCOUNT, SUM(h.C_QUANTITY * p.C_CLOSE_PRICE) AS SV
        FROM T_SI_PORTFOLIO_HOLDING h
        JOIN T_PRICE_DAILY p ON p.C_TICKER=h.C_TICKER AND p.C_BUSINESS_DATE=@d
        GROUP BY h.C_SI_ACCOUNT
    ) m ON m.C_SI_ACCOUNT=w.C_SI_ACCOUNT
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
    FROM T_SI_NAV_CURRENT s
    JOIN T_EOD_WORK w ON w.C_SI_ACCOUNT=s.C_SI_ACCOUNT AND w.C_BUSINESS_DATE=@d;

    -- unit ledger (chỉ ngày có cashflow)
    DELETE FROM T_SI_UNIT_LEDGER WHERE C_BUSINESS_DATE=@d;
    INSERT INTO T_SI_UNIT_LEDGER (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_BUSINESS_DATE,C_CF_NET,C_DELTA_UNIT,C_UNIT)
    SELECT C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,@d,(C_CF_IN-C_CF_OUT),C_DELTA_UNIT,C_UNIT
    FROM T_EOD_WORK WHERE C_BUSINESS_DATE=@d AND (C_CF_IN-C_CF_OUT)<>0;

    -- LỊCH SỬ per sub-account (materialize để vẽ chart FR-03)
    DELETE FROM T_SI_NAV_BALANCE WHERE C_BUSINESS_DATE=@d;
    INSERT INTO T_SI_NAV_BALANCE (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_NAV,C_UNIT,C_UNIT_PRICE,C_DAILY_PNL,C_DAILY_RETURN)
    SELECT @d, C_SI_ACCOUNT, C_CUST_CODE, C_MASTER_CODE, C_NAV, C_UNIT, C_UNIT_PRICE, C_DAILY_PNL,
           CASE WHEN C_LAST_UNIT_PRICE>0 THEN C_UNIT_PRICE/C_LAST_UNIT_PRICE - 1 END
    FROM T_EOD_WORK WHERE C_BUSINESS_DATE=@d;
END
GO

/*===========================================================================
  J11 — SI AGGREGATE → T_MASTER_NAV_BALANCE (composition + NAV + hiệu suất + cổ tức/phí)
         + upsert T_MASTER_NAV_CURRENT (snapshot current cấp SI cho serving)
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_SI_AGG @d DATE
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @prev DATE = dbo.UDF_PREV_BUSINESS_DATE(@d);
    DELETE FROM T_MASTER_NAV_BALANCE WHERE C_BUSINESS_DATE=@d;

    ;WITH agg AS (
        SELECT C_MASTER_CODE,
               SUM(C_CASH) AS CASH, SUM(C_STOCK_VALUE) AS STOCK, SUM(C_PAYABLE_FEE) AS PAY,
               SUM(C_NAV) AS NAV, SUM(C_UNIT) AS UNT, SUM(C_DAILY_PNL) AS PNL
        FROM T_EOD_WORK WHERE C_BUSINESS_DATE=@d GROUP BY C_MASTER_CODE
    ),
    fee AS (   -- cổ tức + phí per-KH → SUM lên SI (sparse). SI không có sự kiện nào → NULL
        SELECT C_MASTER_CODE,                                       -- (LEFT JOIN); loại phí vắng trong ngày có sự kiện → 0
               SUM(CASE WHEN C_TYPE='DIVIDEND'    THEN C_AMOUNT ELSE 0 END) AS DIV,
               SUM(CASE WHEN C_TYPE='CUSTODY_FEE' THEN C_AMOUNT ELSE 0 END) AS CUST,
               SUM(CASE WHEN C_TYPE='MGMT_FEE'    THEN C_AMOUNT ELSE 0 END) AS MGMT
        FROM T_SI_FEE_INCOME WHERE C_BUSINESS_DATE=@d GROUP BY C_MASTER_CODE
    )
    INSERT INTO T_MASTER_NAV_BALANCE (C_BUSINESS_DATE,C_MASTER_CODE,C_CASH,C_STOCK_VALUE,
                             C_CASH_DIVIDEND,C_CUSTODY_FEE,C_MGMT_FEE_ACCRUED,C_PAYABLE_FEE,C_TOTAL_ASSET,
                             C_NAV,C_UNIT,C_UNIT_PRICE,C_DAILY_PNL,C_DAILY_RETURN)
    SELECT @d, a.C_MASTER_CODE, a.CASH, a.STOCK,
           f.DIV, f.CUST, f.MGMT, a.PAY, a.CASH+a.STOCK,
           a.NAV, a.UNT,
           CASE WHEN a.UNT>0 THEN a.NAV/a.UNT END,
           a.PNL,
           CASE WHEN a.UNT>0 AND prev.C_UNIT_PRICE>0 THEN (a.NAV/a.UNT)/prev.C_UNIT_PRICE - 1 END
    FROM agg a
    LEFT JOIN fee f ON f.C_MASTER_CODE=a.C_MASTER_CODE
    LEFT JOIN T_MASTER_NAV_BALANCE prev ON prev.C_MASTER_CODE=a.C_MASTER_CODE AND prev.C_BUSINESS_DATE=@prev;

    -- current cấp SI (overwrite) — đọc nhanh "toàn bộ quỹ hiện tại", khỏi WHERE date=MAX
    MERGE T_MASTER_NAV_CURRENT AS t
    USING (SELECT C_MASTER_CODE,C_CASH,C_STOCK_VALUE,C_TOTAL_ASSET,C_NAV,C_UNIT,C_UNIT_PRICE,C_BUSINESS_DATE
           FROM T_MASTER_NAV_BALANCE WHERE C_BUSINESS_DATE=@d) s
    ON t.C_MASTER_CODE=s.C_MASTER_CODE
    WHEN MATCHED THEN UPDATE SET
        t.C_CASH=s.C_CASH, t.C_STOCK_VALUE=s.C_STOCK_VALUE, t.C_TOTAL_ASSET=s.C_TOTAL_ASSET,
        t.C_LAST_NAV=s.C_NAV, t.C_UNIT=s.C_UNIT, t.C_LAST_UNIT_PRICE=s.C_UNIT_PRICE,
        t.C_LAST_BUSINESS_DATE=s.C_BUSINESS_DATE
    WHEN NOT MATCHED THEN INSERT (C_MASTER_CODE,C_CASH,C_STOCK_VALUE,C_TOTAL_ASSET,C_LAST_NAV,C_UNIT,C_LAST_UNIT_PRICE,C_LAST_BUSINESS_DATE)
        VALUES (s.C_MASTER_CODE,s.C_CASH,s.C_STOCK_VALUE,s.C_TOTAL_ASSET,s.C_NAV,s.C_UNIT,s.C_UNIT_PRICE,s.C_BUSINESS_DATE);
END
GO

/*===========================================================================
  J12 — SI INDEX (danh mục mẫu, 100% cổ phiếu) → T_MASTER_INDEX_DAILY
        Index_t = Index_(t-1) × Σ w^(t) × P_t / P_ref ;  w^(t)=eff_date≤@d mới nhất
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_SI_INDEX @d DATE
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @prev DATE = dbo.UDF_PREV_BUSINESS_DATE(@d);
    DELETE FROM T_MASTER_INDEX_DAILY WHERE C_BUSINESS_DATE=@d;

    ;WITH LD AS (
        SELECT C_MASTER_CODE, MAX(C_EFFECTIVE_DATE) AS ED
        FROM T_MASTER_PORTFOLIO_TICKER WHERE C_EFFECTIVE_DATE<=@d GROUP BY C_MASTER_CODE
    ),
    W AS (
        SELECT mw.C_MASTER_CODE, mw.C_TICKER, mw.C_TARGET_WEIGHT
        FROM T_MASTER_PORTFOLIO_TICKER mw JOIN LD ON LD.C_MASTER_CODE=mw.C_MASTER_CODE AND LD.ED=mw.C_EFFECTIVE_DATE
    ),
    FACT AS (
        SELECT W.C_MASTER_CODE,
               SUM( W.C_TARGET_WEIGHT * p.C_CLOSE_PRICE
                    / COALESCE(ca.C_ADJUSTED_REF_PRICE, pref.C_CLOSE_PRICE, p.C_CLOSE_PRICE) ) AS FACTOR
        FROM W
        JOIN T_PRICE_DAILY p         ON p.C_TICKER=W.C_TICKER AND p.C_BUSINESS_DATE=@d
        LEFT JOIN T_PRICE_DAILY pref ON pref.C_TICKER=W.C_TICKER AND pref.C_BUSINESS_DATE=@prev
        LEFT JOIN T_CORPORATE_ACTION ca ON ca.C_TICKER=W.C_TICKER AND ca.C_EX_DATE=@d
        GROUP BY W.C_MASTER_CODE
    )
    INSERT INTO T_MASTER_INDEX_DAILY (C_BUSINESS_DATE,C_MASTER_CODE,C_INDEX_VALUE,C_DAILY_RETURN)
    SELECT @d, f.C_MASTER_CODE,
           COALESCE(pi.C_INDEX_VALUE, 1000) * f.FACTOR,
           f.FACTOR - 1
    FROM FACT f
    LEFT JOIN T_MASTER_INDEX_DAILY pi ON pi.C_MASTER_CODE=f.C_MASTER_CODE AND pi.C_BUSINESS_DATE=@prev;
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
    -- Σ customer NAV per SI khớp T_MASTER_NAV_BALANCE (derive cùng nguồn → phải khớp)
    SELECT @bad = COUNT(*)
    FROM T_MASTER_NAV_BALANCE p
    JOIN (SELECT C_MASTER_CODE, SUM(C_NAV) NAV FROM T_EOD_WORK WHERE C_BUSINESS_DATE=@d GROUP BY C_MASTER_CODE) a
      ON a.C_MASTER_CODE=p.C_MASTER_CODE AND p.C_BUSINESS_DATE=@d
    WHERE ABS(p.C_NAV - a.NAV) > 1;   -- ngưỡng làm tròn 1 VND
    IF @bad > 0
        THROW 50014, 'RECONCILE: SI NAV != Σ customer NAV.', 1;
END
GO

/*===========================================================================
  J14 — SNAPSHOT: T_MASTER_HOLDING_BALANCE (SI aggregate holdings + tỷ trọng)
         (composition tài sản + NAV đã gộp về T_MASTER_NAV_BALANCE ở J11)
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_SNAPSHOT @d DATE
AS
BEGIN
    SET NOCOUNT ON;
    -- holdings cấp SI + tỷ trọng
    DELETE FROM T_MASTER_HOLDING_BALANCE WHERE C_BUSINESS_DATE=@d;
    ;WITH sih AS (
        SELECT h.C_MASTER_CODE, h.C_TICKER, SUM(h.C_QUANTITY) AS QTY
        FROM T_SI_PORTFOLIO_HOLDING h GROUP BY h.C_MASTER_CODE, h.C_TICKER
    ),
    val AS (
        SELECT sih.C_MASTER_CODE, sih.C_TICKER, sih.QTY, p.C_CLOSE_PRICE,
               sih.QTY*p.C_CLOSE_PRICE AS MV
        FROM sih JOIN T_PRICE_DAILY p ON p.C_TICKER=sih.C_TICKER AND p.C_BUSINESS_DATE=@d
    )
    INSERT INTO T_MASTER_HOLDING_BALANCE (C_BUSINESS_DATE,C_MASTER_CODE,C_TICKER,C_QUANTITY,C_MARKET_PRICE,C_MARKET_VALUE,C_WEIGHT)
    SELECT @d, v.C_MASTER_CODE, v.C_TICKER, v.QTY, v.C_CLOSE_PRICE, v.MV,
           CASE WHEN SUM(v.MV) OVER (PARTITION BY v.C_MASTER_CODE) > 0
                THEN v.MV / SUM(v.MV) OVER (PARTITION BY v.C_MASTER_CODE) END
    FROM val v;
    -- (composition tài sản + NAV cấp SI: đã ghi T_MASTER_NAV_BALANCE ở J11_SI_AGG)
END
GO

/*===========================================================================
  J0 — GATE: chờ đủ FO ingest trước khi chạy EOD. So received (watermark C_LAST_SYNC_DATE=@d)
       vs expected (tiểu khoản ACTIVE). Lệch ⇒ THROW (thiếu/dư data) → chặn EOD + alert.
       (đếm theo state nên DISTINCT sẵn — Kafka redelivery không làm phồng số.)
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_GATE @d DATE
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @expected INT = (SELECT COUNT(*) FROM T_SI_PORTFOLIO WHERE C_STATUS='ACTIVE');
    DECLARE @received INT = (SELECT COUNT(*) FROM T_SI_NAV_CURRENT
                             WHERE C_STATUS='ACTIVE' AND C_LAST_SYNC_DATE=@d);
    IF @received <> @expected
    BEGIN
        DECLARE @msg NVARCHAR(300) = CONCAT('GATE @',CONVERT(VARCHAR,@d,23),': received ',@received,
            '/',@expected,' tiểu khoản — chưa nhận đủ FO ingest, CHẶN EOD.');
        THROW 50010, @msg, 1;
    END
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

    -- (INGEST: FO event per-KH đã vào qua SP_INGEST_CUSTOMER → cash/holdings/history sẵn trong current + interval)
    EXEC SP_EOD_STEP @d, 'J0_GATE',      'SP_EOD_GATE';            -- chờ đủ FO ingest (received=expected) — cổng vào
    -- (ĐÃ BỎ J01_SYNC_FO + J14B_HISTORY) — chuyển sang SP_INGEST_CUSTOMER (per-event realtime)
    -- (ĐÃ BỎ J06_FEE) — phí QL + thuế GD do FO trừ vào cash khi book; SDI KHÔNG accrue lại (tránh double-count)
    EXEC SP_EOD_STEP @d, 'J07_COMPUTE',  'SP_EOD_COMPUTE';         -- MTM→NAV→PnL→Unit + roll-forward + perf per-KH
    EXEC SP_EOD_STEP @d, 'J11_SI_AGG',   'SP_EOD_SI_AGG';
    EXEC SP_EOD_STEP @d, 'J12_SI_INDEX', 'SP_EOD_SI_INDEX';
    EXEC SP_EOD_STEP @d, 'J13_RECONCILE','SP_EOD_RECONCILE';       -- cổng
    EXEC SP_EOD_STEP @d, 'J14_SNAPSHOT', 'SP_EOD_SNAPSHOT';
    -- J15 PUBLISH: push sang Asset (current snapshot + SI series) — adapter riêng
END
GO
