SET QUOTED_IDENTIFIER ON;  -- procs ghi/đọc bảng có filtered index → cần QI ON lúc CREATE PROC
SET ANSI_NULLS ON;
GO
/*==============================================================================
  SDI MODULE — ENGINE CORE (SQL Server)  | ALL-IN-DB, set-based, no RBAR
  Naming: SP_ procs, UDF_ functions, T_/C_ tables/cols.
  Mô hình roll-forward: T_SI_NAV_CURRENT (current) + áp delta ngày @d → tính lại.
  INGEST (Kafka per-KH realtime): SP_INGEST_CUSTOMER → tiền (3 khoản)→state + holdings→current
    + interval history + cổ tức/phí lưu ký. SP_INGEST_FEE_CHARGE → net-off payable khi BO cắt phí.
  Thứ tự (master SP_EOD_RUN): J0_GATE → J07 → J11 → J12 → J13 → J14
  NAV = Tổng tài sản − payable; Tổng tài sản = stock + tiền mặt + tiền bán chờ về + cổ tức tiền.
  Phí QL (BO-driven, cố định): J07 accrue payable theo NGÀY DƯƠNG LỊCH (gated mgmt_fee_rate);
  BO cắt phí 1 cục/tháng → SP_INGEST_FEE_CHARGE net-off payable. SDI KHÔNG sinh lịch. Thuế GD FO net.
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
    @p_d DATE, @p_job VARCHAR(40), @p_status VARCHAR(10),
    @p_rows BIGINT = NULL, @p_msg NVARCHAR(2000) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    MERGE T_EOD_RUN AS t
    USING (SELECT @p_d AS d, @p_job AS j) s ON t.C_BUSINESS_DATE = s.d AND t.C_JOB = s.j
    WHEN MATCHED THEN UPDATE SET
        C_STATUS     = @p_status,
        C_ROWS       = COALESCE(@p_rows, t.C_ROWS),
        C_STARTED_AT = CASE WHEN @p_status='RUNNING' THEN GETDATE() ELSE t.C_STARTED_AT END,
        C_ENDED_AT   = CASE WHEN @p_status IN ('DONE','FAILED') THEN GETDATE() ELSE t.C_ENDED_AT END,
        C_MESSAGE    = @p_msg
    WHEN NOT MATCHED THEN INSERT (C_BUSINESS_DATE,C_JOB,C_STATUS,C_ROWS,C_STARTED_AT,C_ENDED_AT,C_MESSAGE)
        VALUES (@p_d,@p_job,@p_status,@p_rows,
                CASE WHEN @p_status='RUNNING' THEN GETDATE() END,
                CASE WHEN @p_status IN ('DONE','FAILED') THEN GETDATE() END, @p_msg);
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
CREATE OR ALTER PROCEDURE SP_INGEST_CUSTOMER @p_json NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    DECLARE @cust VARCHAR(10) = JSON_VALUE(@p_json,'$.cust_code');
    DECLARE @d    DATE        = TRY_CONVERT(DATE, JSON_VALUE(@p_json,'$.business_date'));
    IF @cust IS NULL OR @d IS NULL THROW 50020, 'INGEST: thiếu cust_code/business_date.', 1;

    BEGIN TRY
        BEGIN TRAN;

        -- sub-account của event; master suy từ config (T_SI_PORTFOLIO)
        DECLARE @sub TABLE (C_SI_ACCOUNT VARCHAR(20) PRIMARY KEY, C_MASTER_CODE VARCHAR(20),
                            C_CASH DECIMAL(20,0), C_PENDING_CASH DECIMAL(20,0), C_DIV_CASH DECIMAL(20,0));
        INSERT INTO @sub (C_SI_ACCOUNT, C_MASTER_CODE, C_CASH, C_PENDING_CASH, C_DIV_CASH)
        SELECT j.C_SI_ACCOUNT, ip.C_MASTER_CODE, j.C_CASH, ISNULL(j.C_PENDING_CASH,0), ISNULL(j.C_DIV_CASH,0)
        FROM OPENJSON(@p_json,'$.sub_accounts') WITH (C_SI_ACCOUNT VARCHAR(20) '$.si_account', C_CASH DECIMAL(20,0) '$.cash',
             C_PENDING_CASH DECIMAL(20,0) '$.pending_cash', C_DIV_CASH DECIMAL(20,0) '$.div_cash') j
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
        FROM OPENJSON(@p_json,'$.sub_accounts') WITH (C_SI_ACCOUNT VARCHAR(20) '$.si_account', holdings NVARCHAR(MAX) '$.holdings' AS JSON) sa
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
        WHEN MATCHED THEN UPDATE SET s.C_CASH=n.C_CASH, s.C_PENDING_CASH=n.C_PENDING_CASH, s.C_DIV_CASH=n.C_DIV_CASH, s.C_LAST_SYNC_DATE=@d
        WHEN NOT MATCHED THEN INSERT (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_UNIT,C_CASH,C_PENDING_CASH,C_DIV_CASH,C_PAYABLE_FEE,C_LAST_NAV,C_LAST_UNIT_PRICE,C_STATUS,C_LAST_SYNC_DATE)
            VALUES (n.C_SI_ACCOUNT,@cust,n.C_MASTER_CODE,0,n.C_CASH,n.C_PENDING_CASH,n.C_DIV_CASH,0,0,NULL,'ACTIVE',@d);

        /* CỔ TỨC/PHÍ: append, dedup theo C_SOURCE_EVENT_ID (idempotent redelivery) */
        INSERT INTO T_SI_FEE_INCOME (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_TYPE,C_TICKER,C_AMOUNT,C_SOURCE,C_SOURCE_EVENT_ID)
        SELECT @d, f.C_SI_ACCOUNT, @cust, s.C_MASTER_CODE, f.C_TYPE, f.C_TICKER, f.C_AMOUNT, 'FO', f.event_id
        FROM (
            SELECT sa.C_SI_ACCOUNT, x.event_id, x.C_TYPE, x.C_TICKER, x.C_AMOUNT
            FROM OPENJSON(@p_json,'$.sub_accounts') WITH (C_SI_ACCOUNT VARCHAR(20) '$.si_account', fees NVARCHAR(MAX) '$.fees' AS JSON) sa
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
--   P_ref cho J12 = C_REF_PRICE (giá tham chiếu đầu phiên sở publish mỗi ngày) ngay trên dòng T_PRICE_DAILY @p_d — KHÔNG tra ngày trước.

-- PHÍ QL (J06, BO-driven, CỐ ĐỊNH — không toggle):
--   SDI accrue payable trong SP_EOD_COMPUTE theo NGÀY DƯƠNG LỊCH (gated mgmt_fee_rate>0).
--   BO cắt phí 1 cục/tháng → event Kafka → SP_INGEST_FEE_CHARGE net-off payable (log T_SI_FEE_CHARGE).
--   SDI KHÔNG sinh lịch/ra lệnh. NAV = total_asset − payable. Thuế GD: LUÔN FO net vào cash.
GO

/*===========================================================================
  SP_EOD_HISTORY — UTILITY (KHÔNG còn trong EOD pipeline). History forward giờ do
        SP_INGEST_CUSTOMER maintain per-event. Proc này = BULK BACKFILL/sửa lỗi: DIFF
        TOÀN BỘ current vs open-row 1 phát (vd init lần đầu, hoặc dựng lại history).
        Idempotent (ngày không biến động → 0 ghi).
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_HISTORY @p_d DATE
AS
BEGIN
    SET NOCOUNT ON;

    /* ---- HOLDINGS: DIFF current (T_SI_PORTFOLIO_HOLDING) vs open-row ---- */
    -- ĐÓNG open-row có qty/avg_cost ĐỔI hoặc mã BIẾN MẤT khỏi current
    UPDATE h SET C_VALID_TO=@p_d
    FROM T_SI_HOLDING_HIST h
    LEFT JOIN T_SI_PORTFOLIO_HOLDING c
      ON c.C_SI_ACCOUNT=h.C_SI_ACCOUNT AND c.C_TICKER=h.C_TICKER
    WHERE h.C_VALID_TO IS NULL
      AND ( c.C_SI_ACCOUNT IS NULL
         OR c.C_QUANTITY <> h.C_QUANTITY
         OR ISNULL(c.C_AVG_COST,-1) <> ISNULL(h.C_AVG_COST,-1) );
    -- MỞ dòng mới cho mã trong current chưa có open-row khớp y hệt (mã mới / vừa đổi)
    INSERT INTO T_SI_HOLDING_HIST (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_TICKER,C_VALID_FROM,C_VALID_TO,C_QUANTITY,C_AVG_COST)
    SELECT c.C_SI_ACCOUNT,c.C_CUST_CODE,c.C_MASTER_CODE,c.C_TICKER,@p_d,NULL,c.C_QUANTITY,c.C_AVG_COST
    FROM T_SI_PORTFOLIO_HOLDING c
    WHERE NOT EXISTS (SELECT 1 FROM T_SI_HOLDING_HIST h
        WHERE h.C_VALID_TO IS NULL AND h.C_SI_ACCOUNT=c.C_SI_ACCOUNT AND h.C_TICKER=c.C_TICKER
          AND h.C_QUANTITY=c.C_QUANTITY AND ISNULL(h.C_AVG_COST,-1)=ISNULL(c.C_AVG_COST,-1));

    /* ---- CASH: DIFF state.cash (T_SI_NAV_CURRENT) vs open-row ---- */
    UPDATE h SET C_VALID_TO=@p_d
    FROM T_SI_CASH_HIST h
    JOIN T_SI_NAV_CURRENT s ON s.C_SI_ACCOUNT=h.C_SI_ACCOUNT
    WHERE h.C_VALID_TO IS NULL AND s.C_CASH <> h.C_CASH;
    INSERT INTO T_SI_CASH_HIST (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_VALID_FROM,C_VALID_TO,C_CASH)
    SELECT s.C_SI_ACCOUNT,s.C_CUST_CODE,s.C_MASTER_CODE,@p_d,NULL,s.C_CASH
    FROM T_SI_NAV_CURRENT s
    WHERE NOT EXISTS (SELECT 1 FROM T_SI_CASH_HIST h
        WHERE h.C_VALID_TO IS NULL AND h.C_SI_ACCOUNT=s.C_SI_ACCOUNT AND h.C_CASH=s.C_CASH);
END
GO

/*===========================================================================
  J07–J10 — COMPUTE: MTM → NAV → PnL → Unit/UnitPrice → roll-forward state
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_COMPUTE @p_d DATE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @dayCount SMALLINT = 365;   -- mẫu số rate/ngày (cố định — spec phí QL không còn config)

    DELETE FROM T_EOD_WORK WHERE C_BUSINESS_DATE=@p_d;

    -- seed từ state. Tiền = C_CASH + C_PENDING_CASH + C_DIV_CASH (FO sync). KEY = C_SI_ACCOUNT.
    INSERT INTO T_EOD_WORK (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_CASH,C_PENDING_CASH,C_DIV_CASH,C_PAYABLE_FEE,C_LAST_NAV,C_LAST_UNIT_PRICE,C_UNIT_PREV)
    SELECT @p_d,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_CASH,C_PENDING_CASH,C_DIV_CASH,C_PAYABLE_FEE,C_LAST_NAV,C_LAST_UNIT_PRICE,C_UNIT
    FROM T_SI_NAV_CURRENT WHERE C_STATUS='ACTIVE';

    -- CF của ngày (cho PnL & unit) — group theo sub-account
    UPDATE w SET w.C_CF_IN = cf.CF_IN, w.C_CF_OUT = cf.CF_OUT
    FROM T_EOD_WORK w
    JOIN (
        SELECT C_SI_ACCOUNT,
               SUM(CASE WHEN C_EVENT_TYPE='WITHDRAW' THEN 0 ELSE C_AMOUNT END) AS CF_IN,
               SUM(CASE WHEN C_EVENT_TYPE='WITHDRAW' THEN C_AMOUNT ELSE 0 END) AS CF_OUT
        FROM T_SI_CASHFLOW_EVENT WHERE C_BUSINESS_DATE=@p_d GROUP BY C_SI_ACCOUNT
    ) cf ON cf.C_SI_ACCOUNT=w.C_SI_ACCOUNT
    WHERE w.C_BUSINESS_DATE=@p_d;

    -- J07 MTM (câu nặng nhất — set-based; prod chạy batch-mode trên columnstore) — group theo sub-account
    UPDATE w SET w.C_STOCK_VALUE = m.SV
    FROM T_EOD_WORK w
    JOIN (
        SELECT h.C_SI_ACCOUNT, SUM(h.C_QUANTITY * p.C_CLOSE_PRICE) AS SV
        FROM T_SI_PORTFOLIO_HOLDING h
        JOIN T_PRICE_DAILY p ON p.C_TICKER=h.C_TICKER AND p.C_BUSINESS_DATE=@p_d
        GROUP BY h.C_SI_ACCOUNT
    ) m ON m.C_SI_ACCOUNT=w.C_SI_ACCOUNT
    WHERE w.C_BUSINESS_DATE=@p_d;

    -- J06 ACCRUE phí QL (luôn chạy, gated rate>0; net-off do SP_INGEST_FEE_CHARGE khi BO cắt):
    --   payable += AUM_gross × rate × (NGÀY DƯƠNG LỊCH kể từ EOD trước) / 365.
    --   AUM_gross = stock + cash + tiền bán chờ về + cổ tức tiền. rate: override tiểu khoản > master.
    --   IDEMPOTENT: chỉ accrue khi CHƯA compute @p_d (C_LAST_BUSINESS_DATE < @p_d) → re-run không cộng đôi.
    UPDATE w SET w.C_PAYABLE_FEE = w.C_PAYABLE_FEE
        + (w.C_STOCK_VALUE + w.C_CASH + w.C_PENDING_CASH + w.C_DIV_CASH)
          * COALESCE(ip.C_MGMT_FEE_RATE, mp.C_MGMT_FEE_RATE)
          * (CASE WHEN s.C_LAST_BUSINESS_DATE IS NULL THEN 1 ELSE DATEDIFF(DAY, s.C_LAST_BUSINESS_DATE, @p_d) END)
          / @dayCount
    FROM T_EOD_WORK w
    JOIN T_SI_NAV_CURRENT   s  ON s.C_SI_ACCOUNT  = w.C_SI_ACCOUNT
    JOIN T_SI_PORTFOLIO     ip ON ip.C_SI_ACCOUNT  = w.C_SI_ACCOUNT
    JOIN T_MASTER_PORTFOLIO mp ON mp.C_MASTER_CODE = w.C_MASTER_CODE
    WHERE w.C_BUSINESS_DATE=@p_d
      AND COALESCE(ip.C_MGMT_FEE_RATE, mp.C_MGMT_FEE_RATE) > 0
      AND (s.C_LAST_BUSINESS_DATE IS NULL OR s.C_LAST_BUSINESS_DATE < @p_d);

    -- J08 NAV = Tổng tài sản − payable. Tổng tài sản = stock + cash + tiền bán chờ về + cổ tức tiền (gồm receivables).
    UPDATE T_EOD_WORK SET C_NAV = C_STOCK_VALUE + C_CASH + C_PENDING_CASH + C_DIV_CASH - C_PAYABLE_FEE
    WHERE C_BUSINESS_DATE=@p_d;

    -- J09 PnL = NAV − NAV_prev + ra − vào
    UPDATE T_EOD_WORK SET C_DAILY_PNL = C_NAV - C_LAST_NAV + C_CF_OUT - C_CF_IN WHERE C_BUSINESS_DATE=@p_d;

    -- J10 Unit: ΔUnit = CF/UnitPrice_(t-1) (giả định cashflow đầu ngày + tham gia đầu tư → giá quy đổi = NAV/unit đầu ngày = UP cuối ngày trước; TWR sạch, không bias)
    --           init khi unit_prev=0 → unit=NAV/10000, UP=10000
    UPDATE T_EOD_WORK SET
        C_DELTA_UNIT = CASE WHEN C_LAST_UNIT_PRICE IS NULL OR C_LAST_UNIT_PRICE=0 OR C_UNIT_PREV=0
                            THEN (C_NAV/10000.0) - C_UNIT_PREV
                            ELSE (C_CF_IN - C_CF_OUT) / C_LAST_UNIT_PRICE END,
        C_UNIT       = CASE WHEN C_LAST_UNIT_PRICE IS NULL OR C_LAST_UNIT_PRICE=0 OR C_UNIT_PREV=0
                            THEN C_NAV/10000.0
                            ELSE C_UNIT_PREV + (C_CF_IN - C_CF_OUT) / C_LAST_UNIT_PRICE END
    WHERE C_BUSINESS_DATE=@p_d;

    UPDATE T_EOD_WORK SET C_UNIT_PRICE = CASE WHEN C_UNIT>0 THEN C_NAV/C_UNIT ELSE 10000 END
    WHERE C_BUSINESS_DATE=@p_d;

    -- roll-forward state (cập nhật unit + last_nav + last_up + payable tích luỹ sau accrue/settle)
    UPDATE s SET
        s.C_UNIT              = w.C_UNIT,
        s.C_PAYABLE_FEE       = w.C_PAYABLE_FEE,
        s.C_LAST_NAV          = w.C_NAV,
        s.C_LAST_UNIT_PRICE   = w.C_UNIT_PRICE,
        s.C_LAST_BUSINESS_DATE= @p_d
    FROM T_SI_NAV_CURRENT s
    JOIN T_EOD_WORK w ON w.C_SI_ACCOUNT=s.C_SI_ACCOUNT AND w.C_BUSINESS_DATE=@p_d;

    -- unit ledger (chỉ ngày có cashflow)
    DELETE FROM T_SI_UNIT_LEDGER WHERE C_BUSINESS_DATE=@p_d;
    INSERT INTO T_SI_UNIT_LEDGER (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_BUSINESS_DATE,C_CF_NET,C_DELTA_UNIT,C_UNIT)
    SELECT C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,@p_d,(C_CF_IN-C_CF_OUT),C_DELTA_UNIT,C_UNIT
    FROM T_EOD_WORK WHERE C_BUSINESS_DATE=@p_d AND (C_CF_IN-C_CF_OUT)<>0;

    -- LỊCH SỬ per sub-account (materialize để vẽ chart FR-03)
    DELETE FROM T_SI_NAV_BALANCE WHERE C_BUSINESS_DATE=@p_d;
    INSERT INTO T_SI_NAV_BALANCE (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_NAV,C_PAYABLE_FEE,C_UNIT,C_UNIT_PRICE,C_DAILY_PNL,C_DAILY_RETURN)
    SELECT @p_d, C_SI_ACCOUNT, C_CUST_CODE, C_MASTER_CODE, C_NAV, C_PAYABLE_FEE, C_UNIT, C_UNIT_PRICE, C_DAILY_PNL,
           CASE WHEN C_LAST_UNIT_PRICE>0 THEN C_UNIT_PRICE/C_LAST_UNIT_PRICE - 1 END
    FROM T_EOD_WORK WHERE C_BUSINESS_DATE=@p_d;

END
GO

/*===========================================================================
  PHÍ QUẢN LÝ — INGEST event BO cắt phí (BO-driven). BO cắt 1 cục → báo Kafka →
  SP_INGEST_FEE_CHARGE: log T_SI_FEE_CHARGE + net-off payable (payable −= amount).
  SDI KHÔNG sinh lịch/ra lệnh. Idempotent qua source_event_id. Ngoài batch EOD.
  JSON: {"charge_date":"YYYY-MM-DD", "charges":[
           {"si_account","amount","period"(opt),"source_event_id"}, ...]}
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_INGEST_FEE_CHARGE @p_json NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    DECLARE @hdr_date DATE = TRY_CONVERT(DATE, JSON_VALUE(@p_json,'$.charge_date'));
    BEGIN TRY
        BEGIN TRAN;

        DECLARE @chg TABLE (C_SI_ACCOUNT VARCHAR(20), C_AMOUNT DECIMAL(20,0),
                            C_CHARGE_DATE DATE, C_PERIOD CHAR(6), C_SOURCE_EVENT_ID VARCHAR(64) PRIMARY KEY);
        INSERT INTO @chg (C_SI_ACCOUNT,C_AMOUNT,C_CHARGE_DATE,C_PERIOD,C_SOURCE_EVENT_ID)
        SELECT j.C_SI_ACCOUNT, j.C_AMOUNT,
               COALESCE(TRY_CONVERT(DATE,j.C_CHARGE_DATE), @hdr_date, CAST(GETDATE() AS DATE)),
               j.C_PERIOD, j.C_SOURCE_EVENT_ID
        FROM OPENJSON(@p_json,'$.charges') WITH (
            C_SI_ACCOUNT VARCHAR(20) '$.si_account', C_AMOUNT DECIMAL(20,0) '$.amount',
            C_CHARGE_DATE VARCHAR(10) '$.charge_date', C_PERIOD CHAR(6) '$.period',
            C_SOURCE_EVENT_ID VARCHAR(64) '$.source_event_id') j;

        -- dedup: bỏ event đã nhận (Kafka redelivery → no-op)
        DELETE c FROM @chg c WHERE EXISTS (SELECT 1 FROM T_SI_FEE_CHARGE e WHERE e.C_SOURCE_EVENT_ID=c.C_SOURCE_EVENT_ID);

        -- log (derive cust/master từ registry)
        INSERT INTO T_SI_FEE_CHARGE (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_CHARGE_DATE,C_PERIOD,C_AMOUNT,C_SOURCE_EVENT_ID)
        SELECT c.C_SI_ACCOUNT, ip.C_CUST_CODE, ip.C_MASTER_CODE, c.C_CHARGE_DATE, c.C_PERIOD, c.C_AMOUNT, c.C_SOURCE_EVENT_ID
        FROM @chg c LEFT JOIN T_SI_PORTFOLIO ip ON ip.C_SI_ACCOUNT=c.C_SI_ACCOUNT;

        -- net-off payable (thiếu đủ cứ trừ; residual treo → carry sang kỳ sau)
        UPDATE s SET s.C_PAYABLE_FEE = s.C_PAYABLE_FEE - x.AMT
        FROM T_SI_NAV_CURRENT s
        JOIN (SELECT C_SI_ACCOUNT, CAST(SUM(C_AMOUNT) AS DECIMAL(20,0)) AS AMT  -- CAST tránh SUM→(38,0) cắt scale payable
              FROM @chg GROUP BY C_SI_ACCOUNT) x
          ON x.C_SI_ACCOUNT = s.C_SI_ACCOUNT;

        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        THROW;
    END CATCH
END
GO

/*===========================================================================
  J11 — SI AGGREGATE → T_MASTER_NAV_BALANCE (composition + NAV + hiệu suất + cổ tức/phí)
         + upsert T_MASTER_NAV_CURRENT (snapshot current cấp SI cho serving)
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_SI_AGG @p_d DATE
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @prev DATE = dbo.UDF_PREV_BUSINESS_DATE(@p_d);
    DELETE FROM T_MASTER_NAV_BALANCE WHERE C_BUSINESS_DATE=@p_d;

    ;WITH agg AS (
        SELECT C_MASTER_CODE,
               SUM(C_CASH) AS CASH, SUM(C_PENDING_CASH) AS PEND, SUM(C_DIV_CASH) AS DIVC,
               SUM(C_STOCK_VALUE) AS STOCK, SUM(C_PAYABLE_FEE) AS PAY,
               SUM(C_NAV) AS NAV, SUM(C_UNIT) AS UNT, SUM(C_DAILY_PNL) AS PNL,
               SUM(C_CF_IN) AS CFIN, SUM(C_CF_OUT) AS CFOUT, COUNT(*) AS ACCT   -- [PM] flow + #tiểu khoản ACTIVE
        FROM T_EOD_WORK WHERE C_BUSINESS_DATE=@p_d GROUP BY C_MASTER_CODE
    ),
    fee AS (   -- cổ tức + phí lưu ký per-KH → SUM lên master (sparse; phí QL KHÔNG ở đây → BO/T_SI_FEE_CHARGE)
        SELECT C_MASTER_CODE,
               SUM(CASE WHEN C_TYPE='DIVIDEND'    THEN C_AMOUNT ELSE 0 END) AS DIV,
               SUM(CASE WHEN C_TYPE='CUSTODY_FEE' THEN C_AMOUNT ELSE 0 END) AS CUST
        FROM T_SI_FEE_INCOME WHERE C_BUSINESS_DATE=@p_d GROUP BY C_MASTER_CODE
    )
    INSERT INTO T_MASTER_NAV_BALANCE (C_BUSINESS_DATE,C_MASTER_CODE,C_CASH,C_PENDING_CASH,C_DIV_CASH,C_STOCK_VALUE,
                             C_CASH_DIVIDEND,C_CUSTODY_FEE,C_MGMT_FEE_ACCRUED,C_PAYABLE_FEE,C_TOTAL_ASSET,
                             C_NAV,C_UNIT,C_UNIT_PRICE,C_DAILY_PNL,C_DAILY_RETURN,C_CASH_IN,C_CASH_OUT,C_TOTAL_ACCOUNT)
    SELECT @p_d, a.C_MASTER_CODE, a.CASH, a.PEND, a.DIVC, a.STOCK,
           f.DIV, f.CUST, a.PAY, a.PAY, (a.CASH + a.PEND + a.DIVC + a.STOCK),   -- mgmt_fee_accrued = payable; total_asset gồm receivables
           a.NAV, a.UNT,
           CASE WHEN a.UNT>0 THEN a.NAV/a.UNT END,
           a.PNL,
           CASE WHEN a.UNT>0 AND prev.C_UNIT_PRICE>0 THEN (a.NAV/a.UNT)/prev.C_UNIT_PRICE - 1 END,
           a.CFIN, a.CFOUT, a.ACCT
    FROM agg a
    LEFT JOIN fee f ON f.C_MASTER_CODE=a.C_MASTER_CODE
    LEFT JOIN T_MASTER_NAV_BALANCE prev ON prev.C_MASTER_CODE=a.C_MASTER_CODE AND prev.C_BUSINESS_DATE=@prev;

    -- current cấp master (overwrite) — đọc nhanh AUM/cash-drag/#KH hiện tại
    MERGE T_MASTER_NAV_CURRENT AS t
    USING (SELECT C_MASTER_CODE,C_CASH,C_PENDING_CASH,C_DIV_CASH,C_STOCK_VALUE,C_TOTAL_ASSET,C_NAV,C_UNIT,C_UNIT_PRICE,C_TOTAL_ACCOUNT,C_BUSINESS_DATE
           FROM T_MASTER_NAV_BALANCE WHERE C_BUSINESS_DATE=@p_d) s
    ON t.C_MASTER_CODE=s.C_MASTER_CODE
    WHEN MATCHED THEN UPDATE SET
        t.C_CASH=s.C_CASH, t.C_PENDING_CASH=s.C_PENDING_CASH, t.C_DIV_CASH=s.C_DIV_CASH, t.C_STOCK_VALUE=s.C_STOCK_VALUE, t.C_TOTAL_ASSET=s.C_TOTAL_ASSET,
        t.C_LAST_NAV=s.C_NAV, t.C_UNIT=s.C_UNIT, t.C_LAST_UNIT_PRICE=s.C_UNIT_PRICE, t.C_TOTAL_ACCOUNT=s.C_TOTAL_ACCOUNT,
        t.C_LAST_BUSINESS_DATE=s.C_BUSINESS_DATE
    WHEN NOT MATCHED THEN INSERT (C_MASTER_CODE,C_CASH,C_PENDING_CASH,C_DIV_CASH,C_STOCK_VALUE,C_TOTAL_ASSET,C_LAST_NAV,C_UNIT,C_LAST_UNIT_PRICE,C_TOTAL_ACCOUNT,C_LAST_BUSINESS_DATE)
        VALUES (s.C_MASTER_CODE,s.C_CASH,s.C_PENDING_CASH,s.C_DIV_CASH,s.C_STOCK_VALUE,s.C_TOTAL_ASSET,s.C_NAV,s.C_UNIT,s.C_UNIT_PRICE,s.C_TOTAL_ACCOUNT,s.C_BUSINESS_DATE);
END
GO

/*===========================================================================
  J12 — SI INDEX (danh mục mẫu, 100% cổ phiếu) → T_MASTER_INDEX_DAILY
        Index_t = Index_(t-1) × Σ w^(t) × P_t / P_ref ;  w^(t)=eff_date≤@d mới nhất
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_SI_INDEX @p_d DATE
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @prev DATE = dbo.UDF_PREV_BUSINESS_DATE(@p_d);
    DELETE FROM T_MASTER_INDEX_DAILY WHERE C_BUSINESS_DATE=@p_d;

    ;WITH LD AS (
        SELECT C_MASTER_CODE, MAX(C_EFFECTIVE_DATE) AS ED
        FROM T_MASTER_PORTFOLIO_TICKER WHERE C_EFFECTIVE_DATE<=@p_d GROUP BY C_MASTER_CODE
    ),
    W AS (
        SELECT mw.C_MASTER_CODE, mw.C_TICKER, mw.C_TARGET_WEIGHT
        FROM T_MASTER_PORTFOLIO_TICKER mw JOIN LD ON LD.C_MASTER_CODE=mw.C_MASTER_CODE AND LD.ED=mw.C_EFFECTIVE_DATE
    ),
    FACT AS (
        SELECT W.C_MASTER_CODE,
               SUM( W.C_TARGET_WEIGHT * p.C_CLOSE_PRICE / p.C_REF_PRICE ) AS FACTOR   -- daily-return self-contained: close / giá tham chiếu đầu phiên (cùng dòng @p_d)
        FROM W
        JOIN T_PRICE_DAILY p ON p.C_TICKER=W.C_TICKER AND p.C_BUSINESS_DATE=@p_d
        GROUP BY W.C_MASTER_CODE
    )
    INSERT INTO T_MASTER_INDEX_DAILY (C_BUSINESS_DATE,C_MASTER_CODE,C_INDEX_VALUE,C_DAILY_RETURN)
    SELECT @p_d, f.C_MASTER_CODE,
           COALESCE(pi.C_INDEX_VALUE, 1000) * f.FACTOR,
           f.FACTOR - 1
    FROM FACT f
    LEFT JOIN T_MASTER_INDEX_DAILY pi ON pi.C_MASTER_CODE=f.C_MASTER_CODE AND pi.C_BUSINESS_DATE=@prev;
END
GO

/*===========================================================================
  J12B — TE ACCUM: lũy kế active return per-KH cho TE prefix-sum (PM tool).
    active aᵢ,d = C_DAILY_RETURN(KH) − C_DAILY_RETURN(master index @d). Chạy SAU J12
    (cần index daily return). accum@d = accum@prev (cùng si) + đóng góp @d.
    IDEMPOTENT: đọc accum @prev (KHÔNG in-place) → re-run @d cho cùng kết quả
      (J07 INSERT lại NAV_BALANCE @d ⇒ 3 cột reset DEFAULT 0 ⇒ J12B set lại đúng).
    Đọc 2 lát ngày (@d, @prev) join theo si (hash) → KHÔNG cần index leading si.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_TE_ACCUM @p_d DATE
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @prev DATE = dbo.UDF_PREV_BUSINESS_DATE(@p_d);

    UPDATE b SET
        b.C_RET_DAY_COUNT = ISNULL(p.C_RET_DAY_COUNT,0)
            + CASE WHEN b.C_DAILY_RETURN IS NULL OR idx.C_DAILY_RETURN IS NULL THEN 0 ELSE 1 END,
        b.C_ACCUM_ACTIVE_RET = ISNULL(p.C_ACCUM_ACTIVE_RET,0)
            + CASE WHEN b.C_DAILY_RETURN IS NULL OR idx.C_DAILY_RETURN IS NULL THEN 0
                   ELSE CAST(b.C_DAILY_RETURN - idx.C_DAILY_RETURN AS FLOAT) END,
        b.C_ACCUM_ACTIVE_RET_SQ = ISNULL(p.C_ACCUM_ACTIVE_RET_SQ,0)
            + CASE WHEN b.C_DAILY_RETURN IS NULL OR idx.C_DAILY_RETURN IS NULL THEN 0
                   ELSE POWER(CAST(b.C_DAILY_RETURN - idx.C_DAILY_RETURN AS FLOAT),2) END
    FROM T_SI_NAV_BALANCE b
    JOIN T_MASTER_INDEX_DAILY idx ON idx.C_MASTER_CODE=b.C_MASTER_CODE AND idx.C_BUSINESS_DATE=@p_d
    LEFT JOIN T_SI_NAV_BALANCE p ON p.C_SI_ACCOUNT=b.C_SI_ACCOUNT AND p.C_BUSINESS_DATE=@prev
    WHERE b.C_BUSINESS_DATE=@p_d;
END
GO

/*===========================================================================
  J13 — RECONCILE (cổng publish): sanity checks; lỗi → THROW chặn publish
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_RECONCILE @p_d DATE
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @bad INT;
    -- NAV âm hoặc unit<=0 nhưng NAV>0 (bất thường)
    SELECT @bad = COUNT(*) FROM T_EOD_WORK
    WHERE C_BUSINESS_DATE=@p_d AND (C_NAV < 0 OR (C_UNIT<=0 AND C_NAV>0));
    IF @bad > 0
        THROW 50013, 'RECONCILE: phát hiện vị thế NAV âm hoặc unit<=0 với NAV>0.', 1;
    -- Σ customer NAV per SI khớp T_MASTER_NAV_BALANCE (derive cùng nguồn → phải khớp)
    SELECT @bad = COUNT(*)
    FROM T_MASTER_NAV_BALANCE p
    JOIN (SELECT C_MASTER_CODE, SUM(C_NAV) NAV FROM T_EOD_WORK WHERE C_BUSINESS_DATE=@p_d GROUP BY C_MASTER_CODE) a
      ON a.C_MASTER_CODE=p.C_MASTER_CODE AND p.C_BUSINESS_DATE=@p_d
    WHERE ABS(p.C_NAV - a.NAV) > 1;   -- ngưỡng làm tròn 1 VND
    IF @bad > 0
        THROW 50014, 'RECONCILE: SI NAV != Σ customer NAV.', 1;
END
GO

/*===========================================================================
  J14 — SNAPSHOT: T_MASTER_HOLDING_BALANCE (SI aggregate holdings + tỷ trọng)
         (composition tài sản + NAV đã gộp về T_MASTER_NAV_BALANCE ở J11)
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_SNAPSHOT @p_d DATE
AS
BEGIN
    SET NOCOUNT ON;
    -- holdings cấp SI + tỷ trọng
    DELETE FROM T_MASTER_HOLDING_BALANCE WHERE C_BUSINESS_DATE=@p_d;
    ;WITH sih AS (
        SELECT h.C_MASTER_CODE, h.C_TICKER, SUM(h.C_QUANTITY) AS QTY
        FROM T_SI_PORTFOLIO_HOLDING h GROUP BY h.C_MASTER_CODE, h.C_TICKER
    ),
    val AS (
        SELECT sih.C_MASTER_CODE, sih.C_TICKER, sih.QTY, p.C_CLOSE_PRICE,
               sih.QTY*p.C_CLOSE_PRICE AS MV
        FROM sih JOIN T_PRICE_DAILY p ON p.C_TICKER=sih.C_TICKER AND p.C_BUSINESS_DATE=@p_d
    )
    INSERT INTO T_MASTER_HOLDING_BALANCE (C_BUSINESS_DATE,C_MASTER_CODE,C_TICKER,C_QUANTITY,C_MARKET_PRICE,C_MARKET_VALUE,C_WEIGHT)
    SELECT @p_d, v.C_MASTER_CODE, v.C_TICKER, v.QTY, v.C_CLOSE_PRICE, v.MV,
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
CREATE OR ALTER PROCEDURE SP_EOD_GATE @p_d DATE
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @expected INT = (SELECT COUNT(*) FROM T_SI_PORTFOLIO WHERE C_STATUS='ACTIVE');
    DECLARE @received INT = (SELECT COUNT(*) FROM T_SI_NAV_CURRENT
                             WHERE C_STATUS='ACTIVE' AND C_LAST_SYNC_DATE=@p_d);
    IF @received <> @expected
    BEGIN
        DECLARE @msg NVARCHAR(300) = CONCAT('GATE @',CONVERT(VARCHAR,@p_d,23),': received ',@received,
            '/',@expected,' tiểu khoản — chưa nhận đủ FO ingest, CHẶN EOD.');
        THROW 50010, @msg, 1;
    END
END
GO

/*===========================================================================
  DISPATCHER: chạy 1 job idempotent + transaction + log (resume-safe)
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_STEP @p_d DATE, @p_job VARCHAR(40), @p_proc SYSNAME
AS
BEGIN
    SET NOCOUNT ON;
    IF EXISTS (SELECT 1 FROM T_EOD_RUN WHERE C_BUSINESS_DATE=@p_d AND C_JOB=@p_job AND C_STATUS='DONE')
        RETURN;
    EXEC SP_EOD_LOG @p_d,@p_job,'RUNNING';
    BEGIN TRY
        BEGIN TRAN;
        -- dynamic SQL: '@p_d' = tên param target proc (đã đổi); '@d' = biến trong batch động (khai báo N'@d DATE')
        DECLARE @sql NVARCHAR(300) = N'EXEC ' + QUOTENAME(@p_proc) + N' @p_d=@d';
        EXEC sp_executesql @sql, N'@d DATE', @d=@p_d;
        COMMIT;
        EXEC SP_EOD_LOG @p_d,@p_job,'DONE';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        DECLARE @err NVARCHAR(2000) = ERROR_MESSAGE();
        EXEC SP_EOD_LOG @p_d,@p_job,'FAILED', NULL, @err;
        THROW;
    END CATCH
END
GO

/*===========================================================================
  MASTER ORCHESTRATOR — app chỉ EXEC proc này
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_RUN @p_business_date DATE
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    DECLARE @d DATE = @p_business_date;

    -- (INGEST: FO event per-KH đã vào qua SP_INGEST_CUSTOMER → cash/holdings/history sẵn trong current + interval)
    EXEC SP_EOD_STEP @d, 'J0_GATE',      'SP_EOD_GATE';            -- chờ đủ FO ingest (received=expected) — cổng vào
    -- (ĐÃ BỎ J01_SYNC_FO + J14B_HISTORY) — chuyển sang SP_INGEST_CUSTOMER (per-event realtime)
    -- (J06 PHÍ QL accrue nằm TRONG J07_COMPUTE; net-off do SP_INGEST_FEE_CHARGE khi BO cắt — không job riêng)
    EXEC SP_EOD_STEP @d, 'J07_COMPUTE',  'SP_EOD_COMPUTE';         -- MTM→NAV→PnL→Unit + roll-forward + perf per-KH
    EXEC SP_EOD_STEP @d, 'J11_SI_AGG',   'SP_EOD_SI_AGG';
    EXEC SP_EOD_STEP @d, 'J12_SI_INDEX', 'SP_EOD_SI_INDEX';
    EXEC SP_EOD_STEP @d, 'J12B_TE_ACCUM', 'SP_EOD_TE_ACCUM';           -- lũy kế active return per-KH (TE prefix-sum)
    EXEC SP_EOD_STEP @d, 'J13_RECONCILE','SP_EOD_RECONCILE';       -- cổng
    EXEC SP_EOD_STEP @d, 'J14_SNAPSHOT', 'SP_EOD_SNAPSHOT';
    -- J15 PUBLISH: push sang Asset (current snapshot + SI series) — adapter riêng
END
GO
