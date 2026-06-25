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

/*---------------------------------------------------- UDF: ngày ĐÃ CÓ GIÁ chưa (data-driven)
  1 nếu BO đã publish giá @d (T_PRICE_DAILY có dòng); 0 nếu chưa. ĐÂY KHÔNG PHẢI "ngày giao dịch theo lịch":
  ngày GD hiện tại mà giá EOD CHƯA về cũng trả 0. Vì vậy CHỈ dùng để chọn ngày QUÁ KHỨ có data (recompute loop).
  Việc "ngày hiện tại có sẵn sàng chạy EOD" do precondition MKT_DATA=READY ở SP_EOD_RUN quyết (không dùng UDF này). */
CREATE OR ALTER FUNCTION UDF_HAS_PRICE_DATA (@d DATE)
RETURNS BIT
AS
BEGIN
    RETURN CASE WHEN @d IS NOT NULL AND EXISTS (SELECT 1 FROM T_PRICE_DAILY WHERE C_BUSINESS_DATE=@d)
                THEN 1 ELSE 0 END;
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
                            C_CASH DECIMAL(20,0));
        INSERT INTO @sub (C_SI_ACCOUNT, C_MASTER_CODE, C_CASH)
        SELECT j.C_SI_ACCOUNT, ip.C_MASTER_CODE, j.C_CASH
        FROM OPENJSON(@p_json,'$.sub_accounts') WITH (C_SI_ACCOUNT VARCHAR(20) '$.si_account', C_CASH DECIMAL(20,0) '$.cash') j
        LEFT JOIN T_SI_PORTFOLIO ip ON ip.C_SI_ACCOUNT = j.C_SI_ACCOUNT AND ip.C_CUST_CODE = @cust;

        IF EXISTS (SELECT 1 FROM @sub WHERE C_MASTER_CODE IS NULL)
            THROW 50022, 'INGEST: sub-account chưa đăng ký (thiếu T_SI_PORTFOLIO cho cust/si_account).', 1;

        -- FORWARD guard: sub-account đã sync ngày MỚI HƠN @d ⇒ event quá khứ
        IF EXISTS (SELECT 1 FROM @sub n INNER JOIN T_SI_NAV_CURRENT s
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
        FROM @hold hd INNER JOIN @sub s ON s.C_SI_ACCOUNT=hd.C_SI_ACCOUNT;

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

        /* [BRD asset-sync] FO chỉ còn gửi HOLDINGS (per-mã, cho composition/near-realtime). Tiền/phí/cổ tức KHÔNG
           qua đây nữa — Asset gửi số tổng (SP_INGEST_ASSET_NAV). Vẫn upsert registry NAV_CURRENT + watermark
           C_LAST_SYNC_DATE (cổng GATE FO holdings). cash/pending/div để 0 — compute fill từ Asset. */
        MERGE T_SI_NAV_CURRENT s
        USING @sub n ON s.C_SI_ACCOUNT=n.C_SI_ACCOUNT
        WHEN MATCHED THEN UPDATE SET s.C_LAST_SYNC_DATE=@d
        WHEN NOT MATCHED THEN INSERT (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_UNIT,C_CASH,C_PAYABLE_FEE,C_LAST_NAV,C_LAST_UNIT_PRICE,C_STATUS,C_LAST_SYNC_DATE)
            VALUES (n.C_SI_ACCOUNT,@cust,n.C_MASTER_CODE,0,0,0,0,NULL,'ACTIVE',@d);

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
--   BO cắt phí 1 cục/tháng → event Kafka → SP_INGEST_FEE_CHARGE net-off payable (log T_SI_INCOME_FEE type MGMT_FEE).
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

    -- [BRD asset-sync] BỎ phần CASH_HIST (đã drop bảng): tiền nhận từ Asset, không còn interval cash ở SDI.
END
GO

/*===========================================================================
  SP_EOD_COMPUTE — LÕI tính cuối ngày 1 phiên @p_d cho TỪNG tiểu khoản (si_account):
    J06 accrue phí → J07 MTM → J08 NAV → J09 PnL → J10 Unit/UnitPrice → roll-forward + ghi lịch sử.

  THUẬT NGỮ (đọc trước khi sửa — viết tắt nhiều):
    • MTM (Mark-to-Market) = ĐỊNH GIÁ THỊ TRƯỜNG: định giá danh mục cổ phiếu theo giá ĐÓNG CỬA
        cuối phiên = Σ(số lượng × giá đóng cửa). Kết quả = "stock value" (C_STOCK_VALUE).
    • Tổng tài sản (AUM gross) = stock value + tiền mặt + tiền bán chờ về (T+) + cổ tức tiền chờ về.
    • accrue (J06) = TRÍCH TRƯỚC phí quản lý theo NGÀY DƯƠNG LỊCH, cộng dồn vào "payable" mỗi phiên.
    • payable (C_PAYABLE_FEE) = phí QL ĐÃ trích trước NHƯNG BO CHƯA cắt thực — khoản PHẢI TRẢ (trừ khỏi NAV).
    • NAV (Net Asset Value) = giá trị tài sản RÒNG = Tổng tài sản − payable.
    • CF (cashflow) = dòng tiền KH nạp (CF_IN) / rút (CF_OUT) trong phiên.
    • PnL (Profit & Loss) = lãi/lỗ TIỀN trong ngày, ĐÃ LOẠI ảnh hưởng nạp/rút (= NAV − NAV_prev + ra − vào).
    • Unit / Unit Price = ĐƠN VỊ QUỸ / GIÁ MỖI ĐƠN VỊ (= NAV/Unit). Unit chỉ thay đổi do nạp/rút
        (KHÔNG do biến động giá) ⇒ Unit Price phản ánh THUẦN lãi/lỗ đầu tư.
    • TWR (Time-Weighted Return) = lợi suất theo Unit Price (UP_t/UP_(t-1) − 1) — không bị méo bởi cashflow.
    • roll-forward state = ghi đè trạng thái current (T_SI_NAV_CURRENT) sang cuối phiên @p_d làm mốc phiên sau.
===========================================================================*/
/*========================================================================================
  SP_EOD_COMPUTE_CORE @p_d — LÕI tính 1 phiên trên T_EOD_WORK ĐÃ SEED (dùng chung forward + rerun quá khứ).
  Giả định caller đã seed T_EOD_WORK @p_d: cash/pending/div, C_STOCK_VALUE (MTM), anchor (C_LAST_NAV/
    C_LAST_UNIT_PRICE/C_UNIT_PREV), C_PAYABLE_FEE = BASE (payable kỳ trước), C_PREV_DATE (mốc accrue+guard),
    C_FEE_CUT (phí BO cắt trừ trong kỳ; forward=0).
  Core: CF (dated) → J06 accrue (staged) → J08 NAV → J09 PnL → J10 Unit → ghi unit_ledger + nav_balance
    (SCOPED theo SI có trong T_EOD_WORK → rerun per-KH KHÔNG đụng SI khác). KHÔNG roll-forward NAV_CURRENT
    (caller quyết: forward roll hết; rerun roll ngày cuối + đúng scope).
========================================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_COMPUTE_CORE @p_d DATE
AS
BEGIN
    SET NOCOUNT ON;

    -- CF ngày @p_d (dated — forward+rerun như nhau). CF_OUT = chỉ WITHDRAW; CF_IN = mọi loại KHÁC (catch-all).
    UPDATE w SET w.C_CF_IN = cf.CF_IN, w.C_CF_OUT = cf.CF_OUT
    FROM T_EOD_WORK w
    INNER JOIN (
        SELECT C_SI_ACCOUNT,
               SUM(CASE WHEN C_EVENT_TYPE =  'WITHDRAW' THEN C_AMOUNT ELSE 0 END) AS CF_OUT,
               SUM(CASE WHEN C_EVENT_TYPE <> 'WITHDRAW' THEN C_AMOUNT ELSE 0 END) AS CF_IN   -- ⚠️ thêm loại OUTFLOW mới phải sửa ĐÂY
        FROM T_SI_CASHFLOW_EVENT WHERE C_BUSINESS_DATE=@p_d GROUP BY C_SI_ACCOUNT
    ) cf ON cf.C_SI_ACCOUNT=w.C_SI_ACCOUNT
    WHERE w.C_BUSINESS_DATE=@p_d;

    -- [BRD asset-sync] BỎ J06 accrue + J08 NAV-lắp: SDI KHÔNG tự tính NAV. C_NAV đã seed TRỰC TIẾP từ Asset
    --   (T_SI_ASSET_DAILY.C_NAV). C_PAYABLE_FEE = lũy kế phải trả (Asset), chỉ để AUM_gross=NAV+nó (PM/FR-06).
    -- J09 PnL = NAV − NAV_prev + ra − vào  (dùng NAV Asset)
    UPDATE T_EOD_WORK SET C_DAILY_PNL = C_NAV - C_LAST_NAV + C_CF_OUT - C_CF_IN WHERE C_BUSINESS_DATE=@p_d;
    -- J10 Unit/UnitPrice (ΔUnit=CF/UP_prev; init khi unit_prev=0 → unit=NAV/10000)
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

    -- unit ledger (SCOPED theo SI trong T_EOD_WORK) — DELETE+INSERT idempotent. forward (all SI) = xoá hết @p_d;
    --   rerun per-KH = chỉ SI của KH đó (KHÔNG đụng SI khác). Chỉ ngày có cashflow.
    DELETE ul FROM T_SI_UNIT_LEDGER ul
        INNER JOIN T_EOD_WORK w ON w.C_SI_ACCOUNT=ul.C_SI_ACCOUNT AND w.C_BUSINESS_DATE=@p_d
        WHERE ul.C_BUSINESS_DATE=@p_d;
    INSERT INTO T_SI_UNIT_LEDGER (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_BUSINESS_DATE,C_CF_NET,C_DELTA_UNIT,C_UNIT)
    SELECT w.C_SI_ACCOUNT,w.C_CUST_CODE,w.C_MASTER_CODE,@p_d,(w.C_CF_IN-w.C_CF_OUT),w.C_DELTA_UNIT,w.C_UNIT
    FROM T_EOD_WORK w
    WHERE w.C_BUSINESS_DATE=@p_d AND (w.C_CF_IN-w.C_CF_OUT)<>0;

    -- nav_balance (SCOPED) — DELETE+INSERT. accum TE reset 0 CÓ CHỦ ĐÍCH (J12B/SP_EOD_TE_ACCUM set lại sau).
    DELETE nb FROM T_SI_NAV_BALANCE nb
        INNER JOIN T_EOD_WORK w ON w.C_SI_ACCOUNT=nb.C_SI_ACCOUNT AND w.C_BUSINESS_DATE=@p_d
        WHERE nb.C_BUSINESS_DATE=@p_d;
    INSERT INTO T_SI_NAV_BALANCE (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_NAV,C_PAYABLE_FEE,C_UNIT,C_UNIT_PRICE,C_DAILY_PNL,C_DAILY_RETURN)
    SELECT @p_d, w.C_SI_ACCOUNT, w.C_CUST_CODE, w.C_MASTER_CODE, w.C_NAV, w.C_PAYABLE_FEE, w.C_UNIT, w.C_UNIT_PRICE, w.C_DAILY_PNL,
           CASE WHEN w.C_LAST_UNIT_PRICE>0 THEN w.C_UNIT_PRICE/w.C_LAST_UNIT_PRICE - 1 END
    FROM T_EOD_WORK w
    WHERE w.C_BUSINESS_DATE=@p_d;
END
GO

CREATE OR ALTER PROCEDURE SP_EOD_COMPUTE @p_d DATE, @p_rows BIGINT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DELETE FROM T_EOD_WORK WHERE C_BUSINESS_DATE=@p_d;

    -- [BRD asset-sync] SEED từ T_SI_ASSET_DAILY @d: Asset gửi NAV RÒNG + components + phí lũy kế phải trả.
    --   SDI KHÔNG tự tính/trừ NAV — dùng C_NAV Asset gửi TRỰC TIẾP. ANCHOR PnL/Unit từ NAV_BALANCE @prev (UP_prev
    --   định giá units nạp/rút; init UP=10.000). C_PAYABLE_FEE = lũy kế phải trả (cho AUM_gross=NAV+nó). C_PREV_DATE/C_FEE_CUT không dùng.
    DECLARE @prev DATE = dbo.UDF_PREV_BUSINESS_DATE(@p_d);
    INSERT INTO T_EOD_WORK (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,
        C_NAV,C_STOCK_VALUE,C_CASH,C_PAYABLE_FEE,C_PREV_DATE,C_FEE_CUT,
        C_LAST_NAV,C_LAST_UNIT_PRICE,C_UNIT_PREV)
    SELECT @p_d, a.C_SI_ACCOUNT, a.C_CUST_CODE, a.C_MASTER_CODE,
           a.C_NAV, a.C_STOCK_VALUE, a.C_CASH, a.C_FEE_ACCUM, @prev, 0,   -- cash = TỔNG tiền (1 số); work pending/div mặc định 0
           ISNULL(pb.C_NAV, 0), pb.C_UNIT_PRICE, ISNULL(pb.C_UNIT, 0)
    FROM T_SI_ASSET_DAILY a
    INNER JOIN T_SI_PORTFOLIO p ON p.C_SI_ACCOUNT=a.C_SI_ACCOUNT AND p.C_STATUS='ACTIVE'
    LEFT JOIN T_SI_NAV_BALANCE pb ON pb.C_SI_ACCOUNT=a.C_SI_ACCOUNT AND pb.C_BUSINESS_DATE=@prev
    WHERE a.C_BUSINESS_DATE=@p_d;

    EXEC SP_EOD_COMPUTE_CORE @p_d;   -- CF (cashflow SDI) → NAV=Asset (KHÔNG lắp) → PnL → Unit/UP → unit_ledger/nav_balance

    -- roll-forward NAV_CURRENT: tiền (từ Asset) + unit/UP (derive). MERGE để tạo dòng nếu SI chưa có.
    MERGE T_SI_NAV_CURRENT t
    USING (SELECT C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_CASH,C_PAYABLE_FEE,C_UNIT,C_NAV,C_UNIT_PRICE
           FROM T_EOD_WORK WHERE C_BUSINESS_DATE=@p_d) w
    ON t.C_SI_ACCOUNT=w.C_SI_ACCOUNT
    WHEN MATCHED THEN UPDATE SET
        t.C_CASH=w.C_CASH,
        t.C_PAYABLE_FEE=w.C_PAYABLE_FEE, t.C_UNIT=w.C_UNIT,
        t.C_LAST_NAV=w.C_NAV, t.C_LAST_UNIT_PRICE=w.C_UNIT_PRICE, t.C_LAST_BUSINESS_DATE=@p_d
    WHEN NOT MATCHED THEN INSERT (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_UNIT,C_CASH,C_PAYABLE_FEE,C_LAST_NAV,C_LAST_UNIT_PRICE,C_STATUS,C_LAST_BUSINESS_DATE)
        VALUES (w.C_SI_ACCOUNT,w.C_CUST_CODE,w.C_MASTER_CODE,w.C_UNIT,w.C_CASH,w.C_PAYABLE_FEE,w.C_NAV,w.C_UNIT_PRICE,'ACTIVE',@p_d);
    SET @p_rows = @@ROWCOUNT;   -- #tiểu khoản xử lý phiên này
END
GO

/*===========================================================================
  [BRD asset-sync] SP_INGEST_ASSET_NAV — Asset gửi số TỔNG per-SI mỗi ngày GD → T_SI_ASSET_DAILY (raw).
  SDI KHÔNG tự định giá/trừ: NAV RÒNG + components (stock/cash/pending/div) + phí lũy kế phải trả + cash_in/out
  đều từ Asset. SDI chỉ derive unit/UP/PnL/return ở SP_EOD_COMPUTE (seed NAV + cashflow). Idempotent
  (DELETE+INSERT theo date,si → re-ingest sửa quá khứ). Validate: si registry + nav/stock/cash NOT NULL.
  JSON: [{"si_account","nav","stock_value","cash"(TỔNG tiền 1 số),"fee_accum","cash_in","cash_out"}, ...]
  err: 0 OK · 20 JSON sai · 21 validate FAIL · -1 runtime.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_INGEST_ASSET_NAV
    @p_json          NVARCHAR(MAX),
    @p_business_date DATE,
    @p_user          VARCHAR(64)   = NULL,
    @p_err_code      INT           OUTPUT,
    @p_err_msg       NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    SET @p_err_code=0; SET @p_err_msg=NULL;
    BEGIN TRY
        IF @p_business_date IS NULL BEGIN SET @p_err_code=20; SET @p_err_msg=N'@p_business_date NULL'; RETURN; END
        IF @p_json IS NULL OR ISJSON(@p_json)<>1 BEGIN SET @p_err_code=20; SET @p_err_msg=N'@p_json không hợp lệ'; RETURN; END

        DECLARE @src TABLE (C_SI_ACCOUNT VARCHAR(20) PRIMARY KEY, C_NAV DECIMAL(20,0), C_STOCK_VALUE DECIMAL(20,0),
                            C_CASH DECIMAL(20,0), C_FEE_ACCUM DECIMAL(20,6), C_CASH_IN DECIMAL(20,0), C_CASH_OUT DECIMAL(20,0));
        INSERT @src
        SELECT j.si_account, j.nav, j.stock_value, j.cash,
               ISNULL(j.fee_accum,0), ISNULL(j.cash_in,0), ISNULL(j.cash_out,0)
        FROM OPENJSON(@p_json) WITH (
            si_account VARCHAR(20) '$.si_account', nav DECIMAL(20,0) '$.nav', stock_value DECIMAL(20,0) '$.stock_value',
            cash DECIMAL(20,0) '$.cash', fee_accum DECIMAL(20,6) '$.fee_accum',
            cash_in DECIMAL(20,0) '$.cash_in', cash_out DECIMAL(20,0) '$.cash_out') j;

        IF EXISTS (SELECT 1 FROM @src WHERE C_SI_ACCOUNT IS NULL OR C_NAV IS NULL OR C_STOCK_VALUE IS NULL OR C_CASH IS NULL)
            BEGIN SET @p_err_code=21; SET @p_err_msg=N'Thiếu si_account/nav/stock_value/cash'; RETURN; END
        IF EXISTS (SELECT 1 FROM @src s WHERE NOT EXISTS (SELECT 1 FROM T_SI_PORTFOLIO p WHERE p.C_SI_ACCOUNT=s.C_SI_ACCOUNT))
            BEGIN SET @p_err_code=21; SET @p_err_msg=N'si_account chưa đăng ký registry (T_SI_PORTFOLIO)'; RETURN; END

        BEGIN TRAN;
        DELETE FROM T_SI_ASSET_DAILY WHERE C_BUSINESS_DATE=@p_business_date
            AND C_SI_ACCOUNT IN (SELECT C_SI_ACCOUNT FROM @src);   -- idempotent re-ingest (sửa quá khứ = gửi lại)
        INSERT INTO T_SI_ASSET_DAILY (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,
            C_NAV,C_STOCK_VALUE,C_CASH,C_FEE_ACCUM,C_CASH_IN,C_CASH_OUT)
        SELECT @p_business_date, s.C_SI_ACCOUNT, p.C_CUST_CODE, p.C_MASTER_CODE,
               s.C_NAV, s.C_STOCK_VALUE, s.C_CASH, s.C_FEE_ACCUM, s.C_CASH_IN, s.C_CASH_OUT
        FROM @src s INNER JOIN T_SI_PORTFOLIO p ON p.C_SI_ACCOUNT=s.C_SI_ACCOUNT;
        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        SET @p_err_code=-1; SET @p_err_msg=ERROR_MESSAGE();
    END CATCH
END
GO

/*===========================================================================
  SP_INGEST_PRICE_DAILY — nạp GIÁ EOD (close + ref + ex-rights) từ Market vào T_PRICE_DAILY.
    AN TOÀN 2 yếu tố:
    (1) TYPE-SAFE: input JSON string → OPENJSON WITH ÉP KIỂU; sai kiểu/thiếu field → NULL → bắt ở validate
        (không tin string thô). (2) ALL-OR-NOTHING: validate TOÀN batch TRƯỚC; sai bất kỳ (ticker rỗng,
        ref/close NULL hoặc ≤0, is_ex_rights ∉{0,1}, TRÙNG mã, hoặc lệch @p_expected_count) → TỪ CHỐI CẢ
        batch, KHÔNG ghi 1 dòng → tránh nạp một-phần làm master index SAI. Hợp lệ → MERGE upsert (date,ticker)
        1 statement (atomic) + XACT_ABORT ON. IDEMPOTENT: gọi lại cùng ngày = ghi đè.
    JSON: [{"ticker":"AAA","ref_price":110.0,"close_price":112.0,"is_ex_rights":0}, ...]
        ref_price = giá tham chiếu đầu phiên (ex-rights = giá SAU chia); is_ex_rights 1=ngày có quyền (default 0).
    ⚠️ Chỉ NẠP giá — KHÔNG set MKT_DATA READY (app gọi SP_EOD_SET_SOURCE_READY sau). Đủ-mã-hay-chưa do
        completeness gate trong SP_EOD_RUN_INDEX chốt (thiếu mã danh mục mẫu → chặn tính index).
    err: 0 OK · 20 JSON sai · 21 validate FAIL (liệt kê lỗi đầu) · 22 lệch số mã dự kiến · -1 runtime.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_INGEST_PRICE_DAILY
    @p_json           NVARCHAR(MAX),
    @p_business_date  DATE,
    @p_expected_count INT           = NULL,   -- (optional) số mã app dự định gửi → chặn payload thiếu/bị cắt
    @p_user           VARCHAR(64)   = NULL,
    @p_err_code       INT           OUTPUT,
    @p_err_msg        NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    SET @p_err_code = 0; SET @p_err_msg = NULL;
    BEGIN TRY
        IF @p_business_date IS NULL
            BEGIN SET @p_err_code=20; SET @p_err_msg=N'@p_business_date NULL'; RETURN; END
        IF @p_json IS NULL OR ISJSON(@p_json) <> 1
            BEGIN SET @p_err_code=20; SET @p_err_msg=N'@p_json không phải JSON hợp lệ'; RETURN; END

        -- PARSE type-safe (OPENJSON WITH ép kiểu) + đánh dấu trùng mã (rn>1)
        DECLARE @src TABLE (C_TICKER VARCHAR(20), C_REF_PRICE DECIMAL(18,4), C_CLOSE_PRICE DECIMAL(18,4),
                            C_IS_EX_RIGHTS TINYINT, rn INT);
        INSERT @src (C_TICKER, C_REF_PRICE, C_CLOSE_PRICE, C_IS_EX_RIGHTS, rn)
        SELECT j.ticker, j.ref_price, j.close_price, ISNULL(j.is_ex_rights, 0),   -- thiếu is_ex_rights = phiên thường (0)
               ROW_NUMBER() OVER (PARTITION BY j.ticker ORDER BY (SELECT NULL))
        FROM OPENJSON(@p_json) WITH (
            ticker       VARCHAR(20)   '$.ticker',
            ref_price    DECIMAL(18,4) '$.ref_price',
            close_price  DECIMAL(18,4) '$.close_price',
            is_ex_rights TINYINT       '$.is_ex_rights'
        ) j;

        DECLARE @n INT = (SELECT COUNT(*) FROM @src);
        IF @n = 0 BEGIN SET @p_err_code=21; SET @p_err_msg=N'Batch rỗng (0 mã) — từ chối'; RETURN; END

        -- VALIDATE toàn batch (all-or-nothing): lấy lỗi ĐẦU TIÊN để báo
        DECLARE @bad NVARCHAR(300) = (
            SELECT TOP 1 CASE
                WHEN C_TICKER IS NULL OR LTRIM(C_TICKER)=''      THEN N'ticker rỗng'
                WHEN rn > 1                                       THEN CONCAT(N'ticker TRÙNG: ', C_TICKER)
                WHEN C_REF_PRICE IS NULL OR C_REF_PRICE <= 0     THEN CONCAT(N'ref_price không hợp lệ @', C_TICKER)
                WHEN C_CLOSE_PRICE IS NULL OR C_CLOSE_PRICE <= 0 THEN CONCAT(N'close_price không hợp lệ @', C_TICKER)
                WHEN C_IS_EX_RIGHTS NOT IN (0,1)                 THEN CONCAT(N'is_ex_rights phải 0/1 @', C_TICKER)
                END
            FROM @src
            WHERE C_TICKER IS NULL OR LTRIM(C_TICKER)='' OR rn>1
               OR C_REF_PRICE IS NULL OR C_REF_PRICE<=0
               OR C_CLOSE_PRICE IS NULL OR C_CLOSE_PRICE<=0
               OR C_IS_EX_RIGHTS NOT IN (0,1));
        IF @bad IS NOT NULL
            BEGIN SET @p_err_code=21;
                SET @p_err_msg=CONCAT(N'Validate FAIL — batch BỊ TỪ CHỐI (không ghi dòng nào): ', @bad);
                RETURN; END

        -- COUNT guard: chặn payload thiếu/bị cắt giữa chừng (app báo trước số mã)
        IF @p_expected_count IS NOT NULL AND @n <> @p_expected_count
            BEGIN SET @p_err_code=22;
                SET @p_err_msg=CONCAT(N'Số mã nhận ', @n, N' != dự kiến ', @p_expected_count, N' — batch BỊ TỪ CHỐI');
                RETURN; END

        -- ATOMIC upsert (MERGE 1 statement; XACT_ABORT ON → lỗi runtime rollback toàn bộ)
        MERGE T_PRICE_DAILY AS t
        USING (SELECT C_TICKER, C_REF_PRICE, C_CLOSE_PRICE, C_IS_EX_RIGHTS FROM @src) s
           ON t.C_BUSINESS_DATE = @p_business_date AND t.C_TICKER = s.C_TICKER
        WHEN MATCHED THEN UPDATE SET
            t.C_REF_PRICE=s.C_REF_PRICE, t.C_CLOSE_PRICE=s.C_CLOSE_PRICE, t.C_IS_EX_RIGHTS=s.C_IS_EX_RIGHTS
        WHEN NOT MATCHED THEN
            INSERT (C_TICKER, C_BUSINESS_DATE, C_REF_PRICE, C_CLOSE_PRICE, C_IS_EX_RIGHTS)
            VALUES (s.C_TICKER, @p_business_date, s.C_REF_PRICE, s.C_CLOSE_PRICE, s.C_IS_EX_RIGHTS);
    END TRY
    BEGIN CATCH
        IF @p_err_code = 0 BEGIN SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE(); END
    END CATCH
END
GO

/*===========================================================================
  J11 — SI AGGREGATE → T_MASTER_NAV_BALANCE (composition + NAV + hiệu suất + cổ tức/phí)
         + upsert T_MASTER_NAV_CURRENT (snapshot current cấp SI cho serving)
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_SI_AGG @p_d DATE, @p_rows BIGINT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @prev DATE = dbo.UDF_PREV_BUSINESS_DATE(@p_d);
    -- DELETE SCOPED theo master có trong T_EOD_WORK @p_d (forward: all masters = xoá hết @p_d; rerun: chỉ master
    --   bị ảnh hưởng → KHÔNG đụng master khác). Behavior forward giữ nguyên.
    DELETE m FROM T_MASTER_NAV_BALANCE m
        WHERE m.C_BUSINESS_DATE=@p_d
          AND EXISTS (SELECT 1 FROM T_EOD_WORK w WHERE w.C_MASTER_CODE=m.C_MASTER_CODE AND w.C_BUSINESS_DATE=@p_d);

    ;WITH agg AS (
        SELECT C_MASTER_CODE,
               SUM(C_CASH) AS CASH,
               SUM(C_STOCK_VALUE) AS STOCK, SUM(C_PAYABLE_FEE) AS PAY,
               SUM(C_NAV) AS NAV, SUM(C_UNIT) AS UNT, SUM(C_DAILY_PNL) AS PNL,
               SUM(C_CF_IN) AS CFIN, SUM(C_CF_OUT) AS CFOUT, COUNT(*) AS ACCT   -- [PM] flow + #tiểu khoản ACTIVE
        FROM T_EOD_WORK WHERE C_BUSINESS_DATE=@p_d GROUP BY C_MASTER_CODE
    )
    INSERT INTO T_MASTER_NAV_BALANCE (C_BUSINESS_DATE,C_MASTER_CODE,C_CASH,
                             C_PAYABLE_FEE,C_AUM,
                             C_NAV,C_UNIT,C_UNIT_PRICE,C_DAILY_PNL,C_DAILY_RETURN,C_CASH_IN,C_CASH_OUT,C_TOTAL_ACCOUNT)
    SELECT @p_d, a.C_MASTER_CODE, a.CASH,
           a.PAY, (a.CASH + a.STOCK),   -- [BRD] AUM gross gồm stock (từ work) + receivables; KHÔNG lưu cột stock riêng (PM không đọc)
           a.NAV, a.UNT,
           CASE WHEN a.UNT>0 THEN a.NAV/a.UNT END,
           a.PNL,
           CASE WHEN a.UNT>0 AND prev.C_UNIT_PRICE>0 THEN (a.NAV/a.UNT)/prev.C_UNIT_PRICE - 1 END,
           a.CFIN, a.CFOUT, a.ACCT
    FROM agg a
    LEFT JOIN T_MASTER_NAV_BALANCE prev ON prev.C_MASTER_CODE=a.C_MASTER_CODE AND prev.C_BUSINESS_DATE=@prev;
    SET @p_rows = @@ROWCOUNT;   -- #master aggregate (bắt NGAY sau INSERT, trước MERGE current bên dưới)

    -- current cấp master (overwrite) — CHỈ khi @p_d là ngày MỚI NHẤT (forward). Rerun ngày QUÁ KHỨ → KHÔNG đụng
    --   current (current phải = hôm nay). Forward luôn tính ngày mới nhất ⇒ chạy như cũ.
    IF @p_d = (SELECT MAX(C_BUSINESS_DATE) FROM T_MASTER_NAV_BALANCE)
    MERGE T_MASTER_NAV_CURRENT AS t
    USING (SELECT C_MASTER_CODE,C_CASH,C_AUM,C_NAV,C_UNIT,C_UNIT_PRICE,C_TOTAL_ACCOUNT,C_BUSINESS_DATE
           FROM T_MASTER_NAV_BALANCE WHERE C_BUSINESS_DATE=@p_d) s
    ON t.C_MASTER_CODE=s.C_MASTER_CODE
    WHEN MATCHED THEN UPDATE SET
        t.C_CASH=s.C_CASH, t.C_AUM=s.C_AUM,
        t.C_LAST_NAV=s.C_NAV, t.C_UNIT=s.C_UNIT, t.C_LAST_UNIT_PRICE=s.C_UNIT_PRICE, t.C_TOTAL_ACCOUNT=s.C_TOTAL_ACCOUNT,
        t.C_LAST_BUSINESS_DATE=s.C_BUSINESS_DATE
    WHEN NOT MATCHED THEN INSERT (C_MASTER_CODE,C_CASH,C_AUM,C_LAST_NAV,C_UNIT,C_LAST_UNIT_PRICE,C_TOTAL_ACCOUNT,C_LAST_BUSINESS_DATE)
        VALUES (s.C_MASTER_CODE,s.C_CASH,s.C_AUM,s.C_NAV,s.C_UNIT,s.C_UNIT_PRICE,s.C_TOTAL_ACCOUNT,s.C_BUSINESS_DATE);
END
GO

/*===========================================================================
  J12 — SI INDEX (danh mục mẫu, 100% cổ phiếu) → T_MASTER_INDEX_DAILY
        Index_t = Index_(t-1) × Σ w^(t) × P_t / P_ref ;  w^(t)=eff_date≤@d mới nhất
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_SI_INDEX @p_d DATE, @p_rows BIGINT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @prev DATE = dbo.UDF_PREV_BUSINESS_DATE(@p_d);

    -- COMPLETENESS HARD-FAIL (mọi đường gọi: forward/direct/recompute): MỌI mã trong rổ hiệu lực @p_d của MỌI
    --   master ACTIVE PHẢI có giá @p_d. Thiếu DÙ 1 mã (của BẤT KỲ master nào) → THROW, KHÔNG ghi index master nào
    --   ngày đó (all-or-nothing) → nghiệp vụ CONTROL được (báo rõ master:mã thiếu), KHÔNG silent skip. Chốt
    --   2026-06-24: bỏ per-master skip cũ vì danh mục thiếu bị bỏ ngầm, nghiệp vụ không kiểm soát được.
    --   THROW trước mọi DML ⇒ atomic: index ngày đó GIỮ NGUYÊN (không xoá) nếu fail. (Forward còn gate err=11 ở
    --   SP_EOD_RUN_INDEX báo sớm/sạch — cùng scope ACTIVE — trước khi tới đây; throw này phủ direct/recompute.)
    DECLARE @missing NVARCHAR(MAX) = (
        SELECT STRING_AGG(CONCAT(x.C_MASTER_CODE, N':', x.C_TICKER), N', ')
               WITHIN GROUP (ORDER BY x.C_MASTER_CODE, x.C_TICKER)
        FROM (
            SELECT mw.C_MASTER_CODE, mw.C_TICKER
            FROM T_MASTER_PORTFOLIO_TICKER mw
            INNER JOIN T_MASTER_PORTFOLIO mp ON mp.C_MASTER_CODE=mw.C_MASTER_CODE AND mp.C_STATUS='ACTIVE'
            INNER JOIN (SELECT C_MASTER_CODE, MAX(C_EFFECTIVE_DATE) AS ED
                        FROM T_MASTER_PORTFOLIO_TICKER WHERE C_EFFECTIVE_DATE<=@p_d GROUP BY C_MASTER_CODE) LD
              ON LD.C_MASTER_CODE=mw.C_MASTER_CODE AND LD.ED=mw.C_EFFECTIVE_DATE
            WHERE NOT EXISTS (SELECT 1 FROM T_PRICE_DAILY p
                              WHERE p.C_TICKER=mw.C_TICKER AND p.C_BUSINESS_DATE=@p_d)
        ) x);
    IF @missing IS NOT NULL
    BEGIN
        DECLARE @emsg NVARCHAR(2000) = CONCAT(N'INDEX completeness FAIL @', CONVERT(VARCHAR(10),@p_d,23),
            N': thiếu giá (master:mã) ', LEFT(@missing,1800),
            N' — KHÔNG tính index master nào ngày này (tránh index sai do thiếu giá).');
        THROW 51011, @emsg, 1;
    END

    -- VALIDATE WEIGHT (TRƯỚC mọi DML): master ACTIVE có Σtarget_weight = 0 (cấu hình rổ sai) ⇒ FACTOR =
    --   numerator/NULLIF(0,0) = NULL ⇒ nếu để chạy tới INSERT sẽ vỡ ràng buộc NOT NULL của C_INDEX_VALUE.
    --   Raise NGAY tại đây (báo rõ master), KHÔNG để lỗi NOT NULL khó hiểu ở tầng insert.
    DECLARE @badw NVARCHAR(MAX) = (
        SELECT STRING_AGG(CONVERT(NVARCHAR(20), z.C_MASTER_CODE), N', ') WITHIN GROUP (ORDER BY z.C_MASTER_CODE)
        FROM (
            SELECT mw.C_MASTER_CODE
            FROM T_MASTER_PORTFOLIO_TICKER mw
            INNER JOIN T_MASTER_PORTFOLIO mp ON mp.C_MASTER_CODE=mw.C_MASTER_CODE AND mp.C_STATUS='ACTIVE'
            INNER JOIN (SELECT C_MASTER_CODE, MAX(C_EFFECTIVE_DATE) AS ED
                        FROM T_MASTER_PORTFOLIO_TICKER WHERE C_EFFECTIVE_DATE<=@p_d GROUP BY C_MASTER_CODE) LD
              ON LD.C_MASTER_CODE=mw.C_MASTER_CODE AND LD.ED=mw.C_EFFECTIVE_DATE
            GROUP BY mw.C_MASTER_CODE
            HAVING SUM(CAST(mw.C_TARGET_WEIGHT AS FLOAT)) = 0
        ) z);
    IF @badw IS NOT NULL
    BEGIN
        DECLARE @wmsg NVARCHAR(2000) = CONCAT(N'INDEX weight INVALID @', CONVERT(VARCHAR(10),@p_d,23),
            N': Σtarget_weight = 0 (cấu hình rổ sai) — master ', LEFT(@badw,1800),
            N' — KHÔNG tính index (FACTOR không xác định).');
        THROW 51012, @wmsg, 1;
    END

    -- DELETE scoped theo master ACTIVE (đúng tập sẽ INSERT lại). KHÔNG xoá index của master ĐÃ CLOSED
    --   → lịch sử index master đã đóng được GIỮ NGUYÊN (đóng băng) khi re-run/recompute ngày quá khứ.
    DELETE idx FROM T_MASTER_INDEX_DAILY idx
    INNER JOIN T_MASTER_PORTFOLIO mp ON mp.C_MASTER_CODE=idx.C_MASTER_CODE AND mp.C_STATUS='ACTIVE'
    WHERE idx.C_BUSINESS_DATE=@p_d;

    ;WITH LD AS (
        SELECT C_MASTER_CODE, MAX(C_EFFECTIVE_DATE) AS ED
        FROM T_MASTER_PORTFOLIO_TICKER WHERE C_EFFECTIVE_DATE<=@p_d GROUP BY C_MASTER_CODE
    ),
    W AS (   -- rổ hiệu lực @p_d của master ACTIVE (cùng scope với guard hard-fail + forward gate)
        SELECT mw.C_MASTER_CODE, mw.C_TICKER, mw.C_TARGET_WEIGHT
        FROM T_MASTER_PORTFOLIO_TICKER mw
        INNER JOIN T_MASTER_PORTFOLIO mp ON mp.C_MASTER_CODE=mw.C_MASTER_CODE AND mp.C_STATUS='ACTIVE'
        INNER JOIN LD ON LD.C_MASTER_CODE=mw.C_MASTER_CODE AND LD.ED=mw.C_EFFECTIVE_DATE
    ),
    FACT AS (
        -- FACTOR = BÌNH QUÂN GIA QUYỀN price-relative = Σ(w × close/ref) / Σ(w). CHIA Σw để BẤT BIẾN với thang
        --   trọng số: dù FO gửi w dạng phân số (Σ=1) hay phần trăm (Σ=100) đều ra ~1.0x/ngày (KHÔNG còn ×100 nổ
        --   cấp số nhân). close/ref = giá đóng / giá tham chiếu đầu phiên (self-contained, cùng dòng @p_d).
        --   ĐỦ giá mọi mã đã được guard hard-fail ở trên đảm bảo ⇒ INNER JOIN price KHÔNG drop mã nào.
        SELECT W.C_MASTER_CODE,
               SUM( CAST(W.C_TARGET_WEIGHT AS FLOAT) * p.C_CLOSE_PRICE / p.C_REF_PRICE )
                 / NULLIF(SUM( CAST(W.C_TARGET_WEIGHT AS FLOAT) ), 0)        AS FACTOR  -- FLOAT: tránh cắt scale do chia decimal (cap 38) → index đúng 6 chữ số
        FROM W
        INNER JOIN T_PRICE_DAILY p ON p.C_TICKER=W.C_TICKER AND p.C_BUSINESS_DATE=@p_d
        GROUP BY W.C_MASTER_CODE
    )
    INSERT INTO T_MASTER_INDEX_DAILY (C_BUSINESS_DATE,C_MASTER_CODE,C_INDEX_VALUE,C_DAILY_RETURN)
    SELECT @p_d, f.C_MASTER_CODE,
           COALESCE(pi.C_INDEX_VALUE, 1000) * f.FACTOR,
           f.FACTOR - 1
    FROM FACT f
    LEFT JOIN T_MASTER_INDEX_DAILY pi ON pi.C_MASTER_CODE=f.C_MASTER_CODE AND pi.C_BUSINESS_DATE=@prev;
    SET @p_rows = @@ROWCOUNT;   -- #master index ghi @p_d
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
CREATE OR ALTER PROCEDURE SP_EOD_TE_ACCUM @p_d DATE, @p_rows BIGINT = NULL OUTPUT
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
    INNER JOIN T_MASTER_INDEX_DAILY idx ON idx.C_MASTER_CODE=b.C_MASTER_CODE AND idx.C_BUSINESS_DATE=@p_d
    LEFT JOIN T_SI_NAV_BALANCE p ON p.C_SI_ACCOUNT=b.C_SI_ACCOUNT AND p.C_BUSINESS_DATE=@prev
    WHERE b.C_BUSINESS_DATE=@p_d
      -- SCOPE theo T_EOD_WORK @p_d: forward (work=mọi SI active) → cập nhật hết như cũ; rerun per-KH (work=SI master
      --   bị ảnh hưởng) → CHỈ cập nhật SI đó (tối ưu, không quét toàn bộ SI mỗi ngày). Mọi flow đều populate work @p_d trước TE.
      AND EXISTS (SELECT 1 FROM T_EOD_WORK w WHERE w.C_SI_ACCOUNT=b.C_SI_ACCOUNT AND w.C_BUSINESS_DATE=@p_d);
    SET @p_rows = @@ROWCOUNT;   -- #tiểu khoản cập nhật TE accum (scope theo work @p_d)
END
GO

/*===========================================================================
  J13 — RECONCILE (RECORDER): GHI chi tiết break vào T_EOD_RECON_BREAK (idempotent), KHÔNG THROW.
        SP_EOD_RUN đọc bảng break SAU bước này để quyết: có break ⇒ RECONCILE_STATUS=BREAK +
        CHẶN publish (không chạy J14). KHÔNG throw ở đây để break được COMMIT (sống sót), không bị
        rollback theo transaction của SP_EOD_STEP. Đọc data ĐÃ committed của J07/J11 (T_EOD_WORK,
        T_MASTER_NAV_BALANCE) — đó là lý do mỗi step commit riêng.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_RECONCILE @p_d DATE, @p_rows BIGINT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DELETE FROM T_EOD_RECON_BREAK WHERE C_BUSINESS_DATE=@p_d;   -- idempotent (re-run)

    -- Check 1: NAV âm hoặc unit<=0 với NAV>0 (per-KH) → ghi từng dòng
    INSERT INTO T_EOD_RECON_BREAK (C_BUSINESS_DATE,C_CHECK_NAME,C_MASTER_CODE,C_SI_ACCOUNT,C_VALUE_SDI,C_MESSAGE)
    SELECT @p_d, 'NAV_NEGATIVE', C_MASTER_CODE, C_SI_ACCOUNT, C_NAV,
           CONCAT(N'NAV=', C_NAV, N' UNIT=', C_UNIT, N' bất thường')
    FROM T_EOD_WORK
    WHERE C_BUSINESS_DATE=@p_d AND (C_NAV < 0 OR (C_UNIT<=0 AND C_NAV>0));

    -- Check 2: NAV master != Σ NAV khách (chênh > 1 VND) → sanity agg (master = Σ SI, cùng nguồn ingest)
    INSERT INTO T_EOD_RECON_BREAK (C_BUSINESS_DATE,C_CHECK_NAME,C_MASTER_CODE,C_VALUE_SDI,C_VALUE_CHECK,C_DIFF,C_MESSAGE)
    SELECT @p_d, 'SI_NAV_MISMATCH', p.C_MASTER_CODE, p.C_NAV, a.NAV, p.C_NAV - a.NAV,
           N'NAV master != Σ NAV khách (agg sai?)'
    FROM T_MASTER_NAV_BALANCE p
    INNER JOIN (SELECT C_MASTER_CODE, SUM(C_NAV) NAV FROM T_EOD_WORK WHERE C_BUSINESS_DATE=@p_d GROUP BY C_MASTER_CODE) a
      ON a.C_MASTER_CODE=p.C_MASTER_CODE
    WHERE p.C_BUSINESS_DATE=@p_d AND ABS(p.C_NAV - a.NAV) > 1;

    -- [BRD] Check 3 CASHFLOW_MISMATCH (đo vênh 2 nguồn — QĐ5): cashflow SDI tự nhập (authoritative cho unit/UP)
    --   vs cash_in/out Asset gửi. Lệch (số nguyên VND, exact) → unit/UP (SDI) & NAV (Asset) KHÔNG cùng dòng tiền.
    ;WITH sdicf AS (
        SELECT C_SI_ACCOUNT,
               SUM(CASE WHEN C_EVENT_TYPE='WITHDRAW' THEN -C_AMOUNT ELSE C_AMOUNT END) AS NET
        FROM T_SI_CASHFLOW_EVENT WHERE C_BUSINESS_DATE=@p_d GROUP BY C_SI_ACCOUNT)
    INSERT INTO T_EOD_RECON_BREAK (C_BUSINESS_DATE,C_CHECK_NAME,C_MASTER_CODE,C_SI_ACCOUNT,C_VALUE_SDI,C_VALUE_CHECK,C_DIFF,C_MESSAGE)
    SELECT @p_d, 'CASHFLOW_MISMATCH', a.C_MASTER_CODE, a.C_SI_ACCOUNT,
           ISNULL(s.NET,0), (a.C_CASH_IN - a.C_CASH_OUT), ISNULL(s.NET,0) - (a.C_CASH_IN - a.C_CASH_OUT),
           N'Cashflow SDI != Asset cash_in/out'
    FROM T_SI_ASSET_DAILY a
    LEFT JOIN sdicf s ON s.C_SI_ACCOUNT=a.C_SI_ACCOUNT
    WHERE a.C_BUSINESS_DATE=@p_d AND ISNULL(s.NET,0) <> (a.C_CASH_IN - a.C_CASH_OUT);

    -- [BRD] Check 4 HOLDINGS_MISMATCH (đo vênh 2 nguồn): Σ(FO holdings × giá BO) vs Asset stock_value (cùng giá BO,
    --   QĐ3) → lệch = FO holdings vs định giá Asset khác nhau (chênh > 1 VND, tránh nhiễu làm tròn).
    ;WITH fohold AS (
        SELECT h.C_SI_ACCOUNT, SUM(h.C_QUANTITY*px.C_CLOSE_PRICE) AS SV
        FROM T_SI_PORTFOLIO_HOLDING h
        INNER JOIN T_PRICE_DAILY px ON px.C_TICKER=h.C_TICKER AND px.C_BUSINESS_DATE=@p_d
        GROUP BY h.C_SI_ACCOUNT)
    INSERT INTO T_EOD_RECON_BREAK (C_BUSINESS_DATE,C_CHECK_NAME,C_MASTER_CODE,C_SI_ACCOUNT,C_VALUE_SDI,C_VALUE_CHECK,C_DIFF,C_MESSAGE)
    SELECT @p_d, 'HOLDINGS_MISMATCH', a.C_MASTER_CODE, a.C_SI_ACCOUNT,
           ISNULL(f.SV,0), a.C_STOCK_VALUE, ISNULL(f.SV,0) - a.C_STOCK_VALUE,
           N'Σ(FO holdings×giá BO) != Asset stock_value'
    FROM T_SI_ASSET_DAILY a
    LEFT JOIN fohold f ON f.C_SI_ACCOUNT=a.C_SI_ACCOUNT
    WHERE a.C_BUSINESS_DATE=@p_d AND ABS(ISNULL(f.SV,0) - a.C_STOCK_VALUE) > 1;

    -- [BRD] Check 5 NAV_CONSISTENCY: NAV Asset gửi vs (stock+cash+pending+div − fee_accum) — giờ NAV ĐỘC LẬP với
    --   components (Asset gửi cả 2) → bắt Asset tự mâu thuẫn nội bộ. Chênh > 1 VND → break.
    INSERT INTO T_EOD_RECON_BREAK (C_BUSINESS_DATE,C_CHECK_NAME,C_MASTER_CODE,C_SI_ACCOUNT,C_VALUE_SDI,C_VALUE_CHECK,C_DIFF,C_MESSAGE)
    SELECT @p_d, 'NAV_CONSISTENCY', a.C_MASTER_CODE, a.C_SI_ACCOUNT,
           a.C_NAV, (a.C_STOCK_VALUE + a.C_CASH - a.C_FEE_ACCUM),
           a.C_NAV - (a.C_STOCK_VALUE + a.C_CASH - a.C_FEE_ACCUM),
           N'NAV Asset != stock + cash − fee (Asset nội bộ lệch)'
    FROM T_SI_ASSET_DAILY a
    WHERE a.C_BUSINESS_DATE=@p_d
      AND ABS(a.C_NAV - (a.C_STOCK_VALUE + a.C_CASH - a.C_FEE_ACCUM)) > 1;

    -- #dòng break (5 check gộp); 0 = sạch. Tổng cuối (khỏi phụ thuộc @@ROWCOUNT riêng check cuối).
    SET @p_rows = (SELECT COUNT(*) FROM T_EOD_RECON_BREAK WHERE C_BUSINESS_DATE=@p_d);
END
GO

/*===========================================================================
  J14 — SNAPSHOT: T_MASTER_HOLDING_BALANCE (SI aggregate holdings + tỷ trọng)
         (composition tài sản + NAV đã gộp về T_MASTER_NAV_BALANCE ở J11)
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_SNAPSHOT @p_d DATE, @p_rows BIGINT = NULL OUTPUT
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
        FROM sih INNER JOIN T_PRICE_DAILY p ON p.C_TICKER=sih.C_TICKER AND p.C_BUSINESS_DATE=@p_d
    )
    INSERT INTO T_MASTER_HOLDING_BALANCE (C_BUSINESS_DATE,C_MASTER_CODE,C_TICKER,C_QUANTITY,C_MARKET_PRICE,C_MARKET_VALUE,C_WEIGHT)
    SELECT @p_d, v.C_MASTER_CODE, v.C_TICKER, v.QTY, v.C_CLOSE_PRICE, v.MV,
           CASE WHEN SUM(v.MV) OVER (PARTITION BY v.C_MASTER_CODE) > 0
                THEN v.MV / SUM(v.MV) OVER (PARTITION BY v.C_MASTER_CODE) END
    FROM val v;
    SET @p_rows = @@ROWCOUNT;   -- #dòng holdings-balance (mã × master) snapshot @p_d
    -- (composition tài sản + NAV cấp SI: đã ghi T_MASTER_NAV_BALANCE ở J11_SI_AGG)
END
GO

/*===========================================================================
  J0 — GATE: chờ đủ FO ingest trước khi chạy EOD. So received (watermark C_LAST_SYNC_DATE=@d)
       vs expected (tiểu khoản ACTIVE). Lệch ⇒ THROW (thiếu/dư data) → chặn EOD + alert.
       (đếm theo state nên DISTINCT sẵn — Kafka redelivery không làm phồng số.)
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_GATE @p_d DATE, @p_rows BIGINT = NULL OUTPUT
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
    SET @p_rows = @received;   -- #tiểu khoản đã nhận đủ FO ingest (= expected khi qua cổng)
END
GO

/*===========================================================================
  DISPATCHER: chạy 1 job idempotent + transaction + log (resume-safe)
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_STEP @p_d DATE, @p_job VARCHAR(40), @p_proc SYSNAME
AS
BEGIN
    SET NOCOUNT ON;
    -- GATE RESUME (driven by T_EOD_PIPELINE.C_EOD_RESET_AT): skip job ĐÃ DONE *sau* lần reset gần nhất.
    --   reset chỉ SET watermark (KHÔNG xoá T_EOD_RUN) → DONE cũ (C_ENDED_AT <= watermark) KHÔNG còn tính → chạy lại.
    DECLARE @resetAt DATETIME = (SELECT C_EOD_RESET_AT FROM T_EOD_PIPELINE WHERE C_BUSINESS_DATE=@p_d);
    IF EXISTS (SELECT 1 FROM T_EOD_RUN WHERE C_BUSINESS_DATE=@p_d AND C_JOB=@p_job AND C_STATUS='DONE'
                 AND C_ENDED_AT > ISNULL(@resetAt, '1900-01-01'))
        RETURN;
    EXEC SP_EOD_LOG @p_d,@p_job,'RUNNING';
    BEGIN TRY
        BEGIN TRAN;
        -- dynamic SQL: '@p_d' = tên param target proc (đã đổi); '@d' = biến trong batch động (khai báo N'@d DATE').
        -- @p_rows OUTPUT: MỌI job proc dispatch qua đây PHẢI có param @p_rows BIGINT=NULL OUTPUT (trả #dòng
        --   xử lý chính) → ghi vào T_EOD_RUN.C_ROWS. (Direct caller khác KHÔNG truyền → default NULL, không ảnh hưởng.)
        DECLARE @rows BIGINT;
        DECLARE @sql NVARCHAR(300) = N'EXEC ' + QUOTENAME(@p_proc) + N' @p_d=@d, @p_rows=@r OUTPUT';
        EXEC sp_executesql @sql, N'@d DATE, @r BIGINT OUTPUT', @d=@p_d, @r=@rows OUTPUT;
        COMMIT;
        EXEC SP_EOD_LOG @p_d,@p_job,'DONE', @rows;
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
CREATE OR ALTER PROCEDURE SP_EOD_RUN
    @p_business_date DATE,
    @p_err_code      INT           OUTPUT,   -- 0=OK, -1=lỗi runtime, -2=reconcile BREAK, 10=precondition chưa đủ
    @p_err_msg       NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    SET @p_err_code = 0; SET @p_err_msg = NULL;
    DECLARE @d DATE = @p_business_date;

    -- (KHÔNG còn guard "ngày giao dịch" bằng price-existence: ngày GD hiện tại mà giá EOD chưa về sẽ bị báo
    --   nhầm "không phải ngày GD". Data-driven: precondition MKT_DATA=READY bên dưới mới là cổng đúng — giá
    --   chưa về ⇒ MKT chưa READY; ngày nghỉ/tương lai ⇒ pipeline không tồn tại/không READY ⇒ cùng err=10.)

    -- PRECONDITION: BO market data READY + FO ingest(holdings) READY + [BRD] ASSET_NAV READY (cần NAV để derive)
    --   + master INDEX đã tính (J12 riêng SP_EOD_RUN_INDEX). J12B TE cần index daily_return.
    DECLARE @mkt VARCHAR(10), @fo VARCHAR(10), @idx VARCHAR(10), @anav VARCHAR(10);
    SELECT @mkt=C_MKT_DATA_STATUS, @fo=C_FO_INGEST_STATUS, @idx=C_INDEX_STATUS, @anav=C_ASSET_NAV_STATUS
    FROM T_EOD_PIPELINE WHERE C_BUSINESS_DATE=@d;
    IF @mkt IS NULL OR @mkt<>'READY' OR @fo<>'READY' OR @anav<>'READY' OR @idx<>'DONE'
    BEGIN
        SET @p_err_code = 10;
        SET @p_err_msg = CONCAT(N'Precondition chưa đủ: MKT_DATA=', ISNULL(@mkt,'(null)'),
                                N', FO_INGEST=', ISNULL(@fo,'(null)'), N', ASSET_NAV=', ISNULL(@anav,'(null)'),
                                N', INDEX=', ISNULL(@idx,'(null)'), N' (cần MKT/FO/ASSET_NAV=READY + INDEX=DONE).');
        RETURN;   -- KHÔNG chạy EOD
    END

    -- [BRD] COMPLETENESS ASSET_NAV per-SI: MỌI tiểu khoản ACTIVE phải có dòng T_SI_ASSET_DAILY @d (Asset gửi đủ).
    --   Thiếu → SI đó sẽ bị bỏ ngầm (không NAV/derive → PM gap, reconcile KHÔNG bắt vì nó duyệt asset_daily). Chặn.
    DECLARE @missSI INT = (SELECT COUNT(*) FROM T_SI_PORTFOLIO p WHERE p.C_STATUS='ACTIVE'
        AND NOT EXISTS (SELECT 1 FROM T_SI_ASSET_DAILY a WHERE a.C_SI_ACCOUNT=p.C_SI_ACCOUNT AND a.C_BUSINESS_DATE=@d));
    IF @missSI > 0
    BEGIN
        SET @p_err_code = 12;
        SET @p_err_msg = CONCAT(N'Thiếu Asset NAV cho ', @missSI, N' tiểu khoản ACTIVE @',
                                CONVERT(VARCHAR(10),@d,23), N' — Asset chưa gửi đủ per-SI, chặn EOD.');
        RETURN;
    END

    UPDATE T_EOD_PIPELINE SET C_EOD_STATUS='RUNNING', C_OVERALL_STATUS='EOD_RUNNING',
           C_UPDATED_AT=GETDATE() WHERE C_BUSINESS_DATE=@d;

    BEGIN TRY
        -- (INGEST: FO event per-KH đã vào qua SP_INGEST_CUSTOMER → cash/holdings/history sẵn trong current + interval)
        EXEC SP_EOD_STEP @d, 'J0_GATE',      'SP_EOD_GATE';            -- chờ đủ FO ingest (received=expected) — cổng vào
        EXEC SP_EOD_STEP @d, 'J07_COMPUTE',  'SP_EOD_COMPUTE';         -- MTM→NAV→PnL→Unit + roll-forward + perf per-KH
        EXEC SP_EOD_STEP @d, 'J11_SI_AGG',   'SP_EOD_SI_AGG';
        -- (J12 SI_INDEX ĐÃ TÁCH sang SP_EOD_RUN_INDEX — chạy khi BO ready, độc lập pipeline customer)
        EXEC SP_EOD_STEP @d, 'J12B_TE_ACCUM', 'SP_EOD_TE_ACCUM';           -- lũy kế active return per-KH (đọc index đã tính sẵn)
        EXEC SP_EOD_STEP @d, 'J13_RECONCILE','SP_EOD_RECONCILE';       -- recorder break (KHÔNG throw)

        -- CỔNG ĐỐI SOÁT: có break ⇒ CHẶN publish, KHÔNG chạy J14.
        DECLARE @nbreak INT = (SELECT COUNT(*) FROM T_EOD_RECON_BREAK WHERE C_BUSINESS_DATE=@d);
        IF @nbreak > 0
        BEGIN
            UPDATE T_EOD_PIPELINE SET C_RECONCILE_STATUS='BREAK', C_RECONCILE_AT=GETDATE(), C_BREAK_COUNT=@nbreak,
                   C_EOD_STATUS='FAILED', C_OVERALL_STATUS='RECONCILE_BREAK', C_UPDATED_AT=GETDATE(),
                   C_MESSAGE=CONCAT(N'Reconcile BREAK: ', @nbreak, N' dòng (xem T_EOD_RECON_BREAK)')
            WHERE C_BUSINESS_DATE=@d;
            SET @p_err_code = -2;
            SET @p_err_msg = CONCAT(N'RECONCILE BREAK: ', @nbreak, N' dòng lệch — chặn publish. Xem T_EOD_RECON_BREAK.');
            RETURN;   -- KHÔNG chạy J14/publish
        END
        UPDATE T_EOD_PIPELINE SET C_RECONCILE_STATUS='PASS', C_RECONCILE_AT=GETDATE(), C_BREAK_COUNT=0,
               C_UPDATED_AT=GETDATE() WHERE C_BUSINESS_DATE=@d;

        EXEC SP_EOD_STEP @d, 'J14_SNAPSHOT', 'SP_EOD_SNAPSHOT';   -- snapshot NỘI BỘ (T_MASTER_HOLDING_BALANCE) cho read API SDI

        -- EOD_DONE = trạng thái CUỐI (BRD 2026-06-22: SDI không push asset sang Asset → bỏ stage COMPLETED/asset-sync).
        UPDATE T_EOD_PIPELINE SET C_EOD_STATUS='DONE', C_EOD_AT=GETDATE(), C_OVERALL_STATUS='EOD_DONE',
               C_UPDATED_AT=GETDATE() WHERE C_BUSINESS_DATE=@d;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;   -- phòng hờ (SP_EOD_STEP thường đã rollback tran của nó)
        UPDATE T_EOD_PIPELINE SET C_EOD_STATUS='FAILED', C_OVERALL_STATUS='FAILED',
               C_UPDATED_AT=GETDATE(), C_MESSAGE=ERROR_MESSAGE() WHERE C_BUSINESS_DATE=@d;
        SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE();   -- trả lỗi về caller, KHÔNG THROW. Step FAILED đã log T_EOD_RUN.
    END CATCH
END
GO

/*===========================================================================
  SP_EOD_RUN_INDEX — LUỒNG RIÊNG tính master index (J12), trigger khi BO market data READY.
    ĐỘC LẬP pipeline customer: chỉ cần giá (BO) + target weight (config), KHÔNG cần FO/holdings KH.
    Tính + lưu T_MASTER_INDEX_DAILY → set C_INDEX_STATUS=DONE. App publish index sang Asset bằng
    SP_GET_ASSET_INDEX_SNAPSHOT ngay sau đó (1 luồng: BO ready → index → Asset).
    err: 0=OK, 10=MKT_DATA chưa READY, -1=runtime. KHÔNG THROW.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_RUN_INDEX
    @p_business_date DATE,
    @p_err_code      INT           OUTPUT,
    @p_err_msg       NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    SET @p_err_code = 0; SET @p_err_msg = NULL;
    DECLARE @d DATE = @p_business_date;

    DECLARE @mkt VARCHAR(10) = (SELECT C_MKT_DATA_STATUS FROM T_EOD_PIPELINE WHERE C_BUSINESS_DATE=@d);
    IF @mkt IS NULL OR @mkt<>'READY'
    BEGIN
        SET @p_err_code=10;
        SET @p_err_msg=CONCAT(N'MKT_DATA chưa READY (=', ISNULL(@mkt,'(null)'), N') — chưa tính được index.');
        RETURN;
    END

    -- COMPLETENESS GATE (chốt chặn index SAI): MỌI mã thành phần danh mục mẫu (rổ hiệu lực ≤ @d, master ACTIVE)
    --   PHẢI có giá @d trong T_PRICE_DAILY. Thiếu DÙ 1 mã → KHÔNG tính index (tránh factor lệch do nạp giá thiếu).
    DECLARE @missing NVARCHAR(400) = (
        SELECT STRING_AGG(req.C_TICKER, ',') WITHIN GROUP (ORDER BY req.C_TICKER)
        FROM (
            SELECT DISTINCT t.C_TICKER
            FROM T_MASTER_PORTFOLIO_TICKER t
            INNER JOIN T_MASTER_PORTFOLIO mp ON mp.C_MASTER_CODE=t.C_MASTER_CODE AND mp.C_STATUS='ACTIVE'
            WHERE t.C_EFFECTIVE_DATE = (SELECT MAX(C_EFFECTIVE_DATE) FROM T_MASTER_PORTFOLIO_TICKER t2
                                        WHERE t2.C_MASTER_CODE=t.C_MASTER_CODE AND t2.C_EFFECTIVE_DATE<=@d)
        ) req
        WHERE NOT EXISTS (SELECT 1 FROM T_PRICE_DAILY p WHERE p.C_TICKER=req.C_TICKER AND p.C_BUSINESS_DATE=@d));
    IF @missing IS NOT NULL
    BEGIN
        SET @p_err_code=11;
        SET @p_err_msg=CONCAT(N'Thiếu giá @', CONVERT(VARCHAR(10),@d,23), N' cho mã danh mục mẫu: ',
                              LEFT(@missing,300), N' — KHÔNG tính index (tránh sai).');
        RETURN;
    END

    BEGIN TRY
        EXEC SP_EOD_STEP @d, 'J12_SI_INDEX', 'SP_EOD_SI_INDEX';   -- tính + lưu T_MASTER_INDEX_DAILY (idempotent + log T_EOD_RUN)
        UPDATE T_EOD_PIPELINE SET C_INDEX_STATUS='DONE', C_INDEX_AT=GETDATE(), C_UPDATED_AT=GETDATE()
        WHERE C_BUSINESS_DATE=@d;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        SET @p_err_code=-1; SET @p_err_msg=ERROR_MESSAGE();
    END CATCH
END
GO

/*===========================================================================
  PIPELINE CONTROL — đánh dấu upstream ready / asset synced / reset re-run.
  Convention API (gọi bởi BO/FO/app/ops): @p_user + @p_err_code/@p_err_msg OUT, KHÔNG THROW.
===========================================================================*/

-- BO/FO báo nguồn đã sync xong cho ngày @p_business_date → set READY (tiền đề chạy EOD).
--   MKT_DATA (BO): chỉ cờ READY (SDI đã pull market data từ API BO 1 lần) — KHÔNG đếm record.
--   FO_INGEST (FO): completeness — @p_total_record = tổng cust_code FO gửi; SDI đếm received cust_code
--     distinct (watermark); READY khi received >= total (else err=4).
CREATE OR ALTER PROCEDURE SP_EOD_SET_SOURCE_READY
    @p_business_date DATE,
    @p_source        VARCHAR(20),               -- 'MKT_DATA' (BO) | 'FO_INGEST' (FO)
    @p_total_record  INT           = NULL,       -- BẮT BUỘC cho FO_INGEST (tổng cust_code); BỎ QUA cho MKT_DATA
    @p_user          VARCHAR(64)   = NULL,
    @p_err_code      INT           OUTPUT,
    @p_err_msg       NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON; SET @p_err_code=0; SET @p_err_msg=NULL;
    BEGIN TRY
    IF @p_source NOT IN ('MKT_DATA','FO_INGEST','ASSET_NAV')
        BEGIN SET @p_err_code=2; SET @p_err_msg=N'@p_source phải MKT_DATA | FO_INGEST | ASSET_NAV'; RAISERROR(@p_err_msg, 16, 1); END

    IF NOT EXISTS (SELECT 1 FROM T_EOD_PIPELINE WHERE C_BUSINESS_DATE=@p_business_date)
        INSERT INTO T_EOD_PIPELINE (C_BUSINESS_DATE, C_UPDATED_BY) VALUES (@p_business_date, @p_user);

    IF @p_source='MKT_DATA'
    BEGIN
        -- BO event ready → SDI đã pull market data 1 lần (giá/index/benchmark). Cờ READY, không đếm.
        UPDATE T_EOD_PIPELINE SET C_MKT_DATA_STATUS='READY', C_MKT_DATA_AT=GETDATE(),
               C_UPDATED_AT=GETDATE(), C_UPDATED_BY=@p_user WHERE C_BUSINESS_DATE=@p_business_date;
    END
    ELSE IF @p_source='ASSET_NAV'
    BEGIN
        -- [BRD] Asset đã gửi số tổng per-SI (SP_INGEST_ASSET_NAV vào T_SI_ASSET_DAILY). Cờ READY → đủ NAV để derive.
        UPDATE T_EOD_PIPELINE SET C_ASSET_NAV_STATUS='READY', C_ASSET_NAV_AT=GETDATE(),
               C_UPDATED_AT=GETDATE(), C_UPDATED_BY=@p_user WHERE C_BUSINESS_DATE=@p_business_date;
    END
    ELSE  -- FO_INGEST: completeness theo total cust_code
    BEGIN
        IF @p_total_record IS NULL
            BEGIN SET @p_err_code=2; SET @p_err_msg=N'FO_INGEST cần @p_total_record (tổng cust_code break event)'; RAISERROR(@p_err_msg, 16, 1); END
        DECLARE @received INT = (SELECT COUNT(DISTINCT C_CUST_CODE) FROM T_SI_NAV_CURRENT
                                 WHERE C_LAST_SYNC_DATE=@p_business_date);
        DECLARE @ok BIT = CASE WHEN @received >= @p_total_record THEN 1 ELSE 0 END;
        UPDATE T_EOD_PIPELINE SET C_FO_INGEST_TOTAL=@p_total_record, C_FO_INGEST_RECEIVED=@received,
               C_FO_INGEST_STATUS = CASE WHEN @ok=1 THEN 'READY' ELSE 'PENDING' END,
               C_FO_INGEST_AT     = CASE WHEN @ok=1 THEN GETDATE() ELSE C_FO_INGEST_AT END,
               C_UPDATED_AT=GETDATE(), C_UPDATED_BY=@p_user WHERE C_BUSINESS_DATE=@p_business_date;
        IF @ok=0
        BEGIN
            SET @p_err_code=4;   -- chưa đủ → KHÔNG READY (không THROW; tình huống nghiệp vụ)
            SET @p_err_msg=CONCAT(N'FO_INGEST: received ', @received, '/', @p_total_record, N' cust_code — CHƯA đủ.');
        END
    END

    -- cả 3 nguồn READY + chưa bắt đầu EOD ⇒ overall READY (đủ điều kiện chạy)
    UPDATE T_EOD_PIPELINE SET C_OVERALL_STATUS='READY'
    WHERE C_BUSINESS_DATE=@p_business_date AND C_MKT_DATA_STATUS='READY' AND C_FO_INGEST_STATUS='READY'
      AND C_ASSET_NAV_STATUS='READY' AND C_OVERALL_STATUS='WAITING_DATA';
    END TRY
    BEGIN CATCH
        IF @p_err_code=0 BEGIN SET @p_err_code=-1; SET @p_err_msg=ERROR_MESSAGE(); END
    END CATCH
END
GO

-- (ĐÃ GỠ SP_EOD_SET_ASSET_SYNCED — BRD 2026-06-22: SDI không còn push asset/perf snapshot sang Asset
--  nên không còn stage "asset synced". Trạng thái CUỐI pipeline = EOD_DONE (sau reconcile PASS). Xem SDI-asset-gap.md.)

-- RESET re-run: sau khi sửa nguồn (FO/BO), cho EOD tính LẠI từ đầu. GIỮ NGUYÊN T_EOD_RUN (log/audit) — chỉ
--   đặt watermark C_EOD_RESET_AT=GETDATE() để gate (SP_EOD_STEP) vô hiệu các DONE cũ → mọi job chạy lại.
--   GIỮ cờ nguồn (MKT_DATA/FO_INGEST) nếu nguồn vẫn ready; reset EOD/RECONCILE về PENDING.
CREATE OR ALTER PROCEDURE SP_EOD_RESET
    @p_business_date DATE,
    @p_user          VARCHAR(64)   = NULL,
    @p_err_code      INT           OUTPUT,
    @p_err_msg       NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON; SET @p_err_code=0; SET @p_err_msg=NULL;
    BEGIN TRY
    -- KHÔNG xoá T_EOD_RUN (giữ log/audit). Watermark vô hiệu DONE cũ → mọi job chạy lại (compute DELETE+INSERT
    --   theo @d: idempotent + sửa-số). Re-run sẽ UPDATE-in-place dòng log (timing/rows của lượt mới nhất).
    -- IDEMPOTENT re-run: KHÔNG cần un-roll T_SI_NAV_CURRENT. SP_EOD_COMPUTE seed anchor PnL/Unit từ
    --   T_SI_NAV_BALANCE @prev (KHÔNG từ NAV_CURRENT đã roll) → tính lại @d ra số Y HỆT dù NAV_CURRENT đã roll sang @d.
    --   ⚠️ Phạm vi: re-run NGÀY HIỆN TẠI (tài sản cash/pending/div trong NAV_CURRENT vẫn của @d). Recompute NGÀY
    --     QUÁ KHỨ từ đầu KHÔNG hỗ trợ đầy đủ (pending_cash/div_cash chưa có lịch sử interval) — lịch sử NAV/holdings
    --     đọc qua read API FR-02/03/06 (reconstruct từ NAV_BALANCE/hist, không tính lại → không lệch).
    DELETE FROM T_EOD_RECON_BREAK WHERE C_BUSINESS_DATE=@p_business_date;
    UPDATE T_EOD_PIPELINE
    SET C_EOD_STATUS='PENDING', C_EOD_AT=NULL, C_EOD_RESET_AT=GETDATE(),   -- watermark: vô hiệu DONE cũ trong T_EOD_RUN
        C_RECONCILE_STATUS='PENDING', C_RECONCILE_AT=NULL, C_BREAK_COUNT=0,
        C_OVERALL_STATUS = CASE WHEN C_MKT_DATA_STATUS='READY' AND C_FO_INGEST_STATUS='READY' THEN 'READY' ELSE 'WAITING_DATA' END,
        C_UPDATED_AT=GETDATE(), C_UPDATED_BY=@p_user, C_MESSAGE=N'RESET để chạy lại'
    WHERE C_BUSINESS_DATE=@p_business_date;
    END TRY
    BEGIN CATCH
        IF @p_err_code=0 BEGIN SET @p_err_code=-1; SET @p_err_msg=ERROR_MESSAGE(); END
    END CATCH
END
GO

/*===========================================================================
  SP_EOD_RECOMPUTE_RANGE — LUỒNG RERUN QUÁ KHỨ (tách riêng, KHÔNG đụng forward).
    Nghiệp vụ: KH báo sai NAV ngày quá khứ → sửa dữ liệu nguồn đã lưu (giá/holdings/cashflow…) rồi
    TÍNH LẠI chuỗi [from..to] mà KHÔNG cần re-feed BO/FO. Reconstruct AS-OF từ dữ liệu DATED đã lưu:
      holdings ← T_SI_HOLDING_HIST@d × giá T_PRICE_DAILY@d ; cash/pending/div ← T_SI_CASH_HIST@d (interval) ;
      anchor + payable base ← T_SI_NAV_BALANCE@prev ; cắt phí ← T_SI_INCOME_FEE(charge_date=@d) ; cashflow dated.
    Phạm vi (chọn 1): cả 2 NULL = TOÀN BỘ; @p_cust_code = theo KH; @p_si_account = 1 tiểu khoản.
      ⟹ recompute ở mức MASTER (mọi SI của các master bị ảnh hưởng) để master-agg đúng (master = Σ mọi SI).
    Mỗi ngày GD: seed as-of (all SI affected masters, active-as-of @d theo registry) → SP_EOD_COMPUTE_CORE
      (per-SI nav_balance/unit_ledger, SCOPED) → SP_EOD_SI_AGG (master nav_balance, SCOPED; current chỉ đổi nếu @d
      là ngày mới nhất) → SP_EOD_TE_ACCUM (TE). Roll-forward NAV_CURRENT (SI) chỉ khi @p_to_date = ngày mới nhất.
    Atomic: cả range trong 1 transaction (XACT_ABORT) → lỗi giữa chừng rollback sạch (chuỗi không nửa vời).
    CÓ recompute T_MASTER_HOLDING_BALANCE (composition PM) AS-OF từ HOLDING_HIST@d × giá@d (KHÔNG dùng J14 vì
      J14 đọc holdings hiện tại — sai cho ngày quá khứ). Scoped affected masters.
    ⚠️ Recompute đúng cho ngày TỪ lúc có history pending/div trở đi (ngày cũ hơn thiếu receivables history).
    err: 0 OK · 1 scope rỗng · 20 range không hợp lệ · -1 runtime.
===========================================================================*/
-- [BRD asset-sync] ĐÃ BỎ SP_EOD_RECOMPUTE_RANGE: SDI không tự tính NAV nên không reconstruct-from-history.
--   SỬA QUÁ KHỨ = RE-INGEST: Asset gửi lại T_SI_ASSET_DAILY ngày cũ (SP_INGEST_ASSET_NAV idempotent) →
--   chạy lại SP_EOD_COMPUTE/SI_AGG/TE_ACCUM ngày đó (derive lại unit/UP/lũy kế). Index vẫn SP_EOD_RECOMPUTE_INDEX_RANGE.

/*===========================================================================
  SP_EOD_RECOMPUTE_INDEX_RANGE — tính LẠI master index (danh mục mẫu) cho [from..to].
    Dùng khi: sửa công thức/giá/weight → chuỗi index cũ SAI (vd nổ cấp số nhân do weight %). Loop ngày GD
    THEO THỨ TỰ (mỗi ngày prev = ngày vừa tính lại → chuỗi đúng). SP_EOD_SI_INDEX idempotent (DELETE+INSERT @d).
    ⚠️ Để sửa chuỗi hỏng phải chạy TỪ INCEPTION (prev ngày đầu = base 1000); chạy từ giữa → prev vẫn số cũ.
    Index là MASTER-level (giá×weight), KHÔNG theo KH → luôn tính mọi master có weight+giá @d.
    err: 0 OK · 11 thiếu giá mã rổ (completeness) · 12 Σweight=0 (cấu hình rổ sai) · 20 range không hợp lệ · -1 runtime.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_RECOMPUTE_INDEX_RANGE
    @p_from_date DATE,
    @p_to_date   DATE          = NULL,
    @p_user      VARCHAR(64)   = NULL,
    @p_err_code  INT           OUTPUT,
    @p_err_msg   NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    SET @p_err_code=0; SET @p_err_msg=NULL;
    IF @p_to_date IS NULL SET @p_to_date = (SELECT MAX(C_BUSINESS_DATE) FROM T_PRICE_DAILY);
    IF @p_from_date IS NULL OR @p_to_date IS NULL OR @p_from_date > @p_to_date
        BEGIN SET @p_err_code=20; SET @p_err_msg=N'Khoảng ngày không hợp lệ.'; RETURN; END
    BEGIN TRY
        BEGIN TRAN;
        DECLARE @d DATE = @p_from_date;
        WHILE @d <= @p_to_date
        BEGIN
            IF dbo.UDF_HAS_PRICE_DATA(@d) = 1 EXEC SP_EOD_SI_INDEX @d;   -- idempotent, prev=ngày có giá trước (đã tính lại)
            SET @d = DATEADD(DAY, 1, @d);
        END
        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        -- map lỗi hard-fail từ SP_EOD_SI_INDEX: 51011 completeness→11, 51012 weight Σ=0→12; còn lại runtime -1.
        SET @p_err_code = CASE ERROR_NUMBER() WHEN 51011 THEN 11 WHEN 51012 THEN 12 ELSE -1 END;
        SET @p_err_msg = ERROR_MESSAGE();
    END CATCH
END
GO
