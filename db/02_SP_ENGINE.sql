SET QUOTED_IDENTIFIER ON;  -- procs ghi/đọc bảng có filtered index → cần QI ON lúc CREATE PROC
SET ANSI_NULLS ON;
GO
/*==============================================================================
  SDI MODULE — ENGINE CORE (SQL Server)  | ALL-IN-DB, set-based, no RBAR
  Naming: SP_ procs, UDF_ functions, T_/C_ tables/cols.
  Mô hình roll-forward: T_SI_CURRENT (current) + áp delta ngày @d → tính lại.
  INGEST: SP_INGEST_ASSET_NAV → Asset gửi NAV RÒNG + components (stock/cash) + cash_in/out per-SI/ngày;
    SP_INGEST_CUSTOMER → FO gửi holdings (per-mã, composition) + registry. SDI derive unit/UP/PnL/return.
  Thứ tự (master SP_EOD_RUN): J0_GATE → J07 → J11 → J12 → J13 → J14 → [J15_FEE_ACCRUE → J16_FEE_CLOSE nếu cài 09_FEE]
  NAV = Asset gửi trực tiếp (đã trừ phí QL); AUM = NAV (không tách payable).
  [BRD asset-sync] SDI KHÔNG còn accrue phí QL: phí do Asset tính & trừ sẵn trong NAV ròng. Thuế GD FO net.
==============================================================================*/
SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON;
GO

/*---------------------------------------------------- UDF: NGÀY GIAO DỊCH? (LỊCH — authority)
  1 = ngày sở có phiên; 0 = T7/CN hoặc ngày nghỉ khai trong T_TRADING_HOLIDAY (hoặc @d NULL).
  ⚠️ ĐÂY LÀ ĐỊNH NGHĨA DUY NHẤT của "ngày giao dịch" trong SDI. (Trước 2026-07-11 hàm này chỉ có trong 09_FEE
     và CHỈ subsystem phí dùng; engine thì suy ngày GD từ "T_PRICE_DAILY có dòng" ⇒ giá carry-forward ngày nghỉ
     biến T7/CN/lễ thành PHIÊN GIẢ và master index — chuỗi NHÂN DỒN — nhân thêm 1 factor mỗi ngày nghỉ ⇒ 1 cuối
     tuần sai +21%. Nay kéo lên CORE, mọi đường TÍNH đều gate bằng nó.)
  Rule cuối tuần: DATEDIFF(DAY,0,@d)%7 → 0=T2..5=T7,6=CN. KHÔNG dùng DATEPART(WEEKDAY) (phụ thuộc @@DATEFIRST).
  ⚠️ KHÔNG gate SP_INGEST_ASSET_NAV bằng hàm này: Asset gửi aum/tiền/daily_return MỌI ngày lịch. */
CREATE OR ALTER FUNCTION UDF_IS_BUSINESS_DATE (@d DATE)
RETURNS BIT AS
BEGIN
    RETURN CASE WHEN @d IS NULL THEN 0
                WHEN DATEDIFF(DAY,0,@d) % 7 >= 5 THEN 0
                WHEN EXISTS (SELECT 1 FROM T_TRADING_HOLIDAY WHERE C_HOLIDAY_DATE=@d) THEN 0
                ELSE 1 END;
END
GO

/*---------------------------------------------------- UDF: ngày GD KẾ tiếp (bỏ cuối tuần + nghỉ) */
CREATE OR ALTER FUNCTION UDF_NEXT_BUSINESS_DATE (@d DATE)
RETURNS DATE AS
BEGIN
    DECLARE @n DATE = DATEADD(DAY,1,@d);
    WHILE dbo.UDF_IS_BUSINESS_DATE(@n) = 0 SET @n = DATEADD(DAY,1,@n);
    RETURN @n;
END
GO

/*---------------------------------------------------- UDF: prev business date
  PHIÊN GD liền trước @d (theo LỊCH) đã có giá. Là anchor của mọi roll-forward (index prev, TE prev) ⇒ nếu trả
  về T7/CN/lễ thì chuỗi nhân dồn thêm 1 phiên giả. Điều kiện lịch INLINE (không gọi UDF_IS_BUSINESS_DATE) để
  tránh scalar-UDF chạy per-row khi quét ngày — rule PHẢI KHỚP UDF_IS_BUSINESS_DATE. */
CREATE OR ALTER FUNCTION UDF_PREV_BUSINESS_DATE (@d DATE)
RETURNS DATE
AS
BEGIN
    RETURN (SELECT MAX(x.d)
            FROM (SELECT DISTINCT C_BUSINESS_DATE AS d FROM T_PRICE_DAILY WHERE C_BUSINESS_DATE < @d) x
            WHERE DATEDIFF(DAY,0,x.d) % 7 < 5
              AND NOT EXISTS (SELECT 1 FROM T_TRADING_HOLIDAY h WHERE h.C_HOLIDAY_DATE = x.d));
END
GO

/*---------------------------------------------------- UDF: PHIÊN GD MỚI NHẤT đã có giá
  "Hôm nay" theo nghĩa dữ liệu thị trường. MAX(C_BUSINESS_DATE) trần trụi sẽ trỏ vào T7/CN nếu 1 dòng giá ngày
  nghỉ lọt vào T_PRICE_DAILY → định giá/serve theo "phiên" không tồn tại. Rule khớp UDF_IS_BUSINESS_DATE. */
CREATE OR ALTER FUNCTION UDF_LAST_BUSINESS_DATE ()
RETURNS DATE
AS
BEGIN
    RETURN (SELECT MAX(x.d)
            FROM (SELECT DISTINCT C_BUSINESS_DATE AS d FROM T_PRICE_DAILY) x
            WHERE DATEDIFF(DAY,0,x.d) % 7 < 5
              AND NOT EXISTS (SELECT 1 FROM T_TRADING_HOLIDAY h WHERE h.C_HOLIDAY_DATE = x.d));
END
GO

/*---------------------------------------------------- UDF: ngày ĐÃ CÓ GIÁ chưa (data-driven)
  1 nếu BO đã publish giá @d (T_PRICE_DAILY có dòng); 0 nếu chưa. ĐÂY KHÔNG PHẢI "ngày giao dịch theo lịch"
  (ngày GD mà giá chưa về cũng trả 0; ngày nghỉ có giá rác lọt vào lại trả 1) ⇒ MỌI loop rerun PHẢI kẹp
  UDF_IS_BUSINESS_DATE(@d)=1 AND UDF_HAS_PRICE_DATA(@d)=1: LỊCH quyết ngày nào ĐƯỢC tính, UDF này chỉ trả lời
  ngày đó ĐÃ CÓ data để tính chưa. */
CREATE OR ALTER FUNCTION UDF_HAS_PRICE_DATA (@d DATE)
RETURNS BIT
AS
BEGIN
    RETURN CASE WHEN @d IS NOT NULL AND EXISTS (SELECT 1 FROM T_PRICE_DAILY WHERE C_BUSINESS_DATE=@d)
                THEN 1 ELSE 0 END;
END
GO

/*===========================================================================
  SP_INGEST_TRADING_HOLIDAY — ops/BO nạp LỊCH NGHỈ. T7/CN KHÔNG cần khai (rule tự loại); bảng chỉ chứa ngày
    nghỉ TRONG TUẦN: lễ dương lịch, Tết/Giỗ Tổ (âm lịch), nghỉ bù.
    JSON: [{"holiday_date":"2026-02-17","note":"Tết Bính Ngọ","is_delete":0}, …]  is_delete=1 → gỡ khỏi lịch.
    VALIDATE all-or-nothing TRƯỚC khi ghi: ngày sai định dạng / TRÙNG trong batch / is_delete ∉{0,1} ⇒ TỪ CHỐI
    CẢ batch (không ghi dòng nào). err: 0 OK · 20 JSON sai · 21 validate FAIL · -1 runtime.
    ⚠️ Nạp lễ cho ngày QUÁ KHỨ đã tính index ⇒ số cũ VẪN SAI: chạy lại SP_EOD_RECOMPUTE_INDEX_RANGE từ inception.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_INGEST_TRADING_HOLIDAY
    @p_json     NVARCHAR(MAX),
    @p_user     VARCHAR(64)   = NULL,
    @p_err_code INT           OUTPUT,
    @p_err_msg  NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    SET @p_err_code = 0; SET @p_err_msg = NULL;
    BEGIN TRY
        IF @p_json IS NULL OR ISJSON(@p_json) <> 1
            BEGIN SET @p_err_code=20; SET @p_err_msg=N'@p_json không phải JSON hợp lệ'; RETURN; END

        -- Đọc holiday_date dạng CHUỖI rồi TRY_CONVERT (KHÔNG ép DATE trong OPENJSON WITH: ngày sai định dạng sẽ
        --   THROW conversion error khó hiểu → err=-1 thay vì báo rõ). rn>1 = TRÙNG ngày (MERGE sẽ vỡ PK).
        --   PARTITION theo ngày ĐÃ CONVERT: 2 chuỗi khác nhau vẫn có thể ra cùng 1 ngày.
        DECLARE @src TABLE (C_RAW NVARCHAR(30), C_HOLIDAY_DATE DATE, C_NOTE NVARCHAR(100), C_IS_DELETE TINYINT, rn INT);
        INSERT @src (C_RAW, C_HOLIDAY_DATE, C_NOTE, C_IS_DELETE, rn)
        SELECT j.holiday_date, TRY_CONVERT(DATE, j.holiday_date, 23), j.note, ISNULL(j.is_delete,0),
               ROW_NUMBER() OVER (PARTITION BY TRY_CONVERT(DATE, j.holiday_date, 23) ORDER BY (SELECT NULL))
        FROM OPENJSON(@p_json) WITH (
            holiday_date NVARCHAR(30)  '$.holiday_date',
            note         NVARCHAR(100) '$.note',
            is_delete    TINYINT       '$.is_delete') j;

        IF NOT EXISTS (SELECT 1 FROM @src)
            BEGIN SET @p_err_code=21; SET @p_err_msg=N'Batch rỗng (0 ngày) — từ chối'; RETURN; END

        DECLARE @bad NVARCHAR(300) = (
            SELECT TOP 1 CASE
                WHEN C_HOLIDAY_DATE IS NULL   THEN CONCAT(N'holiday_date NULL/sai định dạng (cần YYYY-MM-DD): ', ISNULL(C_RAW,N'(null)'))
                WHEN rn > 1                   THEN CONCAT(N'ngày TRÙNG trong batch: ', C_RAW)
                WHEN C_IS_DELETE NOT IN (0,1) THEN CONCAT(N'is_delete phải 0/1 @', C_RAW)
                END
            FROM @src WHERE C_HOLIDAY_DATE IS NULL OR rn > 1 OR C_IS_DELETE NOT IN (0,1));
        IF @bad IS NOT NULL
            BEGIN SET @p_err_code=21;
                SET @p_err_msg=CONCAT(N'Validate FAIL — batch BỊ TỪ CHỐI (không ghi dòng nào): ', @bad);
                RETURN; END

        DELETE h FROM T_TRADING_HOLIDAY h INNER JOIN @src s ON s.C_HOLIDAY_DATE=h.C_HOLIDAY_DATE WHERE s.C_IS_DELETE=1;

        MERGE T_TRADING_HOLIDAY AS t
        USING (SELECT C_HOLIDAY_DATE, C_NOTE FROM @src WHERE C_IS_DELETE=0) s
           ON t.C_HOLIDAY_DATE = s.C_HOLIDAY_DATE
        WHEN MATCHED THEN UPDATE SET t.C_NOTE = s.C_NOTE
        WHEN NOT MATCHED THEN INSERT (C_HOLIDAY_DATE, C_NOTE) VALUES (s.C_HOLIDAY_DATE, s.C_NOTE);
    END TRY
    BEGIN CATCH
        IF @p_err_code = 0 BEGIN SET @p_err_code = -1; SET @p_err_msg = ERROR_MESSAGE(); END
    END CATCH
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
        IF EXISTS (SELECT 1 FROM @sub n INNER JOIN T_SI_CURRENT s
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
        MERGE T_SI_CURRENT s
        USING @sub n ON s.C_SI_ACCOUNT=n.C_SI_ACCOUNT
        WHEN MATCHED THEN UPDATE SET s.C_LAST_SYNC_DATE=@d
        WHEN NOT MATCHED THEN INSERT (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_CASH,C_LAST_AUM,C_STATUS,C_LAST_SYNC_DATE)
            VALUES (n.C_SI_ACCOUNT,@cust,n.C_MASTER_CODE,0,0,'ACTIVE',@d);

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

-- PHÍ QL [BRD asset-sync] — SDI KHÔNG accrue nữa:
--   Phí QL do Asset tính & ĐÃ TRỪ trong NAV ròng gửi sang (Asset KHÔNG gửi số phí lũy kế riêng).
--   SDI không có cột payable/J06 accrue/SP_INGEST_FEE_CHARGE. AUM = NAV. Thuế GD: LUÔN FO net vào cash.
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
  SP_EOD_COMPUTE — [thin-layer] LƯU cuối ngày 1 phiên @p_d cho TỪNG tiểu khoản (si_account):
    seed AUM + daily_return (Asset gửi) → ghi nav_balance → roll-forward current. SDI KHÔNG tính/derive gì.

  THUẬT NGỮ:
    • AUM (Assets Under Management) = giá trị tài sản — Asset GỬI TRỰC TIẾP (= NAV ròng, đã trừ phí). SDI không tự tính.
    • daily_return = lợi suất NGÀY (TWR, Asset ĐÃ khử dòng tiền) — SDI nhận, KHÔNG tự tính.
    • CF (cashflow) = dòng tiền KH nạp (CF_IN) / rút (CF_OUT) — SDI tự nhập, dùng cho net flow + đối soát.
    • %PnL kỳ = ∏(1+daily_return) − 1 (compound ON-READ khi serve PM/FR).
    • TWR = Time-Weighted Return (khử dòng tiền) — do Asset tính, SDI chỉ compound lại.
    • roll-forward state = ghi đè current (T_SI_CURRENT) cuối phiên @p_d làm mốc phiên sau.
===========================================================================*/
/*===========================================================================
  [thin-layer] SP_EOD_COMPUTE @p_d — SDI KHÔNG còn tính gì. Ingest (SP_INGEST_ASSET_NAV) ĐÃ ghi
  T_SI_BALANCE (aum/daily_return/cash/cash_in/cash_out) + roll-forward T_SI_CURRENT. EOD chỉ SEED
  T_EOD_WORK từ balance @d làm SCOPE cho J11 agg + J12B TE + J13 reconcile. (ĐÃ GỠ SP_EOD_COMPUTE_CORE.)
  C_CF_IN/C_CF_OUT = cash_in/out Asset gửi (net flow master + đối soát vs cashflow SDI).
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_COMPUTE @p_d DATE, @p_rows BIGINT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DELETE FROM T_EOD_WORK WHERE C_BUSINESS_DATE=@p_d;
    -- scope = SI ACTIVE có dòng balance @d (Asset đã gửi). C_CF_IN/OUT ← cash_in/out Asset.
    INSERT INTO T_EOD_WORK (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_AUM,C_DAILY_RETURN,C_CASH,C_CF_IN,C_CF_OUT)
    SELECT @p_d, b.C_SI_ACCOUNT, b.C_CUST_CODE, b.C_MASTER_CODE, b.C_AUM, b.C_DAILY_RETURN, b.C_CASH, b.C_CASH_IN, b.C_CASH_OUT
    FROM T_SI_BALANCE b
    INNER JOIN T_SI_PORTFOLIO p ON p.C_SI_ACCOUNT=b.C_SI_ACCOUNT AND p.C_STATUS='ACTIVE'
    WHERE b.C_BUSINESS_DATE=@p_d;
    SET @p_rows = @@ROWCOUNT;   -- #tiểu khoản scope phiên này
END
GO

/*===========================================================================
  [thin-layer] SP_INGEST_ASSET_NAV — Asset gửi per-SI qua Kafka BATCH (≤100 item/msg) → GHI THẲNG
  T_SI_BALANCE (aum/daily_return/cash/cash_in/cash_out) + roll-forward T_SI_CURRENT. KHÔNG landing-table/compute.
  Idempotent (DELETE+INSERT theo date,si → re-ingest/correction = gửi lại).
  [BRD] Asset đẩy CẢ indexing acc KHÔNG thuộc SDI → BỎ QUA (chỉ nhận SI có trong registry T_SI_PORTFOLIO,
    acc lạ tự bị lọc qua INNER JOIN — KHÔNG reject batch). Validate NOT NULL aum/cash CHỈ cho SI thuộc SDI.
  JSON: [{"si_account","aum","daily_return"(NULL ngày đầu),"cash"(TỔNG tiền 1 số),"cash_in","cash_out"}, ...]
  err: 0 OK · 20 JSON sai · 21 thiếu aum/cash (SI thuộc SDI) · -1 runtime.
  ALL-OR-NOTHING: validate TRƯỚC (ngoài tran) + ghi trong BEGIN TRAN + XACT_ABORT ON → err=0 ⟺ ghi ĐỦ (SI thuộc SDI);
    err≠0 ⟺ ghi 0 (rollback). si_account TRÙNG trong batch → vỡ PK @src → từ chối cả batch (0).
  @p_rows OUTPUT = SỐ SI THUỘC SDI đã COMMIT (0 nếu lỗi); có thể < #item Asset gửi (acc lạ bị bỏ qua) → KHÔNG
    dùng để đối chiếu "gửi==ghi". ĐỦ/THIẾU dùng completeness gate: SP_EOD_SET_SOURCE_READY 'ASSET_NAV'
    @p_total_record=<tổng SI ACTIVE của SDI> → READY khi COUNT(DISTINCT si @date) ≥ total (+ EOD err=12 chặn nếu
    còn SI ACTIVE thiếu balance). Re-ingest idempotent nên gửi lại không phồng distinct-count.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_INGEST_ASSET_NAV
    @p_json          NVARCHAR(MAX),
    @p_business_date DATE,
    @p_user          VARCHAR(64)   = NULL,
    @p_err_code      INT           OUTPUT,
    @p_err_msg       NVARCHAR(400) OUTPUT,
    @p_rows          BIGINT        = NULL OUTPUT   -- #dòng SI đã ghi (commit). 0 nếu lỗi/rollback.
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    SET @p_err_code=0; SET @p_err_msg=NULL; SET @p_rows=0;
    BEGIN TRY
        -- ⚠️ KHÔNG gate lịch ở ĐÂY: Asset gửi aum/tiền/daily_return MỌI NGÀY LỊCH (kể cả T7/CN/lễ — nạp/rút cuối
        --   tuần vẫn đổi AUM). Chặn ngày nghỉ ở đây = mất trắng dòng tiền cuối tuần. Lịch chỉ gate TÍNH TOÁN
        --   (index/EOD/TE) và ingest GIÁ (SP_INGEST_PRICE_DAILY err=23).
        IF @p_business_date IS NULL BEGIN SET @p_err_code=20; SET @p_err_msg=N'@p_business_date NULL'; RETURN; END
        IF @p_json IS NULL OR ISJSON(@p_json)<>1 BEGIN SET @p_err_code=20; SET @p_err_msg=N'@p_json không hợp lệ'; RETURN; END

        DECLARE @src TABLE (C_SI_ACCOUNT VARCHAR(20) PRIMARY KEY, C_AUM DECIMAL(20,0), C_DAILY_RETURN DECIMAL(10,6),
                            C_CASH DECIMAL(20,0), C_CASH_IN DECIMAL(20,0), C_CASH_OUT DECIMAL(20,0));
        INSERT @src
        SELECT j.si_account, j.aum, j.daily_return, j.cash,
               ISNULL(j.cash_in,0), ISNULL(j.cash_out,0)
        FROM OPENJSON(@p_json) WITH (
            si_account VARCHAR(20) '$.si_account', aum DECIMAL(20,0) '$.aum', daily_return DECIMAL(10,6) '$.daily_return',
            cash DECIMAL(20,0) '$.cash',
            cash_in DECIMAL(20,0) '$.cash_in', cash_out DECIMAL(20,0) '$.cash_out') j;

        -- [BRD] Asset đẩy CẢ indexing acc KHÔNG có trên SDI → BỎ QUA (KHÔNG reject batch). SDI chỉ nhận SI
        --   có trong registry (T_SI_PORTFOLIO) qua INNER JOIN bên dưới; acc lạ tự bị lọc. "Đủ/thiếu" do
        --   completeness gate lo (SP_EOD_SET_SOURCE_READY 'ASSET_NAV' + EOD err=12), KHÔNG check tồn tại ở đây.
        -- Validate NOT NULL CHỈ trên SI ĐƯỢC NHẬN (thuộc SDI) — acc lạ không cần data sạch.
        IF EXISTS (SELECT 1 FROM @src s INNER JOIN T_SI_PORTFOLIO p ON p.C_SI_ACCOUNT=s.C_SI_ACCOUNT
                   WHERE s.C_AUM IS NULL OR s.C_CASH IS NULL)
            BEGIN SET @p_err_code=21; SET @p_err_msg=N'Thiếu aum/cash cho SI thuộc SDI'; RETURN; END

        BEGIN TRAN;
        -- [thin-layer] GHI THẲNG T_SI_BALANCE (history) — KHÔNG qua landing-table/compute. Idempotent (DELETE+INSERT
        --   theo date,si → re-ingest/correction = gửi lại). accum TE reset 0 (SP_EOD_TE_ACCUM set @EOD).
        DELETE FROM T_SI_BALANCE WHERE C_BUSINESS_DATE=@p_business_date
            AND C_SI_ACCOUNT IN (SELECT C_SI_ACCOUNT FROM @src);
        INSERT INTO T_SI_BALANCE (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,
            C_AUM,C_DAILY_RETURN,C_CASH,C_CASH_IN,C_CASH_OUT)
        SELECT @p_business_date, s.C_SI_ACCOUNT, p.C_CUST_CODE, p.C_MASTER_CODE,
               s.C_AUM, s.C_DAILY_RETURN, s.C_CASH, s.C_CASH_IN, s.C_CASH_OUT
        FROM @src s INNER JOIN T_SI_PORTFOLIO p ON p.C_SI_ACCOUNT=s.C_SI_ACCOUNT;   -- INNER JOIN lọc acc lạ (Asset đẩy dư)
        SET @p_rows = @@ROWCOUNT;   -- #SI THUỘC SDI đã ghi (≤ #item Asset gửi, vì acc lạ bị bỏ qua)

        -- roll-forward T_SI_CURRENT (aum+cash). CHỈ khi @ngày >= ngày current hiện có (re-ingest quá khứ KHÔNG lùi current).
        MERGE T_SI_CURRENT t
        USING (SELECT s.C_SI_ACCOUNT, p.C_CUST_CODE, p.C_MASTER_CODE, s.C_AUM, s.C_CASH
               FROM @src s INNER JOIN T_SI_PORTFOLIO p ON p.C_SI_ACCOUNT=s.C_SI_ACCOUNT) w
        ON t.C_SI_ACCOUNT=w.C_SI_ACCOUNT
        WHEN MATCHED AND @p_business_date >= ISNULL(t.C_LAST_BUSINESS_DATE,'1900-01-01') THEN UPDATE SET
            t.C_CASH=w.C_CASH, t.C_LAST_AUM=w.C_AUM, t.C_LAST_BUSINESS_DATE=@p_business_date
        WHEN NOT MATCHED THEN INSERT (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_CASH,C_LAST_AUM,C_STATUS,C_LAST_BUSINESS_DATE)
            VALUES (w.C_SI_ACCOUNT,w.C_CUST_CODE,w.C_MASTER_CODE,w.C_CASH,w.C_AUM,'ACTIVE',@p_business_date);
        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        SET @p_rows=0;   -- rollback → 0 dòng commit (all-or-nothing)
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
        -- CALENDAR GUARD (err=23): TỪ CHỐI GIÁ ngày KHÔNG GD (T7/CN/lễ). Nguồn giá thường carry-forward phiên
        --   gần nhất cho ngày nghỉ → nhận vào là biến ngày nghỉ thành "phiên giả" trong T_PRICE_DAILY, và master
        --   index (chuỗi nhân dồn) nhân thêm 1 factor. Chặn tại cửa ngõ = data sạch. (Engine vẫn miễn nhiễm nếu
        --   ai đó INSERT thẳng: mọi đường TÍNH đều gate lịch.)
        --   ⚠️ KHÁC SP_INGEST_ASSET_NAV: Asset gửi aum/tiền MỌI ngày lịch → KHÔNG gate. GIÁ thì chỉ có ngày GD.
        IF dbo.UDF_IS_BUSINESS_DATE(@p_business_date) = 0
            BEGIN SET @p_err_code=23;
                SET @p_err_msg=CONCAT(N'Ngày ', CONVERT(VARCHAR(10),@p_business_date,23),
                    N' KHÔNG phải ngày giao dịch (T7/CN hoặc nghỉ lễ trong T_TRADING_HOLIDAY) — TỪ CHỐI batch giá. ',
                    N'Nạp giá ngày nghỉ sẽ làm master index nhân dồn sai. Nếu đây LÀ ngày GD, kiểm tra T_TRADING_HOLIDAY.');
                RETURN; END
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
  J11 — SI AGGREGATE → T_MASTER_BALANCE (composition + NAV + hiệu suất + cổ tức/phí)
         + upsert T_MASTER_CURRENT (snapshot current cấp SI cho serving)
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_SI_AGG @p_d DATE, @p_rows BIGINT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @prev DATE = dbo.UDF_PREV_BUSINESS_DATE(@p_d);
    -- DELETE SCOPED theo master có trong T_EOD_WORK @p_d (forward: all masters = xoá hết @p_d; rerun: chỉ master
    --   bị ảnh hưởng → KHÔNG đụng master khác). Behavior forward giữ nguyên.
    DELETE m FROM T_MASTER_BALANCE m
        WHERE m.C_BUSINESS_DATE=@p_d
          AND EXISTS (SELECT 1 FROM T_EOD_WORK w WHERE w.C_MASTER_CODE=m.C_MASTER_CODE AND w.C_BUSINESS_DATE=@p_d);

    ;WITH agg AS (
        SELECT C_MASTER_CODE,
               SUM(C_CASH) AS CASH,
               SUM(C_AUM) AS AUM,
               SUM(CASE WHEN C_DAILY_RETURN IS NOT NULL THEN C_AUM * C_DAILY_RETURN END) AS WRNUM,  -- Σ AUMᵢ·rᵢ
               SUM(CASE WHEN C_DAILY_RETURN IS NOT NULL THEN C_AUM END)                AS WRDEN,  -- Σ AUMᵢ (có rᵢ)
               SUM(C_CF_IN) AS CFIN, SUM(C_CF_OUT) AS CFOUT, COUNT(*) AS ACCT   -- [PM] flow + #tiểu khoản ACTIVE
        FROM T_EOD_WORK WHERE C_BUSINESS_DATE=@p_d GROUP BY C_MASTER_CODE
    )
    INSERT INTO T_MASTER_BALANCE (C_BUSINESS_DATE,C_MASTER_CODE,C_CASH,
                             C_AUM,C_DAILY_RETURN,C_CASH_IN,C_CASH_OUT,C_TOTAL_ACCOUNT)
    SELECT @p_d, a.C_MASTER_CODE, a.CASH,
           a.AUM,   -- [thin-layer] C_AUM = Σ AUM tiểu khoản
           CAST(CASE WHEN a.WRDEN>0 THEN a.WRNUM/a.WRDEN END AS DECIMAL(10,6)),  -- master daily return = AUM-weighted Σ(AUMᵢ·rᵢ)/ΣAUMᵢ (DM tổng KH)
           a.CFIN, a.CFOUT, a.ACCT
    FROM agg a;
    SET @p_rows = @@ROWCOUNT;   -- #master aggregate (bắt NGAY sau INSERT, trước MERGE current bên dưới)

    -- current cấp master (overwrite) — CHỈ khi @p_d là ngày MỚI NHẤT (forward). Rerun ngày QUÁ KHỨ → KHÔNG đụng
    --   current (current phải = hôm nay). Forward luôn tính ngày mới nhất ⇒ chạy như cũ.
    IF @p_d = (SELECT MAX(C_BUSINESS_DATE) FROM T_MASTER_BALANCE)
    MERGE T_MASTER_CURRENT AS t
    USING (SELECT C_MASTER_CODE,C_CASH,C_AUM,C_TOTAL_ACCOUNT,C_BUSINESS_DATE
           FROM T_MASTER_BALANCE WHERE C_BUSINESS_DATE=@p_d) s
    ON t.C_MASTER_CODE=s.C_MASTER_CODE
    WHEN MATCHED THEN UPDATE SET
        t.C_CASH=s.C_CASH, t.C_AUM=s.C_AUM, t.C_TOTAL_ACCOUNT=s.C_TOTAL_ACCOUNT,
        t.C_LAST_BUSINESS_DATE=s.C_BUSINESS_DATE
    WHEN NOT MATCHED THEN INSERT (C_MASTER_CODE,C_CASH,C_AUM,C_TOTAL_ACCOUNT,C_LAST_BUSINESS_DATE)
        VALUES (s.C_MASTER_CODE,s.C_CASH,s.C_AUM,s.C_TOTAL_ACCOUNT,s.C_BUSINESS_DATE);
END
GO

/*===========================================================================
  J12 — SI INDEX (danh mục mẫu, 100% cổ phiếu) → T_MASTER_INDEX_DAILY
        FACTOR   = Σ w^(t) × P_t / P_ref ; w^(t)=eff_date≤@d mới nhất
        [Cách A] Index_raw_t = Index_raw_(t-1) × FACTOR (chain PRECISION CAO — chống trôi)
                 Index_publish = ROUND(Index_raw_t, 2)                        -- con số 2dp user thấy
                 daily_return  = Index_publish_t / Index_publish_(t-1) − 1     -- TỪ 2dp → user suy ra KHỚP (hết lệch)
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_SI_INDEX @p_d DATE, @p_rows BIGINT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @prev DATE = dbo.UDF_PREV_BUSINESS_DATE(@p_d);

    -- CALENDAR HARD-FAIL (TRƯỚC mọi DML; phủ MỌI đường gọi: forward/direct/recompute). Index là chuỗi NHÂN DỒN
    --   (Index_t = Index_(t-1) × FACTOR) ⇒ tính cho 1 ngày KHÔNG GD = nhân thêm factor của PHIÊN GIẢ (nguồn giá
    --   carry-forward ngày nghỉ ⇒ FACTOR lặp lại lợi suất phiên trước) → index phồng theo cấp số nhân (chuỗi
    --   +10%/phiên: 1 cuối tuần đẩy 1210 → 1464.10, +21%). THROW ⇒ dù có dòng giá ngày nghỉ LỌT vào
    --   T_PRICE_DAILY, KHÔNG bao giờ sinh được index row ngoài lịch.
    IF dbo.UDF_IS_BUSINESS_DATE(@p_d) = 0
    BEGIN
        DECLARE @cmsg NVARCHAR(2000) = CONCAT(N'INDEX @', CONVERT(VARCHAR(10),@p_d,23),
            N': KHÔNG phải ngày giao dịch (T7/CN hoặc nghỉ lễ trong T_TRADING_HOLIDAY) — KHÔNG tính index ',
            N'(tính ngày nghỉ = nhân dồn factor phiên giả ⇒ index sai cấp số nhân).');
        THROW 51013, @cmsg, 1;
    END

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
    -- [Cách A] CHAIN ở raw precision cao (COALESCE prev.raw, nếu legacy thiếu raw thì fallback prev 2dp, else 1000)
    --   → publish = ROUND(raw,2). daily_return = publish_t / publish_(t-1) − 1 (TỪ 2dp đã publish, FLOAT tránh crush scale).
    INSERT INTO T_MASTER_INDEX_DAILY (C_BUSINESS_DATE,C_MASTER_CODE,C_INDEX_VALUE_RAW,C_INDEX_VALUE,C_DAILY_RETURN)
    SELECT @p_d, f.C_MASTER_CODE,
           r.rawval,
           v.pubval,
           CAST(CAST(v.pubval AS FLOAT) / COALESCE(pi.C_INDEX_VALUE, 1000) - 1 AS DECIMAL(10,6))
    FROM FACT f
    LEFT JOIN T_MASTER_INDEX_DAILY pi ON pi.C_MASTER_CODE=f.C_MASTER_CODE AND pi.C_BUSINESS_DATE=@prev
    CROSS APPLY (SELECT rawval = CAST(COALESCE(pi.C_INDEX_VALUE_RAW, pi.C_INDEX_VALUE, 1000) * f.FACTOR AS DECIMAL(28,12))) r
    CROSS APPLY (SELECT pubval = CAST(ROUND(r.rawval, 2) AS DECIMAL(18,2))) v;
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
    FROM T_SI_BALANCE b
    INNER JOIN T_MASTER_INDEX_DAILY idx ON idx.C_MASTER_CODE=b.C_MASTER_CODE AND idx.C_BUSINESS_DATE=@p_d
    LEFT JOIN T_SI_BALANCE p ON p.C_SI_ACCOUNT=b.C_SI_ACCOUNT AND p.C_BUSINESS_DATE=@prev
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
        T_MASTER_BALANCE) — đó là lý do mỗi step commit riêng.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_RECONCILE @p_d DATE, @p_rows BIGINT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DELETE FROM T_EOD_RECON_BREAK WHERE C_BUSINESS_DATE=@p_d;   -- idempotent (re-run)

    -- Check 1: AUM âm (per-KH) → ghi từng dòng
    INSERT INTO T_EOD_RECON_BREAK (C_BUSINESS_DATE,C_CHECK_NAME,C_MASTER_CODE,C_SI_ACCOUNT,C_VALUE_SDI,C_MESSAGE)
    SELECT @p_d, 'NAV_NEGATIVE', C_MASTER_CODE, C_SI_ACCOUNT, C_AUM,
           CONCAT(N'AUM=', C_AUM, N' bất thường (<0)')
    FROM T_EOD_WORK
    WHERE C_BUSINESS_DATE=@p_d AND C_AUM < 0;

    -- Check 2: NAV master != Σ NAV khách (chênh > 1 VND) → sanity agg (master = Σ SI, cùng nguồn ingest)
    INSERT INTO T_EOD_RECON_BREAK (C_BUSINESS_DATE,C_CHECK_NAME,C_MASTER_CODE,C_VALUE_SDI,C_VALUE_CHECK,C_DIFF,C_MESSAGE)
    SELECT @p_d, 'SI_NAV_MISMATCH', p.C_MASTER_CODE, p.C_AUM, a.NAV, p.C_AUM - a.NAV,
           N'AUM master != Σ NAV khách (agg sai?)'
    FROM T_MASTER_BALANCE p
    INNER JOIN (SELECT C_MASTER_CODE, SUM(C_AUM) NAV FROM T_EOD_WORK WHERE C_BUSINESS_DATE=@p_d GROUP BY C_MASTER_CODE) a
      ON a.C_MASTER_CODE=p.C_MASTER_CODE
    WHERE p.C_BUSINESS_DATE=@p_d AND ABS(p.C_AUM - a.NAV) > 1;

    -- [BRD] Check 3 CASHFLOW_MISMATCH (đo vênh 2 nguồn): cashflow SDI tự nhập vs cash_in/out Asset gửi.
    --   Lệch (số nguyên VND, exact) → 2 nguồn net flow KHÔNG khớp (cảnh báo đối soát).
    ;WITH sdicf AS (
        SELECT C_SI_ACCOUNT,
               SUM(CASE WHEN C_EVENT_TYPE='WITHDRAW' THEN -C_AMOUNT ELSE C_AMOUNT END) AS NET
        FROM T_SI_CASHFLOW_EVENT WHERE C_BUSINESS_DATE=@p_d GROUP BY C_SI_ACCOUNT)
    INSERT INTO T_EOD_RECON_BREAK (C_BUSINESS_DATE,C_CHECK_NAME,C_MASTER_CODE,C_SI_ACCOUNT,C_VALUE_SDI,C_VALUE_CHECK,C_DIFF,C_MESSAGE)
    SELECT @p_d, 'CASHFLOW_MISMATCH', a.C_MASTER_CODE, a.C_SI_ACCOUNT,
           ISNULL(s.NET,0), (a.C_CASH_IN - a.C_CASH_OUT), ISNULL(s.NET,0) - (a.C_CASH_IN - a.C_CASH_OUT),
           N'Cashflow SDI != Asset cash_in/out'
    FROM T_SI_BALANCE a
    LEFT JOIN sdicf s ON s.C_SI_ACCOUNT=a.C_SI_ACCOUNT
    WHERE a.C_BUSINESS_DATE=@p_d AND ISNULL(s.NET,0) <> (a.C_CASH_IN - a.C_CASH_OUT);

    -- [thin-layer] ĐÃ GỠ check 4 HOLDINGS_MISMATCH + check 5 NAV_CONSISTENCY: Asset KHÔNG gửi stock_value nữa
    --   (chỉ AUM + daily_return + cash) ⇒ SDI không có components để đối soát holdings / nav-vs-components.

    -- #dòng break (3 check gộp); 0 = sạch. Tổng cuối (khỏi phụ thuộc @@ROWCOUNT riêng check cuối).
    SET @p_rows = (SELECT COUNT(*) FROM T_EOD_RECON_BREAK WHERE C_BUSINESS_DATE=@p_d);
END
GO

/*===========================================================================
  J14 — SNAPSHOT: T_MASTER_HOLDING_BALANCE (SI aggregate holdings + tỷ trọng)
         (composition tài sản + NAV đã gộp về T_MASTER_BALANCE ở J11)
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
    -- (composition tài sản + NAV cấp SI: đã ghi T_MASTER_BALANCE ở J11_SI_AGG)
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
    DECLARE @received INT = (SELECT COUNT(*) FROM T_SI_CURRENT
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

    -- CALENDAR GATE (err=13): ngày KHÔNG GD (T7/CN/lễ) ⇒ KHÔNG có EOD. Guard theo LỊCH, KHÔNG theo
    --   price-existence (guard cũ bỏ vì báo nhầm khi giá EOD chưa về — lịch không có nhược điểm đó: ngày GD mà
    --   giá chưa về vẫn qua gate này rồi dừng ở precondition MKT_DATA=READY, err=10 như cũ).
    --   ⚠️ Asset VẪN ingest AUM/tiền ngày nghỉ (SP_INGEST_ASSET_NAV không bị gate) — chỉ EOD/index không chạy.
    IF dbo.UDF_IS_BUSINESS_DATE(@d) = 0
    BEGIN
        SET @p_err_code = 13;
        SET @p_err_msg = CONCAT(N'Ngày ', CONVERT(VARCHAR(10),@d,23),
            N' KHÔNG phải ngày giao dịch (T7/CN hoặc nghỉ lễ trong T_TRADING_HOLIDAY) — KHÔNG chạy EOD.');
        RETURN;
    END

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

    -- [thin-layer] COMPLETENESS ASSET_NAV per-SI: MỌI tiểu khoản ACTIVE phải có dòng T_SI_BALANCE @d (Asset gửi đủ).
    --   Thiếu → SI đó bị bỏ ngầm (PM gap). Chặn EOD (err=12).
    DECLARE @missSI INT = (SELECT COUNT(*) FROM T_SI_PORTFOLIO p WHERE p.C_STATUS='ACTIVE'
        AND NOT EXISTS (SELECT 1 FROM T_SI_BALANCE a WHERE a.C_SI_ACCOUNT=p.C_SI_ACCOUNT AND a.C_BUSINESS_DATE=@d));
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

        -- [BRD fee] CHỐT PHÍ QL trong EOD (sau reconcile PASS). Asset đã nhận ĐỦ dữ liệu (precondition
        --   ASSET_NAV=READY + completeness per-SI ở trên) → tính phí ngày look-forward (J15) + chốt kỳ (J16,
        --   no-op nếu chưa ngày GD cuối tháng). KHÔNG xử lý trong luồng đồng bộ Asset (eager) nữa — chạy ở đây
        --   để chỉ chốt trên data đã đối soát sạch. Module 09_FEE PLUGGABLE: guard OBJECT_ID → chưa cài 09 thì
        --   bỏ qua (03_SMOKE/04_BENCH không load 09 vẫn chạy). Cả 2 job STEP-compatible + idempotent (MERGE).
        IF OBJECT_ID('dbo.SP_EOD_FEE_ACCRUE','P') IS NOT NULL
        BEGIN
            EXEC SP_EOD_STEP @d, 'J15_FEE_ACCRUE', 'SP_EOD_FEE_ACCRUE';
            EXEC SP_EOD_STEP @d, 'J16_FEE_CLOSE',  'SP_FEE_CLOSE_PERIOD';
        END

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

    -- CALENDAR GATE (err=13) — báo sớm/sạch trước khi tới THROW 51013 trong SP_EOD_SI_INDEX.
    IF dbo.UDF_IS_BUSINESS_DATE(@d) = 0
    BEGIN
        SET @p_err_code=13;
        SET @p_err_msg=CONCAT(N'Ngày ', CONVERT(VARCHAR(10),@d,23),
            N' KHÔNG phải ngày giao dịch (T7/CN hoặc nghỉ lễ trong T_TRADING_HOLIDAY) — KHÔNG tính index ',
            N'(index nhân dồn: tính ngày nghỉ ⇒ sai cấp số nhân).');
        RETURN;
    END

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
    @p_source        VARCHAR(20),               -- 'MKT_DATA' (BO) | 'FO_INGEST' (FO) | 'ASSET_NAV' (Asset)
    @p_total_record  INT           = NULL,       -- BẮT BUỘC cho FO_INGEST (tổng cust_code) + ASSET_NAV (tổng SI); BỎ QUA cho MKT_DATA
    @p_user          VARCHAR(64)   = NULL,
    @p_err_code      INT           OUTPUT,
    @p_err_msg       NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON; SET @p_err_code=0; SET @p_err_msg=NULL;
    BEGIN TRY
    IF @p_source NOT IN ('MKT_DATA','FO_INGEST','ASSET_NAV')
        BEGIN SET @p_err_code=2; SET @p_err_msg=N'@p_source phải MKT_DATA | FO_INGEST | ASSET_NAV'; RAISERROR(@p_err_msg, 16, 1); END

    -- CALENDAR GATE (err=13): KHÔNG mở pipeline EOD cho ngày không GD → không tạo dòng T_EOD_PIPELINE rác cho
    --   T7/CN/lễ. ⚠️ APP: ngày nghỉ VẪN ingest Asset (aum/tiền) bình thường, nhưng KHÔNG gọi proc này và KHÔNG
    --   chạy EOD — gọi vào sẽ nhận err=13 (fail loud, không im lặng).
    IF dbo.UDF_IS_BUSINESS_DATE(@p_business_date) = 0
    BEGIN
        SET @p_err_code=13;
        SET @p_err_msg=CONCAT(N'Ngày ', CONVERT(VARCHAR(10),@p_business_date,23),
            N' KHÔNG phải ngày giao dịch (T7/CN hoặc nghỉ lễ trong T_TRADING_HOLIDAY) — KHÔNG mở pipeline EOD.');
        RETURN;
    END

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
        -- [thin-layer] Asset gửi per-SI qua Kafka BATCH (≤100 item/msg) → SP_INGEST_ASSET_NAV ghi thẳng T_SI_BALANCE.
        --   Completeness: @p_total_record = tổng SI Asset khai; RECEIVED = #SI distinct @ngày; READY khi đủ (else err=4).
        IF @p_total_record IS NULL
            BEGIN SET @p_err_code=2; SET @p_err_msg=N'ASSET_NAV cần @p_total_record (tổng SI Asset gửi)'; RAISERROR(@p_err_msg, 16, 1); END
        DECLARE @anavRecv INT = (SELECT COUNT(DISTINCT C_SI_ACCOUNT) FROM T_SI_BALANCE
                                 WHERE C_BUSINESS_DATE=@p_business_date);
        DECLARE @anavOk BIT = CASE WHEN @anavRecv >= @p_total_record THEN 1 ELSE 0 END;
        UPDATE T_EOD_PIPELINE SET C_ASSET_NAV_TOTAL=@p_total_record, C_ASSET_NAV_RECEIVED=@anavRecv,
               C_ASSET_NAV_STATUS = CASE WHEN @anavOk=1 THEN 'READY' ELSE 'PENDING' END,
               C_ASSET_NAV_AT     = CASE WHEN @anavOk=1 THEN GETDATE() ELSE C_ASSET_NAV_AT END,
               C_UPDATED_AT=GETDATE(), C_UPDATED_BY=@p_user WHERE C_BUSINESS_DATE=@p_business_date;
        IF @anavOk=0
        BEGIN
            SET @p_err_code=4;   -- chưa đủ batch → KHÔNG READY (không THROW; tình huống nghiệp vụ)
            SET @p_err_msg=CONCAT(N'ASSET_NAV: received ', @anavRecv, '/', @p_total_record, N' SI — CHƯA đủ batch.');
        END
    END
    ELSE  -- FO_INGEST: completeness theo total cust_code
    BEGIN
        IF @p_total_record IS NULL
            BEGIN SET @p_err_code=2; SET @p_err_msg=N'FO_INGEST cần @p_total_record (tổng cust_code break event)'; RAISERROR(@p_err_msg, 16, 1); END
        DECLARE @received INT = (SELECT COUNT(DISTINCT C_CUST_CODE) FROM T_SI_CURRENT
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
    --   [BRD fee] KHÔNG chốt phí ở đây nữa (eager trong luồng sync). Phí QL chốt trong EOD (SP_EOD_RUN
    --   J15/J16) — chạy SAU khi Asset nhận đủ data (ASSET_NAV=READY) + reconcile PASS, trên data đã đối soát.
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
    -- IDEMPOTENT re-run: KHÔNG cần un-roll T_SI_CURRENT. SP_EOD_COMPUTE seed anchor PnL/Unit từ
    --   T_SI_BALANCE @prev (KHÔNG từ NAV_CURRENT đã roll) → tính lại @d ra số Y HỆT dù NAV_CURRENT đã roll sang @d.
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

-- [BRD asset-sync] ĐÃ BỎ SP_EOD_RECOMPUTE_RANGE: SDI không tự tính NAV nên không reconstruct-from-history.
--   SỬA QUÁ KHỨ = RE-INGEST: Asset gửi lại per-SI ngày cũ (SP_INGEST_ASSET_NAV ghi thẳng T_SI_BALANCE, idempotent) →
--   chạy lại SP_EOD_COMPUTE/SI_AGG/TE_ACCUM ngày đó (derive lại unit/UP/lũy kế). Index vẫn SP_EOD_RECOMPUTE_INDEX_RANGE.

/*===========================================================================
  SP_EOD_RECOMPUTE_INDEX_RANGE — tính LẠI master index (danh mục mẫu) cho [from..to].
    Dùng khi: sửa công thức/giá/weight/LỊCH NGHỈ → chuỗi index cũ SAI (nổ cấp số nhân do weight %, hoặc do CỘNG
    DỒN NGÀY NGHỈ). Nhận DẢI NGÀY DƯƠNG LỊCH và TỰ LỌC ngày GD (UDF_IS_BUSINESS_DATE), chạy THEO THỨ TỰ (mỗi
    ngày prev = phiên vừa tính lại → chuỗi đúng). SP_EOD_SI_INDEX idempotent (DELETE+INSERT @d).
    ⚠️ Để sửa chuỗi hỏng phải chạy TỪ INCEPTION (prev ngày đầu = base 1000); chạy từ giữa → prev vẫn số cũ.
    ⚠️ KHÔNG xoá index rác đã ghi ở ngày nghỉ (từ trước khi có lịch): chỉ không tạo thêm. Dọn = xoá
       T_MASTER_INDEX_DAILY ở ngày UDF_IS_BUSINESS_DATE=0 rồi chạy lại từ inception.
    Index là MASTER-level (giá×weight), KHÔNG theo KH → luôn tính mọi master có weight+giá @d.
    err: 0 OK · 11 thiếu giá mã rổ (completeness) · 12 Σweight=0 (cấu hình rổ sai) · 13 ngày không GD
         · 20 range không hợp lệ · -1 runtime.
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
    IF @p_to_date IS NULL SET @p_to_date = dbo.UDF_LAST_BUSINESS_DATE();   -- phiên GD mới nhất (bỏ dòng giá rác ngày nghỉ)
    IF @p_from_date IS NULL OR @p_to_date IS NULL OR @p_from_date > @p_to_date
        BEGIN SET @p_err_code=20; SET @p_err_msg=N'Khoảng ngày không hợp lệ.'; RETURN; END
    BEGIN TRY
        BEGIN TRAN;
        DECLARE @d DATE = @p_from_date;
        WHILE @d <= @p_to_date
        BEGIN
            -- LỊCH trước, DATA sau: ngày nghỉ BỊ BỎ QUA dù T_PRICE_DAILY có dòng → loop nhận DẢI NGÀY DƯƠNG LỊCH
            --   vẫn ra chuỗi index đúng (chỉ nhân factor các phiên thật).
            IF dbo.UDF_IS_BUSINESS_DATE(@d) = 1 AND dbo.UDF_HAS_PRICE_DATA(@d) = 1
                EXEC SP_EOD_SI_INDEX @d;   -- idempotent, prev = phiên GD có giá trước đó (đã tính lại)
            SET @d = DATEADD(DAY, 1, @d);
        END
        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        -- map lỗi hard-fail từ SP_EOD_SI_INDEX: 51011 completeness→11, 51012 weight Σ=0→12; còn lại runtime -1.
        -- 51011 completeness→11, 51012 weight Σ=0→12, 51013 ngày không GD→13 (loop đã skip ngày nghỉ nên chỉ
        --   xảy ra nếu lịch bị sửa giữa chừng); còn lại runtime -1.
        SET @p_err_code = CASE ERROR_NUMBER() WHEN 51011 THEN 11 WHEN 51012 THEN 12 WHEN 51013 THEN 13 ELSE -1 END;
        SET @p_err_msg = ERROR_MESSAGE();
    END CATCH
END
GO
