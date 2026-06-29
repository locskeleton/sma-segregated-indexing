SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO
/*==============================================================================
  SDI MODULE — PHÍ QUẢN LÝ (THU PHÍ)  | BRD WS3.1 (fee)
  TÁCH RIÊNG khỏi NAV/AUM (thin-layer). SDI tự tính phí QL hàng ngày =
  rate × AUM_ròng / day_count (ngày dương lịch), chốt kỳ (THÁNG) treo nợ ở
  NGÀY GD CUỐI THÁNG, thu nợ FIFO mỗi NGÀY GD (call BO). Phí KHÔNG net vào AUM —
  BO cắt thật → cash KH giảm → AUM giảm qua feed Asset ngày sau.

  Ngày nghỉ/lễ: KHÔNG chạy job → ngày GD TRƯỚC tính gộp (look-FORWARD). Vd Thứ 6
  tính T6+T7+CN (dùng AUM Thứ 6). Dải nghỉ vắt 2 tháng (30/4–1/5): TÁCH 2 dòng
  daily theo kỳ (tháng 4 vs tháng 5) — kỳ tháng 4 chốt cuối tháng 4, phần tháng 5
  thuộc kỳ tháng 5.

  Load sau 01_TABLES + 02_SP_ENGINE.
==============================================================================*/

/*------------------------------------- Lịch nghỉ (forward) cho look-forward ----*/
-- Ngày GD = KHÔNG cuối tuần (Sat/Sun) VÀ KHÔNG trong T_TRADING_HOLIDAY (nguồn: sở/FO publish trước).
CREATE TABLE T_TRADING_HOLIDAY (
    C_HOLIDAY_DATE DATE NOT NULL,
    C_NOTE         NVARCHAR(100) NULL,
    CONSTRAINT PK_TRADING_HOLIDAY PRIMARY KEY CLUSTERED (C_HOLIDAY_DATE)
);
GO

-- 1 nếu @d là NGÀY GD (không cuối tuần + không nghỉ). DATEDIFF(DAY,0,@d)%7: 0=Mon..5=Sat,6=Sun (độc lập @@DATEFIRST).
CREATE OR ALTER FUNCTION UDF_IS_BUSINESS_DATE (@d DATE)
RETURNS BIT AS
BEGIN
    RETURN CASE WHEN @d IS NULL THEN 0
                WHEN DATEDIFF(DAY,0,@d) % 7 >= 5 THEN 0
                WHEN EXISTS (SELECT 1 FROM T_TRADING_HOLIDAY WHERE C_HOLIDAY_DATE=@d) THEN 0
                ELSE 1 END;
END
GO

-- Ngày GD KẾ tiếp sau @d (bỏ cuối tuần + nghỉ). Dùng cho look-forward accrual + phát hiện cuối tháng.
CREATE OR ALTER FUNCTION UDF_NEXT_BUSINESS_DATE (@d DATE)
RETURNS DATE AS
BEGIN
    DECLARE @n DATE = DATEADD(DAY,1,@d);
    WHILE dbo.UDF_IS_BUSINESS_DATE(@n) = 0 SET @n = DATEADD(DAY,1,@n);
    RETURN @n;
END
GO

/*------------------------------ Rate phí SẢN PHẨM (per-master) effective-dated --*/
-- Mỗi dòng = 1 version (lịch sử). Đổi rate sản phẩm áp sub MỚI; sub cũ giữ rate ở T_SI_FEE_RATE.
-- (Maker/checker = tầng app; ở đây chỉ lưu version ACTIVE + lịch sử.)
CREATE TABLE T_FEE_CONFIG (
    PK_FEE_CONFIG    UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_FEE_CONFIG_PKID DEFAULT NEWID(),
    C_MASTER_CODE    VARCHAR(20)   NOT NULL,
    C_EFFECTIVE_FROM DATE          NOT NULL,
    C_RATE           DECIMAL(10,6) NOT NULL,   -- %/năm thập phân (0.015 = 1.5%/năm)
    C_DAY_COUNT      INT           NOT NULL CONSTRAINT DF_FEE_CONFIG_DC DEFAULT 365,
    C_CREATED_BY     VARCHAR(64)   NULL,
    C_CREATED_AT     DATETIME      NOT NULL CONSTRAINT DF_FEE_CONFIG_AT DEFAULT GETDATE(),
    CONSTRAINT PK_FEE_CONFIG PRIMARY KEY CLUSTERED (PK_FEE_CONFIG),
    CONSTRAINT UQ_FEE_CONFIG_NK UNIQUE (C_MASTER_CODE, C_EFFECTIVE_FROM)
);
GO

/*------------------------------ Rate phí TIỂU KHOẢN (per-SI) effective-dated ----*/
-- NGUỒN TÍNH phí. Onboarding copy từ product; đổi = append dòng eff-date mới (áp FORWARD). Lịch sử = các dòng.
CREATE TABLE T_SI_FEE_RATE (
    PK_SI_FEE_RATE   UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_SI_FEE_RATE_PKID DEFAULT NEWID(),
    C_SI_ACCOUNT     VARCHAR(20)   NOT NULL,
    C_EFFECTIVE_FROM DATE          NOT NULL,
    C_RATE           DECIMAL(10,6) NOT NULL,
    C_DAY_COUNT      INT           NOT NULL CONSTRAINT DF_SI_FEE_RATE_DC DEFAULT 365,
    C_CREATED_BY     VARCHAR(64)   NULL,
    C_CREATED_AT     DATETIME      NOT NULL CONSTRAINT DF_SI_FEE_RATE_AT DEFAULT GETDATE(),
    CONSTRAINT PK_SI_FEE_RATE PRIMARY KEY CLUSTERED (PK_SI_FEE_RATE),
    CONSTRAINT UQ_SI_FEE_RATE_NK UNIQUE (C_SI_ACCOUNT, C_EFFECTIVE_FROM)
);
GO

/*------------------------------ Phí QL HÀNG NGÀY (per-SI / dải) -----------------*/
-- 1 dòng = 1 DẢI ngày dương lịch (ngày GD này + ngày nghỉ liền sau, TRONG CÙNG 1 tháng).
-- Dải vắt tháng → tách nhiều dòng (mỗi tháng 1 dòng). C_BUSINESS_DATE = ngày-BẮT-ĐẦU dải; C_ACCRUED_ON = ngày GD thực tính.
CREATE TABLE T_SI_FEE_DAILY (
    C_SI_FEE_DAILY_ID BIGINT IDENTITY(1,1) NOT NULL,
    PK_SI_FEE_DAILY  UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_SI_FEE_DAILY_PKID DEFAULT NEWID(),
    C_BUSINESS_DATE  DATE          NOT NULL,   -- ngày BẮT ĐẦU dải mà phí áp cho
    C_ACCRUED_ON     DATE          NOT NULL,   -- ngày GD thực tính (idempotent xóa theo cột này)
    C_SI_ACCOUNT     VARCHAR(20)   NOT NULL,
    C_CUST_CODE      VARCHAR(10)   NOT NULL,
    C_MASTER_CODE    VARCHAR(20)   NOT NULL,
    C_PERIOD         CHAR(6)       NOT NULL,   -- YYYYMM kỳ phí (= tháng của C_BUSINESS_DATE)
    C_AUM            DECIMAL(20,0) NOT NULL,   -- AUM cuối ngày GD (C_ACCRUED_ON)
    C_DAYS           INT           NOT NULL,   -- số ngày dương lịch dải gộp
    C_RATE           DECIMAL(10,6) NOT NULL,
    C_DAY_COUNT      INT           NOT NULL,
    C_FEE_AMOUNT     DECIMAL(20,6) NOT NULL,   -- = AUM × rate/day_count × C_DAYS (thập phân, KHÔNG round)
    C_STATUS         VARCHAR(10)   NOT NULL CONSTRAINT DF_SI_FEE_DAILY_ST DEFAULT 'ACCRUED', -- ACCRUED|CLOSED
    CONSTRAINT PK_SI_FEE_DAILY PRIMARY KEY CLUSTERED (C_SI_FEE_DAILY_ID),
    CONSTRAINT UQ_SI_FEE_DAILY_PKID UNIQUE NONCLUSTERED (PK_SI_FEE_DAILY),
    CONSTRAINT UQ_SI_FEE_DAILY_NK UNIQUE (C_SI_ACCOUNT, C_BUSINESS_DATE)   -- idempotency (dải bắt đầu/SI)
);
CREATE INDEX IX_SI_FEE_DAILY_CLOSE   ON T_SI_FEE_DAILY (C_PERIOD, C_STATUS) INCLUDE (C_SI_ACCOUNT, C_CUST_CODE, C_MASTER_CODE, C_BUSINESS_DATE, C_FEE_AMOUNT);
CREATE INDEX IX_SI_FEE_DAILY_ACCRUED ON T_SI_FEE_DAILY (C_ACCRUED_ON);
GO

/*------------------------------ Nợ phí theo KỲ (per-SI / tháng) -----------------*/
-- Treo khi chốt cuối tháng. Thu FIFO (C_PERIOD cũ→mới). 1 KỲ = 1 "món" all-or-nothing.
CREATE TABLE T_SI_FEE_CHARGE (
    PK_SI_FEE_CHARGE UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_SI_FEE_CHARGE_PKID DEFAULT NEWID(),
    C_SI_ACCOUNT     VARCHAR(20)   NOT NULL,
    C_CUST_CODE      VARCHAR(10)   NOT NULL,
    C_MASTER_CODE    VARCHAR(20)   NOT NULL,
    C_PERIOD         CHAR(6)       NOT NULL,   -- YYYYMM
    C_PERIOD_FROM    DATE          NOT NULL,
    C_PERIOD_TO      DATE          NOT NULL,
    C_FEE_TOTAL      DECIMAL(20,6) NOT NULL,   -- Σ daily kỳ (thập phân)
    C_FEE_DUE        DECIMAL(20,0) NOT NULL,   -- số phải thu = ROUND(total) VND (BO cắt số tròn)
    C_FEE_PAID       DECIMAL(20,0) NOT NULL CONSTRAINT DF_SI_FEE_CHARGE_PAID DEFAULT 0,
    C_STATUS         VARCHAR(10)   NOT NULL CONSTRAINT DF_SI_FEE_CHARGE_ST DEFAULT 'UNPAID', -- UNPAID|PAID
    C_CLOSED_AT      DATETIME      NOT NULL CONSTRAINT DF_SI_FEE_CHARGE_CL DEFAULT GETDATE(),
    C_COLLECTED_AT   DATETIME      NULL,
    C_BO_EVENT_ID    VARCHAR(64)   NULL,       -- event gửi BO lần thu gần nhất (map kết quả)
    CONSTRAINT PK_SI_FEE_CHARGE PRIMARY KEY CLUSTERED (PK_SI_FEE_CHARGE),
    CONSTRAINT UQ_SI_FEE_CHARGE_NK UNIQUE (C_SI_ACCOUNT, C_PERIOD)
);
CREATE INDEX IX_SI_FEE_CHARGE_FIFO ON T_SI_FEE_CHARGE (C_SI_ACCOUNT, C_STATUS, C_PERIOD) INCLUDE (C_FEE_DUE, C_CUST_CODE, C_MASTER_CODE);
GO

/*============================================================================
  SP_SET_FEE_CONFIG — khai báo/đổi rate phí SẢN PHẨM (per-master), append version.
============================================================================*/
CREATE OR ALTER PROCEDURE SP_SET_FEE_CONFIG
    @p_master_code   VARCHAR(20),
    @p_rate          DECIMAL(10,6),
    @p_day_count     INT          = 365,
    @p_effective_from DATE        = NULL,
    @p_user          VARCHAR(64)  = NULL,
    @p_err_code      INT          OUTPUT,
    @p_err_msg       NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON; SET @p_err_code=0; SET @p_err_msg=NULL;
    BEGIN TRY
        IF @p_effective_from IS NULL SET @p_effective_from = CAST(GETDATE() AS DATE);
        MERGE T_FEE_CONFIG t USING (SELECT @p_master_code mc, @p_effective_from ef) s
          ON t.C_MASTER_CODE=s.mc AND t.C_EFFECTIVE_FROM=s.ef
        WHEN MATCHED THEN UPDATE SET C_RATE=@p_rate, C_DAY_COUNT=@p_day_count, C_CREATED_BY=@p_user, C_CREATED_AT=GETDATE()
        WHEN NOT MATCHED THEN INSERT (C_MASTER_CODE,C_EFFECTIVE_FROM,C_RATE,C_DAY_COUNT,C_CREATED_BY)
            VALUES (@p_master_code,@p_effective_from,@p_rate,@p_day_count,@p_user);
    END TRY
    BEGIN CATCH SET @p_err_code=-1; SET @p_err_msg=ERROR_MESSAGE(); END CATCH
END
GO

/*============================================================================
  SP_SET_SI_FEE_RATE — gán/đổi rate phí TIỂU KHOẢN (per-SI), append version (áp FORWARD).
    @p_rate NULL → copy rate SẢN PHẨM (master của SI) hiệu lực @effective_from (onboarding default).
============================================================================*/
CREATE OR ALTER PROCEDURE SP_SET_SI_FEE_RATE
    @p_si_account    VARCHAR(20),
    @p_rate          DECIMAL(10,6) = NULL,
    @p_day_count     INT          = NULL,
    @p_effective_from DATE        = NULL,
    @p_user          VARCHAR(64)  = NULL,
    @p_err_code      INT          OUTPUT,
    @p_err_msg       NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON; SET @p_err_code=0; SET @p_err_msg=NULL;
    BEGIN TRY
        IF @p_effective_from IS NULL SET @p_effective_from = CAST(GETDATE() AS DATE);
        DECLARE @master VARCHAR(20) = (SELECT C_MASTER_CODE FROM T_SI_PORTFOLIO WHERE C_SI_ACCOUNT=@p_si_account);
        IF @master IS NULL BEGIN SET @p_err_code=1; SET @p_err_msg=N'si_account chưa đăng ký'; RETURN; END
        -- default từ product nếu không truyền rate
        IF @p_rate IS NULL
            SELECT TOP 1 @p_rate=C_RATE, @p_day_count=ISNULL(@p_day_count,C_DAY_COUNT)
            FROM T_FEE_CONFIG WHERE C_MASTER_CODE=@master AND C_EFFECTIVE_FROM <= @p_effective_from
            ORDER BY C_EFFECTIVE_FROM DESC;
        IF @p_rate IS NULL BEGIN SET @p_err_code=2; SET @p_err_msg=N'Thiếu rate + sản phẩm chưa khai phí'; RETURN; END
        SET @p_day_count = ISNULL(@p_day_count,365);
        MERGE T_SI_FEE_RATE t USING (SELECT @p_si_account si, @p_effective_from ef) s
          ON t.C_SI_ACCOUNT=s.si AND t.C_EFFECTIVE_FROM=s.ef
        WHEN MATCHED THEN UPDATE SET C_RATE=@p_rate, C_DAY_COUNT=@p_day_count, C_CREATED_BY=@p_user, C_CREATED_AT=GETDATE()
        WHEN NOT MATCHED THEN INSERT (C_SI_ACCOUNT,C_EFFECTIVE_FROM,C_RATE,C_DAY_COUNT,C_CREATED_BY)
            VALUES (@p_si_account,@p_effective_from,@p_rate,@p_day_count,@p_user);
    END TRY
    BEGIN CATCH SET @p_err_code=-1; SET @p_err_msg=ERROR_MESSAGE(); END CATCH
END
GO

/*============================================================================
  SP_EOD_FEE_ACCRUE @p_d — tính phí QL hàng ngày (trong EOD, sau khi có AUM @d).
    Dải = [@p_d, ngày_GD_kế) ngày dương lịch (look-FORWARD). TÁCH theo ranh giới THÁNG → mỗi
    tháng 1 dòng. AUM = AUM cuối @p_d (T_SI_BALANCE); rate = eff @seg_start (T_SI_FEE_RATE).
    fee = AUM × rate/day_count × #ngày. Idempotent: xóa theo C_ACCRUED_ON=@p_d.
============================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_FEE_ACCRUE @p_d DATE, @p_rows BIGINT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @next DATE = dbo.UDF_NEXT_BUSINESS_DATE(@p_d);   -- dải = [@p_d, @next)

    DELETE FROM T_SI_FEE_DAILY WHERE C_ACCRUED_ON=@p_d;     -- idempotent re-accrue ngày GD này

    ;WITH months AS (
        SELECT CAST(DATEFROMPARTS(YEAR(@p_d),MONTH(@p_d),1) AS DATE) AS mstart
        UNION ALL
        SELECT DATEADD(MONTH,1,mstart) FROM months WHERE DATEADD(MONTH,1,mstart) < @next
    ), seg AS (
        SELECT CASE WHEN mstart < @p_d THEN @p_d ELSE mstart END AS seg_start,
               CASE WHEN EOMONTH(mstart) < DATEADD(DAY,-1,@next) THEN EOMONTH(mstart) ELSE DATEADD(DAY,-1,@next) END AS seg_end
        FROM months
    )
    INSERT INTO T_SI_FEE_DAILY (C_BUSINESS_DATE,C_ACCRUED_ON,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,
        C_PERIOD,C_AUM,C_DAYS,C_RATE,C_DAY_COUNT,C_FEE_AMOUNT)
    SELECT g.seg_start, @p_d, b.C_SI_ACCOUNT, b.C_CUST_CODE, b.C_MASTER_CODE,
           CONVERT(CHAR(6), g.seg_start, 112),
           b.C_AUM,
           DATEDIFF(DAY, g.seg_start, g.seg_end) + 1,
           rt.C_RATE, rt.C_DAY_COUNT,
           CAST(CAST(CAST(b.C_AUM AS DECIMAL(38,6)) * rt.C_RATE
                * (DATEDIFF(DAY, g.seg_start, g.seg_end) + 1) AS DECIMAL(38,12))
                / rt.C_DAY_COUNT AS DECIMAL(20,6))   -- cast tích lên scale 12 TRƯỚC khi chia → làm tròn 6-dp chuẩn
    FROM seg g
    CROSS JOIN T_SI_BALANCE b
    INNER JOIN T_SI_PORTFOLIO p ON p.C_SI_ACCOUNT=b.C_SI_ACCOUNT AND p.C_STATUS='ACTIVE'
    OUTER APPLY (SELECT TOP 1 r.C_RATE, r.C_DAY_COUNT FROM T_SI_FEE_RATE r
                 WHERE r.C_SI_ACCOUNT=b.C_SI_ACCOUNT AND r.C_EFFECTIVE_FROM <= g.seg_start
                 ORDER BY r.C_EFFECTIVE_FROM DESC) rt
    WHERE b.C_BUSINESS_DATE=@p_d AND rt.C_RATE IS NOT NULL;   -- rate NULL = SI không thu phí

    SET @p_rows = @@ROWCOUNT;
END
GO

/*============================================================================
  SP_FEE_CLOSE_PERIOD @p_d — chốt kỳ (tháng) TREO nợ. CHỈ chạy khi @p_d là NGÀY GD
    CUỐI THÁNG (ngày GD kế sang tháng khác). Gom daily kỳ này per-SI → T_SI_FEE_CHARGE.
    Idempotent: re-close ghi đè record UNPAID của kỳ (đã PAID thì giữ).
============================================================================*/
CREATE OR ALTER PROCEDURE SP_FEE_CLOSE_PERIOD @p_d DATE, @p_rows BIGINT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON; SET @p_rows = 0;
    DECLARE @next DATE = dbo.UDF_NEXT_BUSINESS_DATE(@p_d);
    IF YEAR(@next)=YEAR(@p_d) AND MONTH(@next)=MONTH(@p_d) RETURN;   -- chưa phải ngày GD cuối tháng

    DECLARE @period CHAR(6) = CONVERT(CHAR(6), @p_d, 112);
    DECLARE @pfrom DATE = DATEFROMPARTS(CAST(LEFT(@period,4) AS INT), CAST(RIGHT(@period,2) AS INT), 1);

    DELETE FROM T_SI_FEE_CHARGE WHERE C_PERIOD=@period AND C_STATUS='UNPAID';   -- re-close: chỉ ghi đè chưa thu

    ;WITH agg AS (
        SELECT C_SI_ACCOUNT, MAX(C_CUST_CODE) cust, MAX(C_MASTER_CODE) mc,
               SUM(C_FEE_AMOUNT) total, MIN(C_BUSINESS_DATE) pf, MAX(C_BUSINESS_DATE) pt
        FROM T_SI_FEE_DAILY WHERE C_PERIOD=@period GROUP BY C_SI_ACCOUNT
    )
    INSERT INTO T_SI_FEE_CHARGE (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_PERIOD,C_PERIOD_FROM,C_PERIOD_TO,C_FEE_TOTAL,C_FEE_DUE)
    SELECT a.C_SI_ACCOUNT, a.cust, a.mc, @period, a.pf, a.pt, a.total, CAST(ROUND(a.total,0) AS DECIMAL(20,0))
    FROM agg a
    WHERE a.total > 0
      AND NOT EXISTS (SELECT 1 FROM T_SI_FEE_CHARGE fp WHERE fp.C_SI_ACCOUNT=a.C_SI_ACCOUNT AND fp.C_PERIOD=@period AND fp.C_STATUS='PAID');
    SET @p_rows = @@ROWCOUNT;

    UPDATE T_SI_FEE_DAILY SET C_STATUS='CLOSED' WHERE C_PERIOD=@period AND C_STATUS='ACCRUED';
END
GO

/*============================================================================
  SP_FEE_COLLECT @p_d, @p_event_id — thu nợ FIFO (mỗi NGÀY GD). Per-SI: số dư khả dụng
    (T_SI_CURRENT.C_CASH); FIFO kỳ cũ→mới; món đủ tiền (cộng dồn ≤ số dư) → đưa vào event;
    thiếu → skip (không cắt lẻ). Đánh dấu C_BO_EVENT_ID; trả RS payload gửi BO. KHÔNG mark PAID
    (chờ SP_INGEST_FEE_COLLECT_RESULT). Chỉ chạy ngày GD (caller gác).
============================================================================*/
CREATE OR ALTER PROCEDURE SP_FEE_COLLECT @p_d DATE, @p_event_id VARCHAR(64) = NULL, @p_rows BIGINT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    IF @p_event_id IS NULL SET @p_event_id = CONVERT(VARCHAR(64), CONVERT(CHAR(8),@p_d,112)) + '-' + LEFT(REPLACE(CONVERT(VARCHAR(36),NEWID()),'-',''),12);

    ;WITH unpaid AS (
        SELECT fp.PK_SI_FEE_CHARGE, fp.C_SI_ACCOUNT, fp.C_CUST_CODE, fp.C_MASTER_CODE, fp.C_PERIOD, fp.C_FEE_DUE,
               nc.C_CASH AS avail,
               SUM(fp.C_FEE_DUE) OVER (PARTITION BY fp.C_SI_ACCOUNT ORDER BY fp.C_PERIOD ROWS UNBOUNDED PRECEDING) AS cum_due
        FROM T_SI_FEE_CHARGE fp
        INNER JOIN T_SI_CURRENT nc ON nc.C_SI_ACCOUNT=fp.C_SI_ACCOUNT
        WHERE fp.C_STATUS='UNPAID' AND fp.C_FEE_DUE > 0
    )
    SELECT PK_SI_FEE_CHARGE, C_SI_ACCOUNT, C_CUST_CODE, C_MASTER_CODE, C_PERIOD, C_FEE_DUE
    INTO #tc
    FROM unpaid WHERE cum_due <= avail;   -- FIFO greedy: prefix kỳ cũ nhất phủ trong số dư

    UPDATE fp SET fp.C_BO_EVENT_ID=@p_event_id
    FROM T_SI_FEE_CHARGE fp INNER JOIN #tc t ON t.PK_SI_FEE_CHARGE=fp.PK_SI_FEE_CHARGE;

    SET @p_rows = (SELECT COUNT(*) FROM #tc);
    -- RS payload gửi BO (FIFO)
    SELECT @p_event_id AS C_BO_EVENT_ID, C_SI_ACCOUNT, C_CUST_CODE, C_MASTER_CODE, C_PERIOD, C_FEE_DUE
    FROM #tc ORDER BY C_SI_ACCOUNT, C_PERIOD;
END
GO

/*============================================================================
  SP_INGEST_FEE_COLLECT_RESULT — BO trả kết quả cắt. collected=1 → PAID; 0 → giữ UNPAID (retry).
    JSON: [{"si_account","period","collected"(0|1),"amount"}]
============================================================================*/
CREATE OR ALTER PROCEDURE SP_INGEST_FEE_COLLECT_RESULT
    @p_json     NVARCHAR(MAX),
    @p_err_code INT          OUTPUT,
    @p_err_msg  NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON; SET @p_err_code=0; SET @p_err_msg=NULL;
    BEGIN TRY
        IF @p_json IS NULL OR ISJSON(@p_json)<>1 BEGIN SET @p_err_code=20; SET @p_err_msg=N'@p_json không hợp lệ'; RETURN; END
        DECLARE @res TABLE (C_SI_ACCOUNT VARCHAR(20), C_PERIOD CHAR(6), C_COLLECTED BIT, C_AMOUNT DECIMAL(20,0));
        INSERT @res
        SELECT j.si_account, j.period, ISNULL(j.collected,0), ISNULL(j.amount,0)
        FROM OPENJSON(@p_json) WITH (si_account VARCHAR(20) '$.si_account', period CHAR(6) '$.period',
                                     collected BIT '$.collected', amount DECIMAL(20,0) '$.amount') j;
        BEGIN TRAN;
        UPDATE fp SET fp.C_STATUS='PAID', fp.C_FEE_PAID=r.C_AMOUNT, fp.C_COLLECTED_AT=GETDATE()
        FROM T_SI_FEE_CHARGE fp INNER JOIN @res r ON r.C_SI_ACCOUNT=fp.C_SI_ACCOUNT AND r.C_PERIOD=fp.C_PERIOD
        WHERE r.C_COLLECTED=1 AND fp.C_STATUS='UNPAID';
        -- không cắt → bỏ event id để lần thu sau quét lại
        UPDATE fp SET fp.C_BO_EVENT_ID=NULL
        FROM T_SI_FEE_CHARGE fp INNER JOIN @res r ON r.C_SI_ACCOUNT=fp.C_SI_ACCOUNT AND r.C_PERIOD=fp.C_PERIOD
        WHERE r.C_COLLECTED=0 AND fp.C_STATUS='UNPAID';
        COMMIT;
    END TRY
    BEGIN CATCH IF @@TRANCOUNT>0 ROLLBACK; SET @p_err_code=-1; SET @p_err_msg=ERROR_MESSAGE(); END CATCH
END
GO
