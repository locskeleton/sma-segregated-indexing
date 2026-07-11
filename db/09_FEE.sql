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

/*------------------------------------- Lịch nghỉ — ĐÃ CHUYỂN LÊN CORE (2026-07-11) ------------
  T_TRADING_HOLIDAY (01_TABLES.sql) + UDF_IS_BUSINESS_DATE / UDF_NEXT_BUSINESS_DATE (02_SP_ENGINE.sql).
  Lý do: ENGINE cũng phải dùng lịch (master index là chuỗi nhân dồn — tính nhầm 1 ngày nghỉ là sai cấp số
  nhân), không riêng subsystem phí. 09_FEE giờ CHỈ TIÊU THỤ 2 UDF này. Deploy: 01 → 02 → … → 09 (không đổi).
  Module vẫn PLUGGABLE (EOD guard OBJECT_ID); chỉ khác: 09 không còn tự định nghĩa lịch. */

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

/*------------------------------ Phí QL theo NGÀY (per-SI / mỗi ngày 1 dòng) -----*/
-- [BRD] 1 dòng = 1 NGÀY DƯƠNG LỊCH (C_FEE_DATE). Ngày GD cuối tuần/trước lễ khi tính phí sẽ HẠCH TOÁN
-- THÀNH NHIỀU DÒNG cho từng ngày nghỉ liền sau (Thứ 6 → 3 dòng T6/T7/CN), mỗi dòng dùng AUM ngày GD đó.
-- C_ACCRUED_ON = ngày GD job thực tính (= ngày có AUM). Cross-month tự nhiên: mỗi ngày tự thuộc kỳ của nó.
CREATE TABLE T_SI_FEE_BALANCE (
    C_SI_FEE_BAL_ID  BIGINT IDENTITY(1,1) NOT NULL,
    PK_SI_FEE_BALANCE UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_SI_FEE_BALANCE_PKID DEFAULT NEWID(),
    C_FEE_DATE       DATE          NOT NULL,   -- NGÀY dương lịch phí áp cho (GD hoặc nghỉ)
    C_ACCRUED_ON     DATE          NOT NULL,   -- ngày GD job thực tính (= ngày lấy AUM; chữ ký lần accrue)
    C_SI_ACCOUNT     VARCHAR(20)   NOT NULL,
    C_CUST_CODE      VARCHAR(10)   NOT NULL,
    C_MASTER_CODE    VARCHAR(20)   NOT NULL,
    C_PERIOD         CHAR(6)       NOT NULL,   -- YYYYMM kỳ phí (= tháng của C_FEE_DATE)
    C_AUM            DECIMAL(20,0) NOT NULL,   -- AUM cuối ngày GD (C_ACCRUED_ON)
    C_RATE           DECIMAL(10,6) NOT NULL,
    C_DAY_COUNT      INT           NOT NULL,
    C_FEE_AMOUNT     DECIMAL(20,2) NOT NULL,   -- phí 1 ngày = CEILING(AUM×rate/day_count, 2dp); AUM≤0 ⇒ 0. Làm tròn LÊN 2 số lẻ.
    C_STATUS         BIT           NOT NULL CONSTRAINT DF_SI_FEE_BALANCE_ST DEFAULT 1, -- 1=hợp lệ (vào charge) | 0=invalid (mồ côi/loại). "Đã thu khóa" suy từ T_SI_FEE_CHARGE.C_STATUS='PAID'.
    CONSTRAINT PK_SI_FEE_BALANCE PRIMARY KEY CLUSTERED (C_SI_FEE_BAL_ID),
    CONSTRAINT UQ_SI_FEE_BALANCE_PKID UNIQUE NONCLUSTERED (PK_SI_FEE_BALANCE),
    CONSTRAINT UQ_SI_FEE_BALANCE_NK UNIQUE (C_SI_ACCOUNT, C_FEE_DATE)   -- idempotency: 1 dòng/SI mỗi ngày
);
CREATE INDEX IX_SI_FEE_BALANCE_CLOSE   ON T_SI_FEE_BALANCE (C_PERIOD, C_STATUS) INCLUDE (C_SI_ACCOUNT, C_CUST_CODE, C_MASTER_CODE, C_FEE_DATE, C_FEE_AMOUNT);
CREATE INDEX IX_SI_FEE_BALANCE_ACCRUED ON T_SI_FEE_BALANCE (C_ACCRUED_ON);
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
    C_FEE_TOTAL      DECIMAL(20,2) NOT NULL,   -- Σ daily kỳ (2dp)
    C_FEE_DUE        DECIMAL(20,0) NOT NULL,   -- số phải thu = CEILING(total) VND (chốt kỳ làm tròn LÊN đến đồng)
    C_FEE_PAID       DECIMAL(20,0) NOT NULL CONSTRAINT DF_SI_FEE_CHARGE_PAID DEFAULT 0,
    C_STATUS         VARCHAR(10)   NOT NULL CONSTRAINT DF_SI_FEE_CHARGE_ST DEFAULT 'UNPAID', -- UNPAID|PAID
    C_CLOSED_AT      DATETIME      NOT NULL CONSTRAINT DF_SI_FEE_CHARGE_CL DEFAULT GETDATE(),
    C_COLLECTED_AT   DATETIME      NULL,
    C_BO_EVENT_ID    VARCHAR(64)   NULL,       -- REQUEST ID gửi BO (unique/món = 1 bút toán); BO echo lại id này + trạng thái → map kết quả (KHÔNG cần period)
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
  SP_EOD_FEE_ACCRUE @p_d — tính phí QL (trong EOD, sau khi có AUM @d).
    [BRD] HẠCH TOÁN MỖI NGÀY 1 DÒNG: dải [@p_d, ngày_GD_kế) ngày dương lịch (look-FORWARD) → BUNG
    thành nhiều dòng, MỖI NGÀY 1 dòng (Thứ 6 → T6/T7/CN = 3 dòng). Mỗi dòng: C_FEE_DATE = ngày đó,
    C_AUM = AUM @p_d, fee/ngày = CEILING(AUM×rate/day_count, 2dp) — làm tròn LÊN 2 số lẻ; AUM≤0 ⇒ 0.
    Cross-month tự nhiên (mỗi ngày tự thuộc kỳ). Chốt kỳ: CEILING đến đồng (SP_FEE_CLOSE_PERIOD).
    rate = eff @C_FEE_DATE (T_SI_FEE_RATE). C_ACCRUED_ON = @p_d (chữ ký lần accrue).
    ── AN TOÀN GD THẬT (KHÔNG DELETE + KHÔNG ĐÈ DỮ LIỆU ĐÃ THU) ─────────────────
    Idempotent bằng MERGE upsert trên natural key (SI, C_FEE_DATE):
      • ĐÃ THU BÊN BO (kỳ có T_SI_FEE_CHARGE.C_STATUS='PAID') → KHÓA: KHÔNG update / insert / void.
      • MATCHED + kỳ CHƯA thu → cập nhật TẠI CHỖ + set C_STATUS=1 (hồi sinh dòng mồ côi cũ nếu có).
      • NOT MATCHED + kỳ CHƯA thu → insert dòng ngày mới.
      • Mồ côi (calendar đổi, ngày cũ không còn trong dải; kỳ chưa thu) → đánh C_STATUS=0 (KHÔNG xoá).
============================================================================*/
CREATE OR ALTER PROCEDURE SP_EOD_FEE_ACCRUE @p_d DATE, @p_rows BIGINT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @next DATE = dbo.UDF_NEXT_BUSINESS_DATE(@p_d);   -- dải = [@p_d, @next)

    -- BUNG dải thành từng NGÀY dương lịch [@p_d .. @next-1] → #src (mỗi ngày 1 dòng).
    ;WITH days AS (
        SELECT @p_d AS d
        UNION ALL
        SELECT DATEADD(DAY,1,d) FROM days WHERE DATEADD(DAY,1,d) < @next
    )
    SELECT g.d AS C_FEE_DATE, @p_d AS C_ACCRUED_ON,
           b.C_SI_ACCOUNT, b.C_CUST_CODE, b.C_MASTER_CODE,
           CONVERT(CHAR(6), g.d, 112) AS C_PERIOD, b.C_AUM, rt.C_RATE, rt.C_DAY_COUNT,
           CAST(CASE WHEN b.C_AUM <= 0 THEN 0   -- AUM ≤ 0 ⇒ phí ngày = 0
                     ELSE CEILING(CAST(CAST(b.C_AUM AS DECIMAL(38,6)) * rt.C_RATE AS DECIMAL(38,12))
                                  / rt.C_DAY_COUNT * 100) / 100.0   -- CEILING 2dp = làm tròn LÊN 2 số lẻ
                END AS DECIMAL(20,2)) AS C_FEE_AMOUNT,
           -- KHÓA: kỳ của ngày này đã THU bên BO (charge PAID) chưa? đã thu → bỏ qua mọi thao tác.
           CAST(CASE WHEN EXISTS (SELECT 1 FROM T_SI_FEE_CHARGE c
                       WHERE c.C_SI_ACCOUNT=b.C_SI_ACCOUNT
                         AND c.C_PERIOD=CONVERT(CHAR(6), g.d, 112)
                         AND c.C_STATUS='PAID') THEN 1 ELSE 0 END AS BIT) AS is_paid
    INTO #src
    FROM days g
    CROSS JOIN T_SI_BALANCE b
    INNER JOIN T_SI_PORTFOLIO p ON p.C_SI_ACCOUNT=b.C_SI_ACCOUNT AND p.C_STATUS='ACTIVE'
    OUTER APPLY (SELECT TOP 1 r.C_RATE, r.C_DAY_COUNT FROM T_SI_FEE_RATE r
                 WHERE r.C_SI_ACCOUNT=b.C_SI_ACCOUNT AND r.C_EFFECTIVE_FROM <= g.d
                 ORDER BY r.C_EFFECTIVE_FROM DESC) rt
    WHERE b.C_BUSINESS_DATE=@p_d AND rt.C_RATE IS NOT NULL   -- rate NULL = SI không thu phí
    OPTION (MAXRECURSION 366);

    -- UPSERT (no-delete) trên UQ_SI_FEE_BALANCE_NK (SI, C_FEE_DATE). is_paid=1 → KHÓA (không thao tác).
    MERGE T_SI_FEE_BALANCE AS tgt
    USING #src AS src
      ON tgt.C_SI_ACCOUNT = src.C_SI_ACCOUNT AND tgt.C_FEE_DATE = src.C_FEE_DATE
    WHEN MATCHED AND src.is_paid=0 THEN      -- kỳ CHƯA thu → update tại chỗ + hồi sinh (C_STATUS=1)
        UPDATE SET C_ACCRUED_ON=src.C_ACCRUED_ON, C_CUST_CODE=src.C_CUST_CODE, C_MASTER_CODE=src.C_MASTER_CODE,
                   C_PERIOD=src.C_PERIOD, C_AUM=src.C_AUM, C_RATE=src.C_RATE, C_DAY_COUNT=src.C_DAY_COUNT,
                   C_FEE_AMOUNT=src.C_FEE_AMOUNT, C_STATUS=1
    WHEN NOT MATCHED BY TARGET AND src.is_paid=0 THEN          -- ngày mới + kỳ CHƯA thu → insert
        INSERT (C_FEE_DATE,C_ACCRUED_ON,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_PERIOD,C_AUM,C_RATE,C_DAY_COUNT,C_FEE_AMOUNT)
        VALUES (src.C_FEE_DATE,src.C_ACCRUED_ON,src.C_SI_ACCOUNT,src.C_CUST_CODE,src.C_MASTER_CODE,src.C_PERIOD,src.C_AUM,src.C_RATE,src.C_DAY_COUNT,src.C_FEE_AMOUNT);
    SET @p_rows = @@ROWCOUNT;

    -- Mồ côi: dòng accrue ngày @p_d, đang hợp lệ (=1), NGÀY không còn trong dải hiện tại (calendar đổi)
    --   VÀ kỳ CHƯA thu → đánh C_STATUS=0 (zeroed, loại khỏi tổng) thay vì DELETE. Kỳ đã PAID → giữ nguyên.
    UPDATE d SET d.C_STATUS=0, d.C_FEE_AMOUNT=0
    FROM T_SI_FEE_BALANCE d
    WHERE d.C_ACCRUED_ON=@p_d AND d.C_STATUS=1
      AND NOT EXISTS (SELECT 1 FROM #src s WHERE s.C_SI_ACCOUNT=d.C_SI_ACCOUNT AND s.C_FEE_DATE=d.C_FEE_DATE)
      AND NOT EXISTS (SELECT 1 FROM T_SI_FEE_CHARGE c WHERE c.C_SI_ACCOUNT=d.C_SI_ACCOUNT AND c.C_PERIOD=d.C_PERIOD AND c.C_STATUS='PAID');

    DROP TABLE #src;
END
GO

/*============================================================================
  SP_FEE_CLOSE_PERIOD @p_d — chốt kỳ (tháng) TREO nợ. CHỉ chạy khi @p_d là NGÀY GD
    CUỐI THÁNG (ngày GD kế sang tháng khác). Gom dải HỢP LỆ (C_STATUS=1) kỳ này per-SI → T_SI_FEE_CHARGE.
    ── AN TOÀN GD THẬT (KHÔNG DELETE) ──────────────────────────────────────────
    Re-close idempotent bằng MERGE upsert trên (SI, period):
      • MATCHED + UNPAID + CHƯA gửi BO (C_BO_EVENT_ID NULL) → cập nhật số chốt tại chỗ.
      • MATCHED + PAID / đang thu (event_id đã set) → GIỮ NGUYÊN (không đụng món đã/đang giao dịch BO).
      • NOT MATCHED → insert kỳ mới (UNPAID).
============================================================================*/
CREATE OR ALTER PROCEDURE SP_FEE_CLOSE_PERIOD @p_d DATE, @p_rows BIGINT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON; SET @p_rows = 0;
    DECLARE @next DATE = dbo.UDF_NEXT_BUSINESS_DATE(@p_d);
    IF YEAR(@next)=YEAR(@p_d) AND MONTH(@next)=MONTH(@p_d) RETURN;   -- chưa phải ngày GD cuối tháng

    DECLARE @period CHAR(6) = CONVERT(CHAR(6), @p_d, 112);

    ;WITH agg AS (
        SELECT C_SI_ACCOUNT, MAX(C_CUST_CODE) cust, MAX(C_MASTER_CODE) mc,
               SUM(C_FEE_AMOUNT) total, MIN(C_FEE_DATE) pf, MAX(C_FEE_DATE) pt
        FROM T_SI_FEE_BALANCE WHERE C_PERIOD=@period AND C_STATUS=1 GROUP BY C_SI_ACCOUNT   -- chỉ ngày hợp lệ
        HAVING SUM(C_FEE_AMOUNT) > 0
    )
    MERGE T_SI_FEE_CHARGE AS tgt
    USING agg AS src ON tgt.C_SI_ACCOUNT=src.C_SI_ACCOUNT AND tgt.C_PERIOD=@period
    WHEN MATCHED AND tgt.C_STATUS='UNPAID' AND tgt.C_BO_EVENT_ID IS NULL THEN   -- chưa thu & chưa gửi BO mới ghi đè
        UPDATE SET C_FEE_TOTAL=src.total, C_FEE_DUE=CAST(CEILING(src.total) AS DECIMAL(20,0)),   -- chốt kỳ làm tròn LÊN
                   C_PERIOD_FROM=src.pf, C_PERIOD_TO=src.pt, C_CLOSED_AT=GETDATE()
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_PERIOD,C_PERIOD_FROM,C_PERIOD_TO,C_FEE_TOTAL,C_FEE_DUE)
        VALUES (src.C_SI_ACCOUNT,src.cust,src.mc,@period,src.pf,src.pt,src.total,CAST(CEILING(src.total) AS DECIMAL(20,0)));
    SET @p_rows = @@ROWCOUNT;
    -- (KHÔNG còn mark CLOSED trên balance: "đã chốt/đã thu" suy từ T_SI_FEE_CHARGE; accrue khóa theo charge PAID.)
END
GO

/*============================================================================
  SP_FEE_COLLECT @p_d, @p_batch_id — thu nợ FIFO (mỗi NGÀY GD). Per-SI: số dư khả dụng
    (T_SI_CURRENT.C_CASH); FIFO kỳ cũ→mới; món đủ tiền (cộng dồn ≤ số dư) → đưa vào lệnh thu;
    thiếu → skip (không cắt lẻ). Mỗi món sinh 1 REQUEST ID UNIQUE (= 1 bút toán), lưu C_BO_EVENT_ID;
    trả RS payload gửi BO. KHÔNG mark PAID (chờ SP_INGEST_FEE_COLLECT_RESULT). Chỉ chạy ngày GD.
    @p_batch_id = prefix gom lô (trace), tùy chọn; id thật vẫn unique/món.
============================================================================*/
CREATE OR ALTER PROCEDURE SP_FEE_COLLECT @p_d DATE, @p_batch_id VARCHAR(40) = NULL, @p_rows BIGINT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @batch VARCHAR(40) = ISNULL(@p_batch_id, CONVERT(CHAR(8),@p_d,112));

    ;WITH unpaid AS (
        SELECT fp.PK_SI_FEE_CHARGE, fp.C_SI_ACCOUNT, fp.C_CUST_CODE, fp.C_MASTER_CODE, fp.C_PERIOD, fp.C_FEE_DUE,
               nc.C_CASH AS avail,
               SUM(fp.C_FEE_DUE) OVER (PARTITION BY fp.C_SI_ACCOUNT ORDER BY fp.C_PERIOD ROWS UNBOUNDED PRECEDING) AS cum_due
        FROM T_SI_FEE_CHARGE fp
        INNER JOIN T_SI_CURRENT nc ON nc.C_SI_ACCOUNT=fp.C_SI_ACCOUNT
        WHERE fp.C_STATUS='UNPAID' AND fp.C_FEE_DUE > 0
    )
    SELECT PK_SI_FEE_CHARGE, C_SI_ACCOUNT, C_CUST_CODE, C_MASTER_CODE, C_PERIOD, C_FEE_DUE,
           -- REQUEST ID = id/bút toán gửi BO, UNIQUE mỗi món (BO echo lại để map kết quả). prefix lô + GUID/row.
           CONCAT(@batch, '-', LEFT(REPLACE(CONVERT(VARCHAR(36),NEWID()),'-',''),20)) AS C_REQUEST_ID
    INTO #tc
    FROM unpaid WHERE cum_due <= avail;   -- FIFO greedy: prefix kỳ cũ nhất phủ trong số dư

    UPDATE fp SET fp.C_BO_EVENT_ID = t.C_REQUEST_ID   -- lưu request id/món → map kết quả BO theo id
    FROM T_SI_FEE_CHARGE fp INNER JOIN #tc t ON t.PK_SI_FEE_CHARGE=fp.PK_SI_FEE_CHARGE;

    SET @p_rows = (SELECT COUNT(*) FROM #tc);
    -- RS payload gửi BO: MỖI MÓN 1 request id (BO trả lại id này + trạng thái bút toán)
    SELECT C_REQUEST_ID, C_SI_ACCOUNT, C_CUST_CODE, C_MASTER_CODE, C_PERIOD, C_FEE_DUE
    FROM #tc ORDER BY C_SI_ACCOUNT, C_PERIOD;
END
GO

/*============================================================================
  SP_INGEST_FEE_COLLECT_RESULT — BO trả kết quả thu. Map theo REQUEST ID (= id SDI gửi ở
    SP_FEE_COLLECT, lưu C_BO_EVENT_ID), KÈM trạng thái bút toán. KHÔNG có period/si_account.
    collected=1 (bút toán cắt thành công) → PAID (số cắt = amount nếu BO gửi, mặc định = C_FEE_DUE
      vì all-or-nothing). collected=0 (thất bại) → giữ UNPAID + clear C_BO_EVENT_ID (lần sau quét lại).
    JSON: [{"request_id":"<id SDI đã gửi>","collected":0|1,"amount":<tùy chọn>}]
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
        DECLARE @res TABLE (C_REQUEST_ID VARCHAR(64) NOT NULL, C_COLLECTED BIT, C_AMOUNT DECIMAL(20,0) NULL);
        INSERT @res
        SELECT j.request_id, ISNULL(j.collected,0), j.amount
        FROM OPENJSON(@p_json) WITH (request_id VARCHAR(64) '$.request_id',
                                     collected BIT '$.collected', amount DECIMAL(20,0) '$.amount') j
        WHERE j.request_id IS NOT NULL;
        BEGIN TRAN;
        -- cắt thành công → PAID (map theo request id = C_BO_EVENT_ID); số cắt mặc định = C_FEE_DUE (all-or-nothing)
        UPDATE fp SET fp.C_STATUS='PAID', fp.C_FEE_PAID=ISNULL(r.C_AMOUNT, fp.C_FEE_DUE), fp.C_COLLECTED_AT=GETDATE()
        FROM T_SI_FEE_CHARGE fp INNER JOIN @res r ON r.C_REQUEST_ID = fp.C_BO_EVENT_ID
        WHERE r.C_COLLECTED=1 AND fp.C_STATUS='UNPAID';
        -- thất bại → giữ UNPAID, clear request id để lần thu sau quét lại
        UPDATE fp SET fp.C_BO_EVENT_ID=NULL
        FROM T_SI_FEE_CHARGE fp INNER JOIN @res r ON r.C_REQUEST_ID = fp.C_BO_EVENT_ID
        WHERE r.C_COLLECTED=0 AND fp.C_STATUS='UNPAID';
        COMMIT;
    END TRY
    BEGIN CATCH IF @@TRANCOUNT>0 ROLLBACK; SET @p_err_code=-1; SET @p_err_msg=ERROR_MESSAGE(); END CATCH
END
GO

/*============================================================================
  UDF_SI_FEE_ACCRUING — phí QL kỳ HIỆN TẠI đang TÍCH LŨY (chưa chốt) của 1 SI (DECIMAL(20,6)).
    = Σ C_FEE_AMOUNT các dải HỢP LỆ (C_STATUS=1) trong T_SI_FEE_BALANCE mà kỳ CHƯA có charge
      (NOT EXISTS T_SI_FEE_CHARGE cùng SI+period = chưa tới ngày chốt). Số thập phân (chưa round VND).
    Kỳ đã chốt → balance bị loại (đã nằm ở charge/nợ) → KHÔNG double-count với UDF_SI_FEE_DEBT.
============================================================================*/
CREATE OR ALTER FUNCTION UDF_SI_FEE_ACCRUING (@p_si_account VARCHAR(20))
RETURNS DECIMAL(20,6)
AS
BEGIN
    RETURN ISNULL((SELECT SUM(b.C_FEE_AMOUNT)
                   FROM T_SI_FEE_BALANCE b
                   WHERE b.C_SI_ACCOUNT = @p_si_account AND b.C_STATUS = 1
                     AND NOT EXISTS (SELECT 1 FROM T_SI_FEE_CHARGE c
                                     WHERE c.C_SI_ACCOUNT = b.C_SI_ACCOUNT AND c.C_PERIOD = b.C_PERIOD)), 0);
END
GO

/*============================================================================
  UDF_SI_FEE_DEBT — NỢ PHÍ QL của 1 SI TẠI THỜI ĐIỂM HIỆN TẠI (DECIMAL(20,6)).
    = (A) nợ ĐÃ CHỐT KỲ chưa thu: Σ(C_FEE_DUE − C_FEE_PAID) kỳ UNPAID (T_SI_FEE_CHARGE, VND)
    + (B) phí kỳ HIỆN TẠI đang TÍCH LŨY chưa chốt: UDF_SI_FEE_ACCRUING (thập phân, ước tính tới nay).
    Phần (B) là ước tính (chưa chốt) → caller ROUND nếu cần hiển thị VND. Trả 0 nếu không nợ.
    Chỉ cần phần đã treo (collectible) → dùng riêng: nợ = DEBT − ACCRUING (hoặc query UNPAID charge).
============================================================================*/
CREATE OR ALTER FUNCTION UDF_SI_FEE_DEBT (@p_si_account VARCHAR(20))
RETURNS DECIMAL(20,6)
AS
BEGIN
    -- CAST nợ về (20,0) trước khi cộng: SUM(20,0)→(38,0), cộng thẳng (20,6) sẽ bị ép scale=0 (mất thập phân).
    RETURN CAST(ISNULL((SELECT SUM(C_FEE_DUE - C_FEE_PAID)
                        FROM T_SI_FEE_CHARGE
                        WHERE C_SI_ACCOUNT = @p_si_account AND C_STATUS = 'UNPAID'), 0) AS DECIMAL(20,0))
         + dbo.UDF_SI_FEE_ACCRUING(@p_si_account);
END
GO

/*============================================================================
  BÁO CÁO PHÍ QL (read-only, range/list — miễn date-guard). 3 SP theo BRD WS3.1 §Báo cáo:
    (1) SP_RPT_FEE_DAILY      — Danh sách SINH PHÍ HÀNG NGÀY (per-day accrual)
    (2) SP_RPT_FEE_CHARGE     — Danh sách CHỐT PHÍ HÀNG KỲ (Nợ Phí QL)
    (3) SP_RPT_FEE_COLLECTION — Danh sách GIAO DỊCH THU PHÍ (đã gửi BO / đã thu)
  Mọi filter NULL = bỏ qua (lấy tất cả). Chỉ trả result-set, không OUTPUT err.
============================================================================*/

/*---- (1) SINH PHÍ HÀNG NGÀY — nguồn T_SI_FEE_BALANCE (dòng hợp lệ C_STATUS=1) ----*/
CREATE OR ALTER PROCEDURE SP_RPT_FEE_DAILY
    @p_from_date   DATE,                      -- lọc theo C_FEE_DATE [from,to]
    @p_to_date     DATE,
    @p_si_account  VARCHAR(20) = NULL,
    @p_master_code VARCHAR(20) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SELECT b.C_SI_ACCOUNT, b.C_CUST_CODE, b.C_MASTER_CODE,
           b.C_FEE_DATE, b.C_ACCRUED_ON, b.C_PERIOD,
           b.C_AUM, b.C_RATE, b.C_DAY_COUNT, b.C_FEE_AMOUNT
    FROM T_SI_FEE_BALANCE b
    WHERE b.C_STATUS = 1
      AND b.C_FEE_DATE BETWEEN @p_from_date AND @p_to_date
      AND (@p_si_account  IS NULL OR b.C_SI_ACCOUNT  = @p_si_account)
      AND (@p_master_code IS NULL OR b.C_MASTER_CODE = @p_master_code)
    ORDER BY b.C_SI_ACCOUNT, b.C_FEE_DATE;
END
GO

/*---- (2) CHỐT PHÍ HÀNG KỲ / NỢ PHÍ QL — nguồn T_SI_FEE_CHARGE ----*/
CREATE OR ALTER PROCEDURE SP_RPT_FEE_CHARGE
    @p_period      CHAR(6)     = NULL,        -- YYYYMM; NULL = mọi kỳ
    @p_si_account  VARCHAR(20) = NULL,
    @p_master_code VARCHAR(20) = NULL,
    @p_status      VARCHAR(10) = NULL         -- NULL | 'UNPAID' | 'PAID'
AS
BEGIN
    SET NOCOUNT ON;
    SELECT c.C_SI_ACCOUNT, c.C_CUST_CODE, c.C_MASTER_CODE, c.C_PERIOD,
           c.C_PERIOD_FROM, c.C_PERIOD_TO,
           c.C_FEE_TOTAL, c.C_FEE_DUE, c.C_FEE_PAID,
           C_FEE_REMAIN = c.C_FEE_DUE - c.C_FEE_PAID,   -- còn nợ
           c.C_STATUS, c.C_CLOSED_AT, c.C_COLLECTED_AT, c.C_BO_EVENT_ID
    FROM T_SI_FEE_CHARGE c
    WHERE (@p_period      IS NULL OR c.C_PERIOD      = @p_period)
      AND (@p_si_account  IS NULL OR c.C_SI_ACCOUNT  = @p_si_account)
      AND (@p_master_code IS NULL OR c.C_MASTER_CODE = @p_master_code)
      AND (@p_status      IS NULL OR c.C_STATUS      = @p_status)
    ORDER BY c.C_SI_ACCOUNT, c.C_PERIOD;
END
GO

/*---- (3) GIAO DỊCH THU PHÍ — charge đã gửi BO (có request id) HOẶC đã PAID ----*/
CREATE OR ALTER PROCEDURE SP_RPT_FEE_COLLECTION
    @p_from_date   DATE        = NULL,        -- lọc theo C_COLLECTED_AT [from,to] (NULL = tất cả)
    @p_to_date     DATE        = NULL,
    @p_si_account  VARCHAR(20) = NULL,
    @p_master_code VARCHAR(20) = NULL,
    @p_status      VARCHAR(10) = NULL         -- NULL | 'UNPAID'(đã gửi chờ kết quả) | 'PAID'
AS
BEGIN
    SET NOCOUNT ON;
    SELECT c.C_SI_ACCOUNT, c.C_CUST_CODE, c.C_MASTER_CODE, c.C_PERIOD,
           c.C_BO_EVENT_ID AS C_REQUEST_ID, c.C_FEE_DUE, c.C_FEE_PAID,
           c.C_STATUS, c.C_COLLECTED_AT
    FROM T_SI_FEE_CHARGE c
    WHERE (c.C_BO_EVENT_ID IS NOT NULL OR c.C_STATUS = 'PAID')   -- đã phát sinh giao dịch thu
      AND (@p_from_date   IS NULL OR c.C_COLLECTED_AT >= @p_from_date)
      AND (@p_to_date     IS NULL OR c.C_COLLECTED_AT <  DATEADD(DAY,1,@p_to_date))
      AND (@p_si_account  IS NULL OR c.C_SI_ACCOUNT  = @p_si_account)
      AND (@p_master_code IS NULL OR c.C_MASTER_CODE = @p_master_code)
      AND (@p_status      IS NULL OR c.C_STATUS      = @p_status)
    ORDER BY c.C_COLLECTED_AT, c.C_SI_ACCOUNT, c.C_PERIOD;
END
GO
