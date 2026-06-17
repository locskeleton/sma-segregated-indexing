/*==============================================================================
  SDI MODULE — TABLES (SQL Server)
  Naming convention:
    - Bảng:  prefix T_  (thay cho sdi_) , UPPERCASE
    - Cột:   prefix C_  , UPPERCASE
    - PK:    constraint prefix PK_
    - FK:    KHÔNG hard-set constraint trong code; chỉ dùng NAMING để tham chiếu
             (ghi chú dạng "FK_<child>__<parent>: C_xxx -> T_PARENT.C_xxx")
    - Stored proc: prefix SP_   |   Function: prefix UDF_
  Tiền: DECIMAL(20,4) VND | Unit: DECIMAL(38,10) | Giá/tỷ trọng: DECIMAL
  Prod: chuyển các bảng lớn sang ON ps_year(C_BUSINESS_DATE) + CCI (xem 00_INFRA.sql)
==============================================================================*/

/*------------------------------------------------------------------ MASTER ---*/
CREATE TABLE T_MASTER_PORTFOLIO (
    C_SI_ID          BIGINT          NOT NULL,
    C_SI_CODE        VARCHAR(20)     NOT NULL,
    C_SI_NAME        NVARCHAR(200)   NULL,
    C_STATUS         VARCHAR(10)     NOT NULL CONSTRAINT DF_MASTER_PORTFOLIO_STATUS DEFAULT 'ACTIVE', -- ACTIVE|CLOSED
    C_INCEPTION_DATE DATE            NULL,
    C_MGMT_FEE_RATE  DECIMAL(9,6)    NOT NULL CONSTRAINT DF_MASTER_PORTFOLIO_FEE DEFAULT 0.01,        -- %/năm
    C_BENCHMARK_ID   BIGINT          NULL,
    CONSTRAINT PK_MASTER_PORTFOLIO PRIMARY KEY (C_SI_ID)
);

-- Danh mục mẫu (FO tính & feed). Σ C_TARGET_WEIGHT theo (C_SI_ID, C_EFFECTIVE_DATE) = 1.0 (100% cổ phiếu)
CREATE TABLE T_MASTER_PORTFOLIO_TICKER (
    C_SI_ID          BIGINT          NOT NULL,
    C_EFFECTIVE_DATE DATE            NOT NULL,
    C_TICKER         VARCHAR(20)     NOT NULL,
    C_TARGET_WEIGHT  DECIMAL(12,8)   NOT NULL,
    CONSTRAINT PK_MASTER_PORTFOLIO_TICKER PRIMARY KEY (C_SI_ID, C_EFFECTIVE_DATE, C_TICKER)
    -- FK_MASTER_PORTFOLIO_TICKER__MASTER_PORTFOLIO: C_SI_ID -> T_MASTER_PORTFOLIO.C_SI_ID
);

-- Cấu hình đầu tư KH (1 dòng = 1 tiểu khoản = KH x SI)
CREATE TABLE T_INDEXING_PORTFOLIO (
    C_CUSTOMER_ID    BIGINT          NOT NULL,
    C_SI_ID          BIGINT          NOT NULL,
    C_SUB_ACCOUNT_NO VARCHAR(32)     NULL,
    C_JOIN_DATE      DATE            NOT NULL,
    C_STATUS         VARCHAR(10)     NOT NULL CONSTRAINT DF_INDEXING_PORTFOLIO_STATUS DEFAULT 'ACTIVE',
    C_INITIAL_AMOUNT DECIMAL(20,4)   NULL,
    C_SIP_AMOUNT     DECIMAL(20,4)   NULL,
    C_SIP_SCHEDULE   VARCHAR(50)     NULL,
    C_MGMT_FEE_RATE  DECIMAL(9,6)    NULL,    -- override; NULL = lấy theo T_MASTER_PORTFOLIO
    C_MIN_INVEST     DECIMAL(20,4)   NULL,
    CONSTRAINT PK_INDEXING_PORTFOLIO PRIMARY KEY (C_CUSTOMER_ID, C_SI_ID)
    -- FK_INDEXING_PORTFOLIO__MASTER_PORTFOLIO: C_SI_ID -> T_MASTER_PORTFOLIO.C_SI_ID
);

/*-------------------------------------------------------------- MARKET DATA ---*/
CREATE TABLE T_PRICE_DAILY (
    C_TICKER            VARCHAR(20)  NOT NULL,
    C_BUSINESS_DATE     DATE         NOT NULL,
    C_CLOSE_PRICE       DECIMAL(18,4) NOT NULL,
    C_ADJUSTED_REF_PRICE DECIMAL(18,4) NULL,   -- giá tham chiếu điều chỉnh quyền (nếu có CA)
    CONSTRAINT PK_PRICE_DAILY PRIMARY KEY (C_BUSINESS_DATE, C_TICKER)
);

CREATE TABLE T_CORPORATE_ACTION (
    C_TICKER             VARCHAR(20)  NOT NULL,
    C_EX_DATE            DATE         NOT NULL,
    C_CA_TYPE            VARCHAR(20)  NOT NULL,  -- CASH_DIV | STOCK_DIV | SPLIT | RIGHTS
    C_RATIO              DECIMAL(18,8) NULL,     -- vd split/stock-div ratio
    C_CASH_DIV_PER_SHARE DECIMAL(18,4) NULL,
    C_ADJUSTED_REF_PRICE DECIMAL(18,4) NULL,
    CONSTRAINT PK_CORPORATE_ACTION PRIMARY KEY (C_TICKER, C_EX_DATE, C_CA_TYPE)
);

CREATE TABLE T_BENCHMARK_DAILY (
    C_BENCHMARK_ID   BIGINT          NOT NULL,
    C_BUSINESS_DATE  DATE            NOT NULL,
    C_INDEX_VALUE    DECIMAL(18,4)   NOT NULL,
    CONSTRAINT PK_BENCHMARK_DAILY PRIMARY KEY (C_BENCHMARK_ID, C_BUSINESS_DATE)
);

/*------------------------------------------------------- EVENT / LEDGER -------*/
-- SDI -> FO : trigger rebalance (KHÔNG chứa weights)
CREATE TABLE T_REBALANCE_REQUEST (
    C_REQUEST_ID     BIGINT IDENTITY(1,1) NOT NULL,
    C_SI_ID          BIGINT          NOT NULL,
    C_BUSINESS_DATE  DATE            NOT NULL,
    C_TYPE           VARCHAR(20)     NOT NULL,  -- REBALANCE | DEPLOY | REDEEM
    C_STATUS         VARCHAR(20)     NOT NULL CONSTRAINT DF_REBREQ_STATUS DEFAULT 'NEW',
    CONSTRAINT PK_REBALANCE_REQUEST PRIMARY KEY (C_REQUEST_ID)
);

-- FO -> SDI : ĐỒNG BỘ EOD holdings (per-KH) = SNAPSHOT DATED thuần.
--   SDI KHÔNG quản lý từng lệnh khớp (FO lo) → không có T_EXECUTION_FEED.
--   FO đổ snapshot holdings vào đây mỗi EOD; engine mirror sang current (overwrite).
--   Cũng là nguồn AUDIT + tái dựng holdings lịch sử (THAY T_CUSTOMER_HOLDING_EVENT cũ).
--   Biến động NET/ngày KHÔNG lưu sẵn — suy ra on-demand = qty(D) − qty(D-1) (LAG/self-join 2 snapshot).
CREATE TABLE T_CUSTOMER_HOLDING_DAILY (
    C_BUSINESS_DATE  DATE            NOT NULL,
    C_CUSTOMER_ID    BIGINT          NOT NULL,
    C_SI_ID          BIGINT          NOT NULL,
    C_TICKER         VARCHAR(20)     NOT NULL,
    C_QUANTITY       DECIMAL(20,4)   NOT NULL,
    C_AVG_COST       DECIMAL(18,4)   NULL,
    CONSTRAINT PK_CUSTOMER_HOLDING_DAILY PRIMARY KEY (C_BUSINESS_DATE, C_CUSTOMER_ID, C_SI_ID, C_TICKER)
) WITH (DATA_COMPRESSION = PAGE);
-- Prod: CCI + partition theo năm (history ~20M/ngày).
CREATE TABLE T_FO_CASH_SYNC (
    C_BUSINESS_DATE  DATE            NOT NULL,
    C_CUSTOMER_ID    BIGINT          NOT NULL,
    C_SI_ID          BIGINT          NOT NULL,
    C_CASH           DECIMAL(20,4)   NOT NULL,    -- tổng tiền tài khoản (FO đã phản ánh trade/cổ tức/settlement)
    CONSTRAINT PK_FO_CASH_SYNC PRIMARY KEY (C_BUSINESS_DATE, C_CUSTOMER_ID, C_SI_ID)
);

-- External cashflow (KHÔNG chứa income)
CREATE TABLE T_CASHFLOW_EVENT (
    C_EVENT_ID       BIGINT IDENTITY(1,1) NOT NULL,
    C_CUSTOMER_ID    BIGINT          NOT NULL,
    C_SI_ID          BIGINT          NOT NULL,
    C_BUSINESS_DATE  DATE            NOT NULL,
    C_EVENT_TYPE     VARCHAR(20)     NOT NULL,  -- INITIAL|TOPUP|SIP|INTEREST_IN|WITHDRAW
    C_AMOUNT         DECIMAL(20,4)   NOT NULL,  -- luôn dương; chiều theo C_EVENT_TYPE
    C_CREATED_TIME   DATETIME2       NOT NULL CONSTRAINT DF_CF_CREATED DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_CASHFLOW_EVENT PRIMARY KEY (C_EVENT_ID)
);
CREATE INDEX IX_CASHFLOW_EVENT_DATE ON T_CASHFLOW_EVENT (C_BUSINESS_DATE) INCLUDE (C_CUSTOMER_ID, C_SI_ID, C_EVENT_TYPE, C_AMOUNT);

-- (T_CUSTOMER_HOLDING_EVENT đã BỎ — biến động/ngày suy ra từ snapshot T_CUSTOMER_HOLDING_DAILY khi cần.)

-- Unit thay đổi (ghi dòng khi có cashflow)
CREATE TABLE T_UNIT_LEDGER (
    C_CUSTOMER_ID    BIGINT          NOT NULL,
    C_SI_ID          BIGINT          NOT NULL,
    C_BUSINESS_DATE  DATE            NOT NULL,
    C_CF_NET         DECIMAL(20,4)   NOT NULL,
    C_DELTA_UNIT     DECIMAL(38,10)  NOT NULL,
    C_UNIT           DECIMAL(38,10)  NOT NULL,
    CONSTRAINT PK_UNIT_LEDGER PRIMARY KEY (C_CUSTOMER_ID, C_SI_ID, C_BUSINESS_DATE)
);

/*------------------------------------------- CURRENT STATE (roll-forward) -----*/
-- NAV/state HIỆN TẠI per tiểu khoản (KH×SI) — 1 dòng, cập nhật tại chỗ mỗi EOD.
-- Cache hot cho roll-forward + đọc current nhanh (≠ T_CUSTOMER_NAV_DAILY = lịch sử).
-- Cấp SI không có bản current riêng (query T_SI_NAV_DAILY ngày mới nhất — ~250K dòng, rẻ).
CREATE TABLE T_CUSTOMER_NAV_CURRENT (
    C_CUSTOMER_ID      BIGINT        NOT NULL,
    C_SI_ID            BIGINT        NOT NULL,
    C_UNIT             DECIMAL(38,10) NOT NULL CONSTRAINT DF_CNC_UNIT DEFAULT 0,
    C_CASH             DECIMAL(20,4)  NOT NULL CONSTRAINT DF_CNC_CASH DEFAULT 0,
    C_PAYABLE_FEE      DECIMAL(20,6)  NOT NULL CONSTRAINT DF_CNC_PAY DEFAULT 0, -- phí quản lý lũy kế (accrue)
    C_LAST_NAV         DECIMAL(20,4)  NOT NULL CONSTRAINT DF_CNC_NAV DEFAULT 0,
    C_LAST_UNIT_PRICE  DECIMAL(28,10) NULL,
    C_STATUS           VARCHAR(10)    NOT NULL CONSTRAINT DF_CNC_STATUS DEFAULT 'ACTIVE', -- vòng đời tiểu khoản: ACTIVE | CLOSED…
    C_LAST_BUSINESS_DATE DATE         NULL,
    CONSTRAINT PK_CUSTOMER_NAV_CURRENT PRIMARY KEY (C_CUSTOMER_ID, C_SI_ID)
) WITH (DATA_COMPRESSION = PAGE);

-- Holdings hiện tại (~20M) — MIRROR từ snapshot (T_CUSTOMER_HOLDING_DAILY), overwrite mỗi EOD.
-- Prod: thêm NONCLUSTERED COLUMNSTORE cho MTM (HTAP)
CREATE TABLE T_INDEXING_PORTFOLIO_TICKER (
    C_CUSTOMER_ID    BIGINT          NOT NULL,
    C_SI_ID          BIGINT          NOT NULL,
    C_TICKER         VARCHAR(20)     NOT NULL,
    C_QUANTITY       DECIMAL(20,4)   NOT NULL,
    C_AVG_COST       DECIMAL(18,4)   NULL,
    CONSTRAINT PK_INDEXING_PORTFOLIO_TICKER PRIMARY KEY (C_CUSTOMER_ID, C_SI_ID, C_TICKER)
) WITH (DATA_COMPRESSION = PAGE);
-- Prod: CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_INDEXING_PORTFOLIO_TICKER
--       ON T_INDEXING_PORTFOLIO_TICKER (C_CUSTOMER_ID, C_SI_ID, C_TICKER, C_QUANTITY);

/*--------------------------------------------- BẢNG WORK (transient/EOD) ------*/
-- Giá trị tính trong ngày @d (xóa/ghi lại mỗi run) — nguồn để update state + ghi daily
CREATE TABLE T_EOD_WORK (
    C_BUSINESS_DATE   DATE           NOT NULL,
    C_CUSTOMER_ID     BIGINT         NOT NULL,
    C_SI_ID           BIGINT         NOT NULL,
    C_CASH            DECIMAL(20,4)  NOT NULL DEFAULT 0,
    C_PAYABLE_FEE     DECIMAL(20,6)  NOT NULL DEFAULT 0,
    C_LAST_NAV        DECIMAL(20,4)  NOT NULL DEFAULT 0,
    C_LAST_UNIT_PRICE DECIMAL(28,10) NULL,
    C_UNIT_PREV       DECIMAL(38,10) NOT NULL DEFAULT 0,
    C_CF_IN           DECIMAL(20,4)  NOT NULL DEFAULT 0,
    C_CF_OUT          DECIMAL(20,4)  NOT NULL DEFAULT 0,
    C_STOCK_VALUE     DECIMAL(20,4)  NOT NULL DEFAULT 0,
    C_NAV             DECIMAL(20,4)  NOT NULL DEFAULT 0,
    C_DAILY_PNL       DECIMAL(20,4)  NOT NULL DEFAULT 0,
    C_DELTA_UNIT      DECIMAL(38,10) NOT NULL DEFAULT 0,
    C_UNIT            DECIMAL(38,10) NOT NULL DEFAULT 0,
    C_UNIT_PRICE      DECIMAL(28,10) NULL,
    CONSTRAINT PK_EOD_WORK PRIMARY KEY (C_BUSINESS_DATE, C_CUSTOMER_ID, C_SI_ID)
);

/*--------------- PER-KH NAV DAILY (lịch sử — cần cho chart FR-03) -------------*/
-- NAV/hiệu suất theo (KH×SI×ngày). Vì FO chỉ sync snapshot (overwrite) → holdings KHÔNG
-- còn event-source → KHÔNG derive được NAV/unit_price quá khứ → phải MATERIALIZE mỗi ngày.
-- Quy mô ~2,5 tỷ dòng/10 năm → prod: CLUSTERED COLUMNSTORE + partition (xem SDI-db-architecture).
-- (Giảm tải: điểm thưa tuần/tháng, hoặc chỉ lưu unit_price.)
-- Composition cash/stock + cổ tức/phí per-KH KHÔNG ở đây (sparse → T_CUSTOMER_FEE_INCOME; cash/stock reconstruct).
CREATE TABLE T_CUSTOMER_NAV_DAILY (
    C_BUSINESS_DATE  DATE            NOT NULL,
    C_CUSTOMER_ID    BIGINT          NOT NULL,
    C_SI_ID          BIGINT          NOT NULL,
    C_NAV            DECIMAL(20,4)   NOT NULL,
    C_UNIT           DECIMAL(38,10)  NOT NULL,
    C_UNIT_PRICE     DECIMAL(28,10)  NULL,
    C_DAILY_PNL      DECIMAL(20,4)   NOT NULL,
    C_DAILY_RETURN   DECIMAL(18,10)  NULL,
    CONSTRAINT PK_CUSTOMER_NAV_DAILY PRIMARY KEY (C_BUSINESS_DATE, C_CUSTOMER_ID, C_SI_ID)
);

-- Sổ cái CỔ TỨC + PHÍ per (KH×SI), SPARSE (chỉ ghi khi có sự kiện) — FO đẩy về.
-- KHÔNG derive được từ NAV/holdings (dòng tiền/sự kiện ngoài) → phải capture lúc phát sinh
-- cho báo cáo tài sản FR-06. SI-level: J11 SUM bảng này → cash_dividend/custody_fee/mgmt_fee_accrued
-- của T_SI_NAV_DAILY. (KHÔNG ảnh hưởng NAV — phương án A: FO cash đã NET; đây là thông tin tham khảo.)
CREATE TABLE T_CUSTOMER_FEE_INCOME (
    C_EVENT_ID       BIGINT IDENTITY(1,1) NOT NULL,
    C_BUSINESS_DATE  DATE            NOT NULL,
    C_CUSTOMER_ID    BIGINT          NOT NULL,
    C_SI_ID          BIGINT          NOT NULL,
    C_TYPE           VARCHAR(20)     NOT NULL,  -- DIVIDEND | CUSTODY_FEE | MGMT_FEE
    C_TICKER         VARCHAR(20)     NULL,      -- mã (cho DIVIDEND); NULL cho phí cấp tài khoản
    C_AMOUNT         DECIMAL(20,4)   NOT NULL,  -- luôn dương; ý nghĩa theo C_TYPE
    C_SOURCE         VARCHAR(10)     NOT NULL CONSTRAINT DF_CFI_SRC DEFAULT 'FO',
    C_CREATED_TIME   DATETIME2       NOT NULL CONSTRAINT DF_CFI_CREATED DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_CUSTOMER_FEE_INCOME PRIMARY KEY (C_EVENT_ID)
);
CREATE INDEX IX_CUSTOMER_FEE_INCOME_DATE ON T_CUSTOMER_FEE_INCOME (C_BUSINESS_DATE)
    INCLUDE (C_CUSTOMER_ID, C_SI_ID, C_TYPE, C_AMOUNT);

/*------------------------------------------------ SI-LEVEL DAILY (output) -----*/
-- (T_SI_PERFORMANCE_DAILY đã GỘP vào T_SI_NAV_DAILY: nav/unit/unit_price/daily_pnl/daily_return.)

CREATE TABLE T_SI_INDEX_DAILY (
    C_BUSINESS_DATE  DATE            NOT NULL,
    C_SI_ID          BIGINT          NOT NULL,
    C_INDEX_VALUE    DECIMAL(18,6)   NOT NULL,
    C_DAILY_RETURN   DECIMAL(18,10)  NULL,
    CONSTRAINT PK_SI_INDEX_DAILY PRIMARY KEY (C_BUSINESS_DATE, C_SI_ID)
);

CREATE TABLE T_SI_HOLDING_DAILY (
    C_BUSINESS_DATE  DATE            NOT NULL,
    C_SI_ID          BIGINT          NOT NULL,
    C_TICKER         VARCHAR(20)     NOT NULL,
    C_QUANTITY       DECIMAL(20,4)   NOT NULL,
    C_MARKET_PRICE   DECIMAL(18,4)   NOT NULL,
    C_MARKET_VALUE   DECIMAL(20,4)   NOT NULL,
    C_WEIGHT         DECIMAL(12,8)   NULL,
    CONSTRAINT PK_SI_HOLDING_DAILY PRIMARY KEY (C_BUSINESS_DATE, C_SI_ID, C_TICKER)
);

-- NAV cấp SI/ngày — NGUỒN NAV SI-level DUY NHẤT (composition tài sản + hiệu suất).
-- Gộp từ T_ASSET_SNAPSHOT_DAILY + T_SI_PERFORMANCE_DAILY cũ (cùng grain date×si).
-- cash_dividend/custody_fee/mgmt_fee_accrued = J11 SUM từ T_CUSTOMER_FEE_INCOME (per-KH) lên SI.
CREATE TABLE T_SI_NAV_DAILY (
    C_BUSINESS_DATE    DATE          NOT NULL,
    C_SI_ID            BIGINT        NOT NULL,
    -- composition tài sản
    C_CASH             DECIMAL(20,4) NOT NULL,
    C_STOCK_VALUE      DECIMAL(20,4) NOT NULL,
    C_CASH_DIVIDEND    DECIMAL(20,4) NULL,
    C_CUSTODY_FEE      DECIMAL(20,4) NULL,
    C_MGMT_FEE_ACCRUED DECIMAL(20,6) NULL,    -- FO báo cáo tham khảo (SDI không tự accrue)
    C_PAYABLE_FEE      DECIMAL(20,6) NULL,
    C_TOTAL_ASSET      DECIMAL(20,4) NOT NULL,
    -- NAV + hiệu suất
    C_NAV              DECIMAL(20,4)  NOT NULL,
    C_UNIT             DECIMAL(38,10) NOT NULL,
    C_UNIT_PRICE       DECIMAL(28,10) NULL,
    C_DAILY_PNL        DECIMAL(20,4)  NOT NULL,
    C_DAILY_RETURN     DECIMAL(18,10) NULL,
    CONSTRAINT PK_SI_NAV_DAILY PRIMARY KEY (C_BUSINESS_DATE, C_SI_ID)
);

-- NAV/state HIỆN TẠI cấp SI — 1 dòng/SI (~100), overwrite mỗi EOD (J11 MERGE từ T_SI_NAV_DAILY @d).
-- Đối xứng T_CUSTOMER_NAV_CURRENT; phục vụ ĐỌC current toàn bộ quỹ (FR-01 overview, monitor AUM)
-- khỏi WHERE date=MAX. KHÔNG dùng cho TÍNH EOD (SI agg lại tươi từ per-KH mỗi ngày, không roll-forward).
CREATE TABLE T_SI_NAV_CURRENT (
    C_SI_ID            BIGINT         NOT NULL,
    C_CASH             DECIMAL(20,4)  NOT NULL CONSTRAINT DF_SNC_CASH DEFAULT 0,
    C_STOCK_VALUE      DECIMAL(20,4)  NOT NULL CONSTRAINT DF_SNC_STOCK DEFAULT 0,
    C_TOTAL_ASSET      DECIMAL(20,4)  NOT NULL CONSTRAINT DF_SNC_TOTAL DEFAULT 0,
    C_LAST_NAV         DECIMAL(20,4)  NOT NULL CONSTRAINT DF_SNC_NAV DEFAULT 0,
    C_UNIT             DECIMAL(38,10) NOT NULL CONSTRAINT DF_SNC_UNIT DEFAULT 0,
    C_LAST_UNIT_PRICE  DECIMAL(28,10) NULL,
    C_LAST_BUSINESS_DATE DATE         NULL,
    CONSTRAINT PK_SI_NAV_CURRENT PRIMARY KEY (C_SI_ID)
);

/*------------------------------------------------ CONTROL / ORCHESTRATION -----*/
CREATE TABLE T_EOD_RUN (
    C_BUSINESS_DATE  DATE            NOT NULL,
    C_JOB            VARCHAR(40)     NOT NULL,
    C_STATUS         VARCHAR(10)     NOT NULL,  -- PENDING|RUNNING|DONE|FAILED
    C_ROWS           BIGINT          NULL,
    C_STARTED_AT     DATETIME2       NULL,
    C_ENDED_AT       DATETIME2       NULL,
    C_MESSAGE        NVARCHAR(2000)  NULL,
    CONSTRAINT PK_EOD_RUN PRIMARY KEY (C_BUSINESS_DATE, C_JOB)
);
GO
