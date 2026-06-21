SET QUOTED_IDENTIFIER ON;  -- bắt buộc cho filtered index (open-row); sqlcmd mặc định OFF
SET ANSI_NULLS ON;
GO
/*==============================================================================
  SDI MODULE — TABLES (SQL Server)
  HAI CẤP:
    - MASTER (danh mục mẫu/chiến lược): mã `C_MASTER_CODE` (PK T_MASTER_PORTFOLIO).
      Bảng tổng hợp master-level: T_MASTER_PORTFOLIO(_TICKER), T_MASTER_NAV_BALANCE,
      T_MASTER_HOLDING_BALANCE, T_MASTER_INDEX_DAILY, T_MASTER_NAV_CURRENT (key C_MASTER_CODE).
    - SUB-ACCOUNT (tiểu khoản, customer-level): KH đầu tư 1 master → cấp 1 sub-account, mã
      `C_SI_ACCOUNT` (VARCHAR20, = CUST_CODE + đuôi, sinh khi mở). Close + reopen master ⇒ sub-account
      MỚI (mã khác) → 1 KH có NHIỀU sub-account/master theo thời gian (tối đa 1 ACTIVE/lúc).
      ⇒ MỌI bảng customer-level KHÓA theo `C_SI_ACCOUNT` (giữ C_CUST_CODE, C_MASTER_CODE denormalized
      để query + tổng hợp master).
  Naming: T_/C_ UPPERCASE. Khóa surrogate public GUID `PK_<table>` (NEWID, IDOR-safe). Clustered theo
    tải: append-fact → BIGINT IDENTITY (PK_<t>_ID); point/join → natural (PK_<t>_NK); nhỏ → GUID.
    Natural giữ UQ_<t>_NK (idempotency). T_MASTER_PORTFOLIO: PK = C_MASTER_CODE (không GUID).
  DECIMAL: Tiền & Quantity = (20,0); Giá = (18,4); % / return / fee_rate = (10,6); phí lũy kế ngày (payable/accrued, net-off định kỳ) = (20,6);
    Unit & Unit Price = (18,6); Weight (12,8); CA ratio (18,8); index_value (18,x).
==============================================================================*/

/*------------------------------------------------------------------ MASTER ---*/
CREATE TABLE T_MASTER_PORTFOLIO (
    C_MASTER_CODE    VARCHAR(20)     NOT NULL,
    C_MASTER_NAME        NVARCHAR(200)   NULL,
    C_STATUS         VARCHAR(10)     NOT NULL CONSTRAINT DF_MASTER_PORTFOLIO_STATUS DEFAULT 'ACTIVE', -- ACTIVE|CLOSED
    C_INCEPTION_DATE DATE            NULL,
    -- (phí QL KHÔNG còn cột rate ở đây: chính sách phí cấu hình ở T_FEE_CONFIG theo (master,fee_type).)
    C_BENCHMARK_CODE VARCHAR(20)     NULL,   -- benchmark đối chiếu (vd 'VNINDEX','VN30') → T_BENCHMARK_DAILY
    CONSTRAINT PK_MASTER_PORTFOLIO PRIMARY KEY (C_MASTER_CODE)
);

-- CATALOG CHÍNH SÁCH PHÍ/THUẾ hệ thống per (master × loại phí) — KHÔNG chỉ cho phí lũy kế.
--   Khai mọi loại phí/thu nhập của master với NHÓM hạch toán + (nếu accrue) rate. Sau này dùng làm nguồn
--   quản lý/cài đặt chính sách phí, thuế cho hệ thống. Thêm loại mới = INSERT 1 dòng, KHÔNG sửa schema/SP.
--   ⚠️ C_FEE_TYPE + C_FEE_GROUP DÙNG CHUNG VOCABULARY với T_SI_FEE_LEDGER (mã phí + nhóm phải KHỚP giữa 2 bảng).
--   ACCRUE (J06): CHỈ loại C_FEE_GROUP='PAYABLE' AND C_RATE>0 → payable += AUM_gross × Σ(C_RATE/C_DAY_COUNT) × ngày.
--   Loại không rate (custody/thuế GD point-event, hoặc INCOME như cổ tức) → C_RATE NULL ⇒ KHÔNG accrue.
CREATE TABLE T_FEE_CONFIG (
    PK_FEE_CONFIG UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_FEE_CONFIG_PKID DEFAULT NEWID(),
    C_MASTER_CODE    VARCHAR(20)     NOT NULL,
    C_FEE_TYPE       VARCHAR(20)     NOT NULL,   -- MGMT_FEE|TAX|PERF_FEE|CUSTODY_FEE|DIVIDEND|... (KHỚP T_SI_FEE_LEDGER)
    C_FEE_GROUP      VARCHAR(20)     NOT NULL,   -- INCOME | PAYABLE (KHỚP C_FEE_GROUP của T_SI_FEE_LEDGER)
    C_RATE           DECIMAL(10,6)   NULL,       -- %/NĂM (vd 0.01=1%/năm) cho loại ACCRUE; NULL ⇒ không accrue (point-event/income)
    C_DAY_COUNT      SMALLINT        NOT NULL CONSTRAINT DF_FEE_CONFIG_DAYCOUNT DEFAULT 365,  -- mẫu số quy đổi ngày (accrue)
    C_UPDATED_BY     VARCHAR(64)     NULL,
    C_UPDATED_TIME   DATETIME        NOT NULL CONSTRAINT DF_FEE_CONFIG_TIME DEFAULT GETDATE(),
    CONSTRAINT PK_FEE_CONFIG PRIMARY KEY CLUSTERED (C_MASTER_CODE, C_FEE_TYPE),
    CONSTRAINT UQ_FEE_CONFIG_GUID UNIQUE (PK_FEE_CONFIG)
);

-- Danh mục mẫu (FO tính & feed). Σ C_TARGET_WEIGHT theo (C_MASTER_CODE, C_EFFECTIVE_DATE) = 1.0
CREATE TABLE T_MASTER_PORTFOLIO_TICKER (
    PK_MASTER_PORTFOLIO_TICKER UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_MASTER_PORTFOLIO_TICKER_PKID DEFAULT NEWID(),
    C_MASTER_CODE    VARCHAR(20)     NOT NULL,
    C_EFFECTIVE_DATE DATE            NOT NULL,   -- ngày hiệu lực trọng số = mốc REBALANCE (J12 dùng latest eff_date ≤ @d)
    C_TICKER         VARCHAR(20)     NOT NULL,
    C_TARGET_WEIGHT  DECIMAL(12,8)   NOT NULL,   -- trọng số mục tiêu mã trong danh mục mẫu; Σ theo (master,eff_date)=1.0. Vào công thức index J12 (Σ wᵢ·Pᵢ,t/P_ref)
    CONSTRAINT PK_MASTER_PORTFOLIO_TICKER PRIMARY KEY CLUSTERED (PK_MASTER_PORTFOLIO_TICKER),
    CONSTRAINT UQ_MASTER_PORTFOLIO_TICKER_NK UNIQUE (C_MASTER_CODE, C_EFFECTIVE_DATE, C_TICKER)
);

-- SUB-ACCOUNT (tiểu khoản): 1 dòng/sub-account. C_SI_ACCOUNT = mã sub-account (CUST_CODE+đuôi), UNIQUE.
--   Close+reopen master ⇒ dòng MỚI (C_SI_ACCOUNT mới). Tối đa 1 ACTIVE / (KH×master) tại 1 thời điểm.
CREATE TABLE T_SI_PORTFOLIO (
    PK_SI_PORTFOLIO UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_SI_PORTFOLIO_PKID DEFAULT NEWID(),
    C_SI_ACCOUNT     VARCHAR(20)     NOT NULL,   -- mã sub-account (sub-index, customer-level) = CUST_CODE + đuôi
    C_CUST_CODE      VARCHAR(10)     NOT NULL,
    C_MASTER_CODE    VARCHAR(20)     NOT NULL,   -- master KH đầu tư → T_MASTER_PORTFOLIO.C_MASTER_CODE
    C_SUB_ACCOUNT_NO VARCHAR(32)     NULL,       -- số TK giao dịch tại FO (nếu khác C_SI_ACCOUNT)
    C_JOIN_DATE      DATE            NOT NULL,
    C_CLOSE_DATE     DATE            NULL,        -- ngày đóng sub-account (KH dừng đầu tư master)
    C_STATUS         VARCHAR(10)     NOT NULL CONSTRAINT DF_SI_PORTFOLIO_STATUS DEFAULT 'ACTIVE', -- ACTIVE|CLOSED
    C_INITIAL_AMOUNT DECIMAL(20,0)   NULL,    -- TIỀN (VND) cam kết đầu tư ban đầu khi mở tiểu khoản (tham chiếu; dòng tiền thực = T_SI_CASHFLOW_EVENT INITIAL)
    C_SIP_AMOUNT     DECIMAL(20,0)   NULL,    -- TIỀN (VND) nạp định kỳ (SIP) mỗi kỳ theo C_SIP_SCHEDULE
    C_SIP_SCHEDULE   VARCHAR(50)     NULL,
    -- (KHÔNG có phí cấp tiểu khoản: chính sách phí cấu hình cấp master ở T_FEE_CONFIG theo (master,fee_type).)
    C_MIN_INVEST     DECIMAL(20,0)   NULL,    -- TIỀN (VND) tối thiểu phải duy trì
    CONSTRAINT PK_SI_PORTFOLIO PRIMARY KEY CLUSTERED (PK_SI_PORTFOLIO),
    CONSTRAINT UQ_SI_PORTFOLIO_NK UNIQUE (C_SI_ACCOUNT)   -- mã sub-account duy nhất toàn cục
);
-- Tối đa 1 sub-account ACTIVE / (KH × master) tại 1 thời điểm (closed thì được mở lại = dòng mới)
CREATE UNIQUE INDEX UQ_SI_PORTFOLIO_ACTIVE ON T_SI_PORTFOLIO (C_CUST_CODE, C_MASTER_CODE)
    WHERE C_STATUS = 'ACTIVE';
-- [FR-01] list MỌI sub-account của 1 KH (gồm CLOSED) — by cust. UQ_ACTIVE chỉ phủ ACTIVE nên cần index này.
CREATE INDEX IX_SI_PORTFOLIO_CUST ON T_SI_PORTFOLIO (C_CUST_CODE)
    INCLUDE (C_SI_ACCOUNT, C_MASTER_CODE, C_STATUS, C_JOIN_DATE);

/*-------------------------------------------------------------- MARKET DATA ---*/
-- GỘP giá daily + sự kiện quyền vào 1 bảng: mỗi (mã, phiên) 1 dòng giá. Sở publish EOD
-- đủ thông tin của CHÍNH ngày đó → engine KHÔNG cần ngó bản ghi hôm trước.
-- C_REF_PRICE = giá tham chiếu đầu phiên (mẫu số daily-return J12): phiên thường = close
-- hôm trước; ngày ex-rights (không hưởng quyền) = giá sau chia. C_IS_EX_RIGHTS chỉ là metadata.
-- Bỏ bảng T_CORPORATE_ACTION riêng: type/ratio/cash_div không tham gia tính (cổ tức/quyền vào NAV qua FO sync).
CREATE TABLE T_PRICE_DAILY (
    PK_PRICE_DAILY      UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_PRICE_DAILY_PKID DEFAULT NEWID(),
    C_TICKER            VARCHAR(20)  NOT NULL,
    C_BUSINESS_DATE     DATE         NOT NULL,
    C_REF_PRICE         DECIMAL(18,4) NOT NULL,  -- GIÁ tham chiếu đầu phiên (sở publish): phiên thường = close hôm trước; ex-rights = giá sau chia. Mẫu số daily-return J12 (self-contained, không tra ngày trước)
    C_CLOSE_PRICE       DECIMAL(18,4) NOT NULL,  -- GIÁ đóng cửa — định giá MTM (J07: stock_value = Σ qty×close_price) + tử số index J12 (Pᵢ,t)
    C_IS_EX_RIGHTS      TINYINT      NOT NULL CONSTRAINT DF_PRICE_DAILY_EXR DEFAULT 0,  -- 1 = ngày có sự kiện quyền gây chia giá (ex-rights/ex-div) — metadata reporting/audit; 0 = phiên thường
    CONSTRAINT PK_PRICE_DAILY_NK PRIMARY KEY CLUSTERED (C_BUSINESS_DATE, C_TICKER),  -- natural clustered (join MTM nóng)
    CONSTRAINT UQ_PRICE_DAILY_PKID UNIQUE NONCLUSTERED (PK_PRICE_DAILY)
);

CREATE TABLE T_BENCHMARK_DAILY (
    PK_BENCHMARK_DAILY UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_BENCHMARK_DAILY_PKID DEFAULT NEWID(),
    C_BENCHMARK_CODE VARCHAR(20)     NOT NULL,   -- 'VNINDEX' | 'VN30' | …
    C_BUSINESS_DATE  DATE            NOT NULL,
    C_INDEX_VALUE    DECIMAL(18,4)   NOT NULL,    -- điểm benchmark (PR). So sánh FR-03/US3: (điểm cuối/điểm mốc − 1)
    CONSTRAINT PK_BENCHMARK_DAILY PRIMARY KEY CLUSTERED (PK_BENCHMARK_DAILY),
    CONSTRAINT UQ_BENCHMARK_DAILY_NK UNIQUE (C_BENCHMARK_CODE, C_BUSINESS_DATE)
);

-- Dimension mã→ngành — cho industryWeight alert (SP_GET_MASTER_ALERTS). 1 ticker = 1 ngành.
-- Seed dữ liệu mẫu bên dưới (tạm); NGUỒN NẠP THẬT (FO/market data feed) = task data-ops sau.
CREATE TABLE T_TICKER_INDUSTRY (
    PK_TICKER_INDUSTRY UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_TICKER_INDUSTRY_PKID DEFAULT NEWID(),
    C_TICKER         VARCHAR(20)   NOT NULL,
    C_INDUSTRY_CODE  VARCHAR(20)   NOT NULL,   -- mã ngành (vd ICB/GICS sector)
    C_INDUSTRY_NAME  NVARCHAR(100) NULL,       -- tên ngành hiển thị
    CONSTRAINT PK_TICKER_INDUSTRY_NK PRIMARY KEY CLUSTERED (C_TICKER),  -- point-lookup theo mã
    CONSTRAINT UQ_TICKER_INDUSTRY_PKID UNIQUE NONCLUSTERED (PK_TICKER_INDUSTRY)
);
-- DỮ LIỆU MẪU (tạm — thay bằng datafeed thật FO/market data sau). Map VN30-ish → ngành ICB.
INSERT INTO T_TICKER_INDUSTRY (C_TICKER, C_INDUSTRY_CODE, C_INDUSTRY_NAME) VALUES
 ('VCB','BANK',N'Ngân hàng'),('BID','BANK',N'Ngân hàng'),('CTG','BANK',N'Ngân hàng'),
 ('TCB','BANK',N'Ngân hàng'),('MBB','BANK',N'Ngân hàng'),('ACB','BANK',N'Ngân hàng'),
 ('VPB','BANK',N'Ngân hàng'),('STB','BANK',N'Ngân hàng'),
 ('VIC','REAL',N'Bất động sản'),('VHM','REAL',N'Bất động sản'),('VRE','REAL',N'Bất động sản'),
 ('NVL','REAL',N'Bất động sản'),('KDH','REAL',N'Bất động sản'),('PDR','REAL',N'Bất động sản'),
 ('HPG','MATL',N'Vật liệu'),('HSG','MATL',N'Vật liệu'),('NKG','MATL',N'Vật liệu'),
 ('FPT','TECH',N'Công nghệ'),('CMG','TECH',N'Công nghệ'),
 ('VNM','CONS',N'Tiêu dùng'),('MSN','CONS',N'Tiêu dùng'),('SAB','CONS',N'Tiêu dùng'),
 ('MWG','RETL',N'Bán lẻ'),('PNJ','RETL',N'Bán lẻ'),
 ('GAS','ENGY',N'Năng lượng'),('PLX','ENGY',N'Năng lượng'),('POW','ENGY',N'Năng lượng'),
 ('SSI','SECU',N'Chứng khoán'),('VND','SECU',N'Chứng khoán'),('HCM','SECU',N'Chứng khoán');

/*------------------------------------------------------- EVENT / LEDGER -------*/
CREATE TABLE T_REBALANCE_REQUEST (
    PK_REBALANCE_REQUEST UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_REBREQ_PKID DEFAULT NEWID(),
    C_REQUEST_ID     BIGINT IDENTITY(1,1) NOT NULL,
    C_MASTER_CODE    VARCHAR(20)     NOT NULL,
    C_BUSINESS_DATE  DATE            NOT NULL,
    C_TYPE           VARCHAR(20)     NOT NULL,  -- REBALANCE | DEPLOY | REDEEM
    C_STATUS         VARCHAR(20)     NOT NULL CONSTRAINT DF_REBREQ_STATUS DEFAULT 'NEW',
    CONSTRAINT PK_REBALANCE_REQUEST PRIMARY KEY CLUSTERED (PK_REBALANCE_REQUEST),
    CONSTRAINT UQ_REBALANCE_REQUEST_NK UNIQUE (C_REQUEST_ID)
);

-- HOLDINGS HISTORY theo KHOẢNG (INTERVAL/SCD-2) per SUB-ACCOUNT — full history, no-dup.
--   Reconstruct ngày D: WHERE C_VALID_FROM<=D AND (C_VALID_TO>D OR C_VALID_TO IS NULL).
CREATE TABLE T_SI_HOLDING_HIST (
    C_HOLDING_HIST_ID BIGINT IDENTITY(1,1) NOT NULL,
    PK_SI_HOLDING_HIST UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_SI_HOLDING_HIST_PKID DEFAULT NEWID(),
    C_SI_ACCOUNT     VARCHAR(20)     NOT NULL,   -- sub-account (khóa nghiệp vụ)
    C_CUST_CODE      VARCHAR(10)     NOT NULL,   -- denormalized (query)
    C_MASTER_CODE    VARCHAR(20)     NOT NULL,   -- denormalized (tổng hợp master)
    C_TICKER         VARCHAR(20)     NOT NULL,
    C_VALID_FROM     DATE            NOT NULL,   -- khoảng hiệu lực interval [from, to) — dòng mở C_VALID_TO=NULL
    C_VALID_TO       DATE            NULL,
    C_QUANTITY       DECIMAL(20,0)   NOT NULL,   -- số lượng CP nắm giữ trong khoảng (reconstruct tài sản FR-06)
    C_AVG_COST       DECIMAL(18,4)   NULL,       -- GIÁ vốn bình quân (tham chiếu lãi/lỗ; KHÔNG vào NAV — NAV theo giá thị trường)
    CONSTRAINT PK_SI_HOLDING_HIST_ID PRIMARY KEY CLUSTERED (C_HOLDING_HIST_ID),
    CONSTRAINT UQ_SI_HOLDING_HIST_PKID UNIQUE NONCLUSTERED (PK_SI_HOLDING_HIST),
    CONSTRAINT UQ_SI_HOLDING_HIST_NK UNIQUE (C_SI_ACCOUNT, C_TICKER, C_VALID_FROM)
) WITH (DATA_COMPRESSION = PAGE);
CREATE INDEX IX_SI_HOLDING_HIST_OPEN ON T_SI_HOLDING_HIST (C_SI_ACCOUNT, C_TICKER)
    INCLUDE (C_QUANTITY, C_AVG_COST) WHERE C_VALID_TO IS NULL;
-- (Cash vào qua SP_INGEST_CUSTOMER → state.C_CASH + diff T_SI_CASH_HIST; không có bảng feed batch.)

-- CASH HISTORY theo KHOẢNG (INTERVAL) per SUB-ACCOUNT — đối xứng holding_hist.
CREATE TABLE T_SI_CASH_HIST (
    C_CASH_HIST_ID   BIGINT IDENTITY(1,1) NOT NULL,
    PK_SI_CASH_HIST UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_SI_CASH_HIST_PKID DEFAULT NEWID(),
    C_SI_ACCOUNT     VARCHAR(20)     NOT NULL,
    C_CUST_CODE      VARCHAR(10)     NOT NULL,
    C_MASTER_CODE    VARCHAR(20)     NOT NULL,
    C_VALID_FROM     DATE            NOT NULL,   -- khoảng hiệu lực interval [from, to)
    C_VALID_TO       DATE            NULL,
    C_CASH           DECIMAL(20,0)   NOT NULL,   -- TIỀN MẶT (VND) snapshot trong khoảng (reconstruct tài sản FR-06; chỉ tiền mặt, KHÔNG gồm pending/div)
    CONSTRAINT PK_SI_CASH_HIST_ID PRIMARY KEY CLUSTERED (C_CASH_HIST_ID),
    CONSTRAINT UQ_SI_CASH_HIST_PKID UNIQUE NONCLUSTERED (PK_SI_CASH_HIST),
    CONSTRAINT UQ_SI_CASH_HIST_NK UNIQUE (C_SI_ACCOUNT, C_VALID_FROM)
) WITH (DATA_COMPRESSION = PAGE);
CREATE INDEX IX_SI_CASH_HIST_OPEN ON T_SI_CASH_HIST (C_SI_ACCOUNT)
    INCLUDE (C_CASH) WHERE C_VALID_TO IS NULL;

-- External cashflow (nạp/rút) — SDI-side, per sub-account
CREATE TABLE T_SI_CASHFLOW_EVENT (
    PK_SI_CASHFLOW_EVENT UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_SI_CASHFLOW_EVENT_PKID DEFAULT NEWID(),
    C_EVENT_ID       BIGINT IDENTITY(1,1) NOT NULL,
    C_SI_ACCOUNT     VARCHAR(20)     NOT NULL,
    C_CUST_CODE      VARCHAR(10)     NOT NULL,
    C_MASTER_CODE    VARCHAR(20)     NOT NULL,
    C_BUSINESS_DATE  DATE            NOT NULL,
    C_EVENT_TYPE     VARCHAR(20)     NOT NULL,  -- INITIAL|TOPUP|SIP|INTEREST_IN = TIỀN VÀO; WITHDRAW = TIỀN RA. Là dòng tiền ngoài (external CF), KHÔNG tính vào PnL.
    C_AMOUNT         DECIMAL(20,0)   NOT NULL,  -- TIỀN (VND) > 0. EOD: CF_IN/CF_OUT (J09 PnL=NAV−NAV_prev+ra−vào; J10 ΔUnit=CF_net/UP_prev). WITHDRAW vào CF_OUT.
    C_CREATED_TIME   DATETIME        NOT NULL CONSTRAINT DF_CF_CREATED DEFAULT GETDATE(),
    CONSTRAINT PK_SI_CASHFLOW_EVENT_ID PRIMARY KEY CLUSTERED (C_EVENT_ID),
    CONSTRAINT UQ_SI_CASHFLOW_EVENT_PKID UNIQUE NONCLUSTERED (PK_SI_CASHFLOW_EVENT)
);
CREATE INDEX IX_SI_CASHFLOW_EVENT_DATE ON T_SI_CASHFLOW_EVENT (C_BUSINESS_DATE) INCLUDE (C_SI_ACCOUNT, C_EVENT_TYPE, C_AMOUNT);  -- EOD gom CF theo ngày
-- [FR-02 MWR] gom cashflow theo SUB-ACCOUNT trong range (Modified Dietz) — by si.
CREATE INDEX IX_SI_CASHFLOW_EVENT_ACCT ON T_SI_CASHFLOW_EVENT (C_SI_ACCOUNT, C_BUSINESS_DATE) INCLUDE (C_EVENT_TYPE, C_AMOUNT);

-- Unit thay đổi (ghi dòng khi có cashflow) per sub-account
CREATE TABLE T_SI_UNIT_LEDGER (
    C_SI_UNIT_LEDGER_ID BIGINT IDENTITY(1,1) NOT NULL,
    PK_SI_UNIT_LEDGER   UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_SI_UNIT_LEDGER_PKID DEFAULT NEWID(),
    C_SI_ACCOUNT     VARCHAR(20)     NOT NULL,
    C_CUST_CODE      VARCHAR(10)     NOT NULL,
    C_MASTER_CODE    VARCHAR(20)     NOT NULL,
    C_BUSINESS_DATE  DATE            NOT NULL,
    C_CF_NET         DECIMAL(20,0)   NOT NULL,   -- TIỀN ròng vào trong ngày = CF_IN − CF_OUT
    C_DELTA_UNIT     DECIMAL(18,6)   NOT NULL,   -- ΔUnit = C_CF_NET / UnitPrice_(t-1) (số đơn vị quỹ phát hành/hủy do dòng tiền)
    C_UNIT           DECIMAL(18,6)   NOT NULL,   -- tổng Unit sau biến động (lũy kế)
    CONSTRAINT PK_SI_UNIT_LEDGER_ID PRIMARY KEY CLUSTERED (C_SI_UNIT_LEDGER_ID),
    CONSTRAINT UQ_SI_UNIT_LEDGER_PKID UNIQUE NONCLUSTERED (PK_SI_UNIT_LEDGER),
    CONSTRAINT UQ_SI_UNIT_LEDGER_NK UNIQUE (C_SI_ACCOUNT, C_BUSINESS_DATE)
);

/*------------------------------------------- CURRENT STATE (roll-forward) -----*/
-- NAV/state HIỆN TẠI per SUB-ACCOUNT — 1 dòng, cập nhật tại chỗ mỗi EOD. Clustered theo C_SI_ACCOUNT.
CREATE TABLE T_SI_NAV_CURRENT (
    PK_SI_NAV_CURRENT UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_SI_NAV_CURRENT_PKID DEFAULT NEWID(),
    C_SI_ACCOUNT       VARCHAR(20)   NOT NULL,
    C_CUST_CODE        VARCHAR(10)   NOT NULL,
    C_MASTER_CODE      VARCHAR(20)   NOT NULL,
    C_UNIT             DECIMAL(18,6) NOT NULL CONSTRAINT DF_CNC_UNIT DEFAULT 0,      -- tổng đơn vị quỹ (lũy kế); biến động chỉ do dòng tiền (TWR sạch)
    C_CASH             DECIMAL(20,0)  NOT NULL CONSTRAINT DF_CNC_CASH DEFAULT 0,     -- TIỀN MẶT khả dụng (FO sync). Đã NET phí GD + thuế.
    C_PENDING_CASH     DECIMAL(20,0)  NOT NULL CONSTRAINT DF_CNC_PEND DEFAULT 0,     -- TIỀN bán chờ về (FO, tổng T0+T1+T2) — receivable, vẫn tính vào tài sản
    C_DIV_CASH         DECIMAL(20,0)  NOT NULL CONSTRAINT DF_CNC_DIV DEFAULT 0,      -- TIỀN cổ tức chờ về (FO) — receivable
    C_PAYABLE_FEE      DECIMAL(20,6)  NOT NULL CONSTRAINT DF_CNC_PAY DEFAULT 0,      -- PHÍ QL accrued chưa net-off (khoản PHẢI TRẢ). TIỀN = C_CASH+C_PENDING_CASH+C_DIV_CASH; NAV = (stock+TIỀN) − C_PAYABLE_FEE
    C_LAST_NAV         DECIMAL(20,0)  NOT NULL CONSTRAINT DF_CNC_NAV DEFAULT 0,      -- NAV NET phí gần nhất = tổng tài sản (stock+TIỀN) − payable
    C_LAST_UNIT_PRICE  DECIMAL(18,6) NULL,                                           -- Unit Price gần nhất = NAV/Unit (T0=10.000). %hiệu suất TWR = UP_cuối/UP_mốc − 1
    C_STATUS           VARCHAR(10)    NOT NULL CONSTRAINT DF_CNC_STATUS DEFAULT 'ACTIVE', -- ACTIVE | CLOSED…
    C_LAST_BUSINESS_DATE DATE         NULL,                       -- ngày EOD compute gần nhất
    C_LAST_SYNC_DATE   DATE           NULL,                       -- watermark: ngày FO ingest gần nhất (GATE + guard forward)
    CONSTRAINT PK_SI_NAV_CURRENT_NK PRIMARY KEY CLUSTERED (C_SI_ACCOUNT),  -- natural clustered (MERGE/point roll-forward by si)
    CONSTRAINT UQ_SI_NAV_CURRENT_PKID UNIQUE NONCLUSTERED (PK_SI_NAV_CURRENT)
) WITH (DATA_COMPRESSION = PAGE);
-- [PM tool] PM SP đọc current theo MASTER (WHERE C_MASTER_CODE=@m AND C_STATUS='ACTIVE') — clustered theo si
--   nên by-master phải có index riêng (AUM/cash/up_end/payable per-KH cho US1/US2/US4/US5).
CREATE INDEX IX_SI_NAV_CURRENT_MASTER ON T_SI_NAV_CURRENT (C_MASTER_CODE, C_STATUS)
    INCLUDE (C_SI_ACCOUNT, C_LAST_NAV, C_PAYABLE_FEE, C_CASH, C_PENDING_CASH, C_DIV_CASH, C_LAST_UNIT_PRICE);

-- Holdings HIỆN TẠI — FO nạp THẲNG mỗi EOD (overwrite). NGUỒN DUY NHẤT cho EOD core (MTM/agg).
CREATE TABLE T_SI_PORTFOLIO_HOLDING (
    PK_SI_PORTFOLIO_HOLDING UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_SI_PORTFOLIO_HOLDING_PKID DEFAULT NEWID(),
    C_SI_ACCOUNT     VARCHAR(20)     NOT NULL,
    C_CUST_CODE      VARCHAR(10)     NOT NULL,
    C_MASTER_CODE    VARCHAR(20)     NOT NULL,
    C_TICKER         VARCHAR(20)     NOT NULL,
    C_QUANTITY       DECIMAL(20,0)   NOT NULL,
    C_AVG_COST       DECIMAL(18,4)   NULL,
    CONSTRAINT PK_SI_PORTFOLIO_HOLDING_NK PRIMARY KEY CLUSTERED (C_SI_ACCOUNT, C_TICKER),  -- natural clustered (MTM)
    CONSTRAINT UQ_SI_PORTFOLIO_HOLDING_PKID UNIQUE NONCLUSTERED (PK_SI_PORTFOLIO_HOLDING)
) WITH (DATA_COMPRESSION = PAGE);

/*--------------------------------------------- BẢNG WORK (transient/EOD) ------*/
CREATE TABLE T_EOD_WORK (
    C_BUSINESS_DATE   DATE           NOT NULL,
    C_SI_ACCOUNT      VARCHAR(20)    NOT NULL,
    C_CUST_CODE       VARCHAR(10)    NOT NULL,
    C_MASTER_CODE     VARCHAR(20)    NOT NULL,
    -- seed từ T_SI_NAV_CURRENT (trạng thái đầu ngày) + delta ngày @d:
    C_CASH            DECIMAL(20,0)  NOT NULL DEFAULT 0,   -- TIỀN MẶT @d (FO sync)
    C_PENDING_CASH    DECIMAL(20,0)  NOT NULL DEFAULT 0,   -- TIỀN bán chờ về @d
    C_DIV_CASH        DECIMAL(20,0)  NOT NULL DEFAULT 0,   -- TIỀN cổ tức chờ về @d
    C_PAYABLE_FEE     DECIMAL(20,6)  NOT NULL DEFAULT 0,   -- PHÍ QL phải trả lũy kế sau accrue J06 (payable_prev + accrue ngày)
    C_LAST_NAV        DECIMAL(20,0)  NOT NULL DEFAULT 0,   -- NAV cuối ngày TRƯỚC (để tính PnL J09)
    C_LAST_UNIT_PRICE DECIMAL(18,6) NULL,                  -- Unit Price cuối ngày trước (mẫu số ΔUnit J10)
    C_UNIT_PREV       DECIMAL(18,6) NOT NULL DEFAULT 0,    -- Unit đầu ngày (trước biến động dòng tiền)
    C_CF_IN           DECIMAL(20,0)  NOT NULL DEFAULT 0,   -- TIỀN vào ngày @d (Σ INITIAL/TOPUP/SIP/INTEREST_IN)
    C_CF_OUT          DECIMAL(20,0)  NOT NULL DEFAULT 0,   -- TIỀN ra ngày @d (Σ WITHDRAW)
    C_STOCK_VALUE     DECIMAL(20,0)  NOT NULL DEFAULT 0,   -- J07 MTM = Σ qty × close_price (định giá cổ phiếu)
    C_NAV             DECIMAL(20,0)  NOT NULL DEFAULT 0,   -- J08 = C_STOCK_VALUE + C_CASH + C_PENDING_CASH + C_DIV_CASH − C_PAYABLE_FEE
    C_DAILY_PNL       DECIMAL(20,0)  NOT NULL DEFAULT 0,   -- J09 = C_NAV − C_LAST_NAV + C_CF_OUT − C_CF_IN (loại ảnh hưởng dòng tiền)
    C_DELTA_UNIT      DECIMAL(18,6) NOT NULL DEFAULT 0,    -- J10 = (C_CF_IN − C_CF_OUT) / C_LAST_UNIT_PRICE (init: NAV/10000 khi UP_prev=0)
    C_UNIT            DECIMAL(18,6) NOT NULL DEFAULT 0,    -- J10 = C_UNIT_PREV + C_DELTA_UNIT
    C_UNIT_PRICE      DECIMAL(18,6) NULL,                  -- J10 = C_NAV / C_UNIT (TWR; daily_return = UP_t/UP_(t-1) − 1)
    CONSTRAINT PK_EOD_WORK PRIMARY KEY CLUSTERED (C_BUSINESS_DATE, C_SI_ACCOUNT)  -- transient
);

/*--------------- PER-SUB-ACCOUNT NAV DAILY (lịch sử — chart FR-03) ------------*/
CREATE TABLE T_SI_NAV_BALANCE (
    C_NAV_BALANCE_ID   BIGINT IDENTITY(1,1) NOT NULL,                  -- clustered PK: append tuần tự (~2,5 tỷ)
    PK_SI_NAV_BALANCE UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_SI_NAV_BALANCE_PKID DEFAULT NEWID(),
    C_BUSINESS_DATE  DATE            NOT NULL,
    C_SI_ACCOUNT     VARCHAR(20)     NOT NULL,
    C_CUST_CODE      VARCHAR(10)     NOT NULL,
    C_MASTER_CODE    VARCHAR(20)     NOT NULL,
    C_NAV            DECIMAL(20,0)   NOT NULL,   -- NAV NET phí (= gross − payable). Khi accrual OFF: payable=0 ⇒ = gross
    C_PAYABLE_FEE    DECIMAL(20,6)   NOT NULL CONSTRAINT DF_SI_NAV_BAL_PAY DEFAULT 0,  -- phí QL accrued chưa thu @ngày; NAV_gross = C_NAV + C_PAYABLE_FEE
    C_UNIT           DECIMAL(18,6)  NOT NULL,    -- Unit cuối ngày (snapshot lịch sử)
    C_UNIT_PRICE     DECIMAL(18,6)  NULL,        -- Unit Price NET phí = NAV/Unit. TWR kỳ = UP_cuối/UP_mốc − 1 (chart FR-03, US3 composite)
    C_DAILY_PNL      DECIMAL(20,0)   NOT NULL,   -- lãi/lỗ TIỀN trong ngày (đã loại dòng tiền)
    C_DAILY_RETURN   DECIMAL(10,6)  NULL,        -- lợi suất ngày = UP_t/UP_(t-1) − 1. Vào active return J12B (KH − master index) + TE
    -- [PM tool] TE prefix-sum: lũy kế active return (= KH return − master index return) từ inception.
    --   Cho phép tính STDEV(active) qua range BẤT KỲ bằng HIỆU 2 mốc (base/end) → đọc 2 lát, không quét.
    --   Maintain ở EOD bước SP_EOD_TE_ACCUM (sau J12, cần index daily return). FLOAT (double) cho ổn số.
    C_ACCUM_ACTIVE_RET    FLOAT       NOT NULL CONSTRAINT DF_SI_NAV_BAL_CAR  DEFAULT 0,  -- Σ aᵢ,d  (a = active return)
    C_ACCUM_ACTIVE_RET_SQ FLOAT       NOT NULL CONSTRAINT DF_SI_NAV_BAL_CARSQ DEFAULT 0, -- Σ aᵢ,d²
    C_RET_DAY_COUNT     INT         NOT NULL CONSTRAINT DF_SI_NAV_BAL_RDC  DEFAULT 0,  -- n (số ngày có active return)
    CONSTRAINT PK_SI_NAV_BALANCE_ID PRIMARY KEY CLUSTERED (C_NAV_BALANCE_ID),
    CONSTRAINT UQ_SI_NAV_BALANCE_PKID UNIQUE NONCLUSTERED (PK_SI_NAV_BALANCE),
    CONSTRAINT UQ_SI_NAV_BALANCE_NK UNIQUE (C_BUSINESS_DATE, C_SI_ACCOUNT) -- idempotency
);
-- [PM tool] quét per-master theo ngày (US3 chart) + đọc 2 lát base/end (US1/US2 return+TE prefix-sum).
CREATE INDEX IX_SI_NAV_BALANCE_MASTER ON T_SI_NAV_BALANCE (C_MASTER_CODE, C_BUSINESS_DATE)
    INCLUDE (C_SI_ACCOUNT, C_UNIT_PRICE, C_DAILY_RETURN, C_NAV,
             C_ACCUM_ACTIVE_RET, C_ACCUM_ACTIVE_RET_SQ, C_RET_DAY_COUNT);
-- [Customer API FR-02/03/06] đọc lịch sử theo SUB-ACCOUNT. UQ_NK (date,si) là date-leading (cho EOD
--   DELETE WHERE date=@d) → KHÔNG seek được by si. Index này (si,date) phủ truy vấn per-si (chart/asOf).
CREATE INDEX IX_SI_NAV_BALANCE_ACCT ON T_SI_NAV_BALANCE (C_SI_ACCOUNT, C_BUSINESS_DATE)
    INCLUDE (C_NAV, C_PAYABLE_FEE, C_UNIT, C_UNIT_PRICE, C_DAILY_RETURN);

-- SỔ CÁI PHÍ/THU NHẬP per sub-account (SPARSE, idempotent). GỘP: cổ tức + phí lưu ký (FO đẩy qua
-- SP_INGEST_CUSTOMER) + phí QL BO cắt (BO đẩy qua SP_INGEST_FEE_CHARGE → net-off payable). Phân loại 2 cấp:
--   C_FEE_GROUP = nhóm hạch toán: 'INCOME' (cộng tài sản) | 'PAYABLE' (phí/phải trả). SUM theo group → tổng nhóm.
--   C_FEE_TYPE  = khoản cụ thể: DIVIDEND (INCOME); CUSTODY_FEE, MGMT_FEE (PAYABLE); mở rộng thêm KHÔNG cần đẻ bảng.
-- ⚠️ Accrual phí QL hằng ngày KHÔNG ở đây (vẫn T_SI_NAV_CURRENT.C_PAYABLE_FEE — J06); ledger chỉ event ĐÃ phát sinh.
CREATE TABLE T_SI_FEE_LEDGER (
    C_FEE_LEDGER_ID  BIGINT IDENTITY(1,1) NOT NULL,
    PK_SI_FEE_LEDGER UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_SI_FEE_LEDGER_PKID DEFAULT NEWID(),
    C_BUSINESS_DATE  DATE            NOT NULL,   -- ngày phát sinh (cổ tức/phí lưu ký) HOẶC ngày BO cắt (phí QL)
    C_SI_ACCOUNT     VARCHAR(20)     NOT NULL,
    C_CUST_CODE      VARCHAR(10)     NULL,       -- denormalized (derive lúc ingest)
    C_MASTER_CODE    VARCHAR(20)     NULL,
    C_FEE_GROUP      VARCHAR(20)     NOT NULL,   -- INCOME | PAYABLE — nhóm hạch toán (SUM theo nhóm = tổng phải trả/thu nhập)
    C_FEE_TYPE       VARCHAR(20)     NOT NULL,   -- DIVIDEND | CUSTODY_FEE | MGMT_FEE | (mở rộng)
    C_AMOUNT         DECIMAL(20,0)   NOT NULL,   -- TIỀN (VND) > 0; ý nghĩa ±tài sản theo group (INCOME cộng, PAYABLE trừ)
    C_TICKER         VARCHAR(20)     NULL,       -- cổ tức per-mã (DIVIDEND); NULL cho phí
    C_SOURCE         VARCHAR(10)     NOT NULL CONSTRAINT DF_SI_FEE_LEDGER_SRC DEFAULT 'FO',  -- FO | BO
    C_SOURCE_EVENT_ID VARCHAR(64)    NULL,       -- khóa idempotency (chống Kafka redelivery nhân đôi)
    C_CREATED_TIME   DATETIME        NOT NULL CONSTRAINT DF_SI_FEE_LEDGER_CREATED DEFAULT GETDATE(),
    CONSTRAINT PK_SI_FEE_LEDGER_ID PRIMARY KEY CLUSTERED (C_FEE_LEDGER_ID),
    CONSTRAINT UQ_SI_FEE_LEDGER_PKID UNIQUE NONCLUSTERED (PK_SI_FEE_LEDGER)
);
-- [FR-06] lũy kế/chi tiết theo SUB-ACCOUNT ≤ asOf — by si (INCLUDE group/type/ticker để filter+sum).
CREATE INDEX IX_SI_FEE_LEDGER_ACCT ON T_SI_FEE_LEDGER (C_SI_ACCOUNT, C_BUSINESS_DATE)
    INCLUDE (C_FEE_GROUP, C_FEE_TYPE, C_AMOUNT, C_TICKER, C_SOURCE);
-- EOD/agg theo ngày
CREATE INDEX IX_SI_FEE_LEDGER_DATE ON T_SI_FEE_LEDGER (C_BUSINESS_DATE)
    INCLUDE (C_SI_ACCOUNT, C_MASTER_CODE, C_FEE_GROUP, C_FEE_TYPE, C_AMOUNT);
-- idempotency: source_event_id duy nhất (filtered — FO có thể NULL, BO luôn có)
CREATE UNIQUE INDEX UQ_SI_FEE_LEDGER_SRCEVT ON T_SI_FEE_LEDGER (C_SOURCE_EVENT_ID)
    WHERE C_SOURCE_EVENT_ID IS NOT NULL;

-- LOG ACCRUE PHÍ per (sub-account × loại phí × NGÀY): lượng phí phải trả TRÍCH TRƯỚC trong NGÀY đó cho từng loại.
-- Mục đích: SAO KÊ NAV gửi KH KÊ TỪNG KHOẢN phải trả chính xác @asOf — pending từng loại = Σ accrue(loại,≤asOf)
--   − Σ cắt(loại,≤asOf, T_SI_FEE_LEDGER group PAYABLE). Σ mọi loại = C_PAYABLE_FEE (khớp tổng). Ghi ở J06.
-- (C_PAYABLE_FEE tổng vẫn ở T_SI_NAV_CURRENT/NAV_BALANCE cho NAV; bảng này cho BREAKDOWN exact theo loại + ngày.)
CREATE TABLE T_SI_FEE_ACCRUAL (
    PK_SI_FEE_ACCRUAL UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_SI_FEE_ACCRUAL_PKID DEFAULT NEWID(),
    C_BUSINESS_DATE  DATE            NOT NULL,
    C_SI_ACCOUNT     VARCHAR(20)     NOT NULL,
    C_CUST_CODE      VARCHAR(10)     NULL,
    C_MASTER_CODE    VARCHAR(20)     NULL,
    C_FEE_TYPE       VARCHAR(20)     NOT NULL,   -- MGMT_FEE | TAX | PERF_FEE | ... (group PAYABLE)
    C_ACCRUAL_AMOUNT DECIMAL(20,6)   NOT NULL,   -- lượng accrue loại này TRONG ngày @business_date
    CONSTRAINT PK_SI_FEE_ACCRUAL_NK PRIMARY KEY CLUSTERED (C_SI_ACCOUNT, C_FEE_TYPE, C_BUSINESS_DATE),
    CONSTRAINT UQ_SI_FEE_ACCRUAL_PKID UNIQUE NONCLUSTERED (PK_SI_FEE_ACCRUAL)
) WITH (DATA_COMPRESSION = PAGE);
-- [sao kê] pending per-type ≤ asOf theo si (Σ accrual; trừ Σ cắt từ ledger).
CREATE INDEX IX_SI_FEE_ACCRUAL_ACCT ON T_SI_FEE_ACCRUAL (C_SI_ACCOUNT, C_BUSINESS_DATE)
    INCLUDE (C_FEE_TYPE, C_ACCRUAL_AMOUNT);

/*------------------------------------------------ MASTER-LEVEL DAILY (output) -*/
CREATE TABLE T_MASTER_INDEX_DAILY (
    PK_MASTER_INDEX_DAILY UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_MASTER_INDEX_DAILY_PKID DEFAULT NEWID(),
    C_BUSINESS_DATE  DATE            NOT NULL,
    C_MASTER_CODE    VARCHAR(20)     NOT NULL,
    C_INDEX_VALUE    DECIMAL(18,6)   NOT NULL,   -- Index danh mục mẫu (PR, daily-rebalanced): Index_t = Index_(t-1) × Σ wᵢ·Pᵢ,t/P_ref. Gốc 1000.
    C_DAILY_RETURN   DECIMAL(10,6)  NULL,         -- lợi suất index NGÀY-TRÊN-NGÀY = FACTOR − 1 = Index_t/Index_(t-1) − 1
                                                  --   (so với hôm trước qua P_ref = close hôm trước / giá sau chia ngày ex-rights).
                                                  --   DÙNG Ở: J12B SP_EOD_TE_ACCUM — active return = C_DAILY_RETURN(KH) − C_DAILY_RETURN(index này)
                                                  --   → tích lũy prefix-sum (Σa, Σa²) tính TE (tracking error) cho PM tool US1/US2. KHÔNG bỏ được.
    CONSTRAINT PK_MASTER_INDEX_DAILY PRIMARY KEY CLUSTERED (PK_MASTER_INDEX_DAILY),
    CONSTRAINT UQ_MASTER_INDEX_DAILY_NK UNIQUE (C_BUSINESS_DATE, C_MASTER_CODE)
);

CREATE TABLE T_MASTER_HOLDING_BALANCE (
    PK_MASTER_HOLDING_BALANCE UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_MASTER_HOLDING_BALANCE_PKID DEFAULT NEWID(),
    C_BUSINESS_DATE  DATE            NOT NULL,
    C_MASTER_CODE    VARCHAR(20)     NOT NULL,
    C_TICKER         VARCHAR(20)     NOT NULL,
    C_QUANTITY       DECIMAL(20,0)   NOT NULL,   -- Σ số lượng mã toàn master (Σ holdings sub-account)
    C_MARKET_PRICE   DECIMAL(18,4)   NOT NULL,   -- GIÁ đóng cửa định giá
    C_MARKET_VALUE   DECIMAL(20,0)   NOT NULL,   -- = C_QUANTITY × C_MARKET_PRICE
    C_WEIGHT         DECIMAL(12,8)   NULL,        -- tỷ trọng thực tế = C_MARKET_VALUE / Σ market_value (US3 rebalance detail: cũ→mới)
    CONSTRAINT PK_MASTER_HOLDING_BALANCE PRIMARY KEY CLUSTERED (PK_MASTER_HOLDING_BALANCE),
    CONSTRAINT UQ_MASTER_HOLDING_BALANCE_NK UNIQUE (C_BUSINESS_DATE, C_MASTER_CODE, C_TICKER)
);

-- NAV cấp MASTER/ngày — composition + NAV + hiệu suất (= Σ sub-account của master).
CREATE TABLE T_MASTER_NAV_BALANCE (
    PK_MASTER_NAV_BALANCE    UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_MASTER_NAV_BALANCE_PKID DEFAULT NEWID(),
    C_BUSINESS_DATE    DATE          NOT NULL,
    C_MASTER_CODE      VARCHAR(20)   NOT NULL,
    C_CASH             DECIMAL(20,0) NOT NULL,                                   -- Σ TIỀN MẶT các tiểu khoản
    C_PENDING_CASH     DECIMAL(20,0) NOT NULL CONSTRAINT DF_MNB_PEND DEFAULT 0,  -- Σ TIỀN bán chờ về
    C_DIV_CASH         DECIMAL(20,0) NOT NULL CONSTRAINT DF_MNB_DIV  DEFAULT 0,  -- Σ TIỀN cổ tức chờ về
    C_STOCK_VALUE      DECIMAL(20,0) NOT NULL,                                   -- Σ giá trị cổ phiếu (MTM)
    C_PAYABLE_FEE      DECIMAL(20,6) NULL,     -- Σ phí QL phải trả (Σ payable tiểu khoản). Cổ tức/phí lưu ký theo ngày: tra T_SI_FEE_LEDGER on-demand (không lưu rollup master).
    C_TOTAL_ASSET      DECIMAL(20,0) NOT NULL,  -- TỔNG TÀI SẢN (AUM) = stock + cash + pending + div (gồm tiền chờ về)
    C_NAV              DECIMAL(20,0)  NOT NULL, -- = C_TOTAL_ASSET − C_PAYABLE_FEE (NAV net phí)
    C_UNIT             DECIMAL(18,6) NOT NULL,  -- Σ Unit toàn master
    C_UNIT_PRICE       DECIMAL(18,6) NULL,      -- = C_NAV / C_UNIT (pooled master unit price)
    C_DAILY_PNL        DECIMAL(20,0)  NOT NULL, -- Σ lãi/lỗ TIỀN ngày
    C_DAILY_RETURN     DECIMAL(10,6) NULL,      -- lợi suất pooled master ngày
    C_CASH_IN          DECIMAL(20,0) NOT NULL CONSTRAINT DF_MNB_CIN  DEFAULT 0,  -- [PM] Σ nạp/SIP/initial/lãi master/ngày
    C_CASH_OUT         DECIMAL(20,0) NOT NULL CONSTRAINT DF_MNB_COUT DEFAULT 0,  -- [PM] Σ rút
    C_TOTAL_ACCOUNT    INT           NOT NULL CONSTRAINT DF_MNB_TACC DEFAULT 0,  -- [PM] #tiểu khoản ACTIVE
    CONSTRAINT PK_MASTER_NAV_BALANCE PRIMARY KEY CLUSTERED (PK_MASTER_NAV_BALANCE),
    CONSTRAINT UQ_MASTER_NAV_BALANCE_NK UNIQUE (C_BUSINESS_DATE, C_MASTER_CODE)
);

-- NAV/state HIỆN TẠI cấp MASTER — 1 dòng/master, overwrite mỗi EOD (J11 MERGE từ T_MASTER_NAV_BALANCE @d).
CREATE TABLE T_MASTER_NAV_CURRENT (
    PK_MASTER_NAV_CURRENT  UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_MASTER_NAV_CURRENT_PKID DEFAULT NEWID(),
    C_MASTER_CODE      VARCHAR(20)    NOT NULL,
    C_CASH             DECIMAL(20,0)  NOT NULL CONSTRAINT DF_SNC_CASH DEFAULT 0,
    C_PENDING_CASH     DECIMAL(20,0)  NOT NULL CONSTRAINT DF_SNC_PEND DEFAULT 0,
    C_DIV_CASH         DECIMAL(20,0)  NOT NULL CONSTRAINT DF_SNC_DIV  DEFAULT 0,
    C_STOCK_VALUE      DECIMAL(20,0)  NOT NULL CONSTRAINT DF_SNC_STOCK DEFAULT 0,
    C_TOTAL_ASSET      DECIMAL(20,0)  NOT NULL CONSTRAINT DF_SNC_TOTAL DEFAULT 0,  -- TỔNG TÀI SẢN (AUM) hiện tại = stock+cash+pending+div. Nguồn nhanh cho US1/US2 AUM + cash drag.
    C_LAST_NAV         DECIMAL(20,0)  NOT NULL CONSTRAINT DF_SNC_NAV DEFAULT 0,    -- NAV net phí = C_TOTAL_ASSET − Σpayable
    C_UNIT             DECIMAL(18,6) NOT NULL CONSTRAINT DF_SNC_UNIT DEFAULT 0,
    C_LAST_UNIT_PRICE  DECIMAL(18,6) NULL,                                          -- pooled master unit price = NAV/Unit
    C_TOTAL_ACCOUNT    INT           NOT NULL CONSTRAINT DF_SNC_TACC DEFAULT 0,  -- [PM] #tiểu khoản ACTIVE
    C_LAST_BUSINESS_DATE DATE         NULL,
    CONSTRAINT PK_MASTER_NAV_CURRENT PRIMARY KEY CLUSTERED (PK_MASTER_NAV_CURRENT),
    CONSTRAINT UQ_MASTER_NAV_CURRENT_NK UNIQUE (C_MASTER_CODE)
);

/*------------------------------------------------ CONTROL / ORCHESTRATION -----*/
CREATE TABLE T_EOD_RUN (
    PK_EOD_RUN       UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_EOD_RUN_PKID DEFAULT NEWID(),
    C_BUSINESS_DATE  DATE            NOT NULL,
    C_JOB            VARCHAR(40)     NOT NULL,
    C_STATUS         VARCHAR(10)     NOT NULL,  -- PENDING|RUNNING|DONE|FAILED
    C_ROWS           BIGINT          NULL,
    C_STARTED_AT     DATETIME        NULL,
    C_ENDED_AT       DATETIME        NULL,
    C_MESSAGE        NVARCHAR(2000)  NULL,
    CONSTRAINT PK_EOD_RUN PRIMARY KEY CLUSTERED (PK_EOD_RUN),
    CONSTRAINT UQ_EOD_RUN_NK UNIQUE (C_BUSINESS_DATE, C_JOB)
);

-- TRẠNG THÁI PIPELINE TỔNG /ngày — control toàn luồng: upstream (BO market data + FO ingest) →
--   EOD compute → đối soát (reconcile gate) → sync Asset. 1 dòng/ngày. SP_EOD_RUN chỉ chạy khi
--   MKT_DATA + FO_INGEST = READY; chỉ COMPLETED khi đối soát PASS + Asset sync DONE.
CREATE TABLE T_EOD_PIPELINE (
    C_BUSINESS_DATE      DATE         NOT NULL,
    -- MKT_DATA (dữ liệu thị trường): BO gửi event báo ready → SDI TỰ GỌI API BO pull 1 LẦN (giá/index/
    --   benchmark), KHÔNG qua Kafka, KHÔNG per-cust → chỉ cờ READY sau khi pull xong (không đếm record).
    C_MKT_DATA_STATUS    VARCHAR(10)  NOT NULL CONSTRAINT DF_EODP_MKT  DEFAULT 'PENDING',  -- PENDING|READY (BO)
    C_MKT_DATA_AT        DATETIME     NULL,
    -- FO_INGEST: ingest per-cust qua Kafka; break event kèm TOTAL = số cust_code gửi. SDI tự đếm
    --   RECEIVED = #cust_code distinct nhận @ngày; READY khi RECEIVED >= TOTAL. Đơn vị = cust_code.
    C_FO_INGEST_STATUS   VARCHAR(10)  NOT NULL CONSTRAINT DF_EODP_FO   DEFAULT 'PENDING',  -- PENDING|READY (FO)
    C_FO_INGEST_TOTAL    INT          NULL,        -- total cust_code FO khai báo (break event)
    C_FO_INGEST_RECEIVED INT          NULL,        -- cust_code SDI đếm nhận được
    C_FO_INGEST_AT       DATETIME     NULL,
    C_INDEX_STATUS       VARCHAR(10)  NOT NULL CONSTRAINT DF_EODP_IDX  DEFAULT 'PENDING',  -- PENDING|DONE: master index TÍNH RIÊNG (BO ready), KHÔNG trong pipeline customer
    C_INDEX_AT           DATETIME     NULL,
    C_EOD_STATUS         VARCHAR(10)  NOT NULL CONSTRAINT DF_EODP_EOD  DEFAULT 'PENDING',  -- PENDING|RUNNING|DONE|FAILED (J07→J11→J12B→J13→J14, KHÔNG còn J12 index)
    C_EOD_AT             DATETIME     NULL,
    C_RECONCILE_STATUS   VARCHAR(10)  NOT NULL CONSTRAINT DF_EODP_REC  DEFAULT 'PENDING',  -- PENDING|PASS|BREAK
    C_RECONCILE_AT       DATETIME     NULL,
    C_BREAK_COUNT        INT          NOT NULL CONSTRAINT DF_EODP_BRK  DEFAULT 0,
    C_ASSET_SYNC_STATUS  VARCHAR(10)  NOT NULL CONSTRAINT DF_EODP_AST  DEFAULT 'PENDING',  -- PENDING|DONE|FAILED
    C_ASSET_SYNC_AT      DATETIME     NULL,
    C_OVERALL_STATUS     VARCHAR(20)  NOT NULL CONSTRAINT DF_EODP_OVR  DEFAULT 'WAITING_DATA',
        -- WAITING_DATA|READY|EOD_RUNNING|RECONCILE_BREAK|EOD_DONE|COMPLETED|FAILED
    C_UPDATED_AT         DATETIME     NOT NULL CONSTRAINT DF_EODP_UPD  DEFAULT GETDATE(),
    C_UPDATED_BY         VARCHAR(64)  NULL,
    C_MESSAGE            NVARCHAR(2000) NULL,
    CONSTRAINT PK_EOD_PIPELINE PRIMARY KEY CLUSTERED (C_BUSINESS_DATE)
);

-- CHI TIẾT BREAK đối soát (J13) — nghiệp vụ tra cứu từng dòng lệch. J13 DELETE+INSERT theo ngày
--   (idempotent). Có break ⇒ SP_EOD_RUN CHẶN publish (RECONCILE_STATUS=BREAK).
CREATE TABLE T_EOD_RECON_BREAK (
    C_RECON_BREAK_ID BIGINT IDENTITY(1,1) NOT NULL,
    C_BUSINESS_DATE  DATE          NOT NULL,
    C_CHECK_NAME     VARCHAR(40)   NOT NULL,   -- NAV_NEGATIVE | SI_NAV_MISMATCH | ...
    C_MASTER_CODE    VARCHAR(20)   NULL,
    C_SI_ACCOUNT     VARCHAR(20)   NULL,
    C_VALUE_SDI      DECIMAL(20,6) NULL,        -- giá trị SDI tính
    C_VALUE_CHECK    DECIMAL(20,6) NULL,        -- giá trị đối chiếu
    C_DIFF           DECIMAL(20,6) NULL,        -- chênh lệch
    C_MESSAGE        NVARCHAR(400) NULL,
    C_CREATED_AT     DATETIME      NOT NULL CONSTRAINT DF_EODBRK_AT DEFAULT GETDATE(),
    CONSTRAINT PK_EOD_RECON_BREAK PRIMARY KEY CLUSTERED (C_RECON_BREAK_ID)
);
CREATE INDEX IX_EOD_RECON_BREAK_DATE ON T_EOD_RECON_BREAK (C_BUSINESS_DATE);

-- (Đã BỎ T_SDI_CONFIG — spec phí QL chốt cố định, không còn toggle/knob.
--  Phí QL: accrue luôn theo NGÀY DƯƠNG LỊCH, gated bởi mgmt_fee_rate; day_count = 365 hardcode trong engine.)
GO

-- =====================================================================
-- [PM tool] Cấu hình ngưỡng cảnh báo per-master (PM cài đặt từng danh mục master).
--   Serve-layer cho dashboard PM (US1-US5). Không đụng vào EOD engine.
--   Ngưỡng deviation/cash-drag/TE: dùng để badge & đếm KH vượt. NULL => fallback default hệ thống.
-- =====================================================================
--   Cột NULL => fallback default hệ thống (hằng số trong UDF_PM_CONFIG ở 06_PM_API).
--   TE = decimal ratio (annualized stdev active-return, vd 0.05 = 5%); deviation = BPS (1% = 100).
CREATE TABLE T_MASTER_PM_CONFIG (
    C_MASTER_CODE        VARCHAR(20)     NOT NULL,
    PK_MASTER_PM_CONFIG  UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_MASTER_PM_CONFIG_GUID DEFAULT NEWID(),
    C_TE_BADGE_LOW       DECIMAL(10,6)   NULL,   -- ngưỡng AUM-weighted TE badge low/med (ratio)
    C_TE_BADGE_HIGH      DECIMAL(10,6)   NULL,   -- ngưỡng AUM-weighted TE badge med/high (ratio)
    C_TE_ALERT_THRESHOLD DECIMAL(10,6)   NULL,   -- ngưỡng TE per-KH để ĐẾM #KH vượt (riêng — không = badge_high)
    C_CASH_DRAG_THRESHOLD DECIMAL(9,6)   NULL,   -- Y: ngưỡng cash drag (ratio, vd 0.05) đếm #KH vượt
    -- (deviation A/B KHÔNG để ở config — khớp BRD cấu hình master; SP truyền qua tham số, default tại SP.)
    -- Ngưỡng cảnh báo cấu hình (ratio, vd 0.15 = 15%). NULL = chưa cấu hình. CHƯA có consumer tính alert
    -- (drift/symbol/industry weight) — config plumbing trước, computation + dimension ngành = task sau.
    C_DRIFT_THRESHOLD       DECIMAL(9,6) NULL,   -- độ trôi trọng số THỰC vs MỤC TIÊU → cảnh báo cần rebalance
    C_SYMBOL_WEIGHT_ALERT   DECIMAL(9,6) NULL,   -- ngưỡng tỷ trọng 1 MÃ → cảnh báo tập trung rủi ro cổ phiếu
    C_INDUSTRY_WEIGHT_ALERT DECIMAL(9,6) NULL,   -- ngưỡng tỷ trọng 1 NGÀNH → cảnh báo tập trung ngành (cần dimension mã→ngành khi build consumer)
    C_UPDATED_BY         VARCHAR(64)     NULL,
    C_UPDATED_TIME       DATETIME        NOT NULL CONSTRAINT DF_MASTER_PM_CONFIG_TIME DEFAULT GETDATE(),
    CONSTRAINT PK_MASTER_PM_CONFIG PRIMARY KEY CLUSTERED (C_MASTER_CODE),
    CONSTRAINT UQ_MASTER_PM_CONFIG_GUID UNIQUE (PK_MASTER_PM_CONFIG)
);
GO
