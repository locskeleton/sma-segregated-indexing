SET QUOTED_IDENTIFIER ON;  -- bắt buộc cho filtered index (open-row); sqlcmd mặc định OFF
SET ANSI_NULLS ON;
GO
/*==============================================================================
  SDI MODULE — TABLES (SQL Server)
  HAI CẤP:
    - MASTER (danh mục mẫu/chiến lược): mã `C_MASTER_CODE` (PK T_MASTER_PORTFOLIO).
      Bảng tổng hợp master-level: T_MASTER_PORTFOLIO(_TICKER), T_MASTER_BALANCE,
      T_MASTER_HOLDING_BALANCE, T_MASTER_INDEX_DAILY, T_MASTER_CURRENT (key C_MASTER_CODE).
    - SUB-ACCOUNT (tiểu khoản, customer-level): KH đầu tư 1 master → cấp 1 sub-account, mã
      `C_SI_ACCOUNT` (VARCHAR20, = CUST_CODE + đuôi, sinh khi mở). Close + reopen master ⇒ sub-account
      MỚI (mã khác) → 1 KH có NHIỀU sub-account/master theo thời gian (tối đa 1 ACTIVE/lúc).
      ⇒ MỌI bảng customer-level KHÓA theo `C_SI_ACCOUNT` (giữ C_CUST_CODE, C_MASTER_CODE denormalized
      để query + tổng hợp master).
  Naming: T_/C_ UPPERCASE. Khóa surrogate public GUID `PK_<table>` (NEWID, IDOR-safe). Clustered theo
    tải: append-fact → BIGINT IDENTITY (PK_<t>_ID); point/join → natural (PK_<t>_NK); nhỏ → GUID.
    Natural giữ UQ_<t>_NK (idempotency). T_MASTER_PORTFOLIO: PK = C_MASTER_CODE (không GUID).
  DECIMAL: Tiền & Quantity = (20,0); Giá = (18,4); % / return / fee_rate = (10,6); giá trị đối chiếu/chênh lệch (reconcile) = (20,6);
    Unit & Unit Price = (18,6); Weight (12,8); CA ratio (18,8); index_value (18,x).
==============================================================================*/

/*------------------------------------------------------------------ MASTER ---*/
CREATE TABLE T_MASTER_PORTFOLIO (
    C_MASTER_CODE    VARCHAR(20)     NOT NULL,
    C_MASTER_NAME        NVARCHAR(200)   NULL,
    C_STATUS         VARCHAR(10)     NOT NULL CONSTRAINT DF_MASTER_PORTFOLIO_STATUS DEFAULT 'ACTIVE', -- ACTIVE|CLOSED
    -- Ngày master RA ĐỜI. KHÔNG chỉ là metadata hiển thị: J12 dùng làm VỊ TỪ SCOPE (mp.C_INCEPTION_DATE<=@d)
    --   để recompute lịch sử không kéo master mới ngược về ngày nó chưa tồn tại. NOT NULL là BẮT BUỘC —
    --   NULL sẽ làm vị từ ra UNKNOWN ⇒ master bị loại IM LẶNG khỏi cả 4 scope của J12 (completeness, weight-guard,
    --   CTE W, gate SP_EOD_RUN_INDEX) ⇒ index ngừng được tính mà không một err nào bật.
    C_INCEPTION_DATE DATE            NOT NULL,
    C_ALIAS          VARCHAR(50)     NULL,   -- "Alias DM Index" — tên rút gọn dùng trên báo cáo/UI (BC tổng tài sản AUM lọc theo cột này)
    -- (phí QL KHÔNG còn cột rate ở đây: chính sách phí cấu hình ở T_FEE_CONFIG global theo fee_type.)
    C_BENCHMARK_CODE VARCHAR(20)     NULL,   -- benchmark đối chiếu (vd 'VNINDEX','VN30') → T_BENCHMARK_DAILY
    CONSTRAINT PK_MASTER_PORTFOLIO PRIMARY KEY (C_MASTER_CODE)
);

-- [BRD asset-sync] ĐÃ BỎ T_FEE_CONFIG: SDI KHÔNG còn accrue phí. Phí QL do Asset tính → gửi kèm sync
--   (số tổng lũy kế per-SI). Xem docs/SDI-asset-handover.md.

-- Danh mục mẫu — RỔ HIỆN TẠI (FO tính & feed). 1 dòng / (master, ticker). Σ C_TARGET_WEIGHT = 1.0
--
-- ★ KHÔNG CÓ CHIỀU THỜI GIAN. Bảng này chỉ trả lời "rổ ĐANG là gì", KHÔNG trả lời "rổ ngày 15/03 là gì".
--   Lịch sử nằm ở T_MASTER_PORTFOLIO_TICKER_HIST và CHỈ ở đó ⇒ mọi phép tính as-of (index J12, PM API)
--   PHẢI dựng lại rổ từ HIST, TUYỆT ĐỐI không đọc bảng này rồi coi là rổ của ngày quá khứ.
--   Bảng này là SERVE LAYER (UI/FO xem rổ hiện hành) + nguồn để cổng nạp tính DELTA.
--
-- ★ Chỉ chứa mã CÒN TRONG RỔ (weight > 0). Gỡ mã ⇒ XOÁ dòng ở đây, và ghi dòng weight = 0 vào HIST
--   (xem giải thích ở HIST — không có bản ghi gỡ thì không dựng lại được rổ quá khứ).
--
-- ★ Nạp CHỈ QUA SP_INGEST_MASTER_PORTFOLIO_TICKER. Ghi thẳng vào bảng này là bỏ qua cổng ⇒ (a) HIST
--   không có vết ⇒ rổ as-of quá khứ sai vĩnh viễn, (b) Σweight không ai kiểm ⇒ rổ hụt tỷ trọng vẫn cho ra
--   index "hợp lệ" mà SAI IM LẶNG, vì FACTOR chuẩn hoá bằng /Σw nên thiếu mã không làm vỡ thang, nó chỉ
--   lặng lẽ chuyển sang theo dõi một rổ khác.
CREATE TABLE T_MASTER_PORTFOLIO_TICKER (
    PK_MASTER_PORTFOLIO_TICKER UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_MASTER_PORTFOLIO_TICKER_PKID DEFAULT NEWID(),
    C_MASTER_CODE    VARCHAR(20)     NOT NULL,
    C_TICKER         VARCHAR(20)     NOT NULL,
    C_TARGET_WEIGHT  DECIMAL(12,8)   NOT NULL,   -- trọng số mục tiêu HIỆN TẠI; Σ theo master = 1.0 (hoặc 100 nếu feed dùng thang %)
    CONSTRAINT PK_MASTER_PORTFOLIO_TICKER PRIMARY KEY CLUSTERED (PK_MASTER_PORTFOLIO_TICKER),
    CONSTRAINT UQ_MASTER_PORTFOLIO_TICKER_NK UNIQUE (C_MASTER_CODE, C_TICKER),
    CONSTRAINT CK_MASTER_PORTFOLIO_TICKER_W CHECK (C_TARGET_WEIGHT > 0)   -- rổ hiện tại chỉ chứa mã CÒN trong rổ; 0 = đã gỡ ⇒ xoá dòng, dấu vết nằm ở HIST
);

-- LỊCH SỬ TỶ TRỌNG — LOG THAY ĐỔI (delta), KHÔNG phải snapshot.
--   Mỗi dòng = MỘT lần đổi tỷ trọng của MỘT mã, tại MỘT thời điểm DUYỆT (C_CONFIRM_TIME).
--   Mã không đổi tỷ trọng thì KHÔNG có dòng mới — nó vẫn giữ giá trị ở dòng gần nhất của chính nó.
--
-- ★ ĐÂY LÀ NGUỒN DUY NHẤT dựng lại rổ AS-OF. Rổ cuối ngày @d = với TỪNG MÃ lấy dòng có C_CONFIRM_TIME
--   LỚN NHẤT còn < @d+1, rồi BỎ những mã có weight = 0.
--
-- ★ GỠ MÃ = ghi dòng C_TARGET_WEIGHT = 0 (BẮT BUỘC). Vì đây là log DELTA: nếu gỡ mã mà không ghi gì thì
--   dòng cuối cùng của mã đó (weight dương) SỐNG MÃI ⇒ mọi lần dựng rổ quá khứ về sau đều thừa mã đã gỡ,
--   Σw phình, index sai — và sai IM LẶNG. Đây không phải tuỳ chọn, nó là điều kiện để mô hình delta đúng.
--
-- ★ ĐỌC AS-OF PHẢI dùng ROW_NUMBER/RANK OVER (PARTITION BY C_MASTER_CODE, C_TICKER ORDER BY C_CONFIRM_TIME DESC).
--   PARTITION PHẢI CÓ C_TICKER — đây là log per-mã, không phải snapshot cả rổ. Partition chỉ theo master sẽ
--   chỉ giữ những mã đổi ở lần duyệt cuối cùng và vứt toàn bộ phần còn lại của rổ.
--
-- ⚠️ UNIQUE (master, ticker, confirm_time) không phải để tra nhanh: nó KHỬ HẲN khả năng HOÀ khi xếp hạng.
--   Một mã KHÔNG THỂ có hai tỷ trọng khác nhau tại cùng một thời điểm duyệt — đó là mâu thuẫn nghiệp vụ.
--   Thiếu ràng buộc này thì hai dòng trùng mốc làm "dòng mới nhất" thành KHÔNG XÁC ĐỊNH: cùng input, khác
--   output giữa các lần chạy (phụ thuộc plan/parallelism), không một lỗi nào bật.
-- Clustered theo khoá tự nhiên: append-only, và luôn đọc theo (master, ticker) + quét theo thời gian.
CREATE TABLE T_MASTER_PORTFOLIO_TICKER_HIST (
    PK_MASTER_PORTFOLIO_TICKER_HIST UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_MASTER_PORTFOLIO_TICKER_HIST_PKID DEFAULT NEWID(),
    C_MASTER_CODE    VARCHAR(20)     NOT NULL,
    C_TICKER         VARCHAR(20)     NOT NULL,
    C_TARGET_WEIGHT  DECIMAL(12,8)   NOT NULL,   -- tỷ trọng MỚI của mã sau lần duyệt này. ★ 0 = GỠ mã khỏi rổ
    C_CONFIRM_TIME   DATETIME2(3)    NOT NULL,   -- ★ thời điểm DUYỆT thay đổi — khoá thứ tự để dựng rổ as-of
    C_UPDATER        VARCHAR(64)     NULL,
    CONSTRAINT PK_MASTER_PORTFOLIO_TICKER_HIST PRIMARY KEY CLUSTERED
        (C_MASTER_CODE, C_TICKER, C_CONFIRM_TIME),
    CONSTRAINT UQ_MASTER_PORTFOLIO_TICKER_HIST_PKID UNIQUE NONCLUSTERED (PK_MASTER_PORTFOLIO_TICKER_HIST),
    CONSTRAINT CK_MASTER_PORTFOLIO_TICKER_HIST_W CHECK (C_TARGET_WEIGHT >= 0)
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
    -- (KHÔNG có phí cấp tiểu khoản: chính sách phí cấu hình ở T_FEE_CONFIG global theo fee_type.)
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

-- THÔNG TIN KHÁCH HÀNG + QUY KẾT TỔ CHỨC — DIMENSION cho báo cáo, KHÔNG tham gia phép tính nào.
--
-- ⚠️ SDI KHÔNG PHẢI NGUỒN của dữ liệu này. Nó nằm ở hệ quản lý tài khoản (T_DM_ACCOUNT bên deposit/BO) +
--   CRM (quy kết MKT/phòng ban/đơn vị KD/khối). Bảng này là BẢN SAO ĐƯỢC ĐẨY SANG để báo cáo khỏi phải
--   join xuyên hệ. Chưa đấu nguồn thì bảng rỗng ⇒ báo cáo vẫn chạy, các cột đó ra NULL (LEFT JOIN), và
--   các filter theo tổ chức sẽ KHÔNG khớp dòng nào — đó là hành vi ĐÚNG, không phải lỗi.
--
-- Vì sao tách bảng riêng thay vì nhét vào T_SI_PORTFOLIO: quy kết tổ chức đổi theo thời gian và theo
--   nghiệp vụ CRM (chuyển sale, đổi phòng ban), không phải thuộc tính của tiểu khoản đầu tư. Trộn vào
--   registry sẽ biến bảng đăng ký thành nơi CRM ghi đè.
-- ⚠️ Đây là ảnh HIỆN TẠI (không lịch sử): báo cáo AUM ngày quá khứ vẫn quy kết theo tổ chức HÔM NAY.
--   Muốn quy kết đúng thời điểm thì phải thêm chiều thời gian — chốt với nghiệp vụ trước khi làm.
CREATE TABLE T_CUSTOMER_INFO (
    PK_CUSTOMER_INFO UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_CUSTOMER_INFO_PKID DEFAULT NEWID(),
    C_CUST_CODE          VARCHAR(10)    NOT NULL,   -- khớp T_SI_PORTFOLIO.C_CUST_CODE
    C_TKCK               VARCHAR(32)    NULL,       -- tài khoản chứng khoán (TK mẹ) tại hệ lưu ký
    C_FULL_NAME          NVARCHAR(200)  NULL,       -- họ tên KH
    C_MKT_ID_REFERRER    VARCHAR(32)    NULL,       -- MKT_ID người GIỚI THIỆU
    C_MKT_ID_MANAGER     VARCHAR(32)    NULL,       -- MKT_ID sale QUẢN LÝ
    C_DEPARTMENT_CODE    VARCHAR(32)    NULL,       -- phòng ban
    C_DEPARTMENT_NAME    NVARCHAR(200)  NULL,
    C_BUSINESS_UNIT_CODE VARCHAR(32)    NULL,       -- đơn vị kinh doanh
    C_BUSINESS_UNIT_NAME NVARCHAR(200)  NULL,
    C_DIVISION_CODE      VARCHAR(32)    NULL,       -- khối nghiệp vụ
    C_DIVISION_NAME      NVARCHAR(200)  NULL,
    C_UPDATED_AT         DATETIME2(3)   NULL,
    CONSTRAINT PK_CUSTOMER_INFO_NK PRIMARY KEY CLUSTERED (C_CUST_CODE),
    CONSTRAINT UQ_CUSTOMER_INFO_PKID UNIQUE NONCLUSTERED (PK_CUSTOMER_INFO)
);
-- Filter báo cáo lọc theo tổ chức trước rồi mới join sang số dư ⇒ index theo đúng thứ tự lọc của UI.
CREATE INDEX IX_CUSTOMER_INFO_ORG ON T_CUSTOMER_INFO
    (C_DIVISION_CODE, C_BUSINESS_UNIT_CODE, C_DEPARTMENT_CODE)
    INCLUDE (C_CUST_CODE, C_MKT_ID_REFERRER, C_MKT_ID_MANAGER);

/*-------------------------------------------------------------- MARKET DATA ---*/
-- LỊCH NGHỈ — CORE (trước ở 09_FEE.sql, kéo lên đây 2026-07-11 vì ENGINE cũng phải dùng, không chỉ phí).
-- NGÀY GD = KHÔNG cuối tuần (T7/CN) AND KHÔNG có trong bảng này  → UDF_IS_BUSINESS_DATE (02_SP_ENGINE).
-- VÌ SAO ENGINE CẦN: master index là chuỗi NHÂN DỒN (Index_t = Index_(t-1) × FACTOR_t). Trước đây engine suy
--   "ngày GD" = "T_PRICE_DAILY có dòng" → app backfill giá theo lịch dương (nguồn giá carry-forward phiên gần
--   nhất cho T7/CN/lễ) làm ngày nghỉ thành PHIÊN GIẢ và index nhân thêm 1 factor mỗi ngày nghỉ ⇒ chỉ 1 cuối
--   tuần đã sai +21%. Nay lịch là AUTHORITY: EOD/index/recompute CHỈ chạy ngày GD.
-- ⚠️ KHÔNG áp cho SP_INGEST_ASSET_NAV: Asset gửi aum/tiền/daily_return MỌI NGÀY LỊCH (kể cả T7/CN) → ingest
--   nhận hết; chỉ TÍNH TOÁN (index/EOD/TE) mới bị chặn theo lịch.
CREATE TABLE T_TRADING_HOLIDAY (
    C_HOLIDAY_DATE DATE NOT NULL,
    C_NOTE         NVARCHAR(100) NULL,
    CONSTRAINT PK_TRADING_HOLIDAY PRIMARY KEY CLUSTERED (C_HOLIDAY_DATE)
);
-- SEED: CHỈ lễ dương lịch CỐ ĐỊNH (1/1, 30/4, 1/5, 2/9). KHÔNG seed Tết/Giỗ Tổ/nghỉ bù (âm lịch, đổi từng năm
--   → đoán sai còn tệ hơn không có): ops nạp qua SP_INGEST_TRADING_HOLIDAY theo thông báo của Sở.
INSERT INTO T_TRADING_HOLIDAY (C_HOLIDAY_DATE, C_NOTE) VALUES
 ('2025-01-01',N'Tết Dương lịch'), ('2025-04-30',N'Giải phóng miền Nam'),
 ('2025-05-01',N'Quốc tế Lao động'),('2025-09-02',N'Quốc khánh'),
 ('2026-01-01',N'Tết Dương lịch'), ('2026-04-30',N'Giải phóng miền Nam'),
 ('2026-05-01',N'Quốc tế Lao động'),('2026-09-02',N'Quốc khánh'),
 ('2027-01-01',N'Tết Dương lịch'), ('2027-04-30',N'Giải phóng miền Nam'),
 ('2027-05-01',N'Quốc tế Lao động'),('2027-09-02',N'Quốc khánh');

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
    CONSTRAINT UQ_PRICE_DAILY_PKID UNIQUE NONCLUSTERED (PK_PRICE_DAILY),
    -- GIÁ phải DƯƠNG: C_REF_PRICE là MẪU SỐ daily-return J12 → =0 sẽ chia-0 (Msg 8134) làm cả EOD fail;
    --   close>0 (giá giao dịch thật). Chặn ngay tại tầng data thay vì để nổ ở J12/J07.
    CONSTRAINT CK_PRICE_DAILY_POS CHECK (C_REF_PRICE > 0 AND C_CLOSE_PRICE > 0)
);

CREATE TABLE T_BENCHMARK_DAILY (
    PK_BENCHMARK_DAILY UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_BENCHMARK_DAILY_PKID DEFAULT NEWID(),
    C_BENCHMARK_CODE VARCHAR(20)     NOT NULL,   -- 'VNINDEX' | 'VN30' | …
    C_BUSINESS_DATE  DATE            NOT NULL,
    C_INDEX_VALUE    DECIMAL(18,2)   NOT NULL,    -- điểm benchmark (PR), 2 chữ số thập phân (đồng bộ index). So sánh FR-03/US3: (điểm cuối/điểm mốc − 1)
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

-- [BRD asset-sync] ĐÃ BỎ T_SI_CASH_HIST: SDI không tự tính NAV nên không cần interval cash.
--   Tiền (cash/pending/div) nhận từ Asset per ngày GD (T_SI_ASSET_DAILY).

-- [thin-layer] ĐÃ GỠ T_SI_ASSET_DAILY: SDI KHÔNG còn landing-table riêng. Asset gửi per-SI qua Kafka batch
--   → SP_INGEST_ASSET_NAV ghi THẲNG vào T_SI_BALANCE (history: aum/daily_return/cash/cash_in/cash_out) +
--   roll-forward T_SI_CURRENT. EOD đọc balance trực tiếp (agg/TE/reconcile/completeness). Re-ingest = DELETE+INSERT balance.

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

-- [thin-layer] ĐÃ GỠ T_SI_UNIT_LEDGER: SDI không derive unit nữa (Asset cấp AUM + daily_return).

/*------------------------------------------- CURRENT STATE (roll-forward) -----*/
-- NAV/state HIỆN TẠI per SUB-ACCOUNT — 1 dòng, cập nhật tại chỗ mỗi EOD. Clustered theo C_SI_ACCOUNT.
CREATE TABLE T_SI_CURRENT (
    PK_SI_NAV_CURRENT UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_SI_NAV_CURRENT_PKID DEFAULT NEWID(),
    C_SI_ACCOUNT       VARCHAR(20)   NOT NULL,
    C_CUST_CODE        VARCHAR(10)   NOT NULL,
    C_MASTER_CODE      VARCHAR(20)   NOT NULL,
    C_CASH             DECIMAL(20,0)  NOT NULL CONSTRAINT DF_CNC_CASH DEFAULT 0,     -- TỔNG tiền (Asset gửi) — cho cash drag
    C_CASH_AVAILABLE   DECIMAL(20,0)  NOT NULL CONSTRAINT DF_CNC_CASHAV DEFAULT 0,   -- TIỀN KHẢ DỤNG (Asset gửi) — số THẬT SỰ rút/cắt được.
                                                                                     --   NGUỒN DUY NHẤT của SP_FEE_COLLECT: thu phí phải theo KHẢ DỤNG, KHÔNG theo
                                                                                     --   C_CASH (tổng gồm tiền đang bị phong toả/chờ khớp/T+ chưa về ⇒ thu theo tổng
                                                                                     --   sẽ sinh lệnh thu vượt số rút được → BO reject / âm tiền KH).
    C_LAST_AUM         DECIMAL(20,0)  NOT NULL CONSTRAINT DF_CNC_NAV DEFAULT 0,      -- AUM gần nhất = Asset gửi trực tiếp. (rename→C_AUM ở bước cuối)
    C_STATUS           VARCHAR(10)    NOT NULL CONSTRAINT DF_CNC_STATUS DEFAULT 'ACTIVE', -- ACTIVE | CLOSED…
    C_LAST_BUSINESS_DATE DATE         NULL,                       -- ngày EOD compute gần nhất
    C_LAST_SYNC_DATE   DATE           NULL,                       -- watermark: ngày FO ingest gần nhất (GATE + guard forward)
    CONSTRAINT PK_SI_NAV_CURRENT_NK PRIMARY KEY CLUSTERED (C_SI_ACCOUNT),  -- natural clustered (MERGE/point roll-forward by si)
    CONSTRAINT UQ_SI_NAV_CURRENT_PKID UNIQUE NONCLUSTERED (PK_SI_NAV_CURRENT)
) WITH (DATA_COMPRESSION = PAGE);
-- [PM tool] PM SP đọc current theo MASTER (WHERE C_MASTER_CODE=@m AND C_STATUS='ACTIVE') — clustered theo si
--   nên by-master phải có index riêng (AUM/cash/up_end per-KH cho US1/US2/US4/US5).
CREATE INDEX IX_SI_NAV_CURRENT_MASTER ON T_SI_CURRENT (C_MASTER_CODE, C_STATUS)
    INCLUDE (C_SI_ACCOUNT, C_LAST_AUM, C_CASH);

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
    -- [thin-layer] seed từ T_SI_BALANCE @d (Asset đã ghi thẳng) — SDI KHÔNG derive gì, work chỉ là SCOPE cho agg/TE/reconcile:
    C_CASH            DECIMAL(20,0)  NOT NULL DEFAULT 0,   -- TIỀN @d (Asset gửi) — cash drag
    C_CF_IN           DECIMAL(20,0)  NOT NULL DEFAULT 0,   -- nạp ngày @d (net flow → master agg)
    C_CF_OUT          DECIMAL(20,0)  NOT NULL DEFAULT 0,   -- rút ngày @d (net flow → master agg)
    C_AUM             DECIMAL(20,0)  NOT NULL DEFAULT 0,   -- AUM @d (Asset gửi). (rename→C_AUM ở bước cuối)
    C_DAILY_RETURN    DECIMAL(10,6) NULL,                  -- lợi suất ngày (Asset gửi) → ghi balance + active return TE
    CONSTRAINT PK_EOD_WORK PRIMARY KEY CLUSTERED (C_BUSINESS_DATE, C_SI_ACCOUNT)  -- transient
);

/*--------------- PER-SUB-ACCOUNT NAV DAILY (lịch sử — chart FR-03) ------------*/
CREATE TABLE T_SI_BALANCE (
    C_NAV_BALANCE_ID   BIGINT IDENTITY(1,1) NOT NULL,                  -- clustered PK: append tuần tự (~2,5 tỷ)
    PK_SI_NAV_BALANCE UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_SI_NAV_BALANCE_PKID DEFAULT NEWID(),
    C_BUSINESS_DATE  DATE            NOT NULL,
    C_SI_ACCOUNT     VARCHAR(20)     NOT NULL,
    C_CUST_CODE      VARCHAR(10)     NOT NULL,
    C_MASTER_CODE    VARCHAR(20)     NOT NULL,
    -- ⚠️ 1 DÒNG / NGÀY DƯƠNG LỊCH (365) — KHÔNG chỉ ngày GD: Asset gửi cả T7/CN/lễ vì nạp/rút cuối tuần VẪN
    --   đổi AUM (⇒ đổi base phí ngày đó). Ngày nghỉ: C_DAILY_RETURN phải = 0/NULL (không có phiên ⇒ không có
    --   lợi suất; dòng tiền đã bị khử) — xem docs/SDI-daily-return-contract.md.
    --   Hệ quả: MAX(C_BUSINESS_DATE) trên bảng này CÓ THỂ là T7/CN → chỗ nào cần "phiên GD" phải lọc lịch.
    --   TE accum (3 cột dưới) CHỈ được J12B set ở ngày GD ⇒ dòng ngày nghỉ giữ 0; PM API neo base/end vào
    --   T_MASTER_BALANCE (chỉ EOD ghi = chỉ ngày GD) nên KHÔNG đọc nhầm dòng ngày nghỉ.
    C_AUM            DECIMAL(20,0)   NOT NULL,   -- AUM cuối ngày = Asset gửi TRỰC TIẾP (ingest ghi thẳng, KHÔNG qua compute)
    C_DAILY_RETURN   DECIMAL(10,6)  NULL,        -- lợi suất ngày (Asset gửi, TWR ĐÃ khử dòng tiền). %PnL kỳ = ∏(1+r)−1 (on-read compound). Vào active return TE (KH − master index)
    C_CASH           DECIMAL(20,0)   NOT NULL CONSTRAINT DF_SI_BAL_CASH DEFAULT 0,  -- [thin-layer] TỔNG tiền (Asset gửi) — cash drag lịch sử + master agg
    C_CASH_AVAILABLE DECIMAL(20,0)   NOT NULL CONSTRAINT DF_SI_BAL_CASHAV DEFAULT 0, -- TIỀN KHẢ DỤNG (Asset gửi) — số thật sự rút/cắt được (lịch sử; nguồn thu phí = bản current)
    C_CASH_IN        DECIMAL(20,0)   NOT NULL CONSTRAINT DF_SI_BAL_CIN  DEFAULT 0,  -- [thin-layer] nạp ngày (Asset) — net flow master + reconcile vs cashflow SDI
    C_CASH_OUT       DECIMAL(20,0)   NOT NULL CONSTRAINT DF_SI_BAL_COUT DEFAULT 0,  -- [thin-layer] rút ngày (Asset) — net flow + reconcile
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
CREATE INDEX IX_SI_NAV_BALANCE_MASTER ON T_SI_BALANCE (C_MASTER_CODE, C_BUSINESS_DATE)
    INCLUDE (C_SI_ACCOUNT, C_DAILY_RETURN, C_AUM, C_CASH, C_CASH_IN, C_CASH_OUT,
             C_ACCUM_ACTIVE_RET, C_ACCUM_ACTIVE_RET_SQ, C_RET_DAY_COUNT);
-- [Customer API FR-02/03/06] đọc lịch sử theo SUB-ACCOUNT. UQ_NK (date,si) là date-leading (cho EOD
--   DELETE WHERE date=@d) → KHÔNG seek được by si. Index này (si,date) phủ truy vấn per-si (chart/asOf).
CREATE INDEX IX_SI_NAV_BALANCE_ACCT ON T_SI_BALANCE (C_SI_ACCOUNT, C_BUSINESS_DATE)
    INCLUDE (C_AUM, C_DAILY_RETURN, C_CASH);

-- [BRD asset-sync] ĐÃ BỎ T_SI_INCOME_FEE (chi tiết phí/thu nhập): SDI không quản chi tiết giao dịch phí nữa.
--   Phí QL đã trừ sẵn trong NAV ròng Asset gửi (Asset KHÔNG gửi số phí lũy kế riêng). Cổ tức/lưu ký: đã gộp trong tiền/NAV.
--   ⇒ SDI KHÔNG còn cột phí phải trả (C_PAYABLE_FEE đã gỡ). AUM = NAV (gross = net).

/*------------------------------------------------ MASTER-LEVEL DAILY (output) -*/
CREATE TABLE T_MASTER_INDEX_DAILY (
    PK_MASTER_INDEX_DAILY UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_MASTER_INDEX_DAILY_PKID DEFAULT NEWID(),
    C_BUSINESS_DATE  DATE            NOT NULL,
    C_MASTER_CODE    VARCHAR(20)     NOT NULL,
    C_INDEX_VALUE_RAW DECIMAL(28,12) NULL,       -- [Cách A] Index level PRECISION CAO — CHAIN nội bộ (Index_raw_t = Index_raw_(t-1) × FACTOR), chống trôi. KHÔNG publish. Engine LUÔN set; seed thủ công có thể bỏ trống → chain fallback COALESCE(raw, C_INDEX_VALUE 2dp, 1000).
    C_INDEX_VALUE    DECIMAL(18,2)   NOT NULL,   -- Index PUBLISH = ROUND(C_INDEX_VALUE_RAW, 2). Gốc 1000. 2 chữ số thập phân (index quote chuẩn). Đây là con số user NHÌN THẤY.
    C_DAILY_RETURN   DECIMAL(10,6)  NULL,         -- [Cách A] lợi suất NGÀY-TRÊN-NGÀY = C_INDEX_VALUE_t / C_INDEX_VALUE_(t-1) − 1 (TỪ index 2dp ĐÃ PUBLISH,
                                                  --   KHÔNG phải FACTOR−1) → user suy index_t/index_(t-1)−1 từ 2dp ra KHỚP 100%, hết lệch. Đánh đổi: mịn tới ~2dp cho phép.
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
CREATE TABLE T_MASTER_BALANCE (
    PK_MASTER_NAV_BALANCE    UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_MASTER_NAV_BALANCE_PKID DEFAULT NEWID(),
    C_BUSINESS_DATE    DATE          NOT NULL,
    C_MASTER_CODE      VARCHAR(20)   NOT NULL,
    C_CASH             DECIMAL(20,0) NOT NULL,                                   -- Σ TIỀN MẶT các tiểu khoản
    -- [BRD] BỎ C_STOCK_VALUE cấp master (PM tool KHÔNG đọc; AUM đã gồm stock). Stock per-mã ở T_MASTER_HOLDING_BALANCE.
    C_AUM      DECIMAL(20,0) NOT NULL,  -- TỔNG TÀI SẢN (AUM) = Σ AUM tiểu khoản. AUM = NAV.
    C_DAILY_RETURN     DECIMAL(10,6) NULL,      -- [thin-layer] lợi suất master ngày = AUM-weighted Σ(AUMᵢ·rᵢ)/ΣAUMᵢ (DM tổng KH). Bỏ pooled unit price.
    C_CASH_IN          DECIMAL(20,0) NOT NULL CONSTRAINT DF_MNB_CIN  DEFAULT 0,  -- [PM] Σ nạp/SIP/initial/lãi master/ngày
    C_CASH_OUT         DECIMAL(20,0) NOT NULL CONSTRAINT DF_MNB_COUT DEFAULT 0,  -- [PM] Σ rút
    C_TOTAL_ACCOUNT    INT           NOT NULL CONSTRAINT DF_MNB_TACC DEFAULT 0,  -- [PM] #tiểu khoản ACTIVE
    CONSTRAINT PK_MASTER_NAV_BALANCE PRIMARY KEY CLUSTERED (PK_MASTER_NAV_BALANCE),
    CONSTRAINT UQ_MASTER_NAV_BALANCE_NK UNIQUE (C_BUSINESS_DATE, C_MASTER_CODE)
);

-- NAV/state HIỆN TẠI cấp MASTER — 1 dòng/master, overwrite mỗi EOD (J11 MERGE từ T_MASTER_BALANCE @d).
CREATE TABLE T_MASTER_CURRENT (
    PK_MASTER_NAV_CURRENT  UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_MASTER_NAV_CURRENT_PKID DEFAULT NEWID(),
    C_MASTER_CODE      VARCHAR(20)    NOT NULL,
    C_CASH             DECIMAL(20,0)  NOT NULL CONSTRAINT DF_SNC_CASH DEFAULT 0,
    -- [BRD] BỎ C_STOCK_VALUE cấp master (PM không đọc).
    C_AUM      DECIMAL(20,0)  NOT NULL CONSTRAINT DF_SNC_TOTAL DEFAULT 0,  -- TỔNG TÀI SẢN (AUM) hiện tại = Σ AUM tiểu khoản. Nguồn nhanh US1/US2 AUM + cash drag.
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
--   EOD compute → đối soát (reconcile gate). 1 dòng/ngày. SP_EOD_RUN chỉ chạy khi MKT_DATA + FO_INGEST
--   = READY; trạng thái CUỐI = EOD_DONE khi đối soát PASS. (BRD 2026-06-22: BỎ stage Asset-sync — SDI
--   KHÔNG còn push asset/perf snapshot sang Asset; BO/FO đẩy thẳng Asset. Index là luồng riêng. SDI-asset-gap.md.)
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
    -- [thin-layer] ASSET_NAV: Asset gửi per-SI (aum/daily_return/cash) qua Kafka BATCH (≤100 item/msg) → ghi thẳng T_SI_BALANCE.
    --   Completeness: Asset khai TOTAL = #SI; SDI đếm RECEIVED = #SI distinct @ngày (Σ batch); READY khi RECEIVED>=TOTAL.
    --   Gate: EOD chỉ chạy khi ASSET_NAV=READY. Đơn vị = SI (như FO đếm cust_code).
    C_ASSET_NAV_STATUS   VARCHAR(10)  NOT NULL CONSTRAINT DF_EODP_ANAV DEFAULT 'PENDING',  -- PENDING|READY (Asset, batch)
    C_ASSET_NAV_TOTAL    INT          NULL,        -- total SI Asset khai báo (completeness batch)
    C_ASSET_NAV_RECEIVED INT          NULL,        -- #SI distinct SDI nhận @ngày (Σ batch)
    C_ASSET_NAV_AT       DATETIME     NULL,
    C_INDEX_STATUS       VARCHAR(10)  NOT NULL CONSTRAINT DF_EODP_IDX  DEFAULT 'PENDING',  -- PENDING|DONE: master index TÍNH RIÊNG (BO ready), KHÔNG trong pipeline customer
    C_INDEX_AT           DATETIME     NULL,
    C_EOD_STATUS         VARCHAR(10)  NOT NULL CONSTRAINT DF_EODP_EOD  DEFAULT 'PENDING',  -- PENDING|RUNNING|DONE|FAILED (J07→J11→J12B→J13→J14, KHÔNG còn J12 index)
    C_EOD_AT             DATETIME     NULL,
    -- WATERMARK RESET: mốc reset gần nhất. Gate resume (SP_EOD_STEP) coi job "đã xong cho lượt hiện tại" CHỈ khi
    --   T_EOD_RUN.C_ENDED_AT > mốc này → reset chỉ cần SET = GETDATE() (KHÔNG xoá T_EOD_RUN → log giữ làm audit).
    C_EOD_RESET_AT       DATETIME     NULL,
    C_RECONCILE_STATUS   VARCHAR(10)  NOT NULL CONSTRAINT DF_EODP_REC  DEFAULT 'PENDING',  -- PENDING|PASS|BREAK
    C_RECONCILE_AT       DATETIME     NULL,
    C_BREAK_COUNT        INT          NOT NULL CONSTRAINT DF_EODP_BRK  DEFAULT 0,
    C_OVERALL_STATUS     VARCHAR(20)  NOT NULL CONSTRAINT DF_EODP_OVR  DEFAULT 'WAITING_DATA',
        -- WAITING_DATA|READY|EOD_RUNNING|RECONCILE_BREAK|EOD_DONE|FAILED  (EOD_DONE = trạng thái CUỐI; bỏ COMPLETED/Asset-sync)
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
