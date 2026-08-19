SET QUOTED_IDENTIFIER ON;
SET ANSI_NULLS ON;
GO
/*==============================================================================
  SDI MODULE — KHUNG JOB CHẠY NỀN (generic) + SNAPSHOT NEAR-REALTIME TỪ FO
  Thiết kế đầy đủ + ma trận hỏng hóc: docs/SDI-nearrt-fo-snapshot-design.md
  Chạy SAU 02_SP_ENGINE.sql (cần UDF_IS_BUSINESS_DATE). Độc lập 05/06/09.

  FILE NÀY GỒM 2 TẦNG — ĐỪNG TRỘN:

  ┌─ TẦNG A. KHUNG JOB (generic, KHÔNG biết FO là gì) ────────────────────────┐
  │  T_JOB_DEFINITION  : khai báo loại job (chu kỳ, khung giờ, timeout, retry)│
  │  T_JOB_RUN         : hàng đợi + nhật ký, 1 dòng = 1 LƯỢT chạy            │
  │  SP_JOB_ENQUEUE / _ENQUEUE_DUE / _CLAIM / _HEARTBEAT / _COMPLETE /       │
  │  SP_JOB_REAP / _PURGE / SP_GET_JOB_STATUS                                │
  │  Thêm loại job mới = INSERT 1 dòng T_JOB_DEFINITION + 1 handler C#.      │
  │  KHÔNG sửa proc nào ở tầng này.                                          │
  └───────────────────────────────────────────────────────────────────────────┘
  ┌─ TẦNG B. NGHIỆP VỤ FO SNAPSHOT (dùng tầng A như hạ tầng) ────────────────┐
  │  SP_GET_FO_SNAPSHOT_SCOPE : danh sách KH indexing để worker chia batch 50 │
  │  SP_INGEST_FO_SNAPSHOT_RT : upsert 1 batch → T_SI_BALANCE C_SRC='RT'     │
  │  SP_RT_MASTER_AGG         : gộp cấp master → T_MASTER_BALANCE C_SRC='RT' │
  │  SP_GET_PM_RT_OVERVIEW    : API dashboard PM near-realtime               │
  └───────────────────────────────────────────────────────────────────────────┘

  *** BA NGUYÊN TẮC (kế thừa docs/SDI-kafka-batch-sync-design.md) ***
   ① Redis lo TỐC ĐỘ, DB lo TÍNH ĐÚNG. Redis Streams là đường VẬN CHUYỂN + chuông cửa;
     quyền "ai được chạy lượt này" nằm ở T_JOB_RUN (một UPDATE có điều kiện). Redis mất sạch
     key ⇒ job chậm/phải nhặt lại, KHÔNG BAO GIỜ chạy hai lần và KHÔNG mất vĩnh viễn (SP_JOB_REAP).
   ② Ngoài khung giờ GD thì TUYỆT ĐỐI không gọi FO — chặn ở 3 tầng ĐỘC LẬP (§ dưới), vì
     một tầng bất kỳ cũng có thể bị qua mặt (job nằm chờ trong stream vắt qua 15h00 là tình
     huống BÌNH THƯỜNG, không phải ngoại lệ hiếm).
   ③ Dòng RT KHÔNG BAO GIỜ đè dòng EOD, và KHÔNG BAO GIỜ được coi là số chốt (cột C_SRC).

  *** GUARD KHUNG GIỜ — 3 TẦNG, CỐ Ý TRÙNG NHAU ***
    Tầng 1 — SINH JOB   : SP_JOB_ENQUEUE_DUE / SP_JOB_ENQUEUE không tạo lượt chạy ngoài khung.
    Tầng 2 — NHẬN JOB   : SP_JOB_CLAIM kiểm tra LẠI tại thời điểm worker cầm job → ngoài khung
                          thì đóng dấu SKIPPED, worker KHÔNG chạm FO. Bắt đúng ca "job sinh lúc
                          14h59, pod nhặt lúc 15h02" (stream tồn đọng, pod restart, reaper trả lại).
    Tầng 3 — GHI DỮ LIỆU: SP_INGEST_FO_SNAPSHOT_RT từ chối ngày không phải hôm nay / không phải
                          ngày GD. Đây là lưới cuối: kể cả ai đó gọi proc bằng tay.
    (Tầng 4 nằm ở C#: TradingWindowGuard kiểm TRƯỚC TỪNG HTTP call trong vòng lặp batch —
     một chu kỳ 1000 batch bắt đầu lúc 14h50 PHẢI tự dừng giữa chừng khi chuông 15h00 điểm.)

  *** ERR-CODE ***
    0=OK · 1=job_code không tồn tại · 2=job đang tắt (DISABLED) · 3=NGOÀI khung giờ/không phải
    ngày GD · 4=fire_key đã tồn tại (idempotent, KHÔNG phải lỗi) · 5=claim hụt (pod khác đang giữ)
    · 6=quá số lần thử → DEAD · 7=job đang chạy (singleton) · 20=tham số/JSON sai · 21=validate dòng
    · 22=ngày RT không hợp lệ · 23=đã có dòng EOD ⇒ RT bị từ chối (cảnh báo, không phải lỗi) · -1=runtime.
==============================================================================*/

/*===========================================================================
  ĐỒNG HỒ: giờ VIỆT NAM, KHÔNG phụ thuộc timezone của máy chủ SQL.
    GETDATE() trả giờ HỆ ĐIỀU HÀNH. Một pod SQL chạy UTC là khung giờ "9h–15h" lệch đi 7 tiếng
    → job gọi FO lúc 16h–22h giờ VN, đúng cái điều BRD cấm tuyệt đối. Neo vào UTC rồi đổi múi
    ⇒ dời máy chủ / đổi tz hệ điều hành cũng không xê dịch.
    ⚠️ 'SE Asia Standard Time' = tên Windows của ICT (UTC+7). Trên SQL Server Linux dùng
       'Asia/Ho_Chi_Minh' — nếu triển khai Linux thì SỬA ĐÚNG MỘT CHỖ NÀY.
===========================================================================*/
CREATE OR ALTER FUNCTION UDF_JOB_NOW ()
RETURNS DATETIME
AS
BEGIN
    RETURN CAST(SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'SE Asia Standard Time' AS DATETIME);
END
GO

/*===========================================================================
  T_JOB_DEFINITION — KHAI BÁO loại job. Thêm job mới = INSERT 1 dòng (+1 handler C#).
    C_HANDLER = khoá tra trong JobRegistry của C#. DB không biết handler làm gì — đó là điểm
    khiến khung này generic thật, không phải "generic trên giấy".
===========================================================================*/
IF OBJECT_ID('T_JOB_DEFINITION') IS NULL
CREATE TABLE T_JOB_DEFINITION (
    C_JOB_CODE          VARCHAR(40)   NOT NULL,       -- 'FO_SNAPSHOT_RT', 'EOD_KICK', ...
    PK_JOB_DEFINITION   UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_JOB_DEF_PKID DEFAULT NEWID(),
    C_JOB_NAME          NVARCHAR(200) NOT NULL,
    C_HANDLER           VARCHAR(100)  NOT NULL,       -- khoá handler phía C# (JobRegistry)
    C_ENABLED           BIT           NOT NULL CONSTRAINT DF_JOB_DEF_EN DEFAULT 1,
    -- CHU KỲ: NULL = job KHÔNG định kỳ (chỉ chạy khi có người đẩy qua SP_JOB_ENQUEUE).
    --   Slot được neo vào 00:00 giờ VN ⇒ 900s = 9:00, 9:15, 9:30... (không trôi theo giờ pod khởi động).
    C_INTERVAL_SEC      INT           NULL,
    -- KHUNG GIỜ chạy. NULL/NULL = mọi giờ. Chỉ hỗ trợ khung TRONG NGÀY (from < to) — khung vắt
    --   qua nửa đêm KHÔNG hỗ trợ (cố ý: không có nghiệp vụ nào cần, thêm vào chỉ tăng chỗ sai).
    C_WINDOW_FROM       TIME(0)       NULL,
    C_WINDOW_TO         TIME(0)       NULL,
    C_BUSINESS_DAY_ONLY BIT           NOT NULL CONSTRAINT DF_JOB_DEF_BDO DEFAULT 0,  -- 1 = chỉ ngày GD (UDF_IS_BUSINESS_DATE)
    C_TIMEOUT_SEC       INT           NOT NULL CONSTRAINT DF_JOB_DEF_TO  DEFAULT 300, -- lease: quá hạn mà không heartbeat ⇒ coi như pod chết
    C_MAX_ATTEMPT       INT           NOT NULL CONSTRAINT DF_JOB_DEF_MA  DEFAULT 3,
    C_RETRY_DELAY_SEC   INT           NOT NULL CONSTRAINT DF_JOB_DEF_RD  DEFAULT 60,
    C_SINGLETON         BIT           NOT NULL CONSTRAINT DF_JOB_DEF_SGL DEFAULT 1,  -- 1 = lượt trước còn chạy thì KHÔNG sinh lượt mới
    C_PRIORITY          INT           NOT NULL CONSTRAINT DF_JOB_DEF_PRI DEFAULT 100,
    C_PAYLOAD           NVARCHAR(MAX) NULL,           -- payload mặc định (JSON) cho lượt định kỳ
    C_UPDATED_BY        VARCHAR(64)   NULL,
    C_UPDATED_AT        DATETIME      NOT NULL CONSTRAINT DF_JOB_DEF_UPD DEFAULT GETDATE(),
    CONSTRAINT PK_JOB_DEFINITION PRIMARY KEY CLUSTERED (C_JOB_CODE),
    CONSTRAINT UQ_JOB_DEFINITION_PKID UNIQUE (PK_JOB_DEFINITION),
    CONSTRAINT CK_JOB_DEF_WINDOW CHECK (
        (C_WINDOW_FROM IS NULL AND C_WINDOW_TO IS NULL) OR
        (C_WINDOW_FROM IS NOT NULL AND C_WINDOW_TO IS NOT NULL AND C_WINDOW_FROM < C_WINDOW_TO)),
    CONSTRAINT CK_JOB_DEF_INTERVAL CHECK (C_INTERVAL_SEC IS NULL OR C_INTERVAL_SEC >= 30)
);
GO

/*===========================================================================
  T_JOB_RUN — HÀNG ĐỢI **VÀ** NHẬT KÝ trong CÙNG MỘT bảng. 1 dòng = 1 LƯỢT chạy.
    Không tách queue/log làm 2 bảng: tách ra thì "job này đã chạy chưa" phải hỏi 2 nơi, và
    hai nơi đó sẽ lệch nhau đúng lúc có sự cố — tức là đúng lúc cần tra.

  ★ C_FIRE_KEY — KHOÁ CHỐNG TRÙNG, là thứ giữ tính đúng khi chạy nhiều pod:
      · Job định kỳ : 'yyyyMMddHHmm' của SLOT (vd '202608190915'). 10 pod cùng quét, cùng thấy
                      slot 9:15 tới hạn, cùng INSERT ⇒ UQ (job_code, fire_key) cho ĐÚNG MỘT pod
                      thắng, 9 pod còn lại nhận lỗi trùng khoá và im lặng bỏ qua.
      · Job ad-hoc  : do người gọi truyền (vd requestId của hệ khác) ⇒ gửi lại yêu cầu y hệt
                      KHÔNG sinh thêm lượt chạy. Không truyền → NEWID() (mỗi lần gọi là 1 lượt).
    ⇒ Chống trùng nằm ở RÀNG BUỘC DỮ LIỆU, không nằm ở lock, không nằm ở Redis, không nằm ở
      "hy vọng consumer group giao đúng một lần".

  VÒNG ĐỜI:  READY ──claim──> RUNNING ──> DONE
                ▲                 │  └──> FAILED ──(còn lượt thử)──> READY (sau C_RETRY_DELAY_SEC)
                │                 │                └──(hết lượt)──> DEAD
                └── SP_JOB_REAP ──┘ (lease quá hạn = pod chết giữa chừng)
                              └────> SKIPPED (ra ngoài khung giờ khi tới lượt chạy)
===========================================================================*/
IF OBJECT_ID('T_JOB_RUN') IS NULL
CREATE TABLE T_JOB_RUN (
    C_JOB_RUN_ID     BIGINT IDENTITY(1,1) NOT NULL,   -- clustered: append tuần tự (hàng đợi = bảng ghi nóng)
    PK_JOB_RUN       UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_JOB_RUN_PKID DEFAULT NEWID(),
    C_JOB_CODE       VARCHAR(40)    NOT NULL,
    C_FIRE_KEY       VARCHAR(64)    NOT NULL,         -- ★ xem chú thích trên
    C_STATUS         VARCHAR(10)    NOT NULL CONSTRAINT DF_JOB_RUN_ST DEFAULT 'READY',
    C_BUSINESS_DATE  DATE           NULL,             -- ngày nghiệp vụ job xử lý (FO snapshot = hôm nay)
    C_PAYLOAD        NVARCHAR(MAX)  NULL,
    C_ATTEMPT        INT            NOT NULL CONSTRAINT DF_JOB_RUN_AT  DEFAULT 0,
    C_OWNER          VARCHAR(64)    NULL,             -- pod đang giữ (hostname/podname)
    C_LEASE_UNTIL    DATETIME       NULL,             -- hết hạn mà không heartbeat ⇒ SP_JOB_REAP thu hồi
    C_HEARTBEAT_AT   DATETIME       NULL,
    C_RUN_AFTER      DATETIME       NOT NULL CONSTRAINT DF_JOB_RUN_RA  DEFAULT GETDATE(),  -- backoff sau khi FAILED
    C_STREAM_ID      VARCHAR(40)    NULL,             -- id entry Redis Stream (để XACK/đối chiếu khi mổ xẻ sự cố)
    C_ENQUEUED_AT    DATETIME       NOT NULL CONSTRAINT DF_JOB_RUN_EQ  DEFAULT GETDATE(),
    C_STARTED_AT     DATETIME       NULL,
    C_ENDED_AT       DATETIME       NULL,
    C_ROWS           BIGINT         NULL,             -- #đơn vị đã xử lý (job tự khai)
    C_MESSAGE        NVARCHAR(2000) NULL,
    CONSTRAINT PK_JOB_RUN_ID PRIMARY KEY CLUSTERED (C_JOB_RUN_ID),
    CONSTRAINT UQ_JOB_RUN_PKID UNIQUE NONCLUSTERED (PK_JOB_RUN),
    CONSTRAINT UQ_JOB_RUN_NK UNIQUE (C_JOB_CODE, C_FIRE_KEY),   -- ★ CỖ MÁY CHỐNG TRÙNG
    CONSTRAINT CK_JOB_RUN_STATUS CHECK (C_STATUS IN ('READY','RUNNING','DONE','FAILED','SKIPPED','DEAD'))
);
GO
-- Quét của SP_JOB_REAP + SP_JOB_ENQUEUE_DUE: luôn lọc theo trạng thái trước.
IF IndexProperty(OBJECT_ID('T_JOB_RUN'),'IX_JOB_RUN_STATUS','IndexID') IS NULL
CREATE INDEX IX_JOB_RUN_STATUS ON T_JOB_RUN (C_STATUS, C_RUN_AFTER)
    INCLUDE (C_JOB_CODE, C_LEASE_UNTIL, C_ATTEMPT, C_ENQUEUED_AT);
GO
-- Màn hình theo dõi + SP_JOB_PURGE: tra theo job & thời gian.
IF IndexProperty(OBJECT_ID('T_JOB_RUN'),'IX_JOB_RUN_CODE_TIME','IndexID') IS NULL
CREATE INDEX IX_JOB_RUN_CODE_TIME ON T_JOB_RUN (C_JOB_CODE, C_ENQUEUED_AT DESC)
    INCLUDE (C_STATUS, C_ROWS, C_STARTED_AT, C_ENDED_AT);
GO

/*===========================================================================
  UDF_JOB_IN_WINDOW — "job này ĐƯỢC PHÉP chạy tại thời điểm @p_at hay không?"
    Gộp 3 điều kiện: đang bật · đúng ngày (GD nếu job yêu cầu) · trong khung giờ.
    Trả 0 nếu job_code không tồn tại (không tồn tại thì không được phép chạy — im lặng an toàn).
    ⚠️ Đây là NGUỒN DUY NHẤT của luật khung giờ. Cả tầng 1 (sinh job) và tầng 2 (nhận job) đều
      gọi hàm này ⇒ không thể có chuyện hai tầng hiểu luật khác nhau.
===========================================================================*/
CREATE OR ALTER FUNCTION UDF_JOB_IN_WINDOW (@p_job_code VARCHAR(40), @p_at DATETIME)
RETURNS BIT
AS
BEGIN
    DECLARE @en BIT, @bdo BIT, @wf TIME(0), @wt TIME(0);
    SELECT @en=C_ENABLED, @bdo=C_BUSINESS_DAY_ONLY, @wf=C_WINDOW_FROM, @wt=C_WINDOW_TO
    FROM T_JOB_DEFINITION WHERE C_JOB_CODE=@p_job_code;

    IF @en IS NULL OR @en = 0 RETURN 0;                                   -- không tồn tại / đang tắt
    IF @bdo = 1 AND dbo.UDF_IS_BUSINESS_DATE(CAST(@p_at AS DATE)) = 0 RETURN 0;  -- T7/CN/lễ
    IF @wf IS NULL RETURN 1;                                              -- không khai khung ⇒ mọi giờ
    -- Biên: [from, to) — 15:00:00 chẵn là ĐÃ NGOÀI khung. "Đến 3h chiều" nghĩa là phiên đã đóng
    --   lúc 15:00, không phải "còn được gọi thêm một nhịp lúc 15:00".
    IF CAST(@p_at AS TIME(0)) >= @wf AND CAST(@p_at AS TIME(0)) < @wt RETURN 1;
    RETURN 0;
END
GO

/*===========================================================================
  SP_JOB_ENQUEUE — ĐẨY MỘT JOB (bất kỳ loại nào) vào hàng đợi. "Cứ có job đẩy vào là chạy":
    proc trả @p_job_run_id để app XADD ngay vào Redis Stream; worker đang XREADGROUP BLOCK
    nhận trong vài ms. Không có vòng chờ nào.
  IDEMPOTENT theo (job_code, fire_key): gọi lại cùng fire_key ⇒ err=4 + trả id CŨ, KHÔNG sinh lượt mới.
  err: 0 OK · 1 không có job_code · 2 job tắt · 3 ngoài khung giờ (KHÔNG tạo lượt) · 4 đã tồn tại · -1 runtime.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_JOB_ENQUEUE
    @p_job_code      VARCHAR(40),
    @p_fire_key      VARCHAR(64)   = NULL,           -- NULL ⇒ NEWID() (mỗi lần gọi = 1 lượt riêng)
    @p_payload       NVARCHAR(MAX) = NULL,
    @p_business_date DATE          = NULL,
    @p_ignore_window BIT           = 0,              -- 1 = chạy tay ngoài khung (vận hành). KHÔNG dùng cho job gọi FO.
    @p_user          VARCHAR(64)   = NULL,
    @p_err_code      INT           OUTPUT,
    @p_err_msg       NVARCHAR(400) OUTPUT,
    @p_job_run_id    BIGINT        = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    SET @p_err_code=0; SET @p_err_msg=NULL; SET @p_job_run_id=NULL;
    BEGIN TRY
        DECLARE @now DATETIME = dbo.UDF_JOB_NOW();
        DECLARE @en BIT = (SELECT C_ENABLED FROM T_JOB_DEFINITION WHERE C_JOB_CODE=@p_job_code);
        IF @en IS NULL
            BEGIN SET @p_err_code=1; SET @p_err_msg=CONCAT(N'job_code không tồn tại: ', @p_job_code); RETURN; END
        IF @en = 0
            BEGIN SET @p_err_code=2; SET @p_err_msg=CONCAT(N'Job đang TẮT: ', @p_job_code); RETURN; END

        -- ★ GUARD TẦNG 1. @p_ignore_window là cửa hậu cho vận hành chạy tay các job VÔ HẠI
        --   (dọn dẹp, tính lại...). Job chạm hệ ngoài như FO_SNAPSHOT_RT vẫn bị tầng 2 + tầng 3
        --   chặn, nên cửa hậu này KHÔNG mở được đường gọi FO ngoài giờ.
        IF @p_ignore_window = 0 AND dbo.UDF_JOB_IN_WINDOW(@p_job_code, @now) = 0
        BEGIN
            SET @p_err_code=3;
            SET @p_err_msg=CONCAT(N'Ngoài khung giờ cho phép của job ', @p_job_code, N' (',
                CONVERT(VARCHAR(19), @now, 120), N' giờ VN) — KHÔNG tạo lượt chạy.');
            RETURN;
        END

        DECLARE @fk VARCHAR(64) = ISNULL(@p_fire_key, CONVERT(VARCHAR(36), NEWID()));

        -- Đã có lượt với fire_key này ⇒ trả id cũ. Kiểm TRƯỚC để đường đi bình thường không
        --   phải dựa vào bắt lỗi trùng khoá; nhưng vẫn có TRY/CATCH bên dưới cho ca 2 pod
        --   INSERT đúng cùng mili-giây (kiểm trước KHÔNG phải là khoá).
        SELECT @p_job_run_id = C_JOB_RUN_ID FROM T_JOB_RUN
        WHERE C_JOB_CODE=@p_job_code AND C_FIRE_KEY=@fk;
        IF @p_job_run_id IS NOT NULL
        BEGIN
            SET @p_err_code=4;
            SET @p_err_msg=CONCAT(N'Lượt chạy đã tồn tại (fire_key=', @fk, N') — bỏ qua, không tạo trùng.');
            RETURN;
        END

        BEGIN TRY
            INSERT INTO T_JOB_RUN (C_JOB_CODE,C_FIRE_KEY,C_STATUS,C_BUSINESS_DATE,C_PAYLOAD,C_RUN_AFTER,C_ENQUEUED_AT,C_MESSAGE)
            VALUES (@p_job_code, @fk, 'READY', @p_business_date,
                    ISNULL(@p_payload, (SELECT C_PAYLOAD FROM T_JOB_DEFINITION WHERE C_JOB_CODE=@p_job_code)),
                    @now, @now, CONCAT(N'enqueue by ', ISNULL(@p_user,'(system)')));
            SET @p_job_run_id = SCOPE_IDENTITY();
        END TRY
        BEGIN CATCH
            IF ERROR_NUMBER() IN (2601,2627)   -- vỡ UQ_JOB_RUN_NK = pod khác vừa thắng. ĐÚNG Ý ĐỒ, không phải lỗi.
            BEGIN
                SELECT @p_job_run_id = C_JOB_RUN_ID FROM T_JOB_RUN
                WHERE C_JOB_CODE=@p_job_code AND C_FIRE_KEY=@fk;
                SET @p_err_code=4; SET @p_err_msg=N'Lượt chạy đã tồn tại (pod khác vừa tạo) — bỏ qua.';
            END
            ELSE THROW;
        END CATCH
    END TRY
    BEGIN CATCH
        SET @p_err_code=-1; SET @p_err_msg=ERROR_MESSAGE();
    END CATCH
END
GO

/*===========================================================================
  SP_JOB_ENQUEUE_DUE — QUÉT CẤU HÌNH: job định kỳ nào tới hạn thì tạo lượt chạy.
    MỌI POD đều gọi proc này (vd 10 giây/lần) — an toàn, vì UQ (job_code, fire_key) quyết ai thắng.

  SLOT neo vào 00:00 GIỜ VN:  slot = 00:00 + floor(giây_từ_nửa_đêm / interval) × interval
    ⇒ interval 900s cho ra 9:00 / 9:15 / 9:30... CỐ ĐỊNH. Nếu tính slot theo "lần chạy trước + 15
      phút" thì mỗi lần pod restart / job chậm là mốc trôi đi, và sau một ngày không ai còn đoán
      được job chạy vào phút nào — nhật ký thành thứ không đối chiếu được với dữ liệu FO.

  TRẢ VỀ result set các lượt VỪA TẠO (job_run_id, job_code, payload) → app XADD vào Redis Stream.
  KHÔNG tạo lượt mới nếu: ngoài khung giờ · job tắt · (C_SINGLETON=1 và lượt trước còn RUNNING).
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_JOB_ENQUEUE_DUE
    @p_user     VARCHAR(64)   = NULL,
    @p_err_code INT           OUTPUT,
    @p_err_msg  NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    SET @p_err_code=0; SET @p_err_msg=NULL;
    BEGIN TRY
        DECLARE @now DATETIME = dbo.UDF_JOB_NOW();
        DECLARE @midnight DATETIME = CAST(CAST(@now AS DATE) AS DATETIME);

        -- Ứng viên: job định kỳ, đang bật, ĐANG trong khung giờ, và (nếu singleton) không có lượt đang chạy.
        DECLARE @due TABLE (job_code VARCHAR(40), fire_key VARCHAR(64), payload NVARCHAR(MAX), bdate DATE);
        INSERT @due (job_code, fire_key, payload, bdate)
        SELECT d.C_JOB_CODE,
               -- fire_key = mốc slot 'yyyyMMddHHmm' (style 112 = yyyymmdd, +HH+mm ghép tay)
               CONVERT(CHAR(8), DATEADD(SECOND,
                     (DATEDIFF(SECOND, @midnight, @now) / d.C_INTERVAL_SEC) * d.C_INTERVAL_SEC, @midnight), 112)
               + FORMAT(DATEADD(SECOND,
                     (DATEDIFF(SECOND, @midnight, @now) / d.C_INTERVAL_SEC) * d.C_INTERVAL_SEC, @midnight), 'HHmm'),
               d.C_PAYLOAD,
               CAST(@now AS DATE)
        FROM T_JOB_DEFINITION d
        WHERE d.C_ENABLED = 1
          AND d.C_INTERVAL_SEC IS NOT NULL
          AND dbo.UDF_JOB_IN_WINDOW(d.C_JOB_CODE, @now) = 1        -- ★ GUARD TẦNG 1
          AND ( d.C_SINGLETON = 0
             OR NOT EXISTS (SELECT 1 FROM T_JOB_RUN r
                            WHERE r.C_JOB_CODE = d.C_JOB_CODE
                              AND r.C_STATUS = 'RUNNING'
                              -- lease còn hiệu lực mới tính là "đang chạy"; hết hạn = pod chết,
                              --   để SP_JOB_REAP thu hồi chứ không chặn slot kế tiếp vĩnh viễn.
                              AND r.C_LEASE_UNTIL > @now) );

        -- INSERT ... WHERE NOT EXISTS + UQ: pod nào thắng thì thắng. Không lock, không chờ.
        DECLARE @new TABLE (id BIGINT, job_code VARCHAR(40), payload NVARCHAR(MAX));
        INSERT INTO T_JOB_RUN (C_JOB_CODE,C_FIRE_KEY,C_STATUS,C_BUSINESS_DATE,C_PAYLOAD,C_RUN_AFTER,C_ENQUEUED_AT,C_MESSAGE)
        OUTPUT inserted.C_JOB_RUN_ID, inserted.C_JOB_CODE, inserted.C_PAYLOAD INTO @new
        SELECT u.job_code, u.fire_key, 'READY', u.bdate, u.payload, @now, @now,
               CONCAT(N'scheduler slot ', u.fire_key)
        FROM @due u
        WHERE NOT EXISTS (SELECT 1 FROM T_JOB_RUN r
                          WHERE r.C_JOB_CODE=u.job_code AND r.C_FIRE_KEY=u.fire_key);

        SELECT id AS C_JOB_RUN_ID, job_code AS C_JOB_CODE, payload AS C_PAYLOAD FROM @new;
    END TRY
    BEGIN CATCH
        -- Vỡ UQ = pod khác vừa tạo đúng slot đó. Đây là hành vi MONG MUỐN của thiết kế, không
        --   phải sự cố ⇒ nuốt, trả rỗng. Ném ra thì log mọi pod đỏ lòm 15 phút/lần, và người ta
        --   sẽ học cách phớt lờ log — rồi bỏ sót lỗi thật.
        IF ERROR_NUMBER() IN (2601,2627) BEGIN SELECT TOP 0 CAST(NULL AS BIGINT) AS C_JOB_RUN_ID,
               CAST(NULL AS VARCHAR(40)) AS C_JOB_CODE, CAST(NULL AS NVARCHAR(MAX)) AS C_PAYLOAD; RETURN; END
        SET @p_err_code=-1; SET @p_err_msg=ERROR_MESSAGE();
    END CATCH
END
GO

/*===========================================================================
  SP_JOB_CLAIM — WORKER XIN CẦM MỘT LƯỢT CHẠY. Đây là CHỐT CHẶN TRÙNG THẬT SỰ.
    Một câu UPDATE có điều kiện: chỉ pod nào đổi được trạng thái READY→RUNNING mới được chạy.
    20 pod cùng nhận một message (Redis giao lại, XAUTOCLAIM, người ta XADD nhầm 2 lần...) thì
    19 pod nhận @@ROWCOUNT=0 → err=5 → XACK và đi tiếp. KHÔNG có đường nào cho 2 pod cùng chạy.

    Nhận cả lượt RUNNING đã QUÁ HẠN LEASE (pod cũ chết): giành lại được. Đây là lý do phải có
    heartbeat — pod còn sống thì lease không bao giờ hết hạn, nên không ai giật được job của nó.

  ★ GUARD TẦNG 2 nằm ở đây: kiểm khung giờ tại ĐÚNG thời điểm chạy (không phải lúc sinh job).
  err: 0 OK (trả result set 1 dòng) · 5 claim hụt · 3 ngoài khung ⇒ SKIPPED · 6 quá số lần thử ⇒ DEAD.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_JOB_CLAIM
    @p_job_run_id BIGINT,
    @p_owner      VARCHAR(64),                       -- định danh pod (hostname + pid)
    @p_stream_id  VARCHAR(40)   = NULL,              -- id entry Redis (lưu để đối chiếu khi mổ xẻ sự cố)
    @p_err_code   INT           OUTPUT,
    @p_err_msg    NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    SET @p_err_code=0; SET @p_err_msg=NULL;
    BEGIN TRY
        DECLARE @now DATETIME = dbo.UDF_JOB_NOW();
        DECLARE @code VARCHAR(40), @status VARCHAR(10), @attempt INT, @maxatt INT, @timeout INT;
        SELECT @code=r.C_JOB_CODE, @status=r.C_STATUS, @attempt=r.C_ATTEMPT,
               @maxatt=d.C_MAX_ATTEMPT, @timeout=d.C_TIMEOUT_SEC
        FROM T_JOB_RUN r LEFT JOIN T_JOB_DEFINITION d ON d.C_JOB_CODE=r.C_JOB_CODE
        WHERE r.C_JOB_RUN_ID=@p_job_run_id;

        IF @code IS NULL
            BEGIN SET @p_err_code=1; SET @p_err_msg=CONCAT(N'Không có lượt chạy id=', @p_job_run_id); RETURN; END

        -- Hết lượt thử → DEAD. Chốt ở đây (chứ không chỉ ở COMPLETE) vì lượt bị REAP trả về READY
        --   nhiều lần cũng phải có điểm dừng, nếu không job hỏng sẽ quay vòng mãi mãi.
        IF @attempt >= @maxatt
        BEGIN
            UPDATE T_JOB_RUN SET C_STATUS='DEAD', C_ENDED_AT=@now,
                   C_MESSAGE=CONCAT(N'Vượt số lần thử (', @maxatt, N') — dừng hẳn, cần người xem.')
            WHERE C_JOB_RUN_ID=@p_job_run_id AND C_STATUS IN ('READY','FAILED');
            SET @p_err_code=6; SET @p_err_msg=N'Lượt chạy đã vượt số lần thử → DEAD.'; RETURN;
        END

        -- ★ GUARD TẦNG 2 — bắt ca job nằm trong stream vắt qua 15h00.
        IF dbo.UDF_JOB_IN_WINDOW(@code, @now) = 0
        BEGIN
            UPDATE T_JOB_RUN SET C_STATUS='SKIPPED', C_ENDED_AT=@now, C_OWNER=@p_owner,
                   C_MESSAGE=CONCAT(N'BỎ QUA: tới lượt chạy lúc ', CONVERT(VARCHAR(19),@now,120),
                                    N' (giờ VN) đã NGOÀI khung giờ cho phép của job ', @code, N'.')
            WHERE C_JOB_RUN_ID=@p_job_run_id AND C_STATUS IN ('READY','FAILED');
            SET @p_err_code=3;
            SET @p_err_msg=N'Ngoài khung giờ tại thời điểm chạy — đã đóng dấu SKIPPED, KHÔNG gọi hệ ngoài.';
            RETURN;
        END

        -- ★ CLAIM NGUYÊN TỬ. Điều kiện WHERE là toàn bộ cơ chế chống trùng của hệ.
        UPDATE T_JOB_RUN
           SET C_STATUS='RUNNING', C_OWNER=@p_owner, C_ATTEMPT=C_ATTEMPT+1,
               C_STARTED_AT=@now, C_HEARTBEAT_AT=@now,
               C_LEASE_UNTIL=DATEADD(SECOND, ISNULL(@timeout,300), @now),
               C_STREAM_ID=ISNULL(@p_stream_id, C_STREAM_ID), C_ENDED_AT=NULL
         WHERE C_JOB_RUN_ID=@p_job_run_id
           AND ( C_STATUS IN ('READY','FAILED')
              OR (C_STATUS='RUNNING' AND C_LEASE_UNTIL < @now) );   -- giành lại từ pod đã chết

        IF @@ROWCOUNT = 0
        BEGIN
            SET @p_err_code=5;
            SET @p_err_msg=CONCAT(N'Claim hụt (id=', @p_job_run_id, N', trạng thái=', @status,
                                  N') — pod khác đang giữ hoặc đã xong. Bỏ qua, KHÔNG chạy.');
            RETURN;
        END

        SELECT C_JOB_RUN_ID, C_JOB_CODE, C_FIRE_KEY, C_BUSINESS_DATE, C_PAYLOAD, C_ATTEMPT, C_LEASE_UNTIL
        FROM T_JOB_RUN WHERE C_JOB_RUN_ID=@p_job_run_id;
    END TRY
    BEGIN CATCH
        SET @p_err_code=-1; SET @p_err_msg=ERROR_MESSAGE();
    END CATCH
END
GO

/*===========================================================================
  SP_JOB_HEARTBEAT — worker báo "tao còn sống", gia hạn lease.
    ĐIỀU KIỆN C_OWNER=@p_owner là hàng rào chống pod ZOMBIE: pod bị treo lâu, lease hết hạn,
    pod khác đã giành job; pod cũ tỉnh dậy gọi heartbeat sẽ KHÔNG gia hạn được (0 dòng) → nó
    biết mình đã mất quyền và tự dừng, thay vì hai pod cùng ghi dữ liệu.
  Trả @p_still_mine=0 ⇒ worker PHẢI dừng ngay, KHÔNG ghi thêm gì.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_JOB_HEARTBEAT
    @p_job_run_id  BIGINT,
    @p_owner       VARCHAR(64),
    @p_rows        BIGINT = NULL,                    -- tiến độ (tuỳ chọn) — để màn hình theo dõi thấy job đang nhích
    @p_still_mine  BIT    = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @now DATETIME = dbo.UDF_JOB_NOW();
    DECLARE @timeout INT = (SELECT d.C_TIMEOUT_SEC FROM T_JOB_RUN r
                            INNER JOIN T_JOB_DEFINITION d ON d.C_JOB_CODE=r.C_JOB_CODE
                            WHERE r.C_JOB_RUN_ID=@p_job_run_id);
    UPDATE T_JOB_RUN
       SET C_HEARTBEAT_AT=@now,
           C_LEASE_UNTIL=DATEADD(SECOND, ISNULL(@timeout,300), @now),
           C_ROWS=ISNULL(@p_rows, C_ROWS)
     WHERE C_JOB_RUN_ID=@p_job_run_id AND C_OWNER=@p_owner AND C_STATUS='RUNNING';
    SET @p_still_mine = CASE WHEN @@ROWCOUNT=1 THEN 1 ELSE 0 END;
END
GO

/*===========================================================================
  SP_JOB_COMPLETE — đóng lượt chạy. OK → DONE. Lỗi → FAILED, và nếu còn lượt thử thì tự
    đặt lại READY + C_RUN_AFTER = now + retry_delay (SP_JOB_REAP sẽ đẩy lại vào stream).
    ⚠️ Job đã hết giờ (vd FO 15h00) mà FAILED thì lần thử sau sẽ bị GUARD TẦNG 2 đóng dấu
      SKIPPED — đúng ý đồ: thà bỏ một nhịp 15 phút còn hơn gọi FO ngoài giờ.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_JOB_COMPLETE
    @p_job_run_id BIGINT,
    @p_owner      VARCHAR(64),
    @p_ok         BIT,
    @p_rows       BIGINT        = NULL,
    @p_message    NVARCHAR(2000)= NULL,
    @p_err_code   INT           OUTPUT,
    @p_err_msg    NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    SET @p_err_code=0; SET @p_err_msg=NULL;
    BEGIN TRY
        DECLARE @now DATETIME = dbo.UDF_JOB_NOW();
        DECLARE @attempt INT, @maxatt INT, @delay INT;
        SELECT @attempt=r.C_ATTEMPT, @maxatt=d.C_MAX_ATTEMPT, @delay=d.C_RETRY_DELAY_SEC
        FROM T_JOB_RUN r INNER JOIN T_JOB_DEFINITION d ON d.C_JOB_CODE=r.C_JOB_CODE
        WHERE r.C_JOB_RUN_ID=@p_job_run_id;

        IF @p_ok = 1
            UPDATE T_JOB_RUN SET C_STATUS='DONE', C_ENDED_AT=@now, C_ROWS=@p_rows,
                   C_MESSAGE=@p_message, C_LEASE_UNTIL=NULL
             WHERE C_JOB_RUN_ID=@p_job_run_id AND C_OWNER=@p_owner AND C_STATUS='RUNNING';
        ELSE IF @attempt < @maxatt
            UPDATE T_JOB_RUN SET C_STATUS='READY', C_ENDED_AT=@now, C_ROWS=@p_rows,
                   C_RUN_AFTER=DATEADD(SECOND, ISNULL(@delay,60), @now), C_LEASE_UNTIL=NULL, C_OWNER=NULL,
                   C_MESSAGE=CONCAT(N'Lỗi lần ', @attempt, N'/', @maxatt, N': ', @p_message)
             WHERE C_JOB_RUN_ID=@p_job_run_id AND C_OWNER=@p_owner AND C_STATUS='RUNNING';
        ELSE
            UPDATE T_JOB_RUN SET C_STATUS='DEAD', C_ENDED_AT=@now, C_ROWS=@p_rows, C_LEASE_UNTIL=NULL,
                   C_MESSAGE=CONCAT(N'Lỗi lần cuối (', @attempt, N'/', @maxatt, N'): ', @p_message)
             WHERE C_JOB_RUN_ID=@p_job_run_id AND C_OWNER=@p_owner AND C_STATUS='RUNNING';

        IF @@ROWCOUNT = 0
        BEGIN
            SET @p_err_code=5;
            SET @p_err_msg=N'Không đóng được lượt chạy: pod này KHÔNG còn là chủ (lease đã bị thu hồi) hoặc lượt đã đóng.';
        END
    END TRY
    BEGIN CATCH
        SET @p_err_code=-1; SET @p_err_msg=ERROR_MESSAGE();
    END CATCH
END
GO

/*===========================================================================
  SP_JOB_REAP — LƯỚI AN TOÀN, chạy trên MỌI pod (vd 30 giây/lần). Làm 2 việc:

  (1) THU HỒI lượt RUNNING quá hạn lease (pod chết/bị evict giữa chừng) → READY.
  (2) TRẢ VỀ các lượt READY tới hạn để app XADD (lại) vào Redis Stream.

  ★ (2) LÀ THỨ BÙ ĐẮP CHO VIỆC ĐẶT HÀNG ĐỢI Ở REDIS. Redis Streams giao message rất nhanh
    nhưng nó KHÔNG phải sổ cái: FLUSHALL / mất pod Redis không bền / TTL / XADD hụt vì pod
    chết ngay sau khi INSERT xong — mọi trường hợp đó đều làm message BIẾN MẤT trong khi
    T_JOB_RUN vẫn ghi READY. Không có (2) thì job đó nằm im vĩnh viễn và KHÔNG AI BIẾT.
    Có (2) thì: mất message ⇒ chậm tối đa một nhịp reaper, rồi tự hồi. XADD trùng cũng vô hại
    vì SP_JOB_CLAIM chỉ cho một pod thắng.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_JOB_REAP
    @p_stale_sec INT           = 30,                 -- READY quá ngần này giây mà chưa ai chạy ⇒ nghi mất message
    @p_err_code  INT           OUTPUT,
    @p_err_msg   NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    SET @p_err_code=0; SET @p_err_msg=NULL;
    BEGIN TRY
        DECLARE @now DATETIME = dbo.UDF_JOB_NOW();

        -- (1) Pod chết giữa chừng: lease hết hạn ⇒ trả về hàng đợi. C_ATTEMPT KHÔNG tăng ở đây
        --     (SP_JOB_CLAIM mới tăng) ⇒ số lần thử luôn đếm đúng số lần THỰC SỰ chạy.
        UPDATE T_JOB_RUN
           SET C_STATUS='READY', C_OWNER=NULL, C_LEASE_UNTIL=NULL, C_RUN_AFTER=@now,
               C_MESSAGE=CONCAT(N'Thu hồi: pod ', ISNULL(C_OWNER,'?'), N' mất tín hiệu (lease hết hạn ',
                                CONVERT(VARCHAR(19), C_LEASE_UNTIL, 120), N').')
         WHERE C_STATUS='RUNNING' AND C_LEASE_UNTIL < @now;

        -- (2) Lượt READY tới hạn, nằm lâu bất thường ⇒ trả về cho app XADD lại.
        SELECT r.C_JOB_RUN_ID, r.C_JOB_CODE, r.C_PAYLOAD
        FROM T_JOB_RUN r
        WHERE r.C_STATUS='READY'
          AND r.C_RUN_AFTER <= @now
          AND DATEDIFF(SECOND, r.C_ENQUEUED_AT, @now) >= @p_stale_sec
          -- Không đẩy lại job đã ra ngoài khung giờ: để nó nằm đó, lượt claim kế tiếp (nếu có)
          --   sẽ đóng dấu SKIPPED. Đẩy lại chỉ tổ sinh vòng lặp vô nghĩa lúc 22h đêm.
          AND dbo.UDF_JOB_IN_WINDOW(r.C_JOB_CODE, @now) = 1
        ORDER BY r.C_JOB_RUN_ID;
    END TRY
    BEGIN CATCH
        SET @p_err_code=-1; SET @p_err_msg=ERROR_MESSAGE();
    END CATCH
END
GO

/*===========================================================================
  SP_JOB_PURGE — dọn nhật ký cũ. Job 15 phút × 6 tiếng × ngày GD ≈ 25 dòng/ngày cho FO,
    nhưng job fan-out (nếu sau này có) sinh hàng nghìn dòng/ngày ⇒ phải có đường dọn từ đầu.
    CHỈ xoá lượt ĐÃ ĐÓNG. KHÔNG BAO GIỜ xoá DEAD: đó là những lượt cần người nhìn.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_JOB_PURGE
    @p_keep_days INT = 30,
    @p_rows      BIGINT = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @cut DATETIME = DATEADD(DAY, -@p_keep_days, dbo.UDF_JOB_NOW());
    DELETE FROM T_JOB_RUN
    WHERE C_STATUS IN ('DONE','SKIPPED') AND C_ENDED_AT < @cut;
    SET @p_rows = @@ROWCOUNT;
END
GO

/*===========================================================================
  SP_GET_JOB_STATUS — API theo dõi. RS1: từng job (lượt gần nhất + sức khoẻ).
    RS2: các lượt DEAD/FAILED gần đây (thứ cần người xử lý).
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_JOB_STATUS
    @p_job_code VARCHAR(40)   = NULL,
    @p_user     VARCHAR(64)   = NULL,
    @p_err_code INT           OUTPUT,
    @p_err_msg  NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @p_err_code=0; SET @p_err_msg=NULL;
    BEGIN TRY
        DECLARE @now DATETIME = dbo.UDF_JOB_NOW();
        SELECT d.C_JOB_CODE, d.C_JOB_NAME, d.C_ENABLED, d.C_INTERVAL_SEC,
               d.C_WINDOW_FROM, d.C_WINDOW_TO, d.C_BUSINESS_DAY_ONLY,
               dbo.UDF_JOB_IN_WINDOW(d.C_JOB_CODE, @now) AS C_IN_WINDOW_NOW,
               l.C_JOB_RUN_ID AS C_LAST_RUN_ID, l.C_STATUS AS C_LAST_STATUS,
               l.C_STARTED_AT AS C_LAST_STARTED_AT, l.C_ENDED_AT AS C_LAST_ENDED_AT,
               l.C_ROWS AS C_LAST_ROWS, l.C_MESSAGE AS C_LAST_MESSAGE,
               DATEDIFF(SECOND, l.C_ENDED_AT, @now) AS C_SEC_SINCE_LAST_END,
               q.C_QUEUED, q.C_RUNNING, q.C_DEAD_7D
        FROM T_JOB_DEFINITION d
        OUTER APPLY (SELECT TOP 1 * FROM T_JOB_RUN r WHERE r.C_JOB_CODE=d.C_JOB_CODE
                     ORDER BY r.C_JOB_RUN_ID DESC) l
        OUTER APPLY (SELECT SUM(CASE WHEN r.C_STATUS='READY'   THEN 1 ELSE 0 END) AS C_QUEUED,
                            SUM(CASE WHEN r.C_STATUS='RUNNING' THEN 1 ELSE 0 END) AS C_RUNNING,
                            SUM(CASE WHEN r.C_STATUS='DEAD' AND r.C_ENDED_AT > DATEADD(DAY,-7,@now) THEN 1 ELSE 0 END) AS C_DEAD_7D
                     FROM T_JOB_RUN r WHERE r.C_JOB_CODE=d.C_JOB_CODE) q
        WHERE (@p_job_code IS NULL OR d.C_JOB_CODE=@p_job_code)
        ORDER BY d.C_JOB_CODE;

        SELECT TOP 200 C_JOB_RUN_ID, C_JOB_CODE, C_FIRE_KEY, C_STATUS, C_ATTEMPT,
               C_OWNER, C_STARTED_AT, C_ENDED_AT, C_MESSAGE
        FROM T_JOB_RUN
        WHERE C_STATUS IN ('DEAD','FAILED') AND (@p_job_code IS NULL OR C_JOB_CODE=@p_job_code)
        ORDER BY C_JOB_RUN_ID DESC;
    END TRY
    BEGIN CATCH
        SET @p_err_code=-1; SET @p_err_msg=ERROR_MESSAGE();
    END CATCH
END
GO

/*############################################################################
  TẦNG B — NGHIỆP VỤ: SNAPSHOT NEAR-REALTIME TỪ FO
############################################################################*/

/*===========================================================================
  SP_GET_FO_SNAPSHOT_SCOPE — danh sách tiểu khoản indexing ĐANG MỞ, để worker chia batch.
    Trả cả C_CUST_CODE và C_SUB_ACCOUNT_NO vì FO định danh theo SỐ TÀI KHOẢN, còn BRD chia
    batch theo KHÁCH HÀNG (50 KH/batch) — worker gom theo C_CUST_CODE rồi cắt 50.
    ORDER BY C_CUST_CODE: một khách hàng có nhiều tiểu khoản thì các tiểu khoản đó nằm LIỀN
    NHAU ⇒ cắt batch không bao giờ xẻ đôi một khách hàng.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_FO_SNAPSHOT_SCOPE
    @p_master_code VARCHAR(20)   = NULL,             -- NULL = toàn bộ master
    @p_user        VARCHAR(64)   = NULL,
    @p_err_code    INT           OUTPUT,
    @p_err_msg     NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @p_err_code=0; SET @p_err_msg=NULL;
    BEGIN TRY
        SELECT p.C_CUST_CODE, p.C_SI_ACCOUNT, p.C_SUB_ACCOUNT_NO, p.C_MASTER_CODE
        FROM T_SI_PORTFOLIO p
        WHERE p.C_STATUS='ACTIVE'
          AND (@p_master_code IS NULL OR p.C_MASTER_CODE=@p_master_code)
        ORDER BY p.C_CUST_CODE, p.C_SI_ACCOUNT;
    END TRY
    BEGIN CATCH
        SET @p_err_code=-1; SET @p_err_msg=ERROR_MESSAGE();
    END CATCH
END
GO

/*===========================================================================
  SP_INGEST_FO_SNAPSHOT_RT — GHI 1 BATCH snapshot giữa phiên vào T_SI_BALANCE (C_SRC='RT').

  JSON: [{"si_account":"...","aum":123,"cash":45,"cash_available":40,
          "dividend_pending":0,"sell_pending":0}, ...]
    KHÔNG có daily_return: FO không tính TWR giữa phiên. Dòng RT để C_DAILY_RETURN NULL —
    KHÔNG được điền 0. Điền 0 nghĩa là "hôm nay lãi đúng 0%", một khẳng định sai sẽ chui thẳng
    vào chuỗi compound nếu sau này ai đó lỡ bỏ bộ lọc C_SRC.

  ★ HAI LUẬT BẤT DI BẤT DỊCH:
    (1) RT KHÔNG BAO GIỜ ĐÈ EOD. MERGE chỉ UPDATE khi dòng đích đang là 'RT'. Dòng EOD của
        cùng (ngày, tiểu khoản) đã tồn tại ⇒ BỎ QUA tiểu khoản đó (đếm vào @p_skipped_eod).
        Vì sao: số chốt là thứ đã đi vào báo cáo, phí, đối soát. Một batch RT tới muộn (retry,
        pod zombie, người chạy tay) mà đè lên được thì nó ÂM THẦM thay số chốt bằng số 9h15.
    (2) RT chỉ ghi cho NGÀY HÔM NAY và chỉ trong NGÀY GD. Ghi lùi quá khứ là không có nghĩa
        (quá khứ đã chốt) và là đường ngắn nhất để hỏng dữ liệu lịch sử.

  UPSERT: MERGE theo (C_BUSINESS_DATE, C_SI_ACCOUNT) = đúng khoá UQ_SI_NAV_BALANCE_NK ⇒ gọi
    lại bao nhiêu lần cũng chỉ có 1 dòng/tiểu khoản/ngày. KHÔNG DELETE+INSERT như đường EOD:
    DELETE sẽ xoá nhầm dòng EOD nếu luật (1) bị lọt, và mỗi nhịp 15 phút lại đốt một dãy
    C_NAV_BALANCE_ID mới trên bảng tỷ dòng.

  KHÔNG roll-forward T_SI_CURRENT / T_MASTER_CURRENT — CỐ Ý: T_SI_CURRENT.C_CASH_AVAILABLE là
    NGUỒN DUY NHẤT của SP_FEE_COLLECT. Cho RT ghi vào đó là cắt phí theo số dư giữa phiên.

  err: 0 OK · 20 JSON/tham số sai · 21 thiếu aum/cash · 22 ngày không hợp lệ (không phải hôm nay/
       không phải ngày GD) · -1 runtime.  @p_rows = #tiểu khoản đã ghi RT; @p_skipped_eod = #bỏ qua vì đã có số chốt.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_INGEST_FO_SNAPSHOT_RT
    @p_json          NVARCHAR(MAX),
    @p_business_date DATE,
    @p_snapshot_at   DATETIME      = NULL,           -- thời điểm FO chụp (NULL = giờ VN hiện tại)
    @p_user          VARCHAR(64)   = NULL,
    @p_err_code      INT           OUTPUT,
    @p_err_msg       NVARCHAR(400) OUTPUT,
    @p_rows          BIGINT        = NULL OUTPUT,
    @p_skipped_eod   INT           = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    SET @p_err_code=0; SET @p_err_msg=NULL; SET @p_rows=0; SET @p_skipped_eod=0;
    BEGIN TRY
        DECLARE @now DATETIME = dbo.UDF_JOB_NOW();
        DECLARE @at  DATETIME = ISNULL(@p_snapshot_at, @now);

        IF @p_business_date IS NULL BEGIN SET @p_err_code=20; SET @p_err_msg=N'@p_business_date NULL'; RETURN; END
        IF @p_json IS NULL OR ISJSON(@p_json)<>1 BEGIN SET @p_err_code=20; SET @p_err_msg=N'@p_json không hợp lệ'; RETURN; END

        -- ★ GUARD TẦNG 3 — lưới cuối, chặn cả trường hợp gọi proc bằng tay.
        IF @p_business_date <> CAST(@now AS DATE)
        BEGIN
            SET @p_err_code=22;
            SET @p_err_msg=CONCAT(N'Snapshot RT chỉ ghi cho NGÀY HÔM NAY (', CONVERT(VARCHAR(10),CAST(@now AS DATE),23),
                                  N'), nhận được ', CONVERT(VARCHAR(10),@p_business_date,23), N' — từ chối.');
            RETURN;
        END
        IF dbo.UDF_IS_BUSINESS_DATE(@p_business_date) = 0
        BEGIN
            SET @p_err_code=22;
            SET @p_err_msg=CONCAT(N'Ngày ', CONVERT(VARCHAR(10),@p_business_date,23),
                                  N' KHÔNG phải ngày giao dịch — không có snapshot giữa phiên.');
            RETURN;
        END

        DECLARE @src TABLE (C_SI_ACCOUNT VARCHAR(20) PRIMARY KEY, C_AUM DECIMAL(20,0),
                            C_CASH DECIMAL(20,0), C_CASH_AVAILABLE DECIMAL(20,0),
                            C_DIVIDEND_PENDING DECIMAL(20,0), C_SELL_PENDING DECIMAL(20,0));
        INSERT @src
        SELECT j.si_account, j.aum, j.cash, ISNULL(j.cash_available, j.cash),
               ISNULL(j.dividend_pending,0), ISNULL(j.sell_pending,0)
        FROM OPENJSON(@p_json) WITH (
            si_account       VARCHAR(20)   '$.si_account',
            aum              DECIMAL(20,0) '$.aum',
            cash             DECIMAL(20,0) '$.cash',
            cash_available   DECIMAL(20,0) '$.cash_available',
            dividend_pending DECIMAL(20,0) '$.dividend_pending',
            sell_pending     DECIMAL(20,0) '$.sell_pending') j;

        -- Validate CHỈ trên tiểu khoản thuộc SDI (FO có thể trả cả acc ngoài phạm vi — INNER JOIN lọc).
        IF EXISTS (SELECT 1 FROM @src s INNER JOIN T_SI_PORTFOLIO p
                     ON p.C_SI_ACCOUNT=s.C_SI_ACCOUNT AND p.C_STATUS='ACTIVE'
                   WHERE s.C_AUM IS NULL OR s.C_CASH IS NULL)
            BEGIN SET @p_err_code=21; SET @p_err_msg=N'Thiếu aum/cash cho tiểu khoản thuộc SDI'; RETURN; END

        BEGIN TRAN;
        -- Đếm TRƯỚC phần bị chặn bởi luật (1) — để log nói được "bỏ qua N vì đã chốt", thay vì
        --   im lặng ghi ít hơn và không ai hiểu vì sao.
        SELECT @p_skipped_eod = COUNT(*)
        FROM @src s
        INNER JOIN T_SI_PORTFOLIO p ON p.C_SI_ACCOUNT=s.C_SI_ACCOUNT AND p.C_STATUS='ACTIVE'
        INNER JOIN T_SI_BALANCE b ON b.C_SI_ACCOUNT=s.C_SI_ACCOUNT AND b.C_BUSINESS_DATE=@p_business_date
        WHERE b.C_SRC='EOD';

        MERGE T_SI_BALANCE AS t
        USING (SELECT s.C_SI_ACCOUNT, p.C_CUST_CODE, p.C_MASTER_CODE, s.C_AUM, s.C_CASH,
                      s.C_CASH_AVAILABLE, s.C_DIVIDEND_PENDING, s.C_SELL_PENDING
               FROM @src s
               INNER JOIN T_SI_PORTFOLIO p ON p.C_SI_ACCOUNT=s.C_SI_ACCOUNT AND p.C_STATUS='ACTIVE') w
           ON t.C_BUSINESS_DATE=@p_business_date AND t.C_SI_ACCOUNT=w.C_SI_ACCOUNT
        -- ★ LUẬT (1): điều kiện t.C_SRC='RT' là toàn bộ hàng rào bảo vệ số chốt.
        WHEN MATCHED AND t.C_SRC='RT' THEN UPDATE SET
            t.C_AUM=w.C_AUM, t.C_CASH=w.C_CASH, t.C_CASH_AVAILABLE=w.C_CASH_AVAILABLE,
            t.C_DIVIDEND_PENDING=w.C_DIVIDEND_PENDING, t.C_SELL_PENDING=w.C_SELL_PENDING,
            t.C_MASTER_CODE=w.C_MASTER_CODE, t.C_CUST_CODE=w.C_CUST_CODE, t.C_RT_AT=@at
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_AUM,C_DAILY_RETURN,
                    C_CASH,C_CASH_AVAILABLE,C_CASH_IN,C_CASH_OUT,C_DIVIDEND_PENDING,C_SELL_PENDING,C_SRC,C_RT_AT)
            VALUES (@p_business_date, w.C_SI_ACCOUNT, w.C_CUST_CODE, w.C_MASTER_CODE, w.C_AUM,
                    NULL,          -- ★ daily_return: FO KHÔNG cấp TWR giữa phiên. NULL, tuyệt đối không 0.
                    w.C_CASH, w.C_CASH_AVAILABLE,
                    0, 0,          -- cash_in/out: snapshot không mang dòng tiền cả ngày → 0, EOD ghi số thật
                    w.C_DIVIDEND_PENDING, w.C_SELL_PENDING, 'RT', @at);
        SET @p_rows = @@ROWCOUNT;
        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        SET @p_rows=0;
        SET @p_err_code=-1; SET @p_err_msg=ERROR_MESSAGE();
    END CATCH
END
GO

/*===========================================================================
  SP_RT_MASTER_AGG — GỘP cấp master từ các dòng RT → T_MASTER_BALANCE (C_SRC='RT').
    Worker gọi MỘT LẦN ở cuối chu kỳ (sau khi mọi batch đã ghi xong), không gọi sau từng batch:
    gộp giữa chừng cho ra một con số master "nửa cũ nửa mới" mà dashboard không phân biệt được.

  C_DAILY_RETURN của dòng RT = NULL, CÓ CHỦ ĐÍCH. Lợi suất ngày là TWR (đã khử dòng tiền);
    giữa phiên SDI không có dòng tiền trong ngày nên KHÔNG THỂ tính TWR. Bịa một con số
    (AUM_now/AUM_hôm_qua − 1) rồi đặt vào đúng cột mà cả hệ dùng để compound là cách chắc chắn
    nhất để một ngày nào đó nó chui vào chuỗi hiệu suất. Biến động giữa phiên được tính RIÊNG,
    ON-READ, ở SP_GET_PM_RT_OVERVIEW — nơi nó được gắn nhãn đúng bản chất.

  C_TOTAL_ACCOUNT = số tiểu khoản ACTIVE của master (từ registry) ; C_RT_SI_COUNT = số CÓ số RT.
    Hai con số này lệch nhau ⇒ FO chưa trả đủ ⇒ dashboard hiện "đang cập nhật n/N" thay vì
    hiển thị một tổng AUM thiếu người mà trông y như đủ.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_RT_MASTER_AGG
    @p_business_date DATE,
    @p_user          VARCHAR(64)   = NULL,
    @p_err_code      INT           OUTPUT,
    @p_err_msg       NVARCHAR(400) OUTPUT,
    @p_rows          BIGINT        = NULL OUTPUT
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    SET @p_err_code=0; SET @p_err_msg=NULL; SET @p_rows=0;
    BEGIN TRY
        DECLARE @now DATETIME = dbo.UDF_JOB_NOW();
        IF @p_business_date <> CAST(@now AS DATE)
        BEGIN
            SET @p_err_code=22;
            SET @p_err_msg=N'Gộp RT chỉ áp dụng cho ngày hôm nay.'; RETURN;
        END

        ;WITH agg AS (
            SELECT b.C_MASTER_CODE,
                   SUM(b.C_AUM)  AS AUM,
                   SUM(b.C_CASH) AS CASH,
                   COUNT(*)      AS NRT,
                   MAX(b.C_RT_AT) AS RTAT
            FROM T_SI_BALANCE b
            WHERE b.C_BUSINESS_DATE=@p_business_date AND b.C_SRC='RT'
            GROUP BY b.C_MASTER_CODE
        ), tot AS (
            SELECT C_MASTER_CODE, COUNT(*) AS NACC
            FROM T_SI_PORTFOLIO WHERE C_STATUS='ACTIVE' GROUP BY C_MASTER_CODE
        )
        MERGE T_MASTER_BALANCE AS t
        USING (SELECT a.C_MASTER_CODE, a.AUM, a.CASH, a.NRT, a.RTAT, ISNULL(x.NACC,a.NRT) AS NACC
               FROM agg a LEFT JOIN tot x ON x.C_MASTER_CODE=a.C_MASTER_CODE) s
           ON t.C_BUSINESS_DATE=@p_business_date AND t.C_MASTER_CODE=s.C_MASTER_CODE
        -- ★ LUẬT (1) lặp lại ở cấp master: EOD đã chốt master này rồi thì RT KHÔNG đè.
        WHEN MATCHED AND t.C_SRC='RT' THEN UPDATE SET
            t.C_AUM=s.AUM, t.C_CASH=s.CASH, t.C_TOTAL_ACCOUNT=s.NACC,
            t.C_RT_SI_COUNT=s.NRT, t.C_RT_AT=s.RTAT
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (C_BUSINESS_DATE,C_MASTER_CODE,C_CASH,C_AUM,C_DAILY_RETURN,
                    C_CASH_IN,C_CASH_OUT,C_TOTAL_ACCOUNT,C_SRC,C_RT_AT,C_RT_SI_COUNT)
            VALUES (@p_business_date, s.C_MASTER_CODE, s.CASH, s.AUM,
                    NULL,      -- ★ xem chú thích đầu proc: KHÔNG bịa lợi suất giữa phiên
                    0, 0, s.NACC, 'RT', s.RTAT, s.NRT);
        SET @p_rows = @@ROWCOUNT;
    END TRY
    BEGIN CATCH
        SET @p_err_code=-1; SET @p_err_msg=ERROR_MESSAGE();
    END CATCH
END
GO

/*===========================================================================
  SP_GET_PM_RT_OVERVIEW — API DASHBOARD PM NEAR-REALTIME (cấp master).
    Đây là proc DUY NHẤT được phép đọc dòng RT. 05_API/06_PM_API vẫn chỉ đọc số chốt.

  BIẾN ĐỘNG GIỮA PHIÊN so với phiên chốt trước — tính TRÊN CÙNG TẬP TIỂU KHOẢN:
    C_AUM_PREV_SAMESET = Σ AUM phiên chốt gần nhất, CHỈ CỦA những tiểu khoản có số RT hôm nay.
    Vì sao không lấy thẳng AUM master phiên trước: FO trả về 4.800/5.000 tiểu khoản thì hiệu
    (AUM_RT − AUM_master_hôm_qua) gồm cả 200 khách hàng chưa có số ⇒ dashboard đỏ rực −4%
    trong khi thị trường không hề rơi. Lỗi kiểu này rất khó cãi lại vì con số nào cũng "có thật".

  ⚠️ C_CHANGE_PCT CHƯA KHỬ DÒNG TIỀN — khách nạp 10 tỷ lúc 10h sẽ hiện thành "tăng". Đây là
    biến động tài sản, KHÔNG phải hiệu suất. Nhãn trên giao diện phải nói đúng điều đó; số
    hiệu suất thật vẫn lấy từ 06_PM_API (T-1, TWR).
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_PM_RT_OVERVIEW
    @p_master_code VARCHAR(20)   = NULL,             -- NULL = mọi master ACTIVE
    @p_user        VARCHAR(64)   = NULL,
    @p_err_code    INT           OUTPUT,
    @p_err_msg     NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @p_err_code=0; SET @p_err_msg=NULL;
    BEGIN TRY
        DECLARE @now DATETIME = dbo.UDF_JOB_NOW();
        DECLARE @today DATE = CAST(@now AS DATE);

        SELECT  m.C_MASTER_CODE,
                mp.C_MASTER_NAME,
                rt.C_AUM              AS C_AUM_RT,
                rt.C_CASH             AS C_CASH_RT,
                rt.C_RT_SI_COUNT      AS C_SI_COUNT_RT,
                rt.C_TOTAL_ACCOUNT    AS C_SI_COUNT_TOTAL,
                rt.C_RT_AT            AS C_SNAPSHOT_AT,
                DATEDIFF(MINUTE, rt.C_RT_AT, @now) AS C_STALE_MINUTES,   -- >  ~2 chu kỳ ⇒ job đang hỏng
                pv.C_PREV_DATE,
                pv.C_AUM_PREV_SAMESET,
                rt.C_AUM - pv.C_AUM_PREV_SAMESET AS C_CHANGE_ABS,
                CASE WHEN pv.C_AUM_PREV_SAMESET > 0
                     THEN CAST((rt.C_AUM - pv.C_AUM_PREV_SAMESET) * 1.0 / pv.C_AUM_PREV_SAMESET AS DECIMAL(18,8))
                END AS C_CHANGE_PCT,   -- ⚠️ biến động tài sản, CHƯA khử dòng tiền — KHÔNG phải hiệu suất
                CASE WHEN rt.C_RT_SI_COUNT = rt.C_TOTAL_ACCOUNT THEN 'FULL' ELSE 'PARTIAL' END AS C_COVERAGE,
                dbo.UDF_JOB_IN_WINDOW('FO_SNAPSHOT_RT', @now) AS C_IN_TRADING_WINDOW
        FROM (SELECT DISTINCT C_MASTER_CODE FROM T_MASTER_BALANCE
              WHERE C_BUSINESS_DATE=@today AND C_SRC='RT') m
        INNER JOIN T_MASTER_BALANCE rt
                ON rt.C_MASTER_CODE=m.C_MASTER_CODE AND rt.C_BUSINESS_DATE=@today AND rt.C_SRC='RT'
        LEFT  JOIN T_MASTER_PORTFOLIO mp ON mp.C_MASTER_CODE=m.C_MASTER_CODE
        OUTER APPLY (
            -- Phiên CHỐT gần nhất trước hôm nay, và tổng AUM của nó TRÊN ĐÚNG TẬP tiểu khoản
            --   đang có số RT (xem chú thích đầu proc).
            SELECT d.C_PREV_DATE,
                   (SELECT SUM(pb.C_AUM) FROM T_SI_BALANCE pb
                    WHERE pb.C_MASTER_CODE=m.C_MASTER_CODE AND pb.C_BUSINESS_DATE=d.C_PREV_DATE
                      AND pb.C_SRC='EOD'
                      AND EXISTS (SELECT 1 FROM T_SI_BALANCE rb
                                  WHERE rb.C_SI_ACCOUNT=pb.C_SI_ACCOUNT
                                    AND rb.C_BUSINESS_DATE=@today AND rb.C_SRC='RT')) AS C_AUM_PREV_SAMESET
            FROM (SELECT MAX(C_BUSINESS_DATE) AS C_PREV_DATE FROM T_MASTER_BALANCE
                  WHERE C_MASTER_CODE=m.C_MASTER_CODE AND C_SRC='EOD' AND C_BUSINESS_DATE < @today) d
        ) pv
        WHERE (@p_master_code IS NULL OR m.C_MASTER_CODE=@p_master_code)
        ORDER BY m.C_MASTER_CODE;
    END TRY
    BEGIN CATCH
        SET @p_err_code=-1; SET @p_err_msg=ERROR_MESSAGE();
    END CATCH
END
GO

/*===========================================================================
  SEED — khai báo job quét FO. Đổi chu kỳ/khung giờ = UPDATE bảng này, KHÔNG deploy lại code.
    900s = 15 phút · 09:00–15:00 · chỉ ngày GD · singleton (chu kỳ trước chưa xong thì
    KHÔNG mở chu kỳ mới — 1000 batch mà chồng 2 chu kỳ là tự bắn vào chân mình ở phía FO).
    timeout 840s < 900s chu kỳ: pod chết thì lượt đó được thu hồi TRƯỚC khi slot kế tiếp tới.
===========================================================================*/
IF NOT EXISTS (SELECT 1 FROM T_JOB_DEFINITION WHERE C_JOB_CODE='FO_SNAPSHOT_RT')
INSERT INTO T_JOB_DEFINITION
    (C_JOB_CODE, C_JOB_NAME, C_HANDLER, C_ENABLED, C_INTERVAL_SEC,
     C_WINDOW_FROM, C_WINDOW_TO, C_BUSINESS_DAY_ONLY, C_TIMEOUT_SEC, C_MAX_ATTEMPT,
     C_RETRY_DELAY_SEC, C_SINGLETON, C_PRIORITY, C_PAYLOAD, C_UPDATED_BY)
VALUES
    ('FO_SNAPSHOT_RT', N'Quét snapshot tài sản KH indexing từ FO (near-realtime)',
     'FoSnapshotJobHandler', 1, 900,
     '09:00:00', '15:00:00', 1, 840, 2,
     60, 1, 10, N'{"batchSize":50,"parallel":4}', 'seed');
GO
