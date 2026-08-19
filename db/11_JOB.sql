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
  │  SP_JOB_ENQUEUE / _CLAIM / _HEARTBEAT / _COMPLETE / _REAP / _PURGE       │
  │  SP_SET_JOB_SCHEDULE · SP_GET_SCHEDULABLE_JOBS · SP_GET_JOB_STATUS       │
  │  UDF_JOB_SLOT_AT (bản THAM CHIẾU của phép tính mốc — pod tính, DB đối chiếu)│
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
   ① Redis lo TỐC ĐỘ, DB lo TÍNH ĐÚNG. Redis Pub/Sub chỉ là CHUÔNG CỬA (đánh thức worker);
     quyền "ai được chạy lượt này" nằm ở T_JOB_RUN (một UPDATE có điều kiện). Redis chết/mất
     thông báo ⇒ job chậm tối đa một nhịp SP_JOB_REAP, KHÔNG BAO GIỜ chạy hai lần, KHÔNG mất.
     Pub/Sub KHÔNG lưu gì cả — và đó là điểm mạnh ở đây, vì sổ cái đã nằm trong DB rồi.
   ② Ngoài khung giờ GD thì TUYỆT ĐỐI không gọi FO — chặn ở 3 tầng ĐỘC LẬP (§ dưới), vì
     một tầng bất kỳ cũng có thể bị qua mặt (job nằm chờ trong hàng đợi vắt qua 15h00 là tình
     huống BÌNH THƯỜNG, không phải ngoại lệ hiếm).
   ③ Dòng RT KHÔNG BAO GIỜ đè dòng EOD, và KHÔNG BAO GIỜ được coi là số chốt (cột C_SRC).

  *** GUARD KHUNG GIỜ — 3 TẦNG, CỐ Ý TRÙNG NHAU ***
    Tầng 1 — SINH JOB   : SP_JOB_ENQUEUE không tạo lượt chạy có MỐC ngoài khung.
                          Biên [09:00, 15:00] ĐÓNG HAI ĐẦU và kiểm trên MỐC SLOT ⇒ 15:00 là mốc
                          cuối cùng được sinh; 15:15 thì không, phải đợi phiên GD kế tiếp.
    Tầng 2 — NHẬN JOB   : SP_JOB_CLAIM kiểm HAI thứ, trên MỐC SLOT của chính lượt đó:
                          (a) mốc có nằm trong khung + đúng ngày GD không (UDF_JOB_IN_WINDOW);
                          (b) còn TƯƠI không (C_MAX_DELAY_SEC tính từ mốc).
                          Trượt cái nào cũng đóng dấu SKIPPED, worker KHÔNG chạm FO.
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

-- Cột bổ sung (file này chạy lại được trên DB đã có bảng — CREATE TABLE ở trên bị IF OBJECT_ID chặn).
IF COL_LENGTH('T_JOB_DEFINITION','C_MAX_DELAY_SEC') IS NULL
    ALTER TABLE T_JOB_DEFINITION ADD C_MAX_DELAY_SEC INT NULL;
GO
/*  ★★ C_MAX_DELAY_SEC — HẠN TƯƠI của một lượt chạy, đo từ C_SLOT_AT.
      "Lượt này còn ý nghĩa nữa không, tính từ mốc mà nó ĐÁNG LẼ chạy?"

    ĐÂY LÀ CON SỐ DUY NHẤT quyết định giờ giấc lúc CHẠY. Không có khái niệm "khung giờ + ân hạn"
      nào khác, và cố ý như vậy: số liệu near-realtime chỉ có nghĩa TRONG phiên để PM ra quyết
      định. Trễ 40 phút thì nó không còn là near-realtime, nó là số rác — chạy cho có chỉ tổ gọi
      FO ngoài giờ. Vì thế KHÔNG xây cơ chế "gửi bằng được sau khi FO trễ".

    Đo từ C_SLOT_AT (mốc đáng lẽ chạy), KHÔNG phải C_RUN_AFTER:
      · C_RUN_AFTER bị đẩy lên sau mỗi lần retry ⇒ một lượt thử lại mãi sẽ tự làm mới hạn tươi của
        chính nó và bò qua giờ đóng cửa. Neo vào mốc slot thì hạn tươi là TUYỆT ĐỐI: lượt 15:00 hết
        hiệu lực lúc 15:10, bất kể nó đã thử lại mấy lần.
      · Đánh đổi (biết và chấp nhận): retry chỉ có ý nghĩa khi còn trong hạn tươi. Job cấu hình
        retry_delay dài hơn max_delay thì lượt thử lại sẽ bị SKIPPED — đúng ý đồ, không phải lỗi.

    NULL ⇒ lấy C_INTERVAL_SEC làm mặc định: lượt của job 15 phút mà quá 15 phút thì slot kế tiếp
      đã thay nó rồi — chạy nữa là chạy lại việc cũ.
    NULL cho cả hai (job on-demand, không chu kỳ) ⇒ KHÔNG hết hạn: job đẩy tay phải nằm chờ tới
      lượt, không tự bốc hơi.                                                                     */

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
    -- ★ MỐC mà lượt này ĐÁNG LẼ chạy. Job định kỳ: đúng mốc slot (09:00, 09:15…). Job đẩy tay:
    --   thời điểm đẩy. Đây là NEO của mọi phép kiểm giờ giấc về sau — xem chú thích C_MAX_DELAY_SEC.
    --   Không suy ra từ C_ENQUEUED_AT được: bộ quét chạy mỗi 10 giây nên nó tạo lượt 15:00 vào lúc
    --   15:00:04, và mọi phép so khung giờ dựa trên con số đó sẽ trượt mốc cuối phiên.
    C_SLOT_AT        DATETIME       NULL,
    -- AI ĐÁNH THỨC lượt chạy này: 'notify' (Pub/Sub — đường bình thường) | 'reap' (SP_JOB_REAP
    --   nhặt lại vì thông báo bị mất) | 'manual'. ⚠️ Đây KHÔNG phải cột trang trí: Pub/Sub là
    --   bắn-rồi-quên, nếu nó hỏng thì hệ VẪN CHẠY ĐÚNG nhờ reaper, chỉ chậm đi ~30 giây — và
    --   không ai nhận ra. Thấy cột này toàn 'reap' nghĩa là chuông cửa đã tắt từ lâu.
    C_CLAIM_SOURCE   VARCHAR(40)    NULL,
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
-- Quét của SP_JOB_REAP: luôn lọc theo trạng thái trước.
IF IndexProperty(OBJECT_ID('T_JOB_RUN'),'IX_JOB_RUN_STATUS','IndexID') IS NULL
CREATE INDEX IX_JOB_RUN_STATUS ON T_JOB_RUN (C_STATUS, C_RUN_AFTER)
    INCLUDE (C_JOB_CODE, C_LEASE_UNTIL, C_ATTEMPT, C_ENQUEUED_AT);
GO
-- Màn hình theo dõi + SP_JOB_PURGE: tra theo job & thời gian.
IF IndexProperty(OBJECT_ID('T_JOB_RUN'),'IX_JOB_RUN_CODE_TIME','IndexID') IS NULL
CREATE INDEX IX_JOB_RUN_CODE_TIME ON T_JOB_RUN (C_JOB_CODE, C_ENQUEUED_AT DESC)
    INCLUDE (C_STATUS, C_ROWS, C_STARTED_AT, C_ENDED_AT);
GO

-- Migration cho DB đã chạy bản Redis Streams: đổi tên cột C_STREAM_ID (id entry stream) thành
--   C_CLAIM_SOURCE (nguồn đánh thức). Giữ dữ liệu cũ — id stream cũ nằm lại vài dòng lịch sử là
--   vô hại, và xoá đi thì mất luôn vết của những lượt chạy thời còn dùng Streams.
IF COL_LENGTH('T_JOB_RUN','C_STREAM_ID') IS NOT NULL AND COL_LENGTH('T_JOB_RUN','C_CLAIM_SOURCE') IS NULL
    EXEC sp_rename 'T_JOB_RUN.C_STREAM_ID', 'C_CLAIM_SOURCE', 'COLUMN';
GO
IF COL_LENGTH('T_JOB_RUN','C_CLAIM_SOURCE') IS NULL
    ALTER TABLE T_JOB_RUN ADD C_CLAIM_SOURCE VARCHAR(40) NULL;
GO
IF COL_LENGTH('T_JOB_RUN','C_SLOT_AT') IS NULL
    ALTER TABLE T_JOB_RUN ADD C_SLOT_AT DATETIME NULL;
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
    -- ★ Biên [from, to] ĐÓNG HAI ĐẦU: mốc 15:00 LÀ một mốc sinh job hợp lệ (ảnh chụp đóng cửa).
    --   Khung 09:00–15:00 với chu kỳ 1 tiếng ⇒ 7 mốc: 9,10,11,12,13,14,15.
    IF CAST(@p_at AS TIME(0)) >= @wf AND CAST(@p_at AS TIME(0)) <= @wt RETURN 1;
    RETURN 0;
END
GO

/*===========================================================================
  UDF_JOB_SLOT_AT — MỐC SLOT của job tại thời điểm @p_at. Neo vào 00:00 giờ VN:
      slot = 00:00 + floor(giây_từ_nửa_đêm / chu_kỳ) × chu_kỳ

  ★ VÌ SAO HÀM NÀY TỒN TẠI DÙ C# CŨNG TỰ TÍNH ĐƯỢC:
    Bộ quét lịch chạy trên pod và tự tính mốc để KHÔNG phải hỏi DB mỗi 10 giây (xem
    JobSchedulerService — Redis lọc trước, 99% nhịp quét không chạm DB). Nhưng phép tính mốc là
    thứ TINH TẾ NHẤT của cả khung: lệch một giây là mất mốc đóng cửa, lệch cách neo là mốc trôi
    theo giờ pod khởi động. Để nó CHỈ tồn tại trong C# nghĩa là nó không còn ca kiểm chứng nào
    trong repo này.
    ⇒ Giữ bản tham chiếu ở đây (có smoke test), và SP_JOB_ENQUEUE ĐỐI CHIẾU mốc mà C# gửi lên với
      hàm này. C# tính sai ⇒ bị TỪ CHỐI ngay, có mã lỗi, thay vì trôi lệch âm thầm hàng tháng.
  Trả NULL nếu job không có chu kỳ (job chạy theo yêu cầu — không có khái niệm mốc).
===========================================================================*/
CREATE OR ALTER FUNCTION UDF_JOB_SLOT_AT (@p_job_code VARCHAR(40), @p_at DATETIME)
RETURNS DATETIME
AS
BEGIN
    DECLARE @itv INT = (SELECT C_INTERVAL_SEC FROM T_JOB_DEFINITION WHERE C_JOB_CODE=@p_job_code);
    IF @itv IS NULL RETURN NULL;
    DECLARE @mid DATETIME = CAST(CAST(@p_at AS DATE) AS DATETIME);
    RETURN DATEADD(SECOND, (DATEDIFF(SECOND, @mid, @p_at) / @itv) * @itv, @mid);
END
GO

/*===========================================================================
  UDF_JOB_FIRE_KEY — KHOÁ CHỐNG TRÙNG của một mốc: 'yyyyMMddHHmmss'.
    ★ PHẢI CÓ GIÂY: C_INTERVAL_SEC cho phép tới 30s ⇒ khoá chỉ tới phút thì hai mốc trong cùng một
      phút trùng khoá, UQ nuốt mốc thứ hai, và job khai 30 giây LẶNG LẼ chạy 60 giây/lần.
    Bộ quét trên pod KHÔNG cần tự format khoá này — cứ gửi mốc, SP_JOB_ENQUEUE tự suy ra. Bớt một
    chỗ để C# và SQL có thể hiểu khác nhau.
===========================================================================*/
CREATE OR ALTER FUNCTION UDF_JOB_FIRE_KEY (@p_slot DATETIME)
RETURNS VARCHAR(64)
AS
BEGIN
    RETURN CONVERT(CHAR(8), @p_slot, 112) + FORMAT(@p_slot, 'HHmmss');
END
GO

/*===========================================================================
  SP_GET_SCHEDULABLE_JOBS — danh sách job định kỳ + cấu hình lịch, cho bộ quét trên pod nạp vào
    bộ nhớ (và đẩy lên Redis cache). Đọc 1 lần/phút/pod là cùng, hoặc 0 lần nếu cache Redis còn.
    KHÔNG trả gì ngoài thứ bộ quét cần — payload/handler/timeout để SP_JOB_CLAIM lo.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_GET_SCHEDULABLE_JOBS
    @p_err_code INT           OUTPUT,
    @p_err_msg  NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON; SET @p_err_code=0; SET @p_err_msg=NULL;
    BEGIN TRY
        SELECT C_JOB_CODE, C_INTERVAL_SEC, C_WINDOW_FROM, C_WINDOW_TO,
               C_BUSINESS_DAY_ONLY, C_SINGLETON, C_PAYLOAD,
               -- Bộ quét cần con số này để biết "giờ này còn lượt nào có thể đang sống không" —
               --   ngoài khoảng đó thì KHÔNG có gì để REAP, và nó bỏ luôn nhịp quét DB.
               COALESCE(C_MAX_DELAY_SEC, C_INTERVAL_SEC) AS C_EFFECTIVE_MAX_DELAY_SEC
        FROM T_JOB_DEFINITION
        WHERE C_ENABLED = 1 AND C_INTERVAL_SEC IS NOT NULL
        ORDER BY C_PRIORITY, C_JOB_CODE;
    END TRY
    BEGIN CATCH
        SET @p_err_code=-1; SET @p_err_msg=ERROR_MESSAGE();
    END CATCH
END
GO

/*===========================================================================
  SP_SET_JOB_SCHEDULE — CỔNG DUY NHẤT để đổi lịch chạy của một job
    (chu kỳ 15' / 30' / 1 tiếng…, khung giờ, bật/tắt, payload).

  ★ ĐỔI CẤU HÌNH ⇒ XOÁ SẠCH LƯỢT CHẠY CÒN CHỜ CỦA CẤU HÌNH CŨ.
    Vì sao bắt buộc: `T_JOB_RUN` giữ BẢN CHỤP của cấu hình tại lúc sinh — mốc slot nằm trong
    `C_FIRE_KEY`, tham số nằm trong `C_PAYLOAD`. Đổi chu kỳ 15'→60' lúc 09:16 mà không dọn thì
    lượt 09:15 của lưới CŨ vẫn nằm trong hàng đợi và vẫn chạy. Người vận hành vừa bấm "1 tiếng
    một lần" xong lại thấy job chạy đúng nhịp 15 phút — và sẽ kết luận là cấu hình không ăn.

  XOÁ CÁI GÌ: chỉ lượt `READY` (CHƯA ai chạy). KHÔNG đụng `RUNNING` — không thể dừng một pod
    đang gọi FO dở bằng một câu DELETE; nó sẽ chạy nốt rồi tự đóng sổ. Muốn chặn hẳn thì tắt job
    (`@p_enabled=0`): nhịp heartbeat kế tiếp trả `still_mine=0` và worker tự dừng trong ~20 giây.

  VÌ SAO XOÁ HẲN, KHÔNG ĐÁNH DẤU (khác `SP_EOD_RESET` — proc đó CỐ Ý giữ lại `T_EOD_RUN`):
    `T_EOD_RUN` ghi việc ĐÃ CHẠY ⇒ là bằng chứng, xoá là phá vết. Dòng `READY` bị dọn ở đây ghi
    việc CHƯA BAO GIỜ CHẠY ⇒ nội dung duy nhất của nó là "đã từng được xếp lịch", mà điều đó đã
    nằm trong chính lịch sử cấu hình. Giữ lại chỉ làm hàng đợi bẩn.
    (Số lượt bị dọn trả qua @p_purged_runs — người gọi PHẢI log lại, đừng nuốt.)

  err: 0 OK · 1 job_code không tồn tại · 20 tham số sai · -1 runtime.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_SET_JOB_SCHEDULE
    @p_job_code      VARCHAR(40),
    @p_interval_sec  INT           = NULL,   -- 900=15' · 1800=30' · 3600=1 tiếng · NULL + @p_clear_interval=1 ⇒ chỉ chạy khi đẩy tay
    @p_clear_interval BIT          = 0,      -- 1 = XOÁ chu kỳ (chuyển job sang chạy-theo-yêu-cầu)
    @p_window_from   TIME(0)       = NULL,
    @p_window_to     TIME(0)       = NULL,
    @p_clear_window  BIT           = 0,      -- 1 = XOÁ khung giờ (job chạy mọi giờ)
    @p_business_day_only BIT       = NULL,
    @p_enabled       BIT           = NULL,
    @p_max_delay_sec INT           = NULL,
    @p_payload       NVARCHAR(MAX) = NULL,
    @p_user          VARCHAR(64)   = NULL,
    @p_err_code      INT           OUTPUT,
    @p_err_msg       NVARCHAR(400) OUTPUT,
    @p_purged_runs   INT           = NULL OUTPUT   -- #lượt READY của cấu hình cũ đã bị dọn
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    SET @p_err_code=0; SET @p_err_msg=NULL; SET @p_purged_runs=0;
    BEGIN TRY
        IF NOT EXISTS (SELECT 1 FROM T_JOB_DEFINITION WHERE C_JOB_CODE=@p_job_code)
            BEGIN SET @p_err_code=1; SET @p_err_msg=CONCAT(N'job_code không tồn tại: ', @p_job_code); RETURN; END

        -- Giá trị SAU khi áp (NULL = giữ nguyên; cờ _clear_ = xoá về NULL). Tính TRƯỚC để validate
        --   trên trạng thái ĐÍCH, không phải trên trạng thái hiện tại — nếu không thì đổi mỗi
        --   window_to sẽ được so với window_from cũ và lọt qua một khung giờ đảo ngược.
        DECLARE @newItv INT, @newWf TIME(0), @newWt TIME(0), @newBdo BIT, @newEn BIT, @newMax INT;
        SELECT @newItv = CASE WHEN @p_clear_interval=1 THEN NULL ELSE COALESCE(@p_interval_sec, C_INTERVAL_SEC) END,
               @newWf  = CASE WHEN @p_clear_window=1   THEN NULL ELSE COALESCE(@p_window_from, C_WINDOW_FROM) END,
               @newWt  = CASE WHEN @p_clear_window=1   THEN NULL ELSE COALESCE(@p_window_to,   C_WINDOW_TO)   END,
               @newBdo = COALESCE(@p_business_day_only, C_BUSINESS_DAY_ONLY),
               @newEn  = COALESCE(@p_enabled, C_ENABLED),
               @newMax = COALESCE(@p_max_delay_sec, C_MAX_DELAY_SEC)
        FROM T_JOB_DEFINITION WHERE C_JOB_CODE=@p_job_code;

        IF @newItv IS NOT NULL AND @newItv < 30
            BEGIN SET @p_err_code=20; SET @p_err_msg=N'Chu kỳ tối thiểu 30 giây.'; RETURN; END
        -- (T-SQL không so sánh được hai biểu thức luận lý với nhau ⇒ phải quy về 0/1)
        IF (CASE WHEN @newWf IS NULL THEN 1 ELSE 0 END) <> (CASE WHEN @newWt IS NULL THEN 1 ELSE 0 END)
            BEGIN SET @p_err_code=20; SET @p_err_msg=N'Khung giờ phải khai ĐỦ CẢ HAI đầu, hoặc bỏ trống cả hai (@p_clear_window=1).'; RETURN; END
        IF @newWf IS NOT NULL AND @newWf >= @newWt
            BEGIN SET @p_err_code=20;
                  SET @p_err_msg=N'Khung giờ phải from < to (không hỗ trợ khung vắt qua nửa đêm).'; RETURN; END
        IF @newMax IS NOT NULL AND @newMax < 30
            BEGIN SET @p_err_code=20; SET @p_err_msg=N'@p_max_delay_sec tối thiểu 30 giây.'; RETURN; END

        BEGIN TRAN;
        -- ★ DỌN TRƯỚC, ĐỔI SAU. Ngược lại thì trigger backstop (chạy trong câu UPDATE) đã dọn mất
        --   rồi, DELETE ở đây đếm ra 0 và báo cáo trả về con số sai. Thứ tự này cũng bịt luôn khe
        --   hở: nếu scheduler kịp sinh một lượt theo cấu hình CŨ giữa hai câu lệnh, trigger dọn nốt.
        DELETE FROM T_JOB_RUN WHERE C_JOB_CODE=@p_job_code AND C_STATUS='READY';
        SET @p_purged_runs = @@ROWCOUNT;

        UPDATE T_JOB_DEFINITION
           SET C_INTERVAL_SEC=@newItv, C_WINDOW_FROM=@newWf, C_WINDOW_TO=@newWt,
               C_BUSINESS_DAY_ONLY=@newBdo, C_ENABLED=@newEn, C_MAX_DELAY_SEC=@newMax,
               C_PAYLOAD=COALESCE(@p_payload, C_PAYLOAD),
               C_UPDATED_BY=@p_user, C_UPDATED_AT=GETDATE()
         WHERE C_JOB_CODE=@p_job_code;
        COMMIT;

        SET @p_err_msg = CONCAT(N'Đã đổi lịch ', @p_job_code, N': chu kỳ=',
            ISNULL(CAST(@newItv AS VARCHAR(10)), N'(chạy theo yêu cầu)'), N' giây, khung=',
            ISNULL(CAST(@newWf AS VARCHAR(8)), N'(mọi giờ)'), N'-', ISNULL(CAST(@newWt AS VARCHAR(8)), N''),
            N', bật=', @newEn, N'. Đã dọn ', @p_purged_runs, N' lượt chờ của cấu hình cũ.');
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT>0 ROLLBACK;
        SET @p_purged_runs=0;
        SET @p_err_code=-1; SET @p_err_msg=ERROR_MESSAGE();
    END CATCH
END
GO

/*===========================================================================
  TR_JOB_DEFINITION_PURGE_PENDING — LƯỚI CHẶN CUỐI cho luật "đổi cấu hình ⇒ dọn lượt chờ".

  ⚠️ ĐÂY LÀ TRIGGER DUY NHẤT TRONG REPO. Repo vốn theo lối "cổng proc" (xem SP_INGEST_MASTER_
     PORTFOLIO_TICKER). Ngoại lệ ở đây là có lý do: cổng proc bảo vệ được TÍNH ĐÚNG CỦA DỮ LIỆU
     GHI VÀO, còn thứ cần giữ ở đây là một BẤT BIẾN GIỮA HAI BẢNG — "không lượt chạy nào được
     sống lâu hơn cấu hình sinh ra nó". Bất biến giữa hai bảng thì phải gác ở tầng dữ liệu, y như
     UNIQUE/CHECK, nếu không thì chỉ cần một câu `UPDATE T_JOB_DEFINITION SET C_INTERVAL_SEC=3600`
     gõ tay lúc 2 giờ sáng là luật vỡ — im lặng, và đúng vào lúc không ai ngồi xem.

  CHỈ dọn khi giá trị THẬT SỰ đổi: `UPDATE(cột)` chỉ cho biết cột đó có mặt trong câu SET, nên
     một câu `SET C_INTERVAL_SEC = C_INTERVAL_SEC` (hoặc UPDATE cả hàng từ ORM) sẽ kích hoạt oan
     và xoá mất hàng đợi đang hợp lệ. So inserted vs deleted mới là thứ nói được "đổi thật".
===========================================================================*/
CREATE OR ALTER TRIGGER TR_JOB_DEFINITION_PURGE_PENDING
ON T_JOB_DEFINITION
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    IF NOT UPDATE(C_INTERVAL_SEC) AND NOT UPDATE(C_WINDOW_FROM) AND NOT UPDATE(C_WINDOW_TO)
       AND NOT UPDATE(C_BUSINESS_DAY_ONLY) AND NOT UPDATE(C_ENABLED) AND NOT UPDATE(C_PAYLOAD)
        RETURN;   -- đổi timeout/retry/priority… không làm lượt chờ mất hiệu lực

    DELETE r
    FROM T_JOB_RUN r
    INNER JOIN inserted i ON i.C_JOB_CODE = r.C_JOB_CODE
    INNER JOIN deleted  d ON d.C_JOB_CODE = i.C_JOB_CODE
    WHERE r.C_STATUS = 'READY'
      AND (   ISNULL(i.C_INTERVAL_SEC, -1)                  <> ISNULL(d.C_INTERVAL_SEC, -1)
           OR ISNULL(CAST(i.C_WINDOW_FROM AS VARCHAR(8)),'') <> ISNULL(CAST(d.C_WINDOW_FROM AS VARCHAR(8)),'')
           OR ISNULL(CAST(i.C_WINDOW_TO   AS VARCHAR(8)),'') <> ISNULL(CAST(d.C_WINDOW_TO   AS VARCHAR(8)),'')
           OR i.C_BUSINESS_DAY_ONLY <> d.C_BUSINESS_DAY_ONLY
           OR i.C_ENABLED           <> d.C_ENABLED
           OR ISNULL(i.C_PAYLOAD, N'') <> ISNULL(d.C_PAYLOAD, N'') );
END
GO

/*===========================================================================
  SP_JOB_ENQUEUE — ĐẨY MỘT JOB (bất kỳ loại nào) vào hàng đợi. "Cứ có job đẩy vào là chạy":
    proc trả @p_job_run_id để app PUBLISH ngay lên kênh Redis; worker đang SUBSCRIBE nhận
    trong khoảng một mili-giây (đẩy thật, không hỏi thăm). Không có vòng chờ nào.
  IDEMPOTENT theo (job_code, fire_key): gọi lại cùng fire_key ⇒ err=4 + trả id CŨ, KHÔNG sinh lượt mới.
  err: 0 OK · 1 không có job_code · 2 job tắt · 3 ngoài khung giờ (KHÔNG tạo lượt) · 4 đã tồn tại · -1 runtime.
===========================================================================*/
CREATE OR ALTER PROCEDURE SP_JOB_ENQUEUE
    @p_job_code      VARCHAR(40),
    @p_fire_key      VARCHAR(64)   = NULL,           -- NULL ⇒ NEWID() (mỗi lần gọi = 1 lượt riêng)
    @p_payload       NVARCHAR(MAX) = NULL,
    @p_business_date DATE          = NULL,
    @p_slot_at       DATETIME      = NULL,   -- mốc slot (bộ quét trên pod gửi lên). NULL = đẩy tay ⇒ mốc = bây giờ.
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

        DECLARE @slot DATETIME = ISNULL(@p_slot_at, @now);

        -- ★ ĐỐI CHIẾU MỐC do bộ quét trên pod gửi lên với bản THAM CHIẾU trong DB.
        --   Bộ quét tự tính mốc để khỏi hỏi DB mỗi 10 giây (Redis lọc trước). Đổi lại, DB phải
        --   kiểm lại — nếu không thì một pod chạy bản cũ, lệch múi giờ, hay tính sai công thức sẽ
        --   lặng lẽ sinh job ở mốc lệch, và không có gì phát hiện ra. Sai lệch ⇒ TỪ CHỐI, có mã lỗi.
        IF @p_slot_at IS NOT NULL
        BEGIN
            DECLARE @ref DATETIME = dbo.UDF_JOB_SLOT_AT(@p_job_code, @p_slot_at);
            IF @ref IS NOT NULL AND @ref <> @p_slot_at
            BEGIN
                SET @p_err_code=20;
                SET @p_err_msg=CONCAT(N'Mốc slot không khớp lưới của job: nhận ',
                    CONVERT(VARCHAR(19),@p_slot_at,120), N', lưới cho ra ', CONVERT(VARCHAR(19),@ref,120),
                    N'. Bộ quét trên pod tính sai (lệch múi giờ / chạy bản cũ / sai chu kỳ).');
                RETURN;
            END
        END

        -- ★ SINGLETON — lượt trước còn chạy (lease còn hiệu lực) thì KHÔNG mở lượt mới.
        --   Trước đây nằm trong SP_JOB_ENQUEUE_DUE; chuyển vào đây khi proc đó bị xoá. Áp cho CẢ
        --   job đẩy tay: 1000 batch chồng hai chu kỳ là tự bắn vào chân mình ở phía FO, bất kể ai đẩy.
        IF EXISTS (SELECT 1 FROM T_JOB_DEFINITION d
                   INNER JOIN T_JOB_RUN r ON r.C_JOB_CODE=d.C_JOB_CODE
                   WHERE d.C_JOB_CODE=@p_job_code AND d.C_SINGLETON=1
                     AND r.C_STATUS='RUNNING' AND r.C_LEASE_UNTIL > @now)
        BEGIN
            SET @p_err_code=7;
            SET @p_err_msg=CONCAT(N'Job ', @p_job_code, N' đang chạy (singleton) — không mở lượt mới.');
            RETURN;
        END

        -- ★ GUARD TẦNG 1 — KHÔNG CÓ CỬA HẬU.
        --   Bản trước có tham số @p_ignore_window cho vận hành "chạy tay ngoài khung". Đã BỎ:
        --   một cửa hậu mà job gọi hệ ngoài cũng đi qua được thì nó không còn là cửa hậu, nó là
        --   cái lỗ. Cần chạy job ngoài khung thì sửa khung bằng SP_SET_JOB_SCHEDULE — có dấu vết,
        --   có người chịu trách nhiệm, và tự dọn lượt chờ của cấu hình cũ.
        --   Kiểm trên MỐC, không phải trên @now: bộ quét chạy lúc 15:00:04 cho mốc 15:00:00 —
        --   áp khung lên @now là mất mốc đóng cửa (xem §2c của doc thiết kế).
        IF dbo.UDF_JOB_IN_WINDOW(@p_job_code, @slot) = 0
        BEGIN
            SET @p_err_code=3;
            SET @p_err_msg=CONCAT(N'Mốc ', CONVERT(VARCHAR(19), @slot, 120),
                N' nằm ngoài khung giờ cho phép của job ', @p_job_code, N' — KHÔNG tạo lượt chạy.');
            RETURN;
        END

        -- Khoá chống trùng: người gọi truyền, hoặc suy từ MỐC (job định kỳ), hoặc NEWID (đẩy tay).
        DECLARE @fk VARCHAR(64) = COALESCE(@p_fire_key,
                                           CASE WHEN @p_slot_at IS NOT NULL THEN dbo.UDF_JOB_FIRE_KEY(@p_slot_at) END,
                                           CONVERT(VARCHAR(36), NEWID()));

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
            INSERT INTO T_JOB_RUN (C_JOB_CODE,C_FIRE_KEY,C_STATUS,C_BUSINESS_DATE,C_PAYLOAD,C_RUN_AFTER,C_SLOT_AT,C_ENQUEUED_AT,C_MESSAGE)
            VALUES (@p_job_code, @fk, 'READY', @p_business_date,
                    ISNULL(@p_payload, (SELECT C_PAYLOAD FROM T_JOB_DEFINITION WHERE C_JOB_CODE=@p_job_code)),
                    @now, @slot,
                    @now, CONCAT(N'enqueue by ', ISNULL(@p_user,'(system)')));
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
    @p_source     VARCHAR(40)   = NULL,              -- 'notify' | 'reap' | 'manual' — xem C_CLAIM_SOURCE
    @p_err_code   INT           OUTPUT,
    @p_err_msg    NVARCHAR(400) OUTPUT
AS
BEGIN
    SET NOCOUNT ON; SET XACT_ABORT ON;
    SET @p_err_code=0; SET @p_err_msg=NULL;
    BEGIN TRY
        DECLARE @now DATETIME = dbo.UDF_JOB_NOW();
        DECLARE @code VARCHAR(40), @status VARCHAR(10), @attempt INT, @maxatt INT, @timeout INT,
                @maxdelay INT, @slotat DATETIME, @age INT;
        SELECT @code=r.C_JOB_CODE, @status=r.C_STATUS, @attempt=r.C_ATTEMPT,
               @maxatt=d.C_MAX_ATTEMPT, @timeout=d.C_TIMEOUT_SEC,
               @maxdelay=COALESCE(d.C_MAX_DELAY_SEC, d.C_INTERVAL_SEC),  -- NULL (job on-demand) = không hết hạn
               @slotat=ISNULL(r.C_SLOT_AT, r.C_ENQUEUED_AT)              -- dòng cũ trước migration: lùi về enqueued
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

        -- ★ GUARD TẦNG 2 — kiểm khung giờ trên MỐC SLOT của chính lượt này, KHÔNG phải trên @now.
        --   Lượt sinh đúng mốc 15:00 phải claim được lúc 15:00:03; áp khung lên @now là giết chính
        --   cái ảnh chụp đóng cửa mà mốc đó sinh ra để lấy.
        --   Vẫn chặn: lượt của phiên HÔM QUA / ngày nghỉ (UDF_IS_BUSINESS_DATE trong hàm này) và
        --   lượt có mốc nằm ngoài khung (job đẩy tay lúc 22h). "Còn tươi không" là việc của guard
        --   HẠN TƯƠI ngay bên dưới — hai câu hỏi khác nhau, đừng gộp.
        IF dbo.UDF_JOB_IN_WINDOW(@code, @slotat) = 0
        BEGIN
            UPDATE T_JOB_RUN SET C_STATUS='SKIPPED', C_ENDED_AT=@now, C_OWNER=@p_owner,
                   C_MESSAGE=CONCAT(N'BỎ QUA: mốc ', CONVERT(VARCHAR(19),@slotat,120),
                                    N' nằm ngoài khung giờ cho phép của job ', @code, N'.')
            WHERE C_JOB_RUN_ID=@p_job_run_id AND C_STATUS IN ('READY','FAILED');
            SET @p_err_code=3;
            SET @p_err_msg=N'Ngoài khung giờ tại thời điểm chạy — đã đóng dấu SKIPPED, KHÔNG gọi hệ ngoài.';
            RETURN;
        END

        -- ★ GUARD HẠN TƯƠI — con số DUY NHẤT quyết định "muộn quá thì thôi".
        --   Số liệu near-realtime chỉ có nghĩa TRONG phiên để PM ra quyết định; trễ 40 phút thì nó
        --   là số rác, chạy cho có chỉ tổ gọi FO ngoài giờ. KHÔNG xây cơ chế "gửi bằng được".
        --   Đo từ MỐC SLOT nên hạn tươi là tuyệt đối: lượt 15:00 hết hiệu lực lúc 15:10, bất kể
        --   nó đã nằm chờ hay đã thử lại mấy lần.
        SET @age = DATEDIFF(SECOND, @slotat, @now);
        IF @maxdelay IS NOT NULL AND @age > @maxdelay
        BEGIN
            UPDATE T_JOB_RUN SET C_STATUS='SKIPPED', C_ENDED_AT=@now, C_OWNER=@p_owner,
                   C_MESSAGE=CONCAT(N'BỎ QUA: đã ', @age, N' giây kể từ mốc ',
                        CONVERT(VARCHAR(19),@slotat,120), N' (hạn tươi ', @maxdelay,
                        N's) — số không còn là near-realtime, lượt kế tiếp đã thay nó.')
            WHERE C_JOB_RUN_ID=@p_job_run_id AND C_STATUS IN ('READY','FAILED');
            SET @p_err_code=3;
            SET @p_err_msg=CONCAT(N'Lượt chạy đã quá hạn tươi (', @age, N's > ', @maxdelay, N's) — SKIPPED.');
            RETURN;
        END

        -- ★ CLAIM NGUYÊN TỬ. Điều kiện WHERE là toàn bộ cơ chế chống trùng của hệ.
        UPDATE T_JOB_RUN
           SET C_STATUS='RUNNING', C_OWNER=@p_owner, C_ATTEMPT=C_ATTEMPT+1,
               C_STARTED_AT=@now, C_HEARTBEAT_AT=@now,
               C_LEASE_UNTIL=DATEADD(SECOND, ISNULL(@timeout,300), @now),
               C_CLAIM_SOURCE=ISNULL(@p_source, C_CLAIM_SOURCE), C_ENDED_AT=NULL
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

        -- Trả kèm C_HANDLER + C_TIMEOUT_SEC (JOIN cấu hình): worker cần handler để biết gọi ai, và
        --   cần timeout để tự chọn nhịp tim (nhịp = timeout/4). Thiếu 2 cột này thì tầng C# phải
        --   bắn thêm một query nữa cho MỖI lượt chạy chỉ để đọc hai con số đã nằm sẵn ở đây.
        SELECT r.C_JOB_RUN_ID, r.C_JOB_CODE, d.C_HANDLER, r.C_FIRE_KEY, r.C_BUSINESS_DATE,
               r.C_PAYLOAD, r.C_ATTEMPT, r.C_LEASE_UNTIL, d.C_TIMEOUT_SEC, r.C_CLAIM_SOURCE
        FROM T_JOB_RUN r
        LEFT JOIN T_JOB_DEFINITION d ON d.C_JOB_CODE=r.C_JOB_CODE
        WHERE r.C_JOB_RUN_ID=@p_job_run_id;
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
    -- ★ INNER JOIN + C_ENABLED=1: TẮT job là DỪNG ĐƯỢC CẢ CHU KỲ ĐANG CHẠY. Không có vế này thì
    --   "tắt khẩn cấp" chỉ chặn được lượt SAU, còn chu kỳ đang bắn 1000 request sang FO vẫn bắn nốt
    --   — tức là đúng lúc cần tắt nhất thì nút tắt không có tác dụng. Worker thấy still_mine=0 sẽ
    --   tự huỷ token và dừng trong ~20 giây (nhịp heartbeat).
    UPDATE r
       SET r.C_HEARTBEAT_AT=@now,
           r.C_LEASE_UNTIL=DATEADD(SECOND, ISNULL(@timeout,300), @now),
           r.C_ROWS=ISNULL(@p_rows, r.C_ROWS)
      FROM T_JOB_RUN r
      INNER JOIN T_JOB_DEFINITION d ON d.C_JOB_CODE=r.C_JOB_CODE AND d.C_ENABLED=1
     WHERE r.C_JOB_RUN_ID=@p_job_run_id AND r.C_OWNER=@p_owner AND r.C_STATUS='RUNNING';
    SET @p_still_mine = CASE WHEN @@ROWCOUNT=1 THEN 1 ELSE 0 END;
END
GO

/*===========================================================================
  SP_JOB_COMPLETE — đóng lượt chạy. OK → DONE. Lỗi → FAILED, và nếu còn lượt thử thì tự
    đặt lại READY + C_RUN_AFTER = now + retry_delay (SP_JOB_REAP sẽ đánh thức lại).
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
  SP_JOB_REAP — LƯỚI AN TOÀN, chạy trên MỌI pod (vd 30 giây/lần). Làm 3 việc:

  (1) THU HỒI lượt RUNNING quá hạn lease (pod chết/bị evict giữa chừng) → READY.
  (2) ĐÓNG DẤU SKIPPED lượt nằm chờ QUÁ HẠN (C_MAX_DELAY_SEC) — chống chạy lại việc của giờ trước.
  (3) TRẢ VỀ các lượt READY tới hạn để app PUBLISH (lại) lên kênh Redis.

  ★ (3) LÀ ĐƯỜNG HỒI PHỤC CHÍNH, KHÔNG PHẢI DỰ PHÒNG. Pub/Sub là bắn-rồi-quên: thông báo phát
    ra lúc không pod nào đang nghe (đang restart, mất kết nối, Redis chết, publish hụt vì pod
    chết ngay sau khi INSERT) là MẤT LUÔN, trong khi T_JOB_RUN vẫn ghi READY.
    Không có (3) thì job đó nằm im vĩnh viễn và KHÔNG AI BIẾT.
    Có (3) thì: mất thông báo ⇒ chậm tối đa một nhịp reaper, rồi tự hồi. Publish trùng cũng vô
    hại vì SP_JOB_CLAIM chỉ cho một pod thắng.
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

        -- (2) HẾT HẠN ⇒ SKIPPED ngay, KHÔNG để nằm lại hàng đợi.
        --     ⚠️ Bản đầu KHÔNG có bước này và nó SAI THẬT: lượt sinh 14:59 không kịp chạy sẽ nằm
        --       READY suốt đêm (bước (3) không đẩy vì ngoài khung giờ; không ai đóng dấu nó cả).
        --       Sáng hôm sau 09:00 khung giờ MỞ LẠI ⇒ nó được đẩy ⇒ CHẠY LẠI LƯỢT CỦA HÔM QUA.
        --       Guard khung giờ không cứu được, vì 09:00 hôm sau là hoàn toàn "trong giờ".
        UPDATE r
           SET r.C_STATUS='SKIPPED', r.C_ENDED_AT=@now,
               r.C_MESSAGE=CONCAT(N'BỎ QUA: đã ', DATEDIFF(SECOND, ISNULL(r.C_SLOT_AT,r.C_ENQUEUED_AT), @now),
                    N' giây kể từ mốc, quá hạn tươi ', COALESCE(d.C_MAX_DELAY_SEC, d.C_INTERVAL_SEC),
                    N's — số không còn là near-realtime.')
          FROM T_JOB_RUN r
          INNER JOIN T_JOB_DEFINITION d ON d.C_JOB_CODE=r.C_JOB_CODE
         WHERE r.C_STATUS='READY'
           AND COALESCE(d.C_MAX_DELAY_SEC, d.C_INTERVAL_SEC) IS NOT NULL
           AND DATEDIFF(SECOND, ISNULL(r.C_SLOT_AT,r.C_ENQUEUED_AT), @now)
                 > COALESCE(d.C_MAX_DELAY_SEC, d.C_INTERVAL_SEC);

        -- (3) Lượt READY tới hạn, nằm lâu bất thường ⇒ trả về cho app XADD lại.
        SELECT r.C_JOB_RUN_ID, r.C_JOB_CODE, r.C_PAYLOAD
        FROM T_JOB_RUN r
        WHERE r.C_STATUS='READY'
          AND r.C_RUN_AFTER <= @now
          AND DATEDIFF(SECOND, r.C_ENQUEUED_AT, @now) >= @p_stale_sec
          -- Khung giờ áp lên MỐC SLOT của lượt, không phải @now: lượt 15:00 mất message phải được
          --   đánh thức lại lúc 15:00:35, nếu không thì mốc cuối phiên mất trắng mỗi khi Kafka nấc
          --   một nhịp. Lượt quá hạn tươi thì bước (2) đã đóng dấu SKIPPED rồi.
          AND dbo.UDF_JOB_IN_WINDOW(r.C_JOB_CODE, ISNULL(r.C_SLOT_AT, r.C_ENQUEUED_AT)) = 1
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
    -- ★ SÀN 1 NGÀY — KHÔNG cho dọn lượt của HÔM NAY. Đây không phải sự thận trọng thừa:
    --   dọn một lượt DONE mà mốc slot của nó VẪN LÀ SLOT HIỆN TẠI thì `NOT EXISTS` trong
    --   bộ quét lại thấy mốc đó trống ⇒ sinh lại đúng lượt vừa xong ⇒ JOB CHẠY HAI LẦN trong
    --   một slot. Chính hàng rào chống trùng (UQ fire_key) bị chặt mất chân bởi thao tác dọn dẹp.
    --   Từ 1 ngày trở lên thì fire_key mang ngày cũ, không thể trùng slot hiện tại.
    IF @p_keep_days IS NULL OR @p_keep_days < 1 SET @p_keep_days = 1;
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
               -- ★ Nêu THẲNG chế độ, đừng bắt người xem tự suy từ chỗ C_INTERVAL_SEC bị NULL.
               --   "Đã xoá chu kỳ" và "chưa bao giờ cấu hình chu kỳ" nhìn giống hệt nhau trên dữ
               --   liệu, và cả hai đều làm job im lặng ngừng chạy — đó là thứ phải HIỆN RA.
               CASE WHEN d.C_ENABLED = 0            THEN 'DISABLED'
                    WHEN d.C_INTERVAL_SEC IS NULL   THEN 'ON_DEMAND'   -- chỉ chạy khi có người đẩy
                    ELSE 'INTERVAL' END AS C_SCHEDULE_MODE,
               COALESCE(d.C_MAX_DELAY_SEC, d.C_INTERVAL_SEC) AS C_EFFECTIVE_MAX_DELAY_SEC,
               d.C_WINDOW_FROM, d.C_WINDOW_TO, d.C_BUSINESS_DAY_ONLY,
               dbo.UDF_JOB_IN_WINDOW(d.C_JOB_CODE, @now) AS C_IN_WINDOW_NOW,   -- bây giờ có trong khung không
               l.C_JOB_RUN_ID AS C_LAST_RUN_ID, l.C_STATUS AS C_LAST_STATUS,
               l.C_STARTED_AT AS C_LAST_STARTED_AT, l.C_ENDED_AT AS C_LAST_ENDED_AT,
               l.C_ROWS AS C_LAST_ROWS, l.C_MESSAGE AS C_LAST_MESSAGE,
               l.C_CLAIM_SOURCE AS C_LAST_WOKEN_BY,   -- 'notify' bình thường; toàn 'reap' ⇒ Pub/Sub đã tắt
               q.C_REAP_WAKE_7D,
               DATEDIFF(SECOND, l.C_ENDED_AT, @now) AS C_SEC_SINCE_LAST_END,
               q.C_QUEUED, q.C_RUNNING, q.C_DEAD_7D
        FROM T_JOB_DEFINITION d
        OUTER APPLY (SELECT TOP 1 * FROM T_JOB_RUN r WHERE r.C_JOB_CODE=d.C_JOB_CODE
                     ORDER BY r.C_JOB_RUN_ID DESC) l
        OUTER APPLY (SELECT SUM(CASE WHEN r.C_STATUS='READY'   THEN 1 ELSE 0 END) AS C_QUEUED,
                            SUM(CASE WHEN r.C_STATUS='RUNNING' THEN 1 ELSE 0 END) AS C_RUNNING,
                            SUM(CASE WHEN r.C_STATUS='DEAD' AND r.C_ENDED_AT > DATEADD(DAY,-7,@now) THEN 1 ELSE 0 END) AS C_DEAD_7D,
                            -- #lượt 7 ngày qua phải nhờ reaper đánh thức. >0 lác đác là bình thường;
                            --   xấp xỉ TỔNG số lượt ⇒ chuông cửa Pub/Sub đang hỏng mà hệ vẫn chạy.
                            SUM(CASE WHEN r.C_CLAIM_SOURCE='reap' AND r.C_STARTED_AT > DATEADD(DAY,-7,@now) THEN 1 ELSE 0 END) AS C_REAP_WAKE_7D
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
        --   ⚠️ CỐ Ý chỉ chặn theo NGÀY, KHÔNG chặn theo GIỜ. Điều BRD cấm là GỌI SANG FO ngoài
        --     9h–15h (tầng 1/2/4 lo việc đó). Còn GHI thì khác: một batch lấy về hợp lệ lúc
        --     14:59:58 có thể ghi xong lúc 15:00:01. Chặn theo giờ ở đây sẽ VỨT BỎ dữ liệu đã
        --     lấy đúng luật — mất số của vài chục khách hàng để đổi lấy một cái đúng hình thức.
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

        -- si_account TRÙNG trong cùng một batch: bắt TRƯỚC, bằng thông điệp người đọc hiểu được.
        --   Không bắt ở đây thì nó vỡ PK của bảng tạm @src và trả err=-1 kèm nguyên văn
        --   "Violation of PRIMARY KEY constraint 'PK__#A5C69A4__...'" — người trực đọc xong không
        --   biết là lỗi của FO hay của SDI. (MERGE cũng không cho phép một dòng đích khớp 2 dòng nguồn.)
        DECLARE @dupsi VARCHAR(20) = (
            SELECT TOP 1 si_account FROM OPENJSON(@p_json) WITH (si_account VARCHAR(20) '$.si_account')
            GROUP BY si_account HAVING COUNT(*) > 1 ORDER BY si_account);
        IF @dupsi IS NOT NULL
        BEGIN
            SET @p_err_code=21;
            SET @p_err_msg=CONCAT(N'FO trả TRÙNG tiểu khoản trong cùng batch (vd ', @dupsi,
                                  N') — từ chối cả batch, không ghi dòng nào.');
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

        -- ★ HOLDLOCK (= SERIALIZABLE) BẮT BUỘC cho MERGE: không có nó, hai phiên cùng chạy MỘT
        --   batch (pod mất lease giữa chừng, pod mới chạy lại chu kỳ đó) cùng đọc thấy NOT MATCHED
        --   rồi cùng INSERT ⇒ vỡ UQ_SI_NAV_BALANCE_NK ⇒ cả batch rollback err=-1. HOLDLOCK giữ
        --   khoá dải trên khoá tìm kiếm nên phiên thứ hai chờ rồi đi nhánh MATCHED (update) — đúng
        --   ý nghĩa "upsert". Đây là lỗi kinh điển của MERGE, và nó chỉ lộ ra khi có tải thật.
        MERGE T_SI_BALANCE WITH (HOLDLOCK) AS t
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
        MERGE T_MASTER_BALANCE WITH (HOLDLOCK) AS t   -- ★ xem chú thích HOLDLOCK ở SP_INGEST_FO_SNAPSHOT_RT
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
     C_RETRY_DELAY_SEC, C_SINGLETON, C_PRIORITY, C_MAX_DELAY_SEC, C_PAYLOAD, C_UPDATED_BY)
VALUES
    ('FO_SNAPSHOT_RT', N'Quét snapshot tài sản KH indexing từ FO (near-realtime)',
     'FoSnapshotJobHandler', 1, 900,
     '09:00:00', '15:00:00', 1, 840, 2,
     60, 1, 10,
     600,   -- hết hạn sau 10 phút: lượt quét nằm chờ quá 10 phút thì slot 15 phút kế tiếp sắp
            --   tới — chụp ảnh "bây giờ" bằng lượt MỚI vẫn đúng hơn là chạy lượt cũ.
     N'{"batchSize":50,"parallel":4}', 'seed');
GO
