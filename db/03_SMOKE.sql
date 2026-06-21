/*==============================================================================
  SDI ENGINE — SMOKE TEST (chạy sau 01_TABLES.sql + 02_SP_ENGINE.sql)
  1 SI (AAA 60% / BBB 40%), 1 KH, 4 phiên. FO ingest qua SP_INGEST_CUSTOMER (Kafka per-KH, JSON).
  Mỗi phiên: ingest event (cash+holdings+phí) → SP_EOD_RUN (J0 GATE → compute → ...).
==============================================================================*/
SET NOCOUNT ON; SET QUOTED_IDENTIFIER ON; SET ANSI_NULLS ON;  -- QI ON: DML trên bảng có filtered index
DECLARE @ec INT, @em NVARCHAR(400);

-- reset
DELETE FROM T_EOD_WORK; DELETE FROM T_SI_UNIT_LEDGER; DELETE FROM T_SI_NAV_BALANCE;
DELETE FROM T_MASTER_NAV_BALANCE; DELETE FROM T_MASTER_INDEX_DAILY; DELETE FROM T_MASTER_HOLDING_BALANCE;
DELETE FROM T_SI_FEE_LEDGER; DELETE FROM T_EOD_RUN; DELETE FROM T_MASTER_NAV_CURRENT;
DELETE FROM T_SI_PORTFOLIO_HOLDING; DELETE FROM T_SI_NAV_CURRENT;
DELETE FROM T_SI_CASHFLOW_EVENT; DELETE FROM T_SI_HOLDING_HIST; DELETE FROM T_SI_CASH_HIST;
DELETE FROM T_PRICE_DAILY; DELETE FROM T_MASTER_PORTFOLIO_TICKER; DELETE FROM T_SI_PORTFOLIO; DELETE FROM T_MASTER_PORTFOLIO;

-- master + config tiểu khoản (signup; KHÔNG qua Kafka)
INSERT INTO T_MASTER_PORTFOLIO (C_MASTER_CODE,C_MASTER_NAME,C_STATUS,C_INCEPTION_DATE,C_BENCHMARK_CODE)
VALUES ('SDI01',N'Demo','ACTIVE','2026-01-02','VNINDEX');   -- KHÔNG khai T_FEE_ACCRUAL_CONFIG → không accrue (NAV=gross; phí test riêng)
INSERT INTO T_SI_PORTFOLIO (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_JOIN_DATE,C_STATUS)
VALUES ('SUB00001001','KH00001001','SDI01','2026-01-02','ACTIVE');  -- sub-account = SUB00001001
INSERT INTO T_MASTER_PORTFOLIO_TICKER (C_MASTER_CODE,C_EFFECTIVE_DATE,C_TICKER,C_TARGET_WEIGHT) VALUES
 ('SDI01','2026-01-02','AAA',0.60),('SDI01','2026-01-02','BBB',0.40);

-- C_REF_PRICE = giá tham chiếu đầu phiên (sở publish): phiên thường = close hôm trước; ngày 02 inception = close.
INSERT INTO T_PRICE_DAILY (C_TICKER,C_BUSINESS_DATE,C_REF_PRICE,C_CLOSE_PRICE) VALUES
 ('AAA','2026-01-02',100,100),('BBB','2026-01-02', 50,50),
 ('AAA','2026-01-05',100,110),('BBB','2026-01-05', 50,48),
 ('AAA','2026-01-06',110,110),('BBB','2026-01-06', 48,52),
 ('AAA','2026-01-07',110,110),('BBB','2026-01-07', 52,52);

-- KH nạp 10,000,000 ngày 02 (cashflow nạp/rút = SDI-side → ghi thẳng, KHÔNG qua Kafka)
INSERT INTO T_SI_CASHFLOW_EVENT (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_BUSINESS_DATE,C_EVENT_TYPE,C_AMOUNT)
VALUES ('SUB00001001','KH00001001','SDI01','2026-01-02','INITIAL',10000000);

/*--- Phiên 02: ingest (cash 0, holdings AAA 60000 / BBB 80000) rồi EOD ---*/
EXEC SP_INGEST_CUSTOMER N'{"cust_code":"KH00001001","business_date":"2026-01-02","sub_accounts":[{"si_account":"SUB00001001","cash":0,"holdings":[{"ticker":"AAA","quantity":60000,"avg_cost":100},{"ticker":"BBB","quantity":80000,"avg_cost":50}],"fees":[]}]}';
EXEC SP_EOD_SET_SOURCE_READY @p_business_date='2026-01-02', @p_source='MKT_DATA', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
EXEC SP_EOD_RUN_INDEX '2026-01-02', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;   -- BO ready → tính master index (luồng riêng)
EXEC SP_EOD_SET_SOURCE_READY @p_business_date='2026-01-02', @p_source='FO_INGEST', @p_total_record=1, @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
EXEC SP_EOD_RUN '2026-01-02', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
IF @ec<>0 PRINT CONCAT('  !!! EOD 2026-01-02 FAILED ec=',@ec,' ',@em);

/*--- Phiên 05: holdings KHÔNG đổi → ingest no-dup interval ---*/
EXEC SP_INGEST_CUSTOMER N'{"cust_code":"KH00001001","business_date":"2026-01-05","sub_accounts":[{"si_account":"SUB00001001","cash":0,"holdings":[{"ticker":"AAA","quantity":60000,"avg_cost":100},{"ticker":"BBB","quantity":80000,"avg_cost":50}],"fees":[]}]}';
EXEC SP_EOD_SET_SOURCE_READY @p_business_date='2026-01-05', @p_source='MKT_DATA', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
EXEC SP_EOD_RUN_INDEX '2026-01-05', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;   -- BO ready → tính master index (luồng riêng)
EXEC SP_EOD_SET_SOURCE_READY @p_business_date='2026-01-05', @p_source='FO_INGEST', @p_total_record=1, @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
EXEC SP_EOD_RUN '2026-01-05', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
IF @ec<>0 PRINT CONCAT('  !!! EOD 2026-01-05 FAILED ec=',@ec,' ',@em);

/*--- Phiên 06: cổ tức + phí (event có event_id). Gọi 2 LẦN (Kafka redelivery) → cash/holdings no-op + fee DEDUP ---*/
DECLARE @ev06 NVARCHAR(MAX) = N'{"cust_code":"KH00001001","business_date":"2026-01-06","sub_accounts":[{"si_account":"SUB00001001","cash":0,"holdings":[{"ticker":"AAA","quantity":60000,"avg_cost":100},{"ticker":"BBB","quantity":80000,"avg_cost":50}],"fees":[{"event_id":"FO-D06-1","type":"DIVIDEND","ticker":"AAA","amount":50000},{"event_id":"FO-D06-2","type":"CUSTODY_FEE","amount":1000}]}]}';
EXEC SP_INGEST_CUSTOMER @ev06;
EXEC SP_INGEST_CUSTOMER @ev06;   -- redelivery: phải no-op, fee KHÔNG nhân đôi
EXEC SP_EOD_SET_SOURCE_READY @p_business_date='2026-01-06', @p_source='MKT_DATA', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
EXEC SP_EOD_RUN_INDEX '2026-01-06', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;   -- BO ready → tính master index (luồng riêng)
EXEC SP_EOD_SET_SOURCE_READY @p_business_date='2026-01-06', @p_source='FO_INGEST', @p_total_record=1, @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
EXEC SP_EOD_RUN '2026-01-06', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
IF @ec<>0 PRINT CONCAT('  !!! EOD 2026-01-06 FAILED ec=',@ec,' ',@em);

/*--- Phiên 07: BBB tái cân bằng 80000→90000 (FO gửi holdings mới) → interval close/open ---*/
EXEC SP_INGEST_CUSTOMER N'{"cust_code":"KH00001001","business_date":"2026-01-07","sub_accounts":[{"si_account":"SUB00001001","cash":0,"holdings":[{"ticker":"AAA","quantity":60000,"avg_cost":100},{"ticker":"BBB","quantity":90000,"avg_cost":50}],"fees":[]}]}';
EXEC SP_EOD_SET_SOURCE_READY @p_business_date='2026-01-07', @p_source='MKT_DATA', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
EXEC SP_EOD_RUN_INDEX '2026-01-07', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;   -- BO ready → tính master index (luồng riêng)
EXEC SP_EOD_SET_SOURCE_READY @p_business_date='2026-01-07', @p_source='FO_INGEST', @p_total_record=1, @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
EXEC SP_EOD_RUN '2026-01-07', @p_err_code=@ec OUTPUT, @p_err_msg=@em OUTPUT;
IF @ec<>0 PRINT CONCAT('  !!! EOD 2026-01-07 FAILED ec=',@ec,' ',@em);

/*------------------------------------------------- KẾT QUẢ & KỲ VỌNG --------*/
PRINT '--- T_SI_NAV_BALANCE (per-KH) ---';
SELECT C_BUSINESS_DATE, C_NAV, C_UNIT, C_UNIT_PRICE, C_DAILY_PNL, C_DAILY_RETURN
FROM T_SI_NAV_BALANCE WHERE C_CUST_CODE='KH00001001' ORDER BY C_BUSINESS_DATE;
-- KỲ VỌNG (phương án A: NAV = stock + FO cash):
--  02: NAV=10,000,000 UNIT=1000 UP=10,000 PNL=0
--  05: NAV=10,440,000 UP=10,440 PNL=440,000     06: NAV=10,760,000 UP=10,760 PNL=320,000
--  07: NAV=11,280,000 UP=11,280 (AAA 60000*110 + BBB 90000*52)

PRINT '--- T_MASTER_NAV_BALANCE (SI tổng hợp) ---';
SELECT C_BUSINESS_DATE, C_CASH, C_STOCK_VALUE, C_TOTAL_ASSET, C_NAV, C_UNIT_PRICE, C_DAILY_PNL, C_PAYABLE_FEE
FROM T_MASTER_NAV_BALANCE WHERE C_MASTER_CODE='SDI01' ORDER BY C_BUSINESS_DATE;
-- Cổ tức/phí lưu ký per-ngày KHÔNG còn ở master balance → xem chi tiết per-KH ở T_SI_FEE_LEDGER (block dưới).

PRINT '--- T_MASTER_INDEX_DAILY (danh mục mẫu) ---';
SELECT C_BUSINESS_DATE, C_INDEX_VALUE, C_DAILY_RETURN
FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='SDI01' ORDER BY C_BUSINESS_DATE;
-- KỲ VỌNG: 02→1000, 05→1044, 06→1078.8

PRINT '--- T_SI_HOLDING_HIST (interval: FULL history, no-dup) ---';
SELECT C_TICKER, C_VALID_FROM, C_VALID_TO, C_QUANTITY
FROM T_SI_HOLDING_HIST WHERE C_CUST_CODE='KH00001001' ORDER BY C_TICKER, C_VALID_FROM;
-- KỲ VỌNG: AAA 1 dòng (02→NULL, 60000); BBB 2 dòng (02→07, 80000 ĐÓNG + 07→NULL, 90000 OPEN)

PRINT '--- Reconstruct holdings @2026-01-05 (trước rebalance) ---';
SELECT C_TICKER, C_QUANTITY FROM T_SI_HOLDING_HIST
WHERE C_CUST_CODE='KH00001001' AND C_VALID_FROM<='2026-01-05' AND (C_VALID_TO>'2026-01-05' OR C_VALID_TO IS NULL)
ORDER BY C_TICKER;   -- KỲ VỌNG: AAA 60000, BBB 80000

PRINT '--- Reconstruct holdings @2026-01-07 (sau rebalance) ---';
SELECT C_TICKER, C_QUANTITY FROM T_SI_HOLDING_HIST
WHERE C_CUST_CODE='KH00001001' AND C_VALID_FROM<='2026-01-07' AND (C_VALID_TO>'2026-01-07' OR C_VALID_TO IS NULL)
ORDER BY C_TICKER;   -- KỲ VỌNG: AAA 60000, BBB 90000

PRINT '--- T_SI_CASH_HIST (interval) — cash 0 cố định → 1 dòng open ---';
SELECT C_VALID_FROM, C_VALID_TO, C_CASH FROM T_SI_CASH_HIST WHERE C_CUST_CODE='KH00001001' ORDER BY C_VALID_FROM;

PRINT '--- Cổ tức/phí: idempotent — phải ĐÚNG 2 dòng (event 06 redelivery KHÔNG nhân đôi) ---';
SELECT C_BUSINESS_DATE, C_FEE_GROUP, C_FEE_TYPE, C_TICKER, C_AMOUNT, C_SOURCE_EVENT_ID FROM T_SI_FEE_LEDGER
WHERE C_CUST_CODE='KH00001001' ORDER BY C_FEE_TYPE;   -- KỲ VỌNG: 2 dòng (DIVIDEND/INCOME 50000, CUSTODY_FEE/PAYABLE 1000)

PRINT '--- T_EOD_RUN (job log) ---';
SELECT C_STATUS, COUNT(*) AS N FROM T_EOD_RUN GROUP BY C_STATUS;   -- KỲ VỌNG: 28 DONE (7 job × 4 phiên)

PRINT '--- GUARD 1: event QUÁ KHỨ (business_date 06 < watermark 07) phải bị CHẶN ---';
BEGIN TRY
    EXEC SP_INGEST_CUSTOMER N'{"cust_code":"KH00001001","business_date":"2026-01-06","sub_accounts":[{"si_account":"SUB00001001","cash":999,"holdings":[],"fees":[]}]}';
    PRINT '  !!! LỖI: KHÔNG chặn event quá khứ';
END TRY BEGIN CATCH PRINT '  OK đã chặn: '+ERROR_MESSAGE(); END CATCH;

PRINT '--- GUARD 2: GATE thiếu data (thêm tiểu khoản chưa ingest) phải CHẶN ---';
INSERT INTO T_SI_PORTFOLIO (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_JOIN_DATE,C_STATUS) VALUES ('SUB00009999','KH00009999','SDI01','2026-01-07','ACTIVE');
BEGIN TRY
    EXEC SP_EOD_GATE '2026-01-07';   -- expected=2, received=1 (KH mới chưa ingest)
    PRINT '  !!! LỖI: GATE không chặn khi thiếu data';
END TRY BEGIN CATCH PRINT '  OK GATE chặn: '+ERROR_MESSAGE(); END CATCH;
DELETE FROM T_SI_PORTFOLIO WHERE C_CUST_CODE='KH00009999';

PRINT '--- J12 dùng C_REF_PRICE đầu phiên (self-contained, KHÔNG tra ngày trước); ngày ex-rights C_IS_EX_RIGHTS=1 ---';
-- Phiên scratch 08: AAA KHÔNG hưởng quyền → ref = giá sau chia 115 (≠ close hôm trước 110). Gọi J12 standalone.
INSERT INTO T_PRICE_DAILY (C_TICKER,C_BUSINESS_DATE,C_REF_PRICE,C_CLOSE_PRICE,C_IS_EX_RIGHTS) VALUES
 ('AAA','2026-01-08',115,120,1),   -- ex-rights: ref = giá sau chia 115
 ('BBB','2026-01-08', 52, 52,0);   -- phiên thường: ref = close hôm trước
EXEC SP_EOD_SI_INDEX '2026-01-08';
DECLARE @ret08 DECIMAL(18,8) = (SELECT C_DAILY_RETURN FROM T_MASTER_INDEX_DAILY
    WHERE C_MASTER_CODE='SDI01' AND C_BUSINESS_DATE='2026-01-08');
-- ref 115 → FACTOR = 0.6*120/115 + 0.4*52/52 = 1.0260870 → return ≈ 0.0260870
-- nếu ref SAI lấy close hôm trước 110 → return ≈ 0.0545455
IF @ret08 IS NOT NULL AND ABS(@ret08 - 0.0260870) < 0.0001
    PRINT '  OK J12 dùng C_REF_PRICE (return='+CAST(@ret08 AS VARCHAR(20))+')';
ELSE
    PRINT '  !!! LỖI: return='+ISNULL(CAST(@ret08 AS VARCHAR(20)),'NULL')+' (kỳ vọng ~0.0260870 — ref_price sai?)';
DELETE FROM T_MASTER_INDEX_DAILY WHERE C_BUSINESS_DATE='2026-01-08';   -- dọn scratch
DELETE FROM T_PRICE_DAILY       WHERE C_BUSINESS_DATE='2026-01-08';

PRINT '--- SDI→ASSET SNAPSHOT: SP_GET_ASSET_SNAPSHOT (EOD @07, HISTORY @06, replay-identical) ---';
DECLARE @ecS INT, @emS NVARCHAR(400);
PRINT '-- EOD @2026-01-07 (kỳ vọng 1 KH; nav=11280000; holdings AAA 60000/BBB 90000) --';
EXEC SP_GET_ASSET_SNAPSHOT @p_business_date='2026-01-07', @p_mode='EOD',
     @p_err_code=@ecS OUTPUT, @p_err_msg=@emS OUTPUT;
PRINT '  err='+CAST(@ecS AS VARCHAR(10))+' (kỳ vọng 0)';
PRINT '-- HISTORY @2026-01-06 (replay; nav=10760000; holdings AAA 60000/BBB 80000) --';
EXEC SP_GET_ASSET_SNAPSHOT @p_business_date='2026-01-06', @p_mode='HISTORY',
     @p_err_code=@ecS OUTPUT, @p_err_msg=@emS OUTPUT;
-- REPLAY INVARIANT: payload EOD@07 và HISTORY@07 phải GIỐNG HỆT (chỉ khác field "mode")
CREATE TABLE #snapE (si VARCHAR(20), bd DATE, payload NVARCHAR(MAX));
CREATE TABLE #snapH (si VARCHAR(20), bd DATE, payload NVARCHAR(MAX));
INSERT #snapE EXEC SP_GET_ASSET_SNAPSHOT @p_business_date='2026-01-07', @p_mode='EOD',    @p_err_code=@ecS OUTPUT, @p_err_msg=@emS OUTPUT;
INSERT #snapH EXEC SP_GET_ASSET_SNAPSHOT @p_business_date='2026-01-07', @p_mode='HISTORY', @p_err_code=@ecS OUTPUT, @p_err_msg=@emS OUTPUT;
DECLARE @jEOD NVARCHAR(MAX) = (SELECT payload FROM #snapE WHERE si='SUB00001001');
DECLARE @jHIS NVARCHAR(MAX) = (SELECT payload FROM #snapH WHERE si='SUB00001001');
IF REPLACE(@jEOD,'"mode":"EOD"','"mode":"HISTORY"') = @jHIS
    PRINT '  OK REPLAY: payload EOD@07 == HISTORY@07 (chỉ khác mode) → reconstruct-only ổn';
ELSE
    PRINT '  !!! LỖI REPLAY: payload EOD@07 != HISTORY@07';
PRINT '  EOD@07 payload: ' + ISNULL(@jEOD,'(NULL)');
DROP TABLE #snapE; DROP TABLE #snapH;

PRINT '--- SDI→ASSET MASTER SNAPSHOT: SP_GET_ASSET_MASTER_SNAPSHOT (8b/8c, EOD@07 + replay) ---';
DECLARE @ecM INT, @emM NVARCHAR(400);
CREATE TABLE #mE (m VARCHAR(20), bd DATE, payload NVARCHAR(MAX));
CREATE TABLE #mH (m VARCHAR(20), bd DATE, payload NVARCHAR(MAX));
INSERT #mE EXEC SP_GET_ASSET_MASTER_SNAPSHOT @p_business_date='2026-01-07', @p_mode='EOD',    @p_err_code=@ecM OUTPUT, @p_err_msg=@emM OUTPUT;
INSERT #mH EXEC SP_GET_ASSET_MASTER_SNAPSHOT @p_business_date='2026-01-07', @p_mode='HISTORY', @p_err_code=@ecM OUTPUT, @p_err_msg=@emM OUTPUT;
DECLARE @mEOD NVARCHAR(MAX) = (SELECT payload FROM #mE WHERE m='SDI01');
DECLARE @mHIS NVARCHAR(MAX) = (SELECT payload FROM #mH WHERE m='SDI01');
PRINT '  err='+CAST(@ecM AS VARCHAR(10))+' (kỳ vọng 0); kỳ vọng master_nav=11280000 (index/benchmark TÁCH sang SP riêng)';
IF REPLACE(@mEOD,'"mode":"EOD"','"mode":"HISTORY"') = @mHIS
    PRINT '  OK REPLAY master: EOD@07 == HISTORY@07 (chỉ khác mode)';
ELSE
    PRINT '  !!! LỖI REPLAY master';
PRINT '  master EOD@07 payload: ' + ISNULL(@mEOD,'(NULL)');
DROP TABLE #mE; DROP TABLE #mH;

PRINT '--- SDI→ASSET INDEX SNAPSHOT: SP_GET_ASSET_INDEX_SNAPSHOT (1 bản ghi = JSON array, mỗi master gộp index+benchmark) ---';
DECLARE @ecI INT, @emI NVARCHAR(400);
-- seed 1 benchmark daily để test benchmark_value join theo benchmark_code của master
INSERT T_BENCHMARK_DAILY (C_BENCHMARK_CODE,C_BUSINESS_DATE,C_INDEX_VALUE) VALUES ('VNINDEX','2026-01-07',1250.5);
CREATE TABLE #ix (payload NVARCHAR(MAX));   -- 1 row, 1 col = JSON array
INSERT #ix EXEC SP_GET_ASSET_INDEX_SNAPSHOT @p_business_date='2026-01-07', @p_mode='EOD', @p_err_code=@ecI OUTPUT, @p_err_msg=@emI OUTPUT;
DECLARE @ixarr NVARCHAR(MAX) = (SELECT payload FROM #ix);
PRINT '  err='+CAST(@ecI AS VARCHAR(10))+' (kỳ vọng 0)';
IF LEFT(@ixarr,1)='[' AND RIGHT(@ixarr,1)=']'
   AND @ixarr LIKE '%"master_code":"SDI01"%' AND @ixarr LIKE '%"index_value":1078.8%'
   AND @ixarr LIKE '%"benchmark_code":"VNINDEX"%' AND @ixarr LIKE '%"benchmark_value":1250.5%'
    PRINT '  OK index SP: JSON array, master SDI01 gộp index=1078.8 + benchmark VNINDEX=1250.5';
ELSE
    PRINT '  !!! LỖI index SP: ' + ISNULL(@ixarr,'(NULL)');
PRINT '  index array: ' + ISNULL(@ixarr,'(NULL)');
DELETE FROM T_BENCHMARK_DAILY WHERE C_BENCHMARK_CODE='VNINDEX' AND C_BUSINESS_DATE='2026-01-07';
DROP TABLE #ix;

PRINT '';
PRINT '======== EOD PIPELINE CONTROL (T_EOD_PIPELINE + break + reset) ========';
DECLARE @ecP INT, @emP NVARCHAR(400);

-- (A) Happy: sau 4 phiên chạy, ngày 07 phải EOD_DONE + RECONCILE=PASS
DECLARE @ov VARCHAR(20), @rec VARCHAR(10), @eod VARCHAR(10);
SELECT @eod=C_EOD_STATUS, @rec=C_RECONCILE_STATUS, @ov=C_OVERALL_STATUS FROM T_EOD_PIPELINE WHERE C_BUSINESS_DATE='2026-01-07';
IF @eod='DONE' AND @rec='PASS' AND @ov='EOD_DONE'
    PRINT '  OK pipeline @07: EOD_STATUS=DONE RECONCILE=PASS overall=EOD_DONE';
ELSE PRINT CONCAT('  !!! pipeline @07 sai: eod=',@eod,' rec=',@rec,' overall=',@ov);

-- (B) Precondition gate: ngày chưa có nguồn READY → SP_EOD_RUN trả err=10, KHÔNG chạy
EXEC SP_EOD_RUN '2099-01-01', @p_err_code=@ecP OUTPUT, @p_err_msg=@emP OUTPUT;
IF @ecP=10 PRINT CONCAT('  OK precondition chặn: err=10 (', @emP, ')');
ELSE PRINT CONCAT('  !!! precondition KHÔNG chặn: err=', @ecP);

-- (C) Break recorder: bơm lệch NAV master @07 rồi chạy J13 trực tiếp → ghi break SI_NAV_MISMATCH
UPDATE T_MASTER_NAV_BALANCE SET C_NAV = C_NAV + 5000000 WHERE C_MASTER_CODE='SDI01' AND C_BUSINESS_DATE='2026-01-07';
EXEC SP_EOD_RECONCILE '2026-01-07';
IF EXISTS (SELECT 1 FROM T_EOD_RECON_BREAK WHERE C_BUSINESS_DATE='2026-01-07' AND C_CHECK_NAME='SI_NAV_MISMATCH')
    PRINT '  OK break recorder: ghi SI_NAV_MISMATCH vào T_EOD_RECON_BREAK (chi tiết chênh)';
ELSE PRINT '  !!! break recorder KHÔNG ghi break';
-- khôi phục + dọn break (trả data về đúng)
UPDATE T_MASTER_NAV_BALANCE SET C_NAV = C_NAV - 5000000 WHERE C_MASTER_CODE='SDI01' AND C_BUSINESS_DATE='2026-01-07';
EXEC SP_EOD_RECONCILE '2026-01-07';   -- chạy lại → 0 break (data đã đúng)
IF NOT EXISTS (SELECT 1 FROM T_EOD_RECON_BREAK WHERE C_BUSINESS_DATE='2026-01-07')
    PRINT '  OK break clear sau khi data đúng: T_EOD_RECON_BREAK rỗng';
ELSE PRINT '  !!! break vẫn còn sau khi data đúng';

-- (D) Asset synced → COMPLETED
EXEC SP_EOD_SET_ASSET_SYNCED @p_business_date='2026-01-07', @p_status='DONE', @p_err_code=@ecP OUTPUT, @p_err_msg=@emP OUTPUT;
SELECT @ov=C_OVERALL_STATUS FROM T_EOD_PIPELINE WHERE C_BUSINESS_DATE='2026-01-07';
IF @ecP=0 AND @ov='COMPLETED' PRINT '  OK asset synced: overall=COMPLETED';
ELSE PRINT CONCAT('  !!! asset synced sai: err=',@ecP,' overall=',@ov);

-- (E) Reset: xóa job + đưa pipeline 07 về PENDING/READY (giữ nguồn)
EXEC SP_EOD_RESET @p_business_date='2026-01-07', @p_err_code=@ecP OUTPUT, @p_err_msg=@emP OUTPUT;
SELECT @eod=C_EOD_STATUS, @ov=C_OVERALL_STATUS FROM T_EOD_PIPELINE WHERE C_BUSINESS_DATE='2026-01-07';
IF @eod='PENDING' AND @ov='READY' AND NOT EXISTS (SELECT 1 FROM T_EOD_RUN WHERE C_BUSINESS_DATE='2026-01-07')
    PRINT '  OK reset: EOD_STATUS=PENDING overall=READY, T_EOD_RUN @07 đã xóa (sẵn sàng chạy lại)';
ELSE PRINT CONCAT('  !!! reset sai: eod=',@eod,' overall=',@ov);
