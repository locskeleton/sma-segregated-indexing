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
DELETE FROM T_SI_INCOME_FEE; DELETE FROM T_EOD_RUN; DELETE FROM T_MASTER_NAV_CURRENT;
DELETE FROM T_SI_PORTFOLIO_HOLDING; DELETE FROM T_SI_NAV_CURRENT;
DELETE FROM T_SI_CASHFLOW_EVENT; DELETE FROM T_SI_HOLDING_HIST; DELETE FROM T_SI_CASH_HIST;
DELETE FROM T_PRICE_DAILY; DELETE FROM T_MASTER_PORTFOLIO_TICKER; DELETE FROM T_SI_PORTFOLIO; DELETE FROM T_MASTER_PORTFOLIO;

-- master + config tiểu khoản (signup; KHÔNG qua Kafka)
INSERT INTO T_MASTER_PORTFOLIO (C_MASTER_CODE,C_MASTER_NAME,C_STATUS,C_INCEPTION_DATE,C_BENCHMARK_CODE)
VALUES ('SDI01',N'Demo','ACTIVE','2026-01-02','VNINDEX');   -- KHÔNG khai T_FEE_CONFIG → không accrue (NAV=gross; phí test riêng)
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
-- Cổ tức/phí lưu ký per-ngày KHÔNG còn ở master balance → xem chi tiết per-KH ở T_SI_INCOME_FEE (block dưới).

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
SELECT C_BUSINESS_DATE, C_FEE_GROUP, C_FEE_TYPE, C_TICKER, C_AMOUNT, C_SOURCE_EVENT_ID FROM T_SI_INCOME_FEE
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

-- (BRD 2026-06-22: GỠ producer SDI→Asset ASSET + MASTER snapshot — BO/FO đẩy thẳng Asset; BO trả phí QL lũy kế/ngày.
--  GIỮ INDEX snapshot — SDI vẫn đẩy riêng khi BO price-ready. Xem docs/SDI-asset-gap.md.)
PRINT '--- SDI→ASSET INDEX SNAPSHOT (GIỮ): SP_GET_ASSET_INDEX_SNAPSHOT (1 bản ghi = JSON array index+benchmark/master) ---';
DECLARE @ecI INT, @emI NVARCHAR(400);
INSERT T_BENCHMARK_DAILY (C_BENCHMARK_CODE,C_BUSINESS_DATE,C_INDEX_VALUE) VALUES ('VNINDEX','2026-01-07',1250.5);
CREATE TABLE #ix (payload NVARCHAR(MAX));
INSERT #ix EXEC SP_GET_ASSET_INDEX_SNAPSHOT @p_business_date='2026-01-07', @p_mode='EOD', @p_err_code=@ecI OUTPUT, @p_err_msg=@emI OUTPUT;
DECLARE @ixarr NVARCHAR(MAX) = (SELECT payload FROM #ix);
IF @ecI=0 AND LEFT(@ixarr,1)='[' AND @ixarr LIKE '%"master_code":"SDI01"%' AND @ixarr LIKE '%"index_value":1078.8%'
   AND @ixarr LIKE '%"benchmark_code":"VNINDEX"%' AND @ixarr LIKE '%"benchmark_value":1250.5%'
    PRINT '  OK index SP: JSON array SDI01 gộp index=1078.8 + benchmark VNINDEX=1250.5';
ELSE PRINT '  !!! LỖI index SP: '+ISNULL(@ixarr,'(NULL)');
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

-- (D) Trạng thái CUỐI = EOD_DONE sau reconcile PASS (BRD 2026-06-22: bỏ stage asset-sync/COMPLETED)
SELECT @ov=C_OVERALL_STATUS FROM T_EOD_PIPELINE WHERE C_BUSINESS_DATE='2026-01-07';
IF @ov='EOD_DONE' PRINT '  OK terminal: overall=EOD_DONE (không còn COMPLETED/asset-sync)';
ELSE PRINT CONCAT('  !!! terminal sai: overall=',@ov);

-- (E) Reset: xóa job + đưa pipeline 07 về PENDING/READY (giữ nguồn)
EXEC SP_EOD_RESET @p_business_date='2026-01-07', @p_err_code=@ecP OUTPUT, @p_err_msg=@emP OUTPUT;
SELECT @eod=C_EOD_STATUS, @ov=C_OVERALL_STATUS FROM T_EOD_PIPELINE WHERE C_BUSINESS_DATE='2026-01-07';
IF @eod='PENDING' AND @ov='READY' AND NOT EXISTS (SELECT 1 FROM T_EOD_RUN WHERE C_BUSINESS_DATE='2026-01-07')
    PRINT '  OK reset: EOD_STATUS=PENDING overall=READY, T_EOD_RUN @07 đã xóa (sẵn sàng chạy lại)';
ELSE PRINT CONCAT('  !!! reset sai: eod=',@eod,' overall=',@ov);

PRINT '';
PRINT '======== PHÍ PHẢI TRẢ — breakdown Option B + guard @nAccrue (FR-06 RS5) ========';
-- Scenario ISOLATED: SI riêng (SUBFEE01) + ngày riêng (2026-02-02/03) → KHÔNG đụng SDI01 (fee-free) ở trên.
-- Option B: pending = C_PAYABLE_FEE đã chốt (exact, khớp NAV); paid = Σ cắt loại; accrued = pending + paid.
INSERT INTO T_FEE_CONFIG (C_FEE_TYPE,C_FEE_GROUP,C_RATE) VALUES ('MGMT_FEE','PAYABLE',0.01);  -- 1 loại accrue
INSERT INTO T_SI_NAV_CURRENT (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_UNIT,C_CASH,C_PAYABLE_FEE,C_LAST_NAV,C_STATUS,C_LAST_SYNC_DATE)
 VALUES ('SUBFEE01','KHFEE','SDI01',1000,0,602.739726,11000000,'ACTIVE','2026-02-03');
INSERT INTO T_SI_NAV_BALANCE (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_NAV,C_PAYABLE_FEE,C_UNIT,C_UNIT_PRICE,C_DAILY_PNL,C_DAILY_RETURN)
 VALUES ('2026-02-02','SUBFEE01','KHFEE','SDI01',11000000,302.739726,1000,11000,0,0),
        ('2026-02-03','SUBFEE01','KHFEE','SDI01',11000000,602.739726,1000,11000,0,0);
INSERT INTO T_SI_INCOME_FEE (C_BUSINESS_DATE,C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_FEE_GROUP,C_FEE_TYPE,C_AMOUNT,C_SOURCE,C_SOURCE_EVENT_ID)
 VALUES ('2026-02-02','SUBFEE01','KHFEE','SDI01','PAYABLE','MGMT_FEE',300,'BO','SMK-MGMT-1');   -- đã cắt 300
INSERT INTO T_SI_CASH_HIST (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_VALID_FROM,C_CASH) VALUES ('SUBFEE01','KHFEE','SDI01','2026-02-02',0);

DECLARE @ecF INT, @emF NVARCHAR(400);
-- (A) breakdown nguồn FR-06 RS5 đọc: pending=C_PAYABLE_FEE@asof, paid=Σ cắt loại, accrued=pending+paid; FR-06 chạy err=0.
DECLARE @pendF DECIMAL(20,6)=(SELECT C_PAYABLE_FEE FROM T_SI_NAV_BALANCE WHERE C_SI_ACCOUNT='SUBFEE01' AND C_BUSINESS_DATE='2026-02-03');
DECLARE @paidF DECIMAL(20,6)=(SELECT ISNULL(SUM(C_AMOUNT),0) FROM T_SI_INCOME_FEE WHERE C_SI_ACCOUNT='SUBFEE01' AND C_FEE_GROUP='PAYABLE' AND C_FEE_TYPE='MGMT_FEE' AND C_BUSINESS_DATE<='2026-02-03');
EXEC SP_GET_ASSET_REPORT @p_si_account='SUBFEE01', @p_asof='2026-02-03', @p_err_code=@ecF OUTPUT, @p_err_msg=@emF OUTPUT;
IF @ecF=0 AND ABS(@pendF-602.739726)<0.001 AND ABS(@paidF-300)<0.001
    PRINT CONCAT('  OK FR-06 breakdown: pending(=C_PAYABLE_FEE)=',@pendF,' paid=',@paidF,' accrued=',@pendF+@paidF,' (err=0)');
ELSE PRINT CONCAT('  !!! FR-06 breakdown sai: err=',@ecF,' pending=',@pendF,' paid=',@paidF);

-- (B) GUARD @nAccrue>1: thêm loại phí accrue thứ 2 (TAX) → FR-06 phải trả err=4 (chặn output sai)
INSERT INTO T_FEE_CONFIG (C_FEE_TYPE,C_FEE_GROUP,C_RATE) VALUES ('TAX','PAYABLE',0.005);
EXEC SP_GET_ASSET_REPORT @p_si_account='SUBFEE01', @p_asof='2026-02-03', @p_err_code=@ecF OUTPUT, @p_err_msg=@emF OUTPUT;
IF @ecF=4 PRINT CONCAT('  OK guard FR-06: err=4 (',@emF,')');
ELSE PRINT CONCAT('  !!! guard FR-06 KHÔNG chặn: err=',@ecF);
DELETE FROM T_FEE_CONFIG WHERE C_FEE_TYPE='TAX';   -- dọn: trả về 1 loại accrue

-- (C) Custody (PAYABLE, KHÔNG rate) KHÔNG trip guard (vẫn 1 loại accrue) → FR-06 err=0
INSERT INTO T_FEE_CONFIG (C_FEE_TYPE,C_FEE_GROUP,C_RATE) VALUES ('CUSTODY_FEE','PAYABLE',NULL);
EXEC SP_GET_ASSET_REPORT @p_si_account='SUBFEE01', @p_asof='2026-02-03', @p_err_code=@ecF OUTPUT, @p_err_msg=@emF OUTPUT;
IF @ecF=0 PRINT '  OK custody (no rate) KHÔNG trip guard (vẫn 1 loại accrue)';
ELSE PRINT CONCAT('  !!! custody no-rate sai: err=',@ecF);
DELETE FROM T_FEE_CONFIG WHERE C_FEE_TYPE='CUSTODY_FEE';

-- CLEANUP: gỡ config MGMT_FEE (GLOBAL!) + data SUBFEE01 → trả DB về fee-free, KHÔNG ảnh hưởng script chạy sau (07_PM_SMOKE)
DELETE FROM T_FEE_CONFIG    WHERE C_FEE_TYPE='MGMT_FEE';
DELETE FROM T_SI_INCOME_FEE WHERE C_SI_ACCOUNT='SUBFEE01';
DELETE FROM T_SI_NAV_BALANCE WHERE C_SI_ACCOUNT='SUBFEE01';
DELETE FROM T_SI_NAV_CURRENT WHERE C_SI_ACCOUNT='SUBFEE01';
DELETE FROM T_SI_CASH_HIST  WHERE C_SI_ACCOUNT='SUBFEE01';

PRINT '';
PRINT '======== DELETE+INSERT idempotent unit_ledger + nav_balance (tính lại KHÔNG dup; accum reset→J12B set lại; DELETE scoped) ========';
-- (A) re-run COMPUTE @07: DELETE+INSERT lại → KHÔNG dup dòng; accum bị reset DEFAULT 0 (sentinel mất) — CÓ CHỦ ĐÍCH.
-- IDEMPOTENT VALUE CHECK (un-roll anchor): NAV/PnL/Unit/return @07 phải Y HỆT sau tính-lại (anchor đọc NAV_BALANCE @06,
--   KHÔNG bị roll-forward làm lệch). Trước fix: PnL→0, Unit cộng đôi cashflow. Bắt regression số liệu chạy đi chạy lại.
DECLARE @nav0 DECIMAL(20,0),@pnl0 DECIMAL(20,0),@unit0 DECIMAL(18,6),@ret0 DECIMAL(10,6);
SELECT @nav0=C_NAV,@pnl0=C_DAILY_PNL,@unit0=C_UNIT,@ret0=C_DAILY_RETURN FROM T_SI_NAV_BALANCE WHERE C_SI_ACCOUNT='SUB00001001' AND C_BUSINESS_DATE='2026-01-07';
UPDATE T_SI_NAV_BALANCE SET C_ACCUM_ACTIVE_RET=0.123456 WHERE C_SI_ACCOUNT='SUB00001001' AND C_BUSINESS_DATE='2026-01-07'; -- sentinel giả J12B
DECLARE @nbB INT=(SELECT COUNT(*) FROM T_SI_NAV_BALANCE WHERE C_BUSINESS_DATE='2026-01-07');
DECLARE @ulB INT=(SELECT COUNT(*) FROM T_SI_UNIT_LEDGER);
EXEC SP_EOD_COMPUTE '2026-01-07';   -- tính lại lần 1
EXEC SP_EOD_COMPUTE '2026-01-07';   -- tính lại lần 2 (chạy đi chạy lại nhiều lần)
DECLARE @nbA INT=(SELECT COUNT(*) FROM T_SI_NAV_BALANCE WHERE C_BUSINESS_DATE='2026-01-07');
DECLARE @ulA INT=(SELECT COUNT(*) FROM T_SI_UNIT_LEDGER);
DECLARE @accReset FLOAT=(SELECT C_ACCUM_ACTIVE_RET FROM T_SI_NAV_BALANCE WHERE C_SI_ACCOUNT='SUB00001001' AND C_BUSINESS_DATE='2026-01-07');
DECLARE @nav1 DECIMAL(20,0),@pnl1 DECIMAL(20,0),@unit1 DECIMAL(18,6),@ret1 DECIMAL(10,6);
SELECT @nav1=C_NAV,@pnl1=C_DAILY_PNL,@unit1=C_UNIT,@ret1=C_DAILY_RETURN FROM T_SI_NAV_BALANCE WHERE C_SI_ACCOUNT='SUB00001001' AND C_BUSINESS_DATE='2026-01-07';
IF @nbA=@nbB AND @ulA=@ulB PRINT CONCAT('  OK re-run KHÔNG dup: nav_balance ',@nbB,'→',@nbA,'; unit_ledger ',@ulB,'→',@ulA);
ELSE PRINT CONCAT('  !!! re-run DUP: nav_balance ',@nbB,'→',@nbA,'; unit_ledger ',@ulB,'→',@ulA);
IF @nav1=@nav0 AND @pnl1=@pnl0 AND @unit1=@unit0 AND @ret1=@ret0
    PRINT CONCAT('  OK IDEMPOTENT: NAV/PnL/Unit/return @07 KHÔNG đổi sau 2 lần tính lại (NAV=',@nav1,' PnL=',@pnl1,' UP_ret=',@ret1,')');
ELSE PRINT CONCAT('  !!! LỆCH SỐ khi tính lại: NAV ',@nav0,'→',@nav1,' PnL ',@pnl0,'→',@pnl1,' Unit ',@unit0,'→',@unit1,' ret ',@ret0,'→',@ret1);
IF ABS(@accReset)<0.0000001 PRINT '  OK J07 DELETE+INSERT tạo dòng mới sạch (accum reset DEFAULT 0, sentinel mất)';
ELSE PRINT CONCAT('  !!! J07 KHÔNG tạo lại dòng (accum còn ',@accReset,') → DELETE+INSERT không chạy?');
-- J12B set lại accum sau J07 reset, và IDEMPOTENT (chạy nhiều lần cho cùng kết quả → re-run pipeline an toàn).
EXEC SP_EOD_TE_ACCUM '2026-01-07';
DECLARE @a1 FLOAT=(SELECT C_ACCUM_ACTIVE_RET FROM T_SI_NAV_BALANCE WHERE C_SI_ACCOUNT='SUB00001001' AND C_BUSINESS_DATE='2026-01-07');
EXEC SP_EOD_TE_ACCUM '2026-01-07';
DECLARE @a2 FLOAT=(SELECT C_ACCUM_ACTIVE_RET FROM T_SI_NAV_BALANCE WHERE C_SI_ACCOUNT='SUB00001001' AND C_BUSINESS_DATE='2026-01-07');
IF @a1=@a2 PRINT CONCAT('  OK J12B set lại accum sau J07 reset + IDEMPOTENT (chạy 2 lần =',@a1,')');
ELSE PRINT CONCAT('  !!! J12B KHÔNG idempotent: ',@a1,' vs ',@a2);

-- (B) DELETE scoped + cf→0: bơm 1 dòng unit_ledger GIẢ @01-06 (ngày SUB KHÔNG có cashflow) → tính lại @06 phải XOÁ nó
--     (cf=0 ⇒ không insert lại), nhưng KHÔNG đụng dòng ngày khác (01-02 có INITIAL cashflow phải còn).
INSERT INTO T_SI_UNIT_LEDGER (C_SI_ACCOUNT,C_CUST_CODE,C_MASTER_CODE,C_BUSINESS_DATE,C_CF_NET,C_DELTA_UNIT,C_UNIT)
 VALUES ('SUB00001001','KH00001001','SDI01','2026-01-06',0,0,1000);
EXEC SP_EOD_COMPUTE '2026-01-06';
IF NOT EXISTS (SELECT 1 FROM T_SI_UNIT_LEDGER WHERE C_SI_ACCOUNT='SUB00001001' AND C_BUSINESS_DATE='2026-01-06')
    PRINT '  OK DELETE+INSERT: dòng unit_ledger cf=0 bị gỡ khi tính lại @06';
ELSE PRINT '  !!! KHÔNG gỡ dòng cf=0 @06';
IF EXISTS (SELECT 1 FROM T_SI_UNIT_LEDGER WHERE C_SI_ACCOUNT='SUB00001001' AND C_BUSINESS_DATE='2026-01-02')
    PRINT '  OK DELETE scoped: ngày khác (01-02 INITIAL) KHÔNG bị xoá';
ELSE PRINT '  !!! DELETE xoá NHẦM ngày khác (01-02)';

PRINT '';
PRINT '======== DATE GUARD ngày-bừa-bãi: FR-06 err=5, index err=3, rebalance err=5, EOD trading-day err=10 ========';
DECLARE @ecD INT, @emD NVARCHAR(400);
-- FR-06: ngày hợp lệ (01-07 có NAV_BALANCE) → err=0; gap/tương lai/trước-mở → err=5 (chặn NAV=NULL+asset>0 im lặng)
EXEC SP_GET_ASSET_REPORT 'SUB00001001','2026-01-07',@p_err_code=@ecD OUTPUT,@p_err_msg=@emD OUTPUT;
IF @ecD=0 PRINT '  OK FR-06 ngày GD hợp lệ @01-07 → err=0'; ELSE PRINT CONCAT('  !!! FR-06 ngày hợp lệ sai: err=',@ecD);
EXEC SP_GET_ASSET_REPORT 'SUB00001001','2026-01-03',@p_err_code=@ecD OUTPUT,@p_err_msg=@emD OUTPUT;  -- gap (không có NAV_BALANCE)
IF @ecD=5 PRINT '  OK FR-06 ngày nghỉ/gap @01-03 → err=5'; ELSE PRINT CONCAT('  !!! FR-06 gap KHÔNG chặn: err=',@ecD);
EXEC SP_GET_ASSET_REPORT 'SUB00001001','2026-12-31',@p_err_code=@ecD OUTPUT,@p_err_msg=@emD OUTPUT;  -- tương lai
IF @ecD=5 PRINT '  OK FR-06 ngày tương lai → err=5'; ELSE PRINT CONCAT('  !!! FR-06 tương lai KHÔNG chặn: err=',@ecD);
EXEC SP_GET_ASSET_REPORT 'SUB00001001','2026-01-01',@p_err_code=@ecD OUTPUT,@p_err_msg=@emD OUTPUT;  -- trước khi mở
IF @ecD=5 PRINT '  OK FR-06 ngày trước khi mở TK → err=5'; ELSE PRINT CONCAT('  !!! FR-06 trước-mở KHÔNG chặn: err=',@ecD);
-- index snapshot (GIỮ): tương lai → err=3
EXEC SP_GET_ASSET_INDEX_SNAPSHOT '2026-12-31','EOD',@p_err_code=@ecD OUTPUT,@p_err_msg=@emD OUTPUT;
IF @ecD=3 PRINT '  OK index snapshot ngày tương lai → err=3'; ELSE PRINT CONCAT('  !!! index snapshot KHÔNG chặn: err=',@ecD);
-- rebalance detail: trước inception → err=5
EXEC SP_GET_MASTER_REBALANCE_DETAIL 'SDI01','2026-01-01',@p_err_code=@ecD OUTPUT,@p_err_msg=@emD OUTPUT;
IF @ecD=5 PRINT '  OK rebalance_detail ngày trước inception → err=5'; ELSE PRINT CONCAT('  !!! rebalance KHÔNG chặn: err=',@ecD);
-- SP_EOD_RUN: ngày KHÔNG phải ngày GD (2099, không có giá) → err=10 + thông điệp trading-day
EXEC SP_EOD_RUN '2099-06-15',@p_err_code=@ecD OUTPUT,@p_err_msg=@emD OUTPUT;
IF @ecD=10 PRINT CONCAT('  OK SP_EOD_RUN ngày không-GD → err=10 (',@emD,')'); ELSE PRINT CONCAT('  !!! SP_EOD_RUN không chặn ngày không-GD: err=',@ecD);

PRINT '';
PRINT '======== INGEST GIÁ EOD: SP_INGEST_PRICE_DAILY (atomic+validate) + completeness gate index ========';
DECLARE @ecP2 INT, @emP2 NVARCHAR(400);
-- (A) batch HỢP LỆ → err=0, 3 mã vào T_PRICE_DAILY (PX2 thiếu is_ex_rights → default 0)
EXEC SP_INGEST_PRICE_DAILY N'[{"ticker":"PX1","ref_price":100,"close_price":102,"is_ex_rights":0},{"ticker":"PX2","ref_price":50,"close_price":51},{"ticker":"PX3","ref_price":120,"close_price":118,"is_ex_rights":1}]','2026-03-02',NULL,NULL,@ecP2 OUTPUT,@emP2 OUTPUT;
IF @ecP2=0 AND (SELECT COUNT(*) FROM T_PRICE_DAILY WHERE C_BUSINESS_DATE='2026-03-02' AND C_TICKER IN ('PX1','PX2','PX3'))=3
   AND (SELECT C_IS_EX_RIGHTS FROM T_PRICE_DAILY WHERE C_BUSINESS_DATE='2026-03-02' AND C_TICKER='PX2')=0
   PRINT '  OK ingest batch hợp lệ: 3 mã (PX2 thiếu is_ex_rights → 0)';
ELSE PRINT CONCAT('  !!! ingest hợp lệ sai: err=',@ecP2,' ',@emP2);
-- (B) batch SAI (PX5 close=0) → err=21, KHÔNG ghi MÃ NÀO (PX4 hợp lệ cũng KHÔNG vào)
EXEC SP_INGEST_PRICE_DAILY N'[{"ticker":"PX4","ref_price":10,"close_price":11},{"ticker":"PX5","ref_price":10,"close_price":0}]','2026-03-02',NULL,NULL,@ecP2 OUTPUT,@emP2 OUTPUT;
IF @ecP2=21 AND NOT EXISTS (SELECT 1 FROM T_PRICE_DAILY WHERE C_BUSINESS_DATE='2026-03-02' AND C_TICKER IN ('PX4','PX5'))
   PRINT '  OK batch SAI bị từ chối CẢ batch (PX4 hợp lệ cũng KHÔNG ghi) → err=21, atomic';
ELSE PRINT CONCAT('  !!! batch sai xử lý sai: err=',@ecP2,' (PX4 lọt vào?)');
-- (C) idempotent upsert: gọi lại đổi close PX1 102→105 → ghi đè, KHÔNG dup
EXEC SP_INGEST_PRICE_DAILY N'[{"ticker":"PX1","ref_price":100,"close_price":105}]','2026-03-02',NULL,NULL,@ecP2 OUTPUT,@emP2 OUTPUT;
IF @ecP2=0 AND (SELECT C_CLOSE_PRICE FROM T_PRICE_DAILY WHERE C_BUSINESS_DATE='2026-03-02' AND C_TICKER='PX1')=105
   AND (SELECT COUNT(*) FROM T_PRICE_DAILY WHERE C_BUSINESS_DATE='2026-03-02' AND C_TICKER='PX1')=1
   PRINT '  OK upsert idempotent: PX1 close 102→105, KHÔNG dup';
ELSE PRINT CONCAT('  !!! upsert sai: err=',@ecP2);
-- (D) count mismatch (nhận 1 != dự kiến 5) → err=22
EXEC SP_INGEST_PRICE_DAILY N'[{"ticker":"PX1","ref_price":100,"close_price":105}]','2026-03-02',5,NULL,@ecP2 OUTPUT,@emP2 OUTPUT;
IF @ecP2=22 PRINT '  OK count guard: nhận 1 != dự kiến 5 → err=22 (chặn payload thiếu/cắt)'; ELSE PRINT CONCAT('  !!! count guard sai: err=',@ecP2);
-- (E) trùng mã trong batch → err=21
EXEC SP_INGEST_PRICE_DAILY N'[{"ticker":"PXD","ref_price":1,"close_price":1},{"ticker":"PXD","ref_price":2,"close_price":2}]','2026-03-02',NULL,NULL,@ecP2 OUTPUT,@emP2 OUTPUT;
IF @ecP2=21 AND NOT EXISTS (SELECT 1 FROM T_PRICE_DAILY WHERE C_BUSINESS_DATE='2026-03-02' AND C_TICKER='PXD')
   PRINT '  OK trùng mã trong batch → err=21 (không ghi)'; ELSE PRINT CONCAT('  !!! dup-ticker xử lý sai: err=',@ecP2);
-- (F) JSON sai → err=20
EXEC SP_INGEST_PRICE_DAILY N'khong-phai-json','2026-03-02',NULL,NULL,@ecP2 OUTPUT,@emP2 OUTPUT;
IF @ecP2=20 PRINT '  OK JSON sai → err=20'; ELSE PRINT CONCAT('  !!! json guard sai: err=',@ecP2);
-- (G) COMPLETENESS GATE: ngày chưa nạp đủ giá mã danh mục mẫu → SP_EOD_RUN_INDEX err=11 (chặn index sai)
EXEC SP_EOD_SET_SOURCE_READY @p_business_date='2026-03-09',@p_source='MKT_DATA',@p_err_code=@ecP2 OUTPUT,@p_err_msg=@emP2 OUTPUT;
EXEC SP_EOD_RUN_INDEX '2026-03-09',@p_err_code=@ecP2 OUTPUT,@p_err_msg=@emP2 OUTPUT;
IF @ecP2=11 PRINT '  OK completeness gate: thiếu giá mã danh mục mẫu @09 → index BỊ CHẶN err=11';
ELSE PRINT CONCAT('  !!! gate KHÔNG chặn khi thiếu giá: err=',@ecP2,' ',@emP2);
-- cleanup
DELETE FROM T_PRICE_DAILY  WHERE C_BUSINESS_DATE='2026-03-02';
DELETE FROM T_EOD_RUN      WHERE C_BUSINESS_DATE='2026-03-09';
DELETE FROM T_EOD_PIPELINE WHERE C_BUSINESS_DATE='2026-03-09';

PRINT '';
PRINT '======== RERUN QUÁ KHỨ: SP_EOD_RECOMPUTE_RANGE (reconstruct AS-OF từ history) ========';
-- ⚠️ Block idempotent ở trên đã chạy FORWARD SP_EOD_COMPUTE @06 (đọc holdings HIỆN TẠI = post-rebalance BBB90000)
--   → NAV@06 BỊ SAI (=11.28tr thay vì 10.76tr). ĐÂY chính là lý do cần luồng rerun riêng. Recompute đọc holdings
--   AS-OF @06 (BBB80000 từ holding_hist) → KHÔI PHỤC đúng. Khẳng định recompute = SỬA được số ngày quá khứ.
DECLARE @ecR INT, @emR NVARCHAR(400);
UPDATE T_MASTER_HOLDING_BALANCE SET C_QUANTITY=99999 WHERE C_MASTER_CODE='SDI01' AND C_BUSINESS_DATE='2026-01-06' AND C_TICKER='BBB';  -- corrupt composition @06 để test recompute tái dựng
EXEC SP_EOD_RECOMPUTE_RANGE @p_from_date='2026-01-02', @p_to_date=NULL, @p_cust_code='KH00001001', @p_err_code=@ecR OUTPUT, @p_err_msg=@emR OUTPUT;
DECLARE @n02 DECIMAL(20,0)=(SELECT C_NAV FROM T_SI_NAV_BALANCE WHERE C_SI_ACCOUNT='SUB00001001' AND C_BUSINESS_DATE='2026-01-02');
DECLARE @n05 DECIMAL(20,0)=(SELECT C_NAV FROM T_SI_NAV_BALANCE WHERE C_SI_ACCOUNT='SUB00001001' AND C_BUSINESS_DATE='2026-01-05');
DECLARE @n06 DECIMAL(20,0)=(SELECT C_NAV FROM T_SI_NAV_BALANCE WHERE C_SI_ACCOUNT='SUB00001001' AND C_BUSINESS_DATE='2026-01-06');
DECLARE @n07 DECIMAL(20,0)=(SELECT C_NAV FROM T_SI_NAV_BALANCE WHERE C_SI_ACCOUNT='SUB00001001' AND C_BUSINESS_DATE='2026-01-07');
DECLARE @mR DECIMAL(20,0)=(SELECT C_NAV FROM T_MASTER_NAV_BALANCE WHERE C_MASTER_CODE='SDI01' AND C_BUSINESS_DATE='2026-01-07');
DECLARE @cntR INT=(SELECT COUNT(*) FROM T_SI_NAV_BALANCE WHERE C_SI_ACCOUNT='SUB00001001');
IF @ecR=0 AND @n02=10000000 AND @n05=10440000 AND @n06=10760000 AND @n07=11280000 AND @mR=11280000 AND @cntR=4
   PRINT CONCAT('  OK recompute KH ĐÚNG as-of (02=',@n02,' 05=',@n05,' 06=',@n06,' 07=',@n07,'; master@07=',@mR,'; #rows=',@cntR,') — @06 đã KHÔI PHỤC đúng');
ELSE PRINT CONCAT('  !!! recompute SAI: err=',@ecR,' 02=',@n02,' 05=',@n05,' 06=',@n06,' 07=',@n07,' master@07=',@mR,' #rows=',@cntR,' ',@emR);
-- composition master AS-OF tái dựng đúng: BBB @06=80000 (trước rebalance), @07=90000 (sau) — đã sửa corrupt 99999
DECLARE @b06 DECIMAL(20,0)=(SELECT C_QUANTITY FROM T_MASTER_HOLDING_BALANCE WHERE C_MASTER_CODE='SDI01' AND C_BUSINESS_DATE='2026-01-06' AND C_TICKER='BBB');
DECLARE @b07 DECIMAL(20,0)=(SELECT C_QUANTITY FROM T_MASTER_HOLDING_BALANCE WHERE C_MASTER_CODE='SDI01' AND C_BUSINESS_DATE='2026-01-07' AND C_TICKER='BBB');
IF @b06=80000 AND @b07=90000 PRINT CONCAT('  OK composition as-of tái dựng: BBB @06=',@b06,' @07=',@b07,' (sửa 99999→80000)');
ELSE PRINT CONCAT('  !!! composition as-of SAI: BBB @06=',@b06,' @07=',@b07);
-- per-SI scope (05→07) đúng
EXEC SP_EOD_RECOMPUTE_RANGE @p_from_date='2026-01-05', @p_to_date='2026-01-07', @p_si_account='SUB00001001', @p_err_code=@ecR OUTPUT, @p_err_msg=@emR OUTPUT;
IF @ecR=0 AND (SELECT C_NAV FROM T_SI_NAV_BALANCE WHERE C_SI_ACCOUNT='SUB00001001' AND C_BUSINESS_DATE='2026-01-06')=10760000
   PRINT '  OK recompute per-SI (05→07) đúng'; ELSE PRINT CONCAT('  !!! recompute per-SI sai: err=',@ecR);
-- scope rỗng → err=1
EXEC SP_EOD_RECOMPUTE_RANGE @p_from_date='2026-01-02', @p_cust_code='KHKHONGCO', @p_err_code=@ecR OUTPUT, @p_err_msg=@emR OUTPUT;
IF @ecR=1 PRINT '  OK scope rỗng → err=1'; ELSE PRINT CONCAT('  !!! scope rỗng sai: err=',@ecR);
-- range from>to → err=20
EXEC SP_EOD_RECOMPUTE_RANGE @p_from_date='2026-01-07', @p_to_date='2026-01-02', @p_si_account='SUB00001001', @p_err_code=@ecR OUTPUT, @p_err_msg=@emR OUTPUT;
IF @ecR=20 PRINT '  OK range from>to → err=20'; ELSE PRINT CONCAT('  !!! range guard sai: err=',@ecR);

PRINT '';
PRINT '======== MASTER INDEX: weight dạng % (Σ=100) KHÔNG nổ cấp số nhân (chuẩn hoá /Σw) ========';
INSERT T_MASTER_PORTFOLIO (C_MASTER_CODE,C_MASTER_NAME,C_STATUS,C_INCEPTION_DATE,C_BENCHMARK_CODE) VALUES ('MPCT',N'PctW','ACTIVE','2026-04-01','VNINDEX');
INSERT T_MASTER_PORTFOLIO_TICKER (C_MASTER_CODE,C_EFFECTIVE_DATE,C_TICKER,C_TARGET_WEIGHT) VALUES ('MPCT','2026-04-01','PXA',60),('MPCT','2026-04-01','PXB',40); -- Σ=100 (%)
INSERT T_PRICE_DAILY (C_TICKER,C_BUSINESS_DATE,C_REF_PRICE,C_CLOSE_PRICE) VALUES
 ('PXA','2026-04-01',100,100),('PXB','2026-04-01',50,50),     -- day1: factor=1 → index=1000
 ('PXA','2026-04-02',100,110),('PXB','2026-04-02',50,50);     -- day2: PXA +10% → factor=(60×1.1+40×1)/100=1.06
EXEC SP_EOD_SI_INDEX '2026-04-01';
EXEC SP_EOD_SI_INDEX '2026-04-02';
DECLARE @ix1 DECIMAL(18,6)=(SELECT C_INDEX_VALUE FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MPCT' AND C_BUSINESS_DATE='2026-04-01');
DECLARE @ix2 DECIMAL(18,6)=(SELECT C_INDEX_VALUE FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MPCT' AND C_BUSINESS_DATE='2026-04-02');
DECLARE @rx2 DECIMAL(18,8)=(SELECT C_DAILY_RETURN FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='MPCT' AND C_BUSINESS_DATE='2026-04-02');
IF ABS(@ix1-1000)<0.01 AND ABS(@ix2-1060)<0.01 AND ABS(@rx2-0.06)<0.0001
   PRINT CONCAT('  OK weight % (Σ=100) chuẩn hoá đúng: index 1000→',@ix2,' return=',@rx2,' (KHÔNG nổ ×100)');
ELSE PRINT CONCAT('  !!! index weight% SAI: ix1=',@ix1,' ix2=',@ix2,' rx2=',@rx2);
DELETE FROM T_MASTER_INDEX_DAILY WHERE C_BUSINESS_DATE IN ('2026-04-01','2026-04-02');
DELETE FROM T_MASTER_PORTFOLIO_TICKER WHERE C_MASTER_CODE='MPCT';
DELETE FROM T_MASTER_PORTFOLIO WHERE C_MASTER_CODE='MPCT';
DELETE FROM T_PRICE_DAILY WHERE C_BUSINESS_DATE IN ('2026-04-01','2026-04-02');

PRINT '';
PRINT '======== RECOMPUTE INDEX lịch sử: SP_EOD_RECOMPUTE_INDEX_RANGE (sửa chuỗi index hỏng) ========';
UPDATE T_MASTER_INDEX_DAILY SET C_INDEX_VALUE=99999999 WHERE C_MASTER_CODE='SDI01' AND C_BUSINESS_DATE='2026-01-07';  -- giả lập index nổ
DECLARE @ecIR INT, @emIR NVARCHAR(400);
EXEC SP_EOD_RECOMPUTE_INDEX_RANGE @p_from_date='2026-01-02', @p_to_date=NULL, @p_err_code=@ecIR OUTPUT, @p_err_msg=@emIR OUTPUT;
DECLARE @fix7 DECIMAL(18,2)=(SELECT C_INDEX_VALUE FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='SDI01' AND C_BUSINESS_DATE='2026-01-07');
DECLARE @fix6 DECIMAL(18,2)=(SELECT C_INDEX_VALUE FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='SDI01' AND C_BUSINESS_DATE='2026-01-06');
IF @ecIR=0 AND @fix7=1078.80 AND @fix6=1078.80 PRINT CONCAT('  OK recompute index sửa chuỗi hỏng: @07 99999999 → ',@fix7,' (2 chữ số thập phân)');
ELSE PRINT CONCAT('  !!! recompute index sai: err=',@ecIR,' @06=',@fix6,' @07=',@fix7,' ',@emIR);
