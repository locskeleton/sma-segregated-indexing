/*==============================================================================
  SDI ENGINE — SMOKE TEST (chạy sau 01_TABLES.sql + 02_SP_ENGINE.sql)
  1 SI (AAA 60% / BBB 40%), 1 KH, 4 phiên. FO ingest qua SP_INGEST_CUSTOMER (Kafka per-KH, JSON).
  Mỗi phiên: ingest event (cash+holdings+phí) → SP_EOD_RUN (J0 GATE → compute → ...).
==============================================================================*/
SET NOCOUNT ON; SET QUOTED_IDENTIFIER ON; SET ANSI_NULLS ON;  -- QI ON: DML trên bảng có filtered index

-- reset
DELETE FROM T_EOD_WORK; DELETE FROM T_UNIT_LEDGER; DELETE FROM T_CUSTOMER_NAV_BALANCE;
DELETE FROM T_MASTER_NAV_BALANCE; DELETE FROM T_MASTER_INDEX_DAILY; DELETE FROM T_MASTER_HOLDING_BALANCE;
DELETE FROM T_CUSTOMER_FEE_INCOME; DELETE FROM T_EOD_RUN; DELETE FROM T_MASTER_NAV_CURRENT;
DELETE FROM T_INDEXING_PORTFOLIO_TICKER; DELETE FROM T_CUSTOMER_NAV_CURRENT;
DELETE FROM T_CASHFLOW_EVENT; DELETE FROM T_CUSTOMER_HOLDING_HIST; DELETE FROM T_CUSTOMER_CASH_HIST;
DELETE FROM T_PRICE_DAILY; DELETE FROM T_MASTER_PORTFOLIO_TICKER; DELETE FROM T_INDEXING_PORTFOLIO; DELETE FROM T_MASTER_PORTFOLIO;

-- master + config tiểu khoản (signup; KHÔNG qua Kafka)
INSERT INTO T_MASTER_PORTFOLIO (C_MASTER_CODE,C_SI_NAME,C_STATUS,C_INCEPTION_DATE,C_MGMT_FEE_RATE,C_BENCHMARK_CODE)
VALUES ('SDI01',N'Demo','ACTIVE','2026-01-02',0.01,'VNINDEX');
INSERT INTO T_INDEXING_PORTFOLIO (C_CUST_CODE,C_MASTER_CODE,C_SI_CODE,C_JOIN_DATE,C_STATUS,C_MGMT_FEE_RATE)
VALUES ('KH00001001','SDI01','SUB00001001','2026-01-02','ACTIVE',0.01);  -- sub-account SI code = SUB00001001
INSERT INTO T_MASTER_PORTFOLIO_TICKER (C_MASTER_CODE,C_EFFECTIVE_DATE,C_TICKER,C_TARGET_WEIGHT) VALUES
 ('SDI01','2026-01-02','AAA',0.60),('SDI01','2026-01-02','BBB',0.40);

INSERT INTO T_PRICE_DAILY (C_TICKER,C_BUSINESS_DATE,C_CLOSE_PRICE) VALUES
 ('AAA','2026-01-02',100),('BBB','2026-01-02',50),
 ('AAA','2026-01-05',110),('BBB','2026-01-05',48),
 ('AAA','2026-01-06',110),('BBB','2026-01-06',52),
 ('AAA','2026-01-07',110),('BBB','2026-01-07',52);

-- KH nạp 10,000,000 ngày 02 (cashflow nạp/rút = SDI-side → ghi thẳng, KHÔNG qua Kafka)
INSERT INTO T_CASHFLOW_EVENT (C_CUST_CODE,C_MASTER_CODE,C_BUSINESS_DATE,C_EVENT_TYPE,C_AMOUNT)
VALUES ('KH00001001','SDI01','2026-01-02','INITIAL',10000000);

/*--- Phiên 02: ingest (cash 0, holdings AAA 60000 / BBB 80000) rồi EOD ---*/
EXEC SP_INGEST_CUSTOMER N'{"cust_code":"KH00001001","business_date":"2026-01-02","sub_accounts":[{"si_code":"SDI01","cash":0,"holdings":[{"ticker":"AAA","quantity":60000,"avg_cost":100},{"ticker":"BBB","quantity":80000,"avg_cost":50}],"fees":[]}]}';
EXEC SP_EOD_RUN '2026-01-02';

/*--- Phiên 05: holdings KHÔNG đổi → ingest no-dup interval ---*/
EXEC SP_INGEST_CUSTOMER N'{"cust_code":"KH00001001","business_date":"2026-01-05","sub_accounts":[{"si_code":"SDI01","cash":0,"holdings":[{"ticker":"AAA","quantity":60000,"avg_cost":100},{"ticker":"BBB","quantity":80000,"avg_cost":50}],"fees":[]}]}';
EXEC SP_EOD_RUN '2026-01-05';

/*--- Phiên 06: cổ tức + phí (event có event_id). Gọi 2 LẦN (Kafka redelivery) → cash/holdings no-op + fee DEDUP ---*/
DECLARE @ev06 NVARCHAR(MAX) = N'{"cust_code":"KH00001001","business_date":"2026-01-06","sub_accounts":[{"si_code":"SDI01","cash":0,"holdings":[{"ticker":"AAA","quantity":60000,"avg_cost":100},{"ticker":"BBB","quantity":80000,"avg_cost":50}],"fees":[{"event_id":"FO-D06-1","type":"DIVIDEND","ticker":"AAA","amount":50000},{"event_id":"FO-D06-2","type":"CUSTODY_FEE","amount":1000}]}]}';
EXEC SP_INGEST_CUSTOMER @ev06;
EXEC SP_INGEST_CUSTOMER @ev06;   -- redelivery: phải no-op, fee KHÔNG nhân đôi
EXEC SP_EOD_RUN '2026-01-06';

/*--- Phiên 07: BBB tái cân bằng 80000→90000 (FO gửi holdings mới) → interval close/open ---*/
EXEC SP_INGEST_CUSTOMER N'{"cust_code":"KH00001001","business_date":"2026-01-07","sub_accounts":[{"si_code":"SDI01","cash":0,"holdings":[{"ticker":"AAA","quantity":60000,"avg_cost":100},{"ticker":"BBB","quantity":90000,"avg_cost":50}],"fees":[]}]}';
EXEC SP_EOD_RUN '2026-01-07';

/*------------------------------------------------- KẾT QUẢ & KỲ VỌNG --------*/
PRINT '--- T_CUSTOMER_NAV_BALANCE (per-KH) ---';
SELECT C_BUSINESS_DATE, C_NAV, C_UNIT, C_UNIT_PRICE, C_DAILY_PNL, C_DAILY_RETURN
FROM T_CUSTOMER_NAV_BALANCE WHERE C_CUST_CODE='KH00001001' ORDER BY C_BUSINESS_DATE;
-- KỲ VỌNG (phương án A: NAV = stock + FO cash):
--  02: NAV=10,000,000 UNIT=1000 UP=10,000 PNL=0
--  05: NAV=10,440,000 UP=10,440 PNL=440,000     06: NAV=10,760,000 UP=10,760 PNL=320,000
--  07: NAV=11,280,000 UP=11,280 (AAA 60000*110 + BBB 90000*52)

PRINT '--- T_MASTER_NAV_BALANCE (SI tổng hợp) ---';
SELECT C_BUSINESS_DATE, C_CASH, C_STOCK_VALUE, C_TOTAL_ASSET, C_NAV, C_UNIT_PRICE, C_DAILY_PNL,
       C_CASH_DIVIDEND, C_CUSTODY_FEE, C_MGMT_FEE_ACCRUED
FROM T_MASTER_NAV_BALANCE WHERE C_MASTER_CODE='SDI01' ORDER BY C_BUSINESS_DATE;
-- KỲ VỌNG: 06 cash_dividend=50000, custody_fee=1000; ngày khác NULL.

PRINT '--- T_MASTER_INDEX_DAILY (danh mục mẫu) ---';
SELECT C_BUSINESS_DATE, C_INDEX_VALUE, C_DAILY_RETURN
FROM T_MASTER_INDEX_DAILY WHERE C_MASTER_CODE='SDI01' ORDER BY C_BUSINESS_DATE;
-- KỲ VỌNG: 02→1000, 05→1044, 06→1078.8

PRINT '--- T_CUSTOMER_HOLDING_HIST (interval: FULL history, no-dup) ---';
SELECT C_TICKER, C_VALID_FROM, C_VALID_TO, C_QUANTITY
FROM T_CUSTOMER_HOLDING_HIST WHERE C_CUST_CODE='KH00001001' ORDER BY C_TICKER, C_VALID_FROM;
-- KỲ VỌNG: AAA 1 dòng (02→NULL, 60000); BBB 2 dòng (02→07, 80000 ĐÓNG + 07→NULL, 90000 OPEN)

PRINT '--- Reconstruct holdings @2026-01-05 (trước rebalance) ---';
SELECT C_TICKER, C_QUANTITY FROM T_CUSTOMER_HOLDING_HIST
WHERE C_CUST_CODE='KH00001001' AND C_VALID_FROM<='2026-01-05' AND (C_VALID_TO>'2026-01-05' OR C_VALID_TO IS NULL)
ORDER BY C_TICKER;   -- KỲ VỌNG: AAA 60000, BBB 80000

PRINT '--- Reconstruct holdings @2026-01-07 (sau rebalance) ---';
SELECT C_TICKER, C_QUANTITY FROM T_CUSTOMER_HOLDING_HIST
WHERE C_CUST_CODE='KH00001001' AND C_VALID_FROM<='2026-01-07' AND (C_VALID_TO>'2026-01-07' OR C_VALID_TO IS NULL)
ORDER BY C_TICKER;   -- KỲ VỌNG: AAA 60000, BBB 90000

PRINT '--- T_CUSTOMER_CASH_HIST (interval) — cash 0 cố định → 1 dòng open ---';
SELECT C_VALID_FROM, C_VALID_TO, C_CASH FROM T_CUSTOMER_CASH_HIST WHERE C_CUST_CODE='KH00001001' ORDER BY C_VALID_FROM;

PRINT '--- Cổ tức/phí: idempotent — phải ĐÚNG 2 dòng (event 06 redelivery KHÔNG nhân đôi) ---';
SELECT C_BUSINESS_DATE, C_TYPE, C_TICKER, C_AMOUNT, C_SOURCE_EVENT_ID FROM T_CUSTOMER_FEE_INCOME
WHERE C_CUST_CODE='KH00001001' ORDER BY C_TYPE;   -- KỲ VỌNG: 2 dòng (DIVIDEND 50000, CUSTODY_FEE 1000)

PRINT '--- T_EOD_RUN (job log) ---';
SELECT C_STATUS, COUNT(*) AS N FROM T_EOD_RUN GROUP BY C_STATUS;   -- KỲ VỌNG: 24 DONE (6 job × 4 phiên)

PRINT '--- GUARD 1: event QUÁ KHỨ (business_date 06 < watermark 07) phải bị CHẶN ---';
BEGIN TRY
    EXEC SP_INGEST_CUSTOMER N'{"cust_code":"KH00001001","business_date":"2026-01-06","sub_accounts":[{"si_code":"SDI01","cash":999,"holdings":[],"fees":[]}]}';
    PRINT '  !!! LỖI: KHÔNG chặn event quá khứ';
END TRY BEGIN CATCH PRINT '  OK đã chặn: '+ERROR_MESSAGE(); END CATCH;

PRINT '--- GUARD 2: GATE thiếu data (thêm tiểu khoản chưa ingest) phải CHẶN ---';
INSERT INTO T_INDEXING_PORTFOLIO (C_CUST_CODE,C_MASTER_CODE,C_SI_CODE,C_JOIN_DATE,C_STATUS) VALUES ('KH00009999','SDI01','SUB00009999','2026-01-07','ACTIVE');
BEGIN TRY
    EXEC SP_EOD_GATE '2026-01-07';   -- expected=2, received=1 (KH mới chưa ingest)
    PRINT '  !!! LỖI: GATE không chặn khi thiếu data';
END TRY BEGIN CATCH PRINT '  OK GATE chặn: '+ERROR_MESSAGE(); END CATCH;
DELETE FROM T_INDEXING_PORTFOLIO WHERE C_CUST_CODE='KH00009999';
