# SDI — PM Tool (Dashboard quản lý danh mục master) — Spec

Spec tầng **dữ liệu/SP** cho dashboard PM quản lý danh mục **master**. Bổ trợ [SDI-spec.md](./SDI-spec.md) (công thức NAV/index/EOD), [SDI-db-architecture.md](./SDI-db-architecture.md).

> **Phạm vi tài liệu này = tầng SERVE (SP đọc cho API)**. Phần tổng-hợp-cuối-ngày cần cho PM đã làm ở **Track 1** (`SDI-spec` §8/§9: receivables→NAV, master `C_CASH_IN`/`C_CASH_OUT`/`C_TOTAL_ACCOUNT`). UI/giao diện ngoài phạm vi.

---

## 1. Bối cảnh & nguyên tắc

- **User = PM**, theo dõi **cấp master** (10 master, ~50k KH, ~5k KH/master). US4/US5 có tính per-KH rồi **tổng hợp/xếp hạng** (đếm lãi/lỗ, phân phối, top-N theo mã KH) — KHÔNG drill-down chi tiết tài khoản KH.
- **2 bản chất dữ liệu** (BRD gán nhãn):
  - **Snapshot (current)**: AUM, cash, #KH, cash drag, net flow → tính **on-query từ state hiện tại** (`T_MASTER_NAV_CURRENT`/`T_SI_NAV_CURRENT`), "realtime" = mới tới EOD sync gần nhất.
  - **Hiệu suất (T-1)**: return, %PnL, TE, deviation, index, phân phối → từ daily tables, **T-1 theo bản chất** (cần giá đóng cửa).
- **Tất cả ON-READ** (scale nhỏ): không materialize thêm (TE/deviation/dist tính lúc đọc). Building-block EOD đã đủ. Chỉ cần **1 index** `(C_MASTER_CODE, C_BUSINESS_DATE)` trên `T_SI_NAV_BALANCE` để quét per-master nhanh.
- **SP master-keyed**: nhận `@p_master_code` (+ range), KHÔNG nhận/không trả định danh KH ngoài top-N ranking (mã KH).

---

## 2. Công thức chuẩn (tham chiếu chung cho US1–US5)

| Đại lượng | Công thức | Nguồn |
|---|---|---|
| **AUM** (1 master) | `Σ total_asset` các tiểu khoản ACTIVE = `Σ (stock + tiền mặt + tiền bán chờ về + cổ tức tiền)` | `T_MASTER_NAV_CURRENT.C_TOTAL_ASSET` (current) / `T_MASTER_NAV_BALANCE` (daily) |
| **Tăng trưởng AUM** | `AUM hiện tại − AUM đầu kỳ` ; `% = (AUM_now / AUM_đầu kỳ − 1)×100` | master daily theo range |
| **Net in/out** | `NET_IN = Σ cash_in`, `NET_OUT = Σ cash_out`, `NET = IN − OUT` trong kỳ | `Σ T_MASTER_NAV_BALANCE.C_CASH_IN/C_CASH_OUT` |
| **Cash drag** | `Σ Tiền / Σ AUM × 100%` (Tiền = cash + pending + div) | master current/daily |
| **#KH (DM KH)** | `C_TOTAL_ACCOUNT` (tiểu khoản ACTIVE) | master current |
| **Hiệu suất master (model)** | `Master Index` PR: `Index_t = Index_(t-1) × Σ wᵢ·Pᵢ,t/P_ref` (CA: P_ref điều chỉnh) | `T_MASTER_INDEX_DAILY` |
| **Hiệu suất DM tổng KH** | **AUM-weighted (end-weight, theo BRD)**: `Σ Wᵢ·PnLᵢ`, `Wᵢ = AUM_i cuối kỳ / Σ AUM cuối kỳ`, `PnLᵢ = UP_i(cuối)/UP_i(mốc) − 1` | per-KH `T_SI_NAV_BALANCE.C_UNIT_PRICE` 2 mốc + AUM cuối (current) |
| **Deviation (per dev)** | `(AUM-weighted Return KH − Return Master) × 10000` (BPS) | như trên + index |
| **Tracking Error (TE)** | per-KH `TE_i = STDEV(dᵢ,t) × √X`, `dᵢ,t = R_KH,i,t − R_master,t` (active return ngày t); `X = số ngày GD kỳ (cap 252)`. Master: `Σ(TE_i × AUM_i)/Σ AUM_i` (AUM-weighted) | `T_SI_NAV_BALANCE.C_DAILY_RETURN` (KH) − `T_MASTER_INDEX_DAILY.C_DAILY_RETURN` (master), STDEV on-read |
| **%PnL per-KH** | `UP_i(cuối kỳ)/UP_i(mốc) − 1` (TWR, miễn nhiễm dòng tiền) | `T_SI_NAV_BALANCE.C_UNIT_PRICE` |
| **VN-Index** | `(điểm cuối/điểm mốc − 1)×100` | `T_BENCHMARK_DAILY` |

**Ngày mốc (đầu kỳ)** theo filter `1D/1W/MTD/1M/QTD/3T/6T/YTD/INCEP` — dùng `UDF_RANGE_CUTOFF` (đã có ở 05_API). KH/master tham gia sau mốc → mốc = ngày tham gia.

⚠️ **TR vs PR**: R_KH là TWR (ăn cổ tức), Master Index là PR → active return có drift cổ tức; TE đo độ biến động (stdev) nên ít méo — chấp nhận theo BRD.

---

## 3. User Stories

### US1 — Tổng quan TẤT CẢ master
- **Header**: tổng #master ACTIVE, tổng #KH (Σ C_TOTAL_ACCOUNT).
- **Tổng tài sản**: Σ AUM toàn hệ + tăng trưởng vs đầu năm (%) + chart AUM theo kỳ (từ ngày khởi tạo master sớm nhất nếu DM tạo sau đầu kỳ).
- **Net in/out**: Σ in/out/net toàn hệ kỳ chọn; bar chart %in / %out.
- **Cash drag**: `Σ Tiền / Σ AUM` toàn hệ; **#master có cash drag > ngưỡng** (config).
- **List master** (sort: AUM↓ / hiệu suất↓ / deviation↓ / TE↓): mỗi master = tên/mã, AUM (tỷ đồng, 1 lẻ), #KH, hiệu suất master (kỳ), hiệu suất KH (AUM-weighted), per-dev (BPS), AUM-weighted TE, cash drag.

### US2 — Tổng quan 1 master
- Info master: tên/mã, #KH indexing, inception, benchmark, status, bộ lọc kỳ.
- ΣAUM + tăng trưởng %; Net in/out (in/out/net); **AUM-weighted TE** + badge low/med/high + #KH TE>ngưỡng; Cash drag + #KH cash>ngưỡng Y; **Performance Deviation** + #KH dev>A / <B.

### US3 — Hiệu quả đầu tư (chart)
- Line chart 3 đường theo kỳ: **DM Master** (model index PR) · **DM tổng KH** (AUM-weighted) · **VN-Index** (PR). Điểm đầu = 0%.
- Đánh dấu **mốc rebalance** (effective_date của `T_MASTER_PORTFOLIO_TICKER`); click → chi tiết rebalance (weight cũ/mới + net delta holdings — xem §4 SP).
- Resolution: ngày / tuần (kỳ >21 ngày) / tháng (kỳ >90 ngày).

### US4 — Lãi/lỗ DM KH
- #KH lãi (%PnL>0) / lỗ, tỷ lệ; tương quan lãi/lỗ; **histogram phân phối %PnL** (buckets <-20%…≥30%, đánh dấu trung vị); PnL bình quân (AUM-weighted), trung vị (`PERCENTILE_CONT`).

### US5 — Top KH lãi/lỗ
- Rank mã KH theo %PnL (TR) giảm/tăng dần, top-N.

---

## 4. SP cung cấp cho API (mới — master-keyed, on-read)

| SP | US | Tham số | Trả |
|---|---|---|---|
| `SP_GET_PM_OVERVIEW_ALL` | US1 | `@p_range` | RS1 header (#master,#KH); RS2 tổng (AUM+growth, net in/out, cash drag, #master cash>ngưỡng); RS3 list master (AUM/#KH/hiệu suất master/hiệu suất KH/dev/TE/cash-drag, sort) |
| `SP_GET_MASTER_OVERVIEW` | US2 | `@p_master_code, @p_range` | AUM+growth, net in/out, AUM-weighted TE+badge+#vượt, cash drag+#vượt Y, deviation+#vượt A/B |
| `SP_GET_MASTER_PERFORMANCE` | US3 | `@p_master_code, @p_range, @p_resolution` (NULL=auto: D/W/M theo độ dài kỳ) | RS1 chuỗi: master index (PR) + `C_KH_COMPOSITE` (DM tổng KH AUM-weighted end-weight, base=1.0) + benchmark (PR) — app rebase 0%; RS2 mốc rebalance |
| `SP_GET_MASTER_REBALANCE_DETAIL` | US3 click | `@p_master_code, @p_date` | RS1 target weight cũ→mới per mã (`T_MASTER_PORTFOLIO_TICKER`, FULL OUTER → mã ra/vào); RS2 net delta holdings THỰC TẾ per mã từ **`T_MASTER_HOLDING_BALANCE`** (@phiên ≤ eff vs phiên trước) — master-level daily holdings, chính xác hơn agg per-KH hist |
| `SP_GET_MASTER_PNL_DIST` | US4 | `@p_master_code, @p_range` | #lãi/#lỗ + tỷ lệ, histogram buckets, AUM-weighted avg %PnL, trung vị |
| `SP_GET_MASTER_TOP_KH` | US5 | `@p_master_code, @p_range, @p_topn, @p_dir` | rank mã KH theo %PnL (TR) |
| `SP_SET_MASTER_PM_CONFIG` | (cấu hình) | `@p_master_code, ngưỡng...` | upsert ngưỡng PM per-master |

**Mẫu tính TE on-read** (1 master, kỳ [a,b]):
```sql
;WITH ar AS (   -- active return ngày = R_KH − R_master_index
  SELECT b.C_SI_ACCOUNT, (b.C_DAILY_RETURN - idx.C_DAILY_RETURN) AS d
  FROM T_SI_NAV_BALANCE b
  JOIN T_MASTER_INDEX_DAILY idx ON idx.C_MASTER_CODE=b.C_MASTER_CODE AND idx.C_BUSINESS_DATE=b.C_BUSINESS_DATE
  WHERE b.C_MASTER_CODE=@m AND b.C_BUSINESS_DATE BETWEEN @a AND @b)
SELECT C_SI_ACCOUNT, STDEV(d) * SQRT(@X) AS TE_KH FROM ar GROUP BY C_SI_ACCOUNT;
-- Master = Σ(TE_KH × AUM_i)/ΣAUM_i (AUM_i = current AUM cuối kỳ)
```

---

## 5. Schema

- **`T_MASTER_PM_CONFIG`** (per-master, PM cài đặt — **bảng RIÊNG của PM tool**, sở hữu ở doc này):
  `C_MASTER_CODE` (UNIQUE/PK) · `C_TE_BADGE_LOW` · `C_TE_BADGE_HIGH` · `C_TE_ALERT_THRESHOLD` · `C_CASH_DRAG_THRESHOLD` (Y) · `C_DEV_THRESHOLD_HIGH` (A) · `C_DEV_THRESHOLD_LOW` (B) · `C_DRIFT_THRESHOLD` · `C_SYMBOL_WEIGHT_ALERT` · `C_INDUSTRY_WEIGHT_ALERT` · `C_UPDATED_BY` · `C_UPDATED_TIME`.
  - Fallback: master chưa cấu hình → default hệ thống (`UDF_PM_CONFIG` hardcode cho TE/cash-drag/deviation). Chỉ giữ current + updated_by/time (không lịch sử).
  - **`C_DRIFT_THRESHOLD` / `C_SYMBOL_WEIGHT_ALERT` / `C_INDUSTRY_WEIGHT_ALERT`** (ratio, vd 0.15=15%): mới ở mức **config plumbing** (set/read được, KHÔNG default — NULL=chưa cấu hình). **CHƯA có consumer tính alert** — drift/symbol cần logic so trọng số thực vs mục tiêu; industry cần dimension mã→ngành (chưa có). Sẽ build ở task riêng.
- **3 cột TE prefix-sum trên `T_SI_NAV_BALANCE`** (`accum_active_ret`, `accum_active_ret_sq`, `ret_day_count`) + **EOD job J12B** maintain chúng + **index `IX_SI_NAV_BALANCE_MASTER`**: **KHÔNG định nghĩa ở đây — thuộc BRD EOD** ([SDI-spec.md](./SDI-spec.md) §8 schema + §9.2 job J12B). PM tool chỉ **TIÊU THỤ**. (Cột cùng bảng EOD ⇒ giữ một nguồn định nghĩa, tránh tách rời nhiều doc.)
- **Cách serve-layer tiêu thụ** (đọc 2 lát base/end, không quét): TE range = HIỆU 2 mốc `Var=(ΣA²−(ΣA)²/n)/(n−1)`, `TEᵢ=√Var×√min(n,252)` (n per-KH). Return/deviation = `UPᵢ,end` (current) + `UPᵢ,base` (lát @base; KH join sau base → 10000). ⇒ US1 ~48s→~1s, **end-weight GIỮ NGUYÊN**.

---

## 6. Implement plan (phase, sau khi spec duyệt)

| Phase | Nội dung | Phụ thuộc |
|---|---|---|
| **P1** | `T_MASTER_PM_CONFIG` + `SP_SET_MASTER_PM_CONFIG` + index master-scoped | — |
| **P2** | US2/US3 (1 master): `SP_GET_MASTER_OVERVIEW`, `SP_GET_MASTER_PERFORMANCE`, `SP_GET_MASTER_REBALANCE_DETAIL` | P1 |
| **P3** | US4/US5 (per-KH agg): `SP_GET_MASTER_PNL_DIST`, `SP_GET_MASTER_TOP_KH` | P1 |
| **P4** | US1 (toàn hệ, nặng nhất): `SP_GET_PM_OVERVIEW_ALL` | P2/P3 |
| **P5** | Validate scale (50k KH): đo US1/US4 on-read; nếu chậm → rollup per-master kỳ-mặc-định | P4 |

Mỗi phase: build SP + test bằng dataset (smoke/bench), verify công thức tay (TE/deviation/dist/AUM-weighted).

---

## 7. Quyết định đã chốt (tham chiếu)
- "DM tổng KH" = **AUM-weighted end-weight** (`ΣWᵢPnLᵢ`, theo BRD) — KHÔNG dùng pooled `master unit_price` (begin-weight, khác).
- TE = on-read (join 2 chuỗi return có sẵn, STDEV × √X); scale nhỏ → không materialize.
- Snapshot realtime-on-query, hiệu suất T-1.
- Ngưỡng per-master (`T_MASTER_PM_CONFIG`).
- Rebalance detail = target weight cũ/mới (`T_MASTER_PORTFOLIO_TICKER`) + net delta holdings thực tế từ `T_MASTER_HOLDING_BALANCE` (KHÔNG execution từng lệnh — SDI không có).

## 8. Quyết định (đã chốt khi duyệt — trước open)
- **TE alert threshold RIÊNG** (`C_TE_ALERT_THRESHOLD`, không = badge_high). ✅
- **Default ngưỡng hệ thống** (UDF_PM_CONFIG, fallback khi cột NULL): TE badge low=0.02/high=0.05; TE alert=0.05; cash drag Y=0.05; deviation A=+100 BPS / B=−100 BPS. ✅
- **"DM tổng KH" = end-weight cố định** (`Wᵢ=AUMᵢ,end/ΣAUM`); chuỗi US3: `value_t = Σ Wᵢ·(UPᵢ,t/UPᵢ,base) / Σ Wᵢ(present)` (equi-join sample-date set-based — KHÔNG OUTER APPLY per-KH; renormalize Σweight present → KH join/đóng giữa kỳ không méo), base=1.0. ✅
- **Resolution US3**: NULL=auto (kỳ >90 ngày→tháng, >21→tuần, còn lại→ngày); chọn phiên cuối mỗi bucket + luôn gồm base/end. ✅

## 9. P5 — đo perf scale thật (50k KH, 10 master, 250 phiên = 12.5M dòng NAV_BALANCE; warm, SQLEXPRESS)
| SP | thời gian | trạng thái |
|---|---|---|
| US3 REBALANCE_DETAIL | ~1 ms | ✅ |
| US3 PERFORMANCE | ~1.4 s | ✅ (composite đã sửa từ OUTER APPLY per-KH → equi-join set-based; nếu giữ bản cũ = treo hàng giờ) |
| US4 PNL_DIST | ~2.4 s | ✅ chấp nhận |
| US5 TOP_KH | ~2.3 s | ✅ chấp nhận |
| US2 OVERVIEW (1 master, 5k KH) | ~3.5 s | 🟡 chậm-nhẹ (drill-down) |
| **US1 OVERVIEW_ALL** | **~48 s** | 🔴 **không xài được on-read** |

**Nguyên nhân US1**: quét toàn bộ NAV_BALANCE (mọi master, 12.5M) 2 lần — `up_base` (GROUP BY si) + TE STDEV (GROUP BY si) — tức chạy phần nặng của US2 × 10 master. RS1/RS2 (header, ΣAUM, net flow, cash drag) đọc từ master tables = nhanh (ms); chỉ RS3 (KH-return/deviation/TE per-master) là điểm chết.

## 10. P6 — fix perf (GIỮ end-weight, KHÔNG đổi ngữ nghĩa). Đo lại @12.5M, warm:
| SP | P5 (trước) | P6 (sau) | |
|---|---|---|---|
| US1 OVERVIEW_ALL | ~48 s | **~1.0 s** | **45× nhanh** |
| US2 OVERVIEW | ~3.5 s | **80 ms** | 44× |
| US3 PERFORMANCE | ~1.4 s | 160 ms | |
| US4 PNL_DIST | ~2.4 s | 48 ms | |
| US5 TOP_KH | ~2.3 s | 16 ms | |

**Cách fix** (end-weight nguyên vẹn): (1) **Return** — bỏ subquery GROUP-BY quét lịch sử, đọc thẳng lát `@base` + current (return chỉ cần `UPᵢ,base`/`UPᵢ,end`, KHÔNG cần precompute). (2) **TE** — prefix-sum 3 cột lũy kế trên `T_SI_NAV_BALANCE` (EOD J12B), query đọc HIỆU 2 mốc base/end. Cả 2 đọc 2 lát ngày (dùng `IX_SI_NAV_BALANCE_MASTER`) thay vì quét toàn bộ. Verify số khớp 100% bản STDEV trực tiếp (smoke). EOD J12B ~676ms/phiên @50k account (bench medium), J07 không regression.
- Lưu ý: TE annualize bằng `√(n per-KH)` (n = số ngày active trong range của từng KH) — KH join giữa kỳ scale theo cửa sổ thực của họ (chính xác hơn dùng X master đồng nhất).

## 11. Còn mở
- Master ACTIVE chưa có EOD data → hiện bị loại khỏi #master count/list. Sau cần hiển thị "mới tạo" thì điều chỉnh.
- ~~Index leading `C_SI_ACCOUNT` cho customer FR-02/03/06~~ ✅ ĐÃ THÊM (`IX_SI_NAV_BALANCE_ACCT` + cashflow/fee_income acct + `IX_SI_PORTFOLIO_CUST` + `IX_SI_NAV_CURRENT_MASTER`).
- Backfill ngày quá khứ ⇒ accum các ngày sau lệch (EOD forward-only nên không phải luồng thường); nếu cần resync phải recompute accum xuôi từ ngày sửa.
