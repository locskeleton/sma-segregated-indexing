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
- **SP master-keyed**: nhận `@C_MASTER_CODE` (+ range), KHÔNG nhận/không trả định danh KH ngoài top-N ranking (mã KH).

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
| `SP_GET_PM_OVERVIEW_ALL` | US1 | `@range` | RS1 header (#master,#KH); RS2 tổng (AUM+growth, net in/out, cash drag, #master cash>ngưỡng); RS3 list master (AUM/#KH/hiệu suất master/hiệu suất KH/dev/TE/cash-drag, sort) |
| `SP_GET_MASTER_OVERVIEW` | US2 | `@C_MASTER_CODE, @range` | AUM+growth, net in/out, AUM-weighted TE+badge+#vượt, cash drag+#vượt Y, deviation+#vượt A/B |
| `SP_GET_MASTER_PERFORMANCE` | US3 | `@C_MASTER_CODE, @range, @resolution` | chuỗi: master index (PR) + DM tổng KH (AUM-weighted) + VN-Index, + danh sách mốc rebalance |
| `SP_GET_MASTER_REBALANCE_DETAIL` | US3 click | `@C_MASTER_CODE, @date` | weight cũ→mới per mã (`T_MASTER_PORTFOLIO_TICKER`) + net delta holdings (`T_SI_HOLDING_HIST` agg quanh ngày) |
| `SP_GET_MASTER_PNL_DIST` | US4 | `@C_MASTER_CODE, @range` | #lãi/#lỗ + tỷ lệ, histogram buckets, AUM-weighted avg %PnL, trung vị |
| `SP_GET_MASTER_TOP_KH` | US5 | `@C_MASTER_CODE, @range, @topN, @dir` | rank mã KH theo %PnL (TR) |
| `SP_SET_MASTER_PM_CONFIG` | (cấu hình) | `@C_MASTER_CODE, ngưỡng...` | upsert ngưỡng PM per-master |

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

## 5. Schema bổ sung (Track 2)

- **`T_MASTER_PM_CONFIG`** (per-master, PM cài đặt):
  `C_MASTER_CODE` (UNIQUE/PK) · `C_TE_BADGE_LOW` · `C_TE_BADGE_HIGH` · `C_TE_ALERT_THRESHOLD` · `C_CASH_DRAG_THRESHOLD` (Y) · `C_DEV_THRESHOLD_HIGH` (A) · `C_DEV_THRESHOLD_LOW` (B) · `C_UPDATED_BY` · `C_UPDATED_TIME`.
  - Fallback: master chưa cấu hình → default hệ thống (row `C_MASTER_CODE='*'` hoặc hardcode). Chỉ giữ current + updated_by/time (không lịch sử).
- **Index** `IX_SI_NAV_BALANCE_MASTER (C_MASTER_CODE, C_BUSINESS_DATE)` INCLUDE `(C_SI_ACCOUNT, C_UNIT_PRICE, C_DAILY_RETURN, C_NAV)` — quét per-master cho US3/US4/US5/TE.
- **KHÔNG** đụng EOD (Track 1 đã cung cấp building-block).

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
- Rebalance detail = weight cũ/mới + net delta holdings (KHÔNG execution từng lệnh — SDI không có).

## 8. Open / cần xác nhận
- TE alert threshold riêng hay = badge_high (đề xuất riêng).
- Default ngưỡng khi master chưa cấu hình.
- "DM tổng KH" AUM-weight: end-weight theo BRD (đã chốt) — chart per-điểm tính lại theo weight ngày đó.
- US1 ở scale thật: on-read hay cần rollup (đo ở P5).
